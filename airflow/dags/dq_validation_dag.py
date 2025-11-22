"""
Airflow DAG for DQ Validation using KubernetesPodOperator

This DAG orchestrates data quality testing by:
1. Running Pytest tests inside a Kubernetes Pod (Docker container)
2. Using incremental time-based batching (data_interval_start/end)
3. Generating Allure reports
4. Implementing circuit breaker pattern

Schedule: Hourly (supports 1000s of tables via parallelization)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s


# Default arguments
default_args = {
    'owner': 'dq-framework',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='dq_validation_hourly',
    default_args=default_args,
    description='Hourly Data Quality Validation with Circuit Breaker',
    schedule_interval='@hourly',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'testing', 'iceberg'],
)


def generate_allure_report(**context):
    """
    Generate Allure HTML report from test results.
    This runs on the Airflow worker (not in K8s pod).
    """
    import subprocess
    import os
    
    results_dir = "/Users/amohiuddeen/dq-testing-framework/reports/allure-results"
    report_dir = "/Users/amohiuddeen/dq-testing-framework/reports/allure-report"
    
    if os.path.exists(results_dir):
        try:
            # Generate Allure report
            subprocess.run(
                ["allure", "generate", results_dir, "-o", report_dir, "--clean"],
                check=True
            )
            print(f"✓ Allure report generated at: {report_dir}/index.html")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to generate Allure report: {e}")
    else:
        print(f"⚠ Results directory not found: {results_dir}")


# Volume mount for reports (Docker-to-Host)
volume_mount = k8s.V1VolumeMount(
    name='dq-reports',
    mount_path='/app/reports',
    sub_path=None,
)

volume = k8s.V1Volume(
    name='dq-reports',
    host_path=k8s.V1HostPathVolumeSource(
        path='/Users/amohiuddeen/dq-testing-framework/reports',
        type='DirectoryOrCreate'
    ),
)

# Volume mount for warehouse (Iceberg data)
warehouse_volume_mount = k8s.V1VolumeMount(
    name='dq-warehouse',
    mount_path='/app/warehouse',
    sub_path=None,
)

warehouse_volume = k8s.V1Volume(
    name='dq-warehouse',
    host_path=k8s.V1HostPathVolumeSource(
        path='/Users/amohiuddeen/dq-testing-framework/warehouse',
        type='DirectoryOrCreate'
    ),
)

# Environment variables for the pod
env_vars = [
    k8s.V1EnvVar(name='MINIO_ENDPOINT', value='minio-service.dq-framework.svc.cluster.local:9000'),
    k8s.V1EnvVar(name='MINIO_ACCESS_KEY', value='minioadmin'),
    k8s.V1EnvVar(name='MINIO_SECRET_KEY', value='minioadmin'),
    k8s.V1EnvVar(name='AWS_ACCESS_KEY_ID', value='minioadmin'),
    k8s.V1EnvVar(name='AWS_SECRET_ACCESS_KEY', value='minioadmin'),
    k8s.V1EnvVar(name='DQ_THRESHOLD', value='0.10'),
    k8s.V1EnvVar(name='PYTHONUNBUFFERED', value='1'),
    # Pass Airflow execution dates for incremental processing
    k8s.V1EnvVar(name='DATA_INTERVAL_START', value='{{ data_interval_start }}'),
    k8s.V1EnvVar(name='DATA_INTERVAL_END', value='{{ data_interval_end }}'),
]


# Task 1: Run DQ Tests in Kubernetes Pod
run_dq_tests = KubernetesPodOperator(
    task_id='run_dq_tests',
    name='dq-test-runner',
    namespace='dq-framework',
    image='dq-runner:latest',  # Built from Dockerfile.dq-runner
    image_pull_policy='IfNotPresent',
    cmds=['python3', '-m', 'pytest'],
    arguments=[
        '-v',
        '--tb=short',
        '--alluredir=/app/reports/allure-results',
        '-m', 'structured',  # Run only structured data tests (can be parameterized)
        '/app/tests/'
    ],
    env_vars=env_vars,
    volumes=[volume, warehouse_volume],
    volume_mounts=[volume_mount, warehouse_volume_mount],
    is_delete_operator_pod=True,  # Clean up pod after completion
    get_logs=True,
    log_events_on_failure=True,
    do_xcom_push=False,
    in_cluster=False,  # Running from local Airflow
    config_file='/Users/amohiuddeen/.kube/config',  # Path to your kubeconfig
    dag=dag,
)


# Task 2: Generate Allure Report (runs on Airflow worker)
generate_report = PythonOperator(
    task_id='generate_allure_report',
    python_callable=generate_allure_report,
    trigger_rule='all_done',  # Run even if tests fail
    dag=dag,
)


# Task dependencies
run_dq_tests >> generate_report


# Optional: Add parallel execution for multiple table groups
# You can create multiple KubernetesPodOperator tasks, one per table group
# This enables horizontal scaling to support 1000s of tables

"""
Example for parallel execution:

def create_dq_task(table_group_name, table_filter):
    return KubernetesPodOperator(
        task_id=f'run_dq_tests_{table_group_name}',
        name=f'dq-test-{table_group_name}',
        namespace='dq-framework',
        image='dq-runner:latest',
        cmds=['python3', '-m', 'pytest'],
        arguments=[
            '-v',
            '-k', table_filter,  # Filter tests by table name
            '--alluredir=/app/reports/allure-results',
            '/app/tests/'
        ],
        env_vars=env_vars,
        volumes=[volume, warehouse_volume],
        volume_mounts=[volume_mount, warehouse_volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=False,
        config_file='/Users/amohiuddeen/.kube/config',
        dag=dag,
    )

# Create parallel tasks for different table groups
task_group_1 = create_dq_task('group1', 'transactions')
task_group_2 = create_dq_task('group2', 'user_profiles')

# All groups run in parallel, then generate report
[task_group_1, task_group_2] >> generate_report
"""

