"""
DQ Validation DAG - Runs Hourly
Generates data → Tests quality → Reports results
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s


default_args = {
    'owner': 'dq-framework',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dq_validation_hourly',
    default_args=default_args,
    description='Hourly DQ Validation',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-quality'],
)


def generate_allure_report(**context):
    """Generate Allure HTML report"""
    import subprocess
    import os
    
    results_dir = "/Users/amohiuddeen/dq-testing-framework/reports/allure-results"
    report_dir = "/Users/amohiuddeen/dq-testing-framework/reports/allure-report"
    
    if os.path.exists(results_dir):
        subprocess.run(["allure", "generate", results_dir, "-o", report_dir, "--clean"], check=True)
        print(f"Report: {report_dir}/index.html")


# Volume mounts
reports_volume = k8s.V1Volume(
    name='dq-reports',
    host_path=k8s.V1HostPathVolumeSource(
        path='/Users/amohiuddeen/dq-testing-framework/reports',
        type='DirectoryOrCreate'
    ),
)

warehouse_volume = k8s.V1Volume(
    name='dq-warehouse',
    host_path=k8s.V1HostPathVolumeSource(
        path='/Users/amohiuddeen/dq-testing-framework/warehouse',
        type='DirectoryOrCreate'
    ),
)

reports_mount = k8s.V1VolumeMount(name='dq-reports', mount_path='/app/reports')
warehouse_mount = k8s.V1VolumeMount(name='dq-warehouse', mount_path='/app/warehouse')

# Environment variables
env_vars = [
    k8s.V1EnvVar(name='DQ_THRESHOLD', value='0.10'),
    k8s.V1EnvVar(name='DATA_INTERVAL_START', value='{{ data_interval_start }}'),
    k8s.V1EnvVar(name='DATA_INTERVAL_END', value='{{ data_interval_end }}'),
]


# Task 1: Generate hourly data (Iceberg + JSON logs)
generate_data = KubernetesPodOperator(
    task_id='generate_data',
    name='dq-data-gen',
    namespace='dq-framework',
    image='dq-runner:latest',
    image_pull_policy='IfNotPresent',
    cmds=['python3', '/app/scripts/seed_data_iceberg.py'],
    arguments=[
        '--mode', 'append',
        '--start-time', '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}',
        '--end-time', '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}',
        '--num-transactions', '100',
        '--num-users', '50',
        '--num-logs', '10',
    ],
    env_vars=env_vars,
    volumes=[warehouse_volume],
    volume_mounts=[warehouse_mount],
    is_delete_operator_pod=True,
    get_logs=True,
    in_cluster=False,
    config_file='/Users/amohiuddeen/.kube/config',
    dag=dag,
)


# Task 2: Run DQ tests (Iceberg + Unstructured)
run_tests = KubernetesPodOperator(
    task_id='run_tests',
    name='dq-test-runner',
    namespace='dq-framework',
    image='dq-runner:latest',
    image_pull_policy='IfNotPresent',
    cmds=['python3', '-m', 'pytest'],
    arguments=[
        '-v',
        '--alluredir=/app/reports/allure-results',
        '/app/tests/test_metadata_iceberg.py',
        '/app/tests/test_dq_iceberg.py',
        '/app/tests/test_unstructured.py',
    ],
    env_vars=env_vars,
    volumes=[reports_volume, warehouse_volume],
    volume_mounts=[reports_mount, warehouse_mount],
    is_delete_operator_pod=True,
    get_logs=True,
    in_cluster=False,
    config_file='/Users/amohiuddeen/.kube/config',
    dag=dag,
)


# Task 3: Generate report
generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_allure_report,
    trigger_rule='all_done',
    dag=dag,
)


# Flow: generate → test → report
generate_data >> run_tests >> generate_report
