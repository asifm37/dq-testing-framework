#!/bin/bash

# Run Tests Script for DQ Framework
# This script runs the DQ tests and generates Allure reports

set -e  # Exit on error

echo "=============================================================================="
echo "DQ Framework - Test Runner"
echo "=============================================================================="

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export DQ_THRESHOLD="${DQ_THRESHOLD:-0.10}"
export ALLURE_RESULTS_DIR="./reports/allure-results"
export ALLURE_REPORT_DIR="./reports/allure-report"

# Clean previous reports
echo ""
echo "[1/4] Cleaning previous reports..."
rm -rf "$ALLURE_RESULTS_DIR"
rm -rf "$ALLURE_REPORT_DIR"
mkdir -p "$ALLURE_RESULTS_DIR"
mkdir -p "$ALLURE_REPORT_DIR"
echo "  ✓ Reports directory cleaned"

# Run pytest with Allure
echo ""
echo "[2/4] Running DQ tests..."
echo "  - Test suite: tests/"
echo "  - Allure results: $ALLURE_RESULTS_DIR"
echo ""

python3 -m pytest \
    -v \
    --tb=short \
    --alluredir="$ALLURE_RESULTS_DIR" \
    tests/

TEST_EXIT_CODE=$?

# Generate Allure report
echo ""
echo "[3/4] Generating Allure report..."
if command -v allure &> /dev/null; then
    allure generate "$ALLURE_RESULTS_DIR" -o "$ALLURE_REPORT_DIR" --clean
    echo "  ✓ Allure report generated: $ALLURE_REPORT_DIR/index.html"
else
    echo "  ⚠ Allure CLI not found. Install it to generate HTML reports."
    echo "    Install: https://docs.qameta.io/allure/#_installing_a_commandline"
fi

# Summary
echo ""
echo "[4/4] Test Summary"
echo "=============================================================================="
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✓ All tests passed!"
else
    echo "✗ Some tests failed (exit code: $TEST_EXIT_CODE)"
fi
echo "=============================================================================="
echo ""
echo "Reports:"
echo "  - Allure Results: $ALLURE_RESULTS_DIR"
echo "  - Allure Report:  $ALLURE_REPORT_DIR/index.html"
echo ""
echo "To view the report, run:"
echo "  allure serve $ALLURE_RESULTS_DIR"
echo "  or"
echo "  open $ALLURE_REPORT_DIR/index.html"
echo "=============================================================================="

exit $TEST_EXIT_CODE

