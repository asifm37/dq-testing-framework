#!/bin/bash
set -e

echo "=== Running DQ Tests (Iceberg + Unstructured) ==="
echo ""

# Setup
export DQ_THRESHOLD="${DQ_THRESHOLD:-0.10}"
RESULTS_DIR="./reports/allure-results"
REPORT_DIR="./reports/allure-report"

# Clean old reports
echo "[1/3] Cleaning old reports..."
rm -rf "$RESULTS_DIR" "$REPORT_DIR"
mkdir -p "$RESULTS_DIR" "$REPORT_DIR"

# Run tests (Iceberg + Unstructured)
echo ""
echo "[2/3] Running tests..."
echo "  - Metadata tests (Iceberg tables)"
echo "  - DQ tests (NOT NULL, Range, Regex, Cross-Column)"
echo "  - Unstructured tests (JSON logs)"
echo ""

docker run --rm \
  -v $(pwd)/warehouse:/app/warehouse \
  -v $(pwd)/reports:/app/reports \
  -v $(pwd)/dq_framework:/app/dq_framework \
  -v $(pwd)/tests:/app/tests \
  -e DQ_THRESHOLD=$DQ_THRESHOLD \
  dq-runner:latest \
  pytest /app/tests/test_metadata_iceberg.py \
         /app/tests/test_dq_iceberg.py \
         /app/tests/test_unstructured.py \
         -v --alluredir=/app/reports/allure-results

TEST_STATUS=$?

# Generate report
echo ""
echo "[3/3] Generating report..."
if command -v allure &>/dev/null; then
    allure generate "$RESULTS_DIR" -o "$REPORT_DIR" --clean 2>/dev/null
    echo "✓ Report: $REPORT_DIR/index.html"
else
    echo "⚠ Install allure to generate HTML reports"
fi

echo ""
if [ $TEST_STATUS -eq 0 ]; then
    echo "✓ All tests passed"
else
    echo "✗ Tests failed (code: $TEST_STATUS)"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                      VIEW ALLURE REPORT                        ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Option 1 (Recommended): Start web server"
echo "  cd reports/allure-report && python3 -m http.server 8888"
echo "  Then open: http://localhost:8888"
echo ""
echo "Option 2: Use allure serve"
echo "  allure serve $RESULTS_DIR"
echo ""
echo "Note: Opening index.html directly may show 'loading' due to CORS"
echo ""

exit $TEST_STATUS
