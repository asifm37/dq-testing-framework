#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                   ALLURE REPORT VIEWER                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if report exists
if [ ! -f "reports/allure-report/index.html" ]; then
    echo "❌ Report not found. Run ./scripts/run_tests.sh first"
    exit 1
fi

# Kill any existing servers
pkill -f "python.*http.server.*8888" 2>/dev/null

# Start web server
echo "🚀 Starting web server on http://localhost:8888"
echo ""

cd reports/allure-report
python3 -m http.server 8888 &
SERVER_PID=$!

sleep 2

# Open browser
open http://localhost:8888 2>/dev/null || echo "Open http://localhost:8888 in your browser"

echo ""
echo "✅ Report is now accessible at: http://localhost:8888"
echo ""
echo "📊 All widgets should load properly (no CORS issues)"
echo ""
echo "To stop the server:"
echo "  • Press Ctrl+C"
echo "  • Or run: kill $SERVER_PID"
echo ""

# Wait for Ctrl+C
trap "kill $SERVER_PID 2>/dev/null; echo ''; echo '✓ Server stopped'; exit" INT TERM

wait $SERVER_PID

