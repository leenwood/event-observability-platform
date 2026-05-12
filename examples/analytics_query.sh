#!/usr/bin/env bash
# Analytics API examples for the Event Observability Platform.
# Prerequisites: server running on localhost:8080, ClickHouse populated with events.

BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "=== Daily event stats for a date range ==="
curl -s "$BASE_URL/analytics/daily-events?from=2026-05-01&to=2026-05-31" | jq .

echo ""
echo "=== Single day ==="
curl -s "$BASE_URL/analytics/daily-events?from=2026-05-25&to=2026-05-25" | jq .

echo ""
echo "=== Missing parameters (400) ==="
curl -s "$BASE_URL/analytics/daily-events" | jq .

echo ""
echo "=== Invalid date format (400) ==="
curl -s "$BASE_URL/analytics/daily-events?from=25-05-2026&to=2026-05-31" | jq .

echo ""
echo "=== Range too large >366 days (400) ==="
curl -s "$BASE_URL/analytics/daily-events?from=2024-01-01&to=2026-12-31" | jq .
