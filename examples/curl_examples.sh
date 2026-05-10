#!/usr/bin/env bash
# Examples of interacting with the Event Observability Platform API.
# Prerequisites: server running on localhost:8080

BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "=== Health check ==="
curl -s "$BASE_URL/health" | jq .

echo ""
echo "=== Readiness probe ==="
curl -s "$BASE_URL/ready" | jq .

echo ""
echo "=== Ingest a webhook event (202 Accepted) ==="
curl -s -X POST "$BASE_URL/webhooks/events" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {
      "order_id": "12345",
      "tracking_number": "1Z999AA10123456784",
      "carrier": "ups"
    }
  }' | jq .

echo ""
echo "=== Send duplicate (same idempotency_key) — returns 200, not processed twice ==="
curl -si -X POST "$BASE_URL/webhooks/events" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "order-shipped-12345",
    "source": "shopify",
    "event_type": "order.shipped",
    "payload": {
      "order_id": "12345",
      "tracking_number": "1Z999AA10123456784",
      "carrier": "ups"
    }
  }' | head -20

echo ""
echo "=== Validation error — missing required fields (422) ==="
curl -s -X POST "$BASE_URL/webhooks/events" \
  -H "Content-Type: application/json" \
  -d '{"source": "shopify"}' | jq .

echo ""
echo "=== Wrong Content-Type (415) ==="
curl -s -X POST "$BASE_URL/webhooks/events" \
  -H "Content-Type: text/plain" \
  -d 'raw body' | jq .

echo ""
echo "=== Prometheus metrics ==="
curl -s "$BASE_URL/metrics" | grep platform_ | head -20
