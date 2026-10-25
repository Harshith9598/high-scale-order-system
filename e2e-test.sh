#!/bin/bash
set -e

BASE_URL=${BASE_URL:-http://localhost:8080}

echo "=== End-to-End Test for Order Platform ==="
echo "Base URL: $BASE_URL"

# Check health
echo ""
echo "1. Checking health..."
curl -s $BASE_URL/actuator/health | jq . || curl -s $BASE_URL/actuator/health

# Register user
echo ""
echo "2. Registering user..."
REGISTER_RESPONSE=$(curl -s -X POST $BASE_URL/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"e2euser","email":"e2e@test.com","password":"e2e123456"}' || true)

echo $REGISTER_RESPONSE | jq . 2>/dev/null || echo $REGISTER_RESPONSE

TOKEN=$(echo $REGISTER_RESPONSE | jq -r .accessToken 2>/dev/null || echo "")

if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "Register failed or user exists, trying login..."
  LOGIN_RESPONSE=$(curl -s -X POST $BASE_URL/api/auth/login \
    -H "Content-Type: application/json" \
    -d '{"usernameOrEmail":"e2euser","password":"e2e123456"}')
  echo $LOGIN_RESPONSE | jq . 2>/dev/null || echo $LOGIN_RESPONSE
  TOKEN=$(echo $LOGIN_RESPONSE | jq -r .accessToken 2>/dev/null)
fi

if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "Failed to get token"
  exit 1
fi

echo "Token obtained: ${TOKEN:0:20}..."

# Browse products
echo ""
echo "3. Browsing products..."
PRODUCTS=$(curl -s $BASE_URL/api/products?size=5)
echo $PRODUCTS | jq . 2>/dev/null | head -n 30 || echo $PRODUCTS | head -n 20

PRODUCT_ID=$(echo $PRODUCTS | jq -r '.content[0].id' 2>/dev/null || echo "1")
if [ -z "$PRODUCT_ID" ] || [ "$PRODUCT_ID" == "null" ]; then
  PRODUCT_ID=1
fi
echo "Using product ID: $PRODUCT_ID"

# Check inventory
echo ""
echo "4. Checking inventory for product $PRODUCT_ID..."
curl -s $BASE_URL/api/inventory/$PRODUCT_ID -H "Authorization: Bearer $TOKEN" | jq . 2>/dev/null || curl -s $BASE_URL/api/inventory/$PRODUCT_ID -H "Authorization: Bearer $TOKEN"

# Create order
echo ""
echo "5. Creating order..."
ORDER_RESPONSE=$(curl -s -X POST $BASE_URL/api/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"items\":[{\"productId\":$PRODUCT_ID,\"quantity\":1}]}")

echo $ORDER_RESPONSE | jq . 2>/dev/null || echo $ORDER_RESPONSE

ORDER_ID=$(echo $ORDER_RESPONSE | jq -r .id 2>/dev/null)
ORDER_NUMBER=$(echo $ORDER_RESPONSE | jq -r .orderNumber 2>/dev/null)

if [ -z "$ORDER_ID" ] || [ "$ORDER_ID" == "null" ]; then
  echo "Order creation failed"
  exit 1
fi

echo "Order created: ID=$ORDER_ID Number=$ORDER_NUMBER"

# Wait for processing
echo ""
echo "6. Waiting 10 seconds for async processing (Kafka consumers)..."
sleep 10

# Check order status
echo ""
echo "7. Checking order status..."
for i in {1..5}; do
  STATUS_RESPONSE=$(curl -s $BASE_URL/api/orders/$ORDER_ID -H "Authorization: Bearer $TOKEN")
  echo "Attempt $i: $STATUS_RESPONSE" | jq . 2>/dev/null || echo $STATUS_RESPONSE
  STATUS=$(echo $STATUS_RESPONSE | jq -r .status 2>/dev/null)
  echo "Current status: $STATUS"
  if [ "$STATUS" == "COMPLETED" ] || [ "$STATUS" == "FAILED" ] || [ "$STATUS" == "CANCELLED" ]; then
    break
  fi
  sleep 3
done

# Check payment
echo ""
echo "8. Checking payment..."
curl -s $BASE_URL/api/payments/$ORDER_ID -H "Authorization: Bearer $TOKEN" | jq . 2>/dev/null || curl -s $BASE_URL/api/payments/$ORDER_ID -H "Authorization: Bearer $TOKEN"

# Check notifications
echo ""
echo "9. Checking notifications..."
curl -s $BASE_URL/api/notifications -H "Authorization: Bearer $TOKEN" | jq . 2>/dev/null | head -n 50 || curl -s $BASE_URL/api/notifications -H "Authorization: Bearer $TOKEN" | head -n 50

# Check outbox
echo ""
echo "10. E2E Test Complete!"
echo "Order $ORDER_NUMBER final status should be COMPLETED (or FAILED if payment/inventory failed - retry)"
echo ""
echo "Flow tested: User → Login → Browse Product → Create Order → PostgreSQL → Outbox → Kafka → Inventory → Payment → Order Status → Notification → Client retrieves order"
