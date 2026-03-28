#!/bin/bash
set -euo pipefail

# Usage: STRIPE_SECRET_KEY=sk_test_... ./scripts/setup-stripe.sh

if [[ -z "${STRIPE_SECRET_KEY:-}" ]]; then
  echo "Error: STRIPE_SECRET_KEY not set"
  echo "Usage: STRIPE_SECRET_KEY=sk_test_... ./scripts/setup-stripe.sh"
  exit 1
fi

SK="$STRIPE_SECRET_KEY"

echo "=== 1. Creating Product ==="
PRODUCT=$(curl -sf https://api.stripe.com/v1/products \
  -u "$SK:" \
  -d "name=d3ftly Pro" \
  -d "description=AI-powered code review for your repositories" \
  -d "metadata[app]=d3ftly")

PRODUCT_ID=$(echo "$PRODUCT" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Product ID: $PRODUCT_ID"

echo ""
echo "=== 2. Creating Price (\$29.99/month) ==="
PRICE=$(curl -sf https://api.stripe.com/v1/prices \
  -u "$SK:" \
  -d "product=$PRODUCT_ID" \
  -d "unit_amount=2999" \
  -d "currency=usd" \
  -d "recurring[interval]=month" \
  -d "metadata[app]=d3ftly")

PRICE_ID=$(echo "$PRICE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Price ID: $PRICE_ID"

echo ""
echo "=== 3. Configuring Customer Portal ==="
PORTAL=$(curl -sf https://api.stripe.com/v1/billing_portal/configurations \
  -u "$SK:" \
  -d "business_profile[headline]=Manage your d3ftly subscription" \
  -d "business_profile[privacy_policy_url]=https://d3ftly.com/privacy" \
  -d "business_profile[terms_of_service_url]=https://d3ftly.com/terms" \
  -d "features[customer_update][enabled]=true" \
  -d "features[customer_update][allowed_updates][0]=email" \
  -d "features[invoice_history][enabled]=true" \
  -d "features[payment_method_update][enabled]=true" \
  -d "features[subscription_cancel][enabled]=true" \
  -d "features[subscription_cancel][mode]=at_period_end" \
  -d "features[subscription_cancel][cancellation_reason][enabled]=true" \
  -d "features[subscription_cancel][cancellation_reason][options][0]=too_expensive" \
  -d "features[subscription_cancel][cancellation_reason][options][1]=missing_features" \
  -d "features[subscription_cancel][cancellation_reason][options][2]=switched_service" \
  -d "features[subscription_cancel][cancellation_reason][options][3]=unused" \
  -d "features[subscription_cancel][cancellation_reason][options][4]=other" \
  -d "default_return_url=https://app.d3ftly.com/dashboard/billing")

PORTAL_ID=$(echo "$PORTAL" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Portal Config ID: $PORTAL_ID"

echo ""
echo "=== DONE ==="
echo ""
echo "Add this to your .env:"
echo "  STRIPE_PRICE_ID=$PRICE_ID"
