#!/bin/bash
set -euo pipefail

# Idempotent Stripe setup — safe to run in CI on every deploy.
# Pulls Stripe key from AWS Secrets Manager (d3ftly/prod/secrets).
# Requires AWS credentials to be configured.

STAGE="${STAGE:-prod}"
SECRET_NAME="d3ftly/${STAGE}/secrets"

echo "Fetching Stripe key from AWS Secrets Manager (${SECRET_NAME})..."
SK=$(aws secretsmanager get-secret-value \
  --secret-id "$SECRET_NAME" \
  --query 'SecretString' \
  --output text | python3 -c "import sys,json; print(json.load(sys.stdin).get('stripe_secret_key',''))")

if [[ -z "$SK" ]]; then
  echo "Warning: No stripe_secret_key found in ${SECRET_NAME}, skipping Stripe setup"
  exit 0
fi

DESIRED_AMOUNT=19900  # $199.00/month

echo "=== 1. Finding or creating Product ==="
EXISTING=$(curl -sf "https://api.stripe.com/v1/products/search?query=metadata[%27app%27]:%27d3ftly%27" \
  -u "$SK:" | python3 -c "
import sys, json
data = json.load(sys.stdin)
products = [p for p in data.get('data', []) if p.get('active')]
print(products[0]['id'] if products else '')
")

if [[ -n "$EXISTING" ]]; then
  PRODUCT_ID="$EXISTING"
  echo "Using existing product: $PRODUCT_ID"
else
  PRODUCT=$(curl -sf https://api.stripe.com/v1/products \
    -u "$SK:" \
    -d "name=d3ftly Pro" \
    -d "description=AI-powered code review for your repositories" \
    -d "metadata[app]=d3ftly")
  PRODUCT_ID=$(echo "$PRODUCT" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created product: $PRODUCT_ID"
fi

echo ""
echo "=== 2. Finding or creating Price (\$199/month) ==="
PRICE_ID=$(curl -sf "https://api.stripe.com/v1/prices?product=$PRODUCT_ID&active=true&limit=10" \
  -u "$SK:" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('data', []):
    if p.get('unit_amount') == $DESIRED_AMOUNT and p.get('recurring', {}).get('interval') == 'month':
        print(p['id'])
        break
else:
    print('')
")

if [[ -n "$PRICE_ID" ]]; then
  echo "Using existing price: $PRICE_ID"
else
  PRICE=$(curl -sf https://api.stripe.com/v1/prices \
    -u "$SK:" \
    -d "product=$PRODUCT_ID" \
    -d "unit_amount=$DESIRED_AMOUNT" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "metadata[app]=d3ftly")
  PRICE_ID=$(echo "$PRICE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created price: $PRICE_ID"
fi

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
