#!/bin/bash
set -euo pipefail

# Idempotent Stripe setup — safe to run in CI on every deploy.
# Pulls Stripe key from AWS Secrets Manager.

STAGE="${STAGE:-prod}"
SECRET_NAME="coderhelm/${STAGE}/secrets"

echo "Fetching Stripe key from AWS Secrets Manager (${SECRET_NAME})..."
SK=$(aws secretsmanager get-secret-value \
  --secret-id "$SECRET_NAME" \
  --query 'SecretString' \
  --output text | python3 -c "import sys,json; print(json.load(sys.stdin).get('stripe_secret_key',''))")

if [[ -z "$SK" ]]; then
  echo "Warning: No stripe_secret_key found in ${SECRET_NAME}, skipping Stripe setup"
  exit 0
fi

# ── Stripe HTTP helpers ───────────────────────────────────────────────────────
# Uses curl -s (NOT -f) so the response body is always captured.
# Appends a sentinel+status_code as the last line, then splits them.
_stripe_req() {
  local method="$1" url="$2"; shift 2
  local raw http body err
  raw=$(curl -s -w '\n__HTTP__%{http_code}' -X "$method" "$url" -u "$SK:" "$@")
  http=$(printf '%s\n' "$raw" | tail -n1 | grep -oE '[0-9]+$')
  body=$(printf '%s\n' "$raw" | sed '$d')  # drop last line (sentinel)
  if [[ "${http:-0}" -ge 400 ]]; then
    err=$(printf '%s\n' "$body" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','(unknown)'))" \
      2>/dev/null || printf '%s\n' "$body")
    echo "Stripe error (HTTP ${http}): ${err}" >&2
    exit 1
  fi
  printf '%s\n' "$body"
}
stripe_get()  { _stripe_req GET  "$@"; }
stripe_post() { _stripe_req POST "$@"; }
# ─────────────────────────────────────────────────────────────────────────────

DESIRED_AMOUNT=19900  # $199.00/month

echo "=== 1. Finding or creating Product ==="
# List all active products and filter by metadata client-side.
# Avoids the /products/search API which has strict query-encoding requirements.
EXISTING=$(stripe_get "https://api.stripe.com/v1/products?limit=100&active=true" | python3 -c "
import sys, json
data = json.load(sys.stdin)
products = [p for p in data.get('data', []) if p.get('metadata', {}).get('app') == 'coderhelm' and p.get('active')]
print(products[0]['id'] if products else '')
")

if [[ -n "$EXISTING" ]]; then
  PRODUCT_ID="$EXISTING"
  echo "Using existing product: $PRODUCT_ID"
else
  PRODUCT=$(stripe_post "https://api.stripe.com/v1/products" \
    -d "name=Coderhelm Pro" \
    -d "description=AI-powered autonomous coding agent for your repositories" \
    -d "metadata[app]=coderhelm")
  PRODUCT_ID=$(printf '%s\n' "$PRODUCT" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created product: $PRODUCT_ID"
fi

echo ""
echo "=== 2. Finding or creating Price (\$199/month) ==="
PRICE_ID=$(stripe_get "https://api.stripe.com/v1/prices?product=${PRODUCT_ID}&active=true&limit=10" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('data', []):
    if p.get('unit_amount') == ${DESIRED_AMOUNT} and p.get('recurring', {}).get('interval') == 'month':
        print(p['id'])
        break
else:
    print('')
")

if [[ -n "$PRICE_ID" ]]; then
  echo "Using existing price: $PRICE_ID"
else
  PRICE=$(stripe_post "https://api.stripe.com/v1/prices" \
    -d "product=${PRODUCT_ID}" \
    -d "unit_amount=${DESIRED_AMOUNT}" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "metadata[app]=coderhelm")
  PRICE_ID=$(printf '%s\n' "$PRICE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created price: $PRICE_ID"
fi

echo ""
echo "=== 3. Finding or creating Plans Meter and Price (\$10/plan overage) ==="

# Find or create the billing meter for plans
PLANS_METER_ID=$(stripe_get "https://api.stripe.com/v1/billing/meters?limit=100" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for m in data.get('data', []):
    if m.get('event_name') == 'coderhelm_plans_overage' and m.get('status') == 'active':
        print(m['id'])
        break
else:
    print('')
")

if [[ -n "$PLANS_METER_ID" ]]; then
  echo "Using existing plans meter: $PLANS_METER_ID"
else
  PLANS_METER=$(stripe_post "https://api.stripe.com/v1/billing/meters" \
    -d "display_name=Plan Overages" \
    -d "event_name=coderhelm_plans_overage" \
    -d "default_aggregation[formula]=sum")
  PLANS_METER_ID=$(printf '%s\n' "$PLANS_METER" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created plans meter: $PLANS_METER_ID"
fi

# Find or create the metered price backed by the meter
PLANS_PRICE_ID=$(stripe_get "https://api.stripe.com/v1/prices?product=${PRODUCT_ID}&active=true&limit=50" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('data', []):
    if p.get('nickname') == 'plans_overage' and p.get('recurring', {}).get('meter'):
        print(p['id'])
        break
else:
    print('')
")

if [[ -n "$PLANS_PRICE_ID" ]]; then
  echo "Using existing plans overage price: $PLANS_PRICE_ID"
else
  PLANS_PRICE=$(stripe_post "https://api.stripe.com/v1/prices" \
    -d "product=${PRODUCT_ID}" \
    -d "unit_amount=1000" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "recurring[meter]=${PLANS_METER_ID}" \
    -d "nickname=plans_overage" \
    -d "metadata[app]=coderhelm" \
    -d "metadata[type]=plans_overage")
  PLANS_PRICE_ID=$(printf '%s\n' "$PLANS_PRICE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created plans overage price: $PLANS_PRICE_ID"
fi

echo ""
echo "=== 4. Finding or creating Tokens Meter and Price (\$50/1M token overage) ==="

# Find or create the billing meter for tokens
TOKENS_METER_ID=$(stripe_get "https://api.stripe.com/v1/billing/meters?limit=100" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for m in data.get('data', []):
    if m.get('event_name') == 'coderhelm_token_overage' and m.get('status') == 'active':
        print(m['id'])
        break
else:
    print('')
")

if [[ -n "$TOKENS_METER_ID" ]]; then
  echo "Using existing tokens meter: $TOKENS_METER_ID"
else
  TOKENS_METER=$(stripe_post "https://api.stripe.com/v1/billing/meters" \
    -d "display_name=Token Overages (per 1K tokens)" \
    -d "event_name=coderhelm_token_overage" \
    -d "default_aggregation[formula]=sum")
  TOKENS_METER_ID=$(printf '%s\n' "$TOKENS_METER" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created tokens meter: $TOKENS_METER_ID"
fi

# Find or create the metered price backed by the meter
# Priced at $0.05 per 1K tokens ($50 per 1M tokens)
TOKENS_PRICE_ID=$(stripe_get "https://api.stripe.com/v1/prices?product=${PRODUCT_ID}&active=true&limit=50" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data.get('data', []):
    if p.get('nickname') == 'tokens_overage' and p.get('recurring', {}).get('meter'):
        print(p['id'])
        break
else:
    print('')
")

if [[ -n "$TOKENS_PRICE_ID" ]]; then
  echo "Using existing tokens overage price: $TOKENS_PRICE_ID"
else
  TOKENS_PRICE=$(stripe_post "https://api.stripe.com/v1/prices" \
    -d "product=${PRODUCT_ID}" \
    -d "unit_amount=5" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "recurring[meter]=${TOKENS_METER_ID}" \
    -d "nickname=tokens_overage" \
    -d "metadata[app]=coderhelm" \
    -d "metadata[type]=tokens_overage")
  TOKENS_PRICE_ID=$(printf '%s\n' "$TOKENS_PRICE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  echo "Created tokens overage price: $TOKENS_PRICE_ID"
fi

echo ""
echo "=== 5. Configuring Customer Portal (non-fatal) ==="
# Portal configuration may already exist — treat failure as a warning, not an error.
PORTAL_JSON=$(curl -s -X POST "https://api.stripe.com/v1/billing_portal/configurations" \
  -u "$SK:" \
  -d "business_profile[headline]=Manage your Coderhelm subscription" \
  -d "business_profile[privacy_policy_url]=https://coderhelm.com/privacy" \
  -d "business_profile[terms_of_service_url]=https://coderhelm.com/terms" \
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
  -d "default_return_url=https://app.coderhelm.com/billing")
PORTAL_ID=$(printf '%s\n' "$PORTAL_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('id') or 'warning: ' + d.get('error',{}).get('message','unknown'))" \
  2>/dev/null || echo "(skipped)")
echo "Portal: ${PORTAL_ID}"

echo ""
echo "=== 6. Updating Secrets Manager with price IDs ==="
python3 << PYEOF
import json, subprocess, sys

raw = subprocess.check_output([
    "aws", "secretsmanager", "get-secret-value",
    "--secret-id", "${SECRET_NAME}",
    "--query", "SecretString", "--output", "text"
])
secrets = json.loads(raw)
secrets["stripe_price_id"] = "${PRICE_ID}"
secrets["stripe_overage_price_id"] = "${TOKENS_PRICE_ID}"

subprocess.check_call([
    "aws", "secretsmanager", "put-secret-value",
    "--secret-id", "${SECRET_NAME}",
    "--secret-string", json.dumps(secrets)
])
print("Updated secrets: stripe_price_id={}, stripe_overage_price_id={}".format(
    secrets["stripe_price_id"], secrets["stripe_overage_price_id"]))
PYEOF

echo ""
echo "=== DONE ==="
echo ""
echo "  STRIPE_PRICE_ID=${PRICE_ID}"
echo "  STRIPE_PLANS_OVERAGE_PRICE_ID=${PLANS_PRICE_ID}"
echo "  STRIPE_TOKENS_OVERAGE_PRICE_ID=${TOKENS_PRICE_ID}"
