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
echo "=== 3. Configuring Customer Portal (non-fatal) ==="
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
echo "=== 4. Ensuring Webhook endpoint has all required events ==="
WEBHOOK_URL="https://api.coderhelm.com/webhooks/stripe"
REQUIRED_EVENTS=(
  checkout.session.completed
  customer.subscription.updated
  customer.subscription.deleted
  invoice.created
  invoice.payment_succeeded
  invoice.payment_failed
  invoice.finalized
  charge.refunded
)

# Find existing webhook for our URL
WEBHOOK_ID=$(stripe_get "https://api.stripe.com/v1/webhook_endpoints?limit=20" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for w in data.get('data', []):
    if w.get('url') == '${WEBHOOK_URL}':
        print(w['id'])
        break
else:
    print('')
")

EVENT_ARGS=""
for i in "${!REQUIRED_EVENTS[@]}"; do
  EVENT_ARGS="${EVENT_ARGS} -d enabled_events[${i}]=${REQUIRED_EVENTS[$i]}"
done

if [[ -n "$WEBHOOK_ID" ]]; then
  echo "Updating existing webhook: $WEBHOOK_ID"
  stripe_post "https://api.stripe.com/v1/webhook_endpoints/${WEBHOOK_ID}" ${EVENT_ARGS} > /dev/null
else
  echo "No webhook found for ${WEBHOOK_URL} — skipping (create manually in Stripe dashboard)"
fi
echo "Webhook events: ${REQUIRED_EVENTS[*]}"

echo ""
echo "=== 5. Updating Secrets Manager with price ID ==="
python3 << PYEOF
import json, subprocess, sys

raw = subprocess.check_output([
    "aws", "secretsmanager", "get-secret-value",
    "--secret-id", "${SECRET_NAME}",
    "--query", "SecretString", "--output", "text"
])
secrets = json.loads(raw)
secrets["stripe_price_id"] = "${PRICE_ID}"

subprocess.check_call([
    "aws", "secretsmanager", "put-secret-value",
    "--secret-id", "${SECRET_NAME}",
    "--secret-string", json.dumps(secrets)
])
print("Updated secrets: stripe_price_id={}".format(secrets["stripe_price_id"]))
PYEOF

echo ""
echo "=== DONE ==="
echo ""
echo "  STRIPE_PRICE_ID=${PRICE_ID}"
