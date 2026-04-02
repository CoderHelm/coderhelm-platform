# Pricing Changes — April 2026

## Token Rate Change

| Setting | Old | New |
|---------|-----|-----|
| `OVERAGE_PER_1K_TOKENS_CENTS` | 1000 ($10.00/1K) | 5 ($0.05/1K) |
| `INCLUDED_TOKENS` (Pro) | 1,000,000 (1M) | 5,000,000 (5M) |
| `FREE_TIER_TOKENS` | 500,000 (500K) | No change |

## Files to Update

### Gateway (`services/gateway/src/routes/billing.rs`)
- `INCLUDED_TOKENS`: 1_000_000 → 5_000_000
- `OVERAGE_PER_1K_TOKENS_CENTS`: 1000 → 5

### Worker (`services/worker/src/clients/billing.rs`)
- `INCLUDED_TOKENS`: 1_000_000 → 5_000_000

### GitHub Webhook (`services/gateway/src/routes/github_webhook.rs`)
- Uses constants from billing.rs — no direct changes

### Dashboard
- `dashboard/src/app/settings/budget/page.tsx`: Token estimate formula (uses rate)
- `dashboard/src/app/billing/page.tsx`: Overage info text

## Stripe Changes

### New Price
- Current metered price `price_1THZI7R3MCJVQinbaoPJ9A1A` is $10/unit
- Need new price at $0.05/unit on the same meter
- Swap metered price on subscription `sub_1THZPDR3MCJVQinbAS0arXaI`

### Billing Threshold
- Currently $100 (`amount_gte=10000`)
- At $0.05/1K tokens, $100 threshold = 2M overage tokens before invoice fires
- Decision: keep at $100 or adjust?

### Current Period Meter Events
- 201 units already reported at old $10/unit rate
- Options: void the draft invoices, issue credit, or reset meter

## Impact Examples

| Scenario | Old Cost | New Cost |
|----------|----------|----------|
| 2M total tokens (Pro) | $199 + $10,000 overage | $199 + $0 (within 5M) |
| 6M total tokens (Pro) | $199 + $50,000 overage | $199 + $50 overage |
| 10M total tokens (Pro) | $199 + $90,000 overage | $199 + $250 overage |
