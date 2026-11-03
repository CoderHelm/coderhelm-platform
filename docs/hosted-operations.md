# Operating CoderHelm's own deployment

Steps a person does by hand on the hosted deployment (`infra/config/hosted.prod.json`). Self-hosted installs never need these.

## Key policies

The hosted account also runs other products, so each CoderHelm key's policy names who may use it (`infra/lib/key-policy.ts`) instead of leaving every decision to IAM:

- **Managing a key** (its policy, rotation, deletion, tags): the SSO administrator role and CDK's CloudFormation execution role. Neither statement allows encrypting, decrypting or signing.
- **Using a key**: roles named `<prefix>-*` (every role of the deployment), the execution role (DynamoDB and S3 create their grants through it) and the SSO administrator role (the operator's tools read the tables and the bucket). Each role still needs its own IAM grant.
- Deletion always waits 30 days.

## Signing key policy

The hybrid signing key's policy also denies `kms:PutKeyPolicy`, `kms:ScheduleKeyDeletion`, `kms:DisableKey` and `kms:CreateGrant` to everyone except the SSO administrator role. CloudFormation is not excepted, so a deploy can never rewrite who signs what installs trust.

The template sets `BypassPolicyLockoutSafetyCheck` on that key, because KMS would otherwise refuse a policy that keeps CloudFormation from changing it later.

When a release changes the signing key's policy:

1. The deploy fails on the key with AccessDenied on `kms:PutKeyPolicy`. That's expected.
2. Signed in through SSO as the administrator, apply the new policy from the synthesized template (`cdk synth coderhelm-prod-api`, the `HybridSigningKey` resource's `KeyPolicy`):
   ```
   aws kms put-key-policy --key-id alias/coderhelm-prod-hybrid-signing --policy-name default --policy file://signing-key-policy.json --bypass-policy-lockout-safety-check
   ```
3. Run the deploy again: CloudFormation sees the policy already matches.

## Hybrid master key secret

Every install's registration secret and relay key are derived from the hybrid master key. It lives in its own secret, `coderhelm/prod/hybrid-master`, encrypted with `alias/coderhelm-prod-secrets`, with a resource policy that lets only the gateway role, the worker role and the SSO administrator role read it. The gateway and worker read it at start (`HYBRID_MASTER_SECRET`). While it doesn't exist or can't be read, they use the `hybrid_master_key` field of `coderhelm/prod/secrets`, so deploys and the copy can happen in either order.

To move it (once):

1. Create the new secret from the old field, without printing it: the operator script's `master-copy` step.
2. Deploy. The gateway and worker logs show no "Hybrid master secret not read" warning.
3. Remove the old field from `coderhelm/prod/secrets`: the script's `master-cleanup` step. The streaming function, which reads that secret, then never sees the master key.

Rotating the master key changes every install's derived keys, so it needs key versioning first. Don't rotate it by replacing the value.

## Stack termination protection

Every hosted stack has termination protection. To delete one on purpose, turn it off first:

```
aws cloudformation update-termination-protection --no-enable-termination-protection --stack-name <stack>
```
