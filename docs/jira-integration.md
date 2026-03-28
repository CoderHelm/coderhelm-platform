# Jira Integration (Easiest Path)

This is the fastest setup pattern used by most teams:

1. Keep Jira as the source of work.
2. Use Jira Automation (or Jira webhook) to POST issue events to d3ftly.
3. Include minimal mapping fields in the JSON body (`repo_owner`, `repo_name`, and `installation_id` or `tenant_id`).
4. Validate payload shape with the integration check endpoint before enabling at scale.

## Endpoints

- Intake webhook: `POST /webhooks/jira`
- Integration check: `GET /api/integrations/jira/check`
- Payload validator: `POST /api/integrations/jira/check`

## Why this is the easiest

This avoids building a full Atlassian app first. Teams can ship with a single Jira rule:

- Trigger: Issue Created + Issue Updated
- Action: Send web request (JSON body included)
- URL: your d3ftly webhook URL
- Secret: set once and verify with `X-Hub-Signature` headers

d3ftly acknowledges quickly and processes asynchronously, which aligns with Jira webhook guidance for reliability and retries.

## Sample Automation Body

```json
{
  "webhookEvent": "jira:issue_updated",
  "repo_owner": "your-org",
  "repo_name": "your-repo",
  "installation_id": 123456,
  "issue": {
    "key": "PROJ-123",
    "fields": {
      "summary": "Add dark mode toggle",
      "description": "Scope and acceptance criteria"
    }
  },
  "user": {
    "displayName": "Jane Doe"
  }
}
```

## Validation workflow

1. Call `GET /api/integrations/jira/check`.
2. Confirm:
   - `secret_configured = true`
   - at least one enabled repo
3. Call `POST /api/integrations/jira/check` with your candidate JSON.
4. Ensure `valid = true` and `missing = []`.
5. Enable Jira rule for real traffic.
