---
title: Domains
description: Use CloudFront's default URL, a domain in Route 53, or a domain hosted anywhere else.
order: 8
---

CoderHelm serves the dashboard, the API and the chat stream from one hostname through one CloudFront distribution:

| Path | Goes to |
|---|---|
| `/api/plans/chat/stream` | the streaming function (server-sent events) |
| `/api/*`, `/auth/*`, `/webhooks/*`, `/email/*`, `/integrations/*` | the HTTP API |
| everything else | the dashboard (static files in S3) |

One hostname keeps the session cookie first-party and removes the need for CORS.

## No domain

```json
"domain": null
```

CloudFront gives you a URL like `https://d1234abcd.cloudfront.net`. It is only known after the first deploy, so the web stack writes it to SSM (`/coderhelm-<stage>/public-url`), and the services read it there when they start. Everything works, including the GitHub App; Google sign-in needs a domain.

## A domain in Route 53

```json
"domain": { "hostname": "coderhelm.example.com", "hostedZone": "example.com" }
```

The deploy creates the certificate (validated by DNS) and the `A` and `AAAA` alias records. CloudFront only uses certificates from us-east-1; in any other region the deploy adds a small stack there for the certificate, and the installer bootstraps CDK in us-east-1 too.

## A domain anywhere else

1. In ACM **in us-east-1**, request a certificate for the hostname and add the validation CNAME at your DNS provider.
2. Set:
   ```json
   "domain": {
     "hostname": "coderhelm.example.com",
     "certificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/..."
   }
   ```
3. Deploy. The output `DnsRecord` gives the CNAME to create (`coderhelm.example.com CNAME dxxxx.cloudfront.net`).

## Changing the domain later

Deploy with the new `domain`, then update the GitHub App's URLs (see [The GitHub App](./github-app.md#urls-it-uses)) and, if you use them, the Jira webhook and the Google OAuth client.
