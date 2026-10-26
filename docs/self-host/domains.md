---
title: Domains
description: Use CloudFront's address, or your own domain in Route 53 or at any other DNS provider, at install or any time after, with checks that say what to fix.
order: 8
---

CoderHelm serves the dashboard, the API and the chat stream from one hostname through one CloudFront distribution:

| Path | Goes to |
|---|---|
| `/api/plans/chat/stream` | the streaming function (server-sent events) |
| `/api/*`, `/auth/*`, `/webhooks/*`, `/email/*`, `/integrations/*` | the HTTP API |
| everything else | the dashboard (static files in S3) |

One hostname keeps the session cookie first-party and removes the need for CORS.

## Three ways

| | What you do | What CoderHelm does |
|---|---|---|
| **The AWS address** (default) | Nothing | Answers on `https://dxxxx.cloudfront.net` |
| **A domain in Route 53 in the same account** | Type the hostname | Finds the zone, makes the certificate (DNS validation) and the `A` and `AAAA` records |
| **A domain at any other DNS provider** | Type the hostname, then add two `CNAME` records | Requests the certificate, checks your records until they show up, then moves to the domain |

Google sign-in needs your own domain (Google requires a fixed sign-in address); everything else works on the AWS address.

## At install

In the [install guide](https://docs.coderhelm.com/self-host/install/), open **More options**, choose **My own domain** and type the hostname, for example `coderhelm.example.com`. The Launch Stack's `Hostname` parameter carries it:

- When a public Route 53 zone in the account contains the hostname, and public DNS uses that zone (its name servers match), the deploy makes the certificate and the records. The install comes up on the domain.
- Otherwise the install comes up on its AWS address, and the guide shows the two records to add. Once the certificate is issued CoderHelm moves to the domain by itself (about 10 minutes), and the guide carries on there, so the GitHub App is created with the domain's URLs.

You can skip the domain step in the guide and add the domain later.

## Any time after: Settings, then System

**Settings, then System, then Domain** (install admins) adds, changes or removes the domain without a terminal:

1. Type the hostname and press **Use this domain**.
2. With a Route 53 zone in the account, the validation record is added for you; otherwise the page shows the records with copy buttons and where to add them for common providers.
3. The page checks by itself. When AWS issues the certificate, CoderHelm redeploys the release it runs with the new address (about 10 minutes; the update history shows it). CoderHelm keeps answering on the AWS address meanwhile.
4. With another DNS provider, the page then asks for the address record and checks until it resolves and CoderHelm answers there over HTTPS.

**Use the AWS address** removes the domain again (Google sign-in must be off first).

Behind the page: the updater function requests the certificate in us-east-1 (CloudFront only uses certificates there), adds only the validation `CNAME` to a Route 53 zone in the account, and every 5 minutes checks whether the certificate is issued. The update build then runs `coderhelm-selfhost domain apply` with the release that runs, which writes `domain` into the config and deploys.

## The records

With another DNS provider you add two `CNAME` records. The exact names and values are on the page (and in the install guide):

| Record | Name | Value | Why |
|---|---|---|---|
| Certificate | `_<random>.coderhelm.example.com` | `_<random>.acm-validations.aws` | Proves to AWS that you control the domain, so it issues the certificate. Keep it: AWS renews the certificate with it. |
| Address | `coderhelm.example.com` | `dxxxx.cloudfront.net` | Sends visitors to CoderHelm |

Where to add them:

| Provider | Where |
|---|---|
| Cloudflare | **DNS, Records, Add record**. Set **Proxy status** to **DNS only** (a proxied record hides CloudFront and its certificate). |
| GoDaddy | **My Products, your domain, DNS, Add New Record**. Type only the part before your domain as the name. |
| Namecheap | **Domain List, Manage, Advanced DNS, Add New Record**. The host is the part before your domain. |
| Squarespace Domains (formerly Google Domains) | **Domains, your domain, DNS, Custom records**. |
| Porkbun | **Domain Management, your domain, DNS**. |
| Route 53 in another account | **Hosted zones, your zone, Create record**. |
| Azure DNS | **DNS zones, your zone, Record set**. |
| Anything else | The DNS or zone settings of the company you bought the domain from, or of whoever runs its name servers |

Many providers add your domain to the name for you: if the form shows `.example.com` after the box, type only `coderhelm` (and `_<random>.coderhelm` for the certificate record).

## Checks

Settings, then Setup, Settings, then System, and the install guide check the domain from the outside: the records in public DNS (asked over DNS-over-HTTPS from the install), the certificate in ACM, and whether CoderHelm answers on `https://<hostname>` (the page and the API). Each result has one sentence and one fix:

| Code | What it means | The fix |
|---|---|---|
| `none` | No domain: the AWS address | Optional: add one in Settings, then System |
| `validating` | The certificate waits for its DNS record | Add the records; new records can take up to an hour to show up |
| `zone_not_delegated` | The Route 53 zone is in the account, but public DNS uses other name servers | Set the name servers shown at your registrar (up to a day) |
| `validation_wrong_value` | The certificate record exists with another value | Change its value to the one shown |
| `caa_blocked` | Your domain's CAA records don't allow Amazon to issue certificates | Add `0 issue "amazon.com"` as a CAA record, then **Try again** |
| `certificate_failed` | AWS couldn't issue the certificate | Check the records, then **Try again** (a new certificate) |
| `attaching` | The certificate is issued; CoderHelm is moving to the domain | Nothing: about 10 minutes |
| `attach_failed` | The redeploy failed (most often: the hostname is on another CloudFront distribution) | Remove it there, then **Try again**; the update history has the log |
| `dns_missing` | The address record isn't there yet | Add it; it can take up to an hour |
| `dns_wrong_value` | The hostname points somewhere else | Change the value to the CloudFront address |
| `cname_at_apex` | The hostname is the root of the domain, where a `CNAME` isn't allowed | Use a subdomain, or an ALIAS, ANAME or flattened CNAME if your provider has one |
| `https_failed` | Something answers, but not CoderHelm over HTTPS | Point the record at CloudFront with no proxy in front |
| `api_unreachable` | The page loads but the API doesn't answer on the domain | Run the doctor |
| `removing` | Moving back to the AWS address | Nothing: about 10 minutes |
| `ok` | CoderHelm answers on the domain with its certificate | |

## After the move

- **The GitHub App** still has the old URLs. The webhook keeps arriving (the AWS address still answers), but update the app's **Homepage URL**, **Callback URL**, **Setup URL** and **Webhook URL** to the new address; the GitHub webhook check in Settings, then Setup names the value to set. See [The GitHub App](./github-app.md#urls-it-uses).
- **Jira** webhooks and the Forge app (`set-gateway.js`), and the **Google OAuth client**, if you use them.
- People's sessions start again on the new address.

## In the config

The CLI and the config do the same without the dashboard:

```json
"domain": { "hostname": "coderhelm.example.com", "hostedZone": "example.com" }
```

Route 53: the deploy makes the certificate and the records. In any region other than us-east-1 it adds a small stack there for the certificate, and the installer bootstraps CDK in us-east-1 too.

```json
"domain": {
  "hostname": "coderhelm.example.com",
  "certificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/..."
}
```

Another DNS provider, with a certificate you requested in ACM **in us-east-1**: the deploy prints the record to create as the `DnsRecord` output. A Route 53 zone with your own certificate (`hostedZone`, `hostedZoneId` and `certificateArn`) gets the records without the us-east-1 stack; that is what Settings, then System writes.

```sh
npx -y github:CoderHelm/coderhelm-platform domain apply --hostname coderhelm.example.com \
  --certificate-arn arn:aws:acm:us-east-1:123456789012:certificate/...
npx -y github:CoderHelm/coderhelm-platform domain apply --action remove
```

`domain apply` writes the config and redeploys the release that runs, with no CDK bootstrap.

## Without an updater

With `updates.enabled: false` there is no updater to request the certificate: set `domain` in the config and deploy.
