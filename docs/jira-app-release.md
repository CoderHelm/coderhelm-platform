# Jira app release

The Jira app (Atlassian Forge, in `coderhelm-jira/`) publishes itself. Nobody runs `forge deploy` by hand.

## How it works

`.github/workflows/jira-app-release.yml` runs on every push to `main` that changes `coderhelm-jira/`, and on demand from the Actions tab (**Jira app release → Run workflow**). Only one release runs at a time; a second one waits, and a release is never cancelled halfway.

Each run:

1. Installs the pinned Forge CLI (`FORGE_CLI_VERSION` in the workflow) and the app's dependencies, rebuilds the settings page, and runs the unit tests.
2. Checks for new permissions. It compares `manifest.yml` with the manifest of the last released commit, which the `jira-app-production` tag marks. It also runs `forge lint --environment production`, which flags a major version against what production runs now.
3. Deploys to `staging`, if that environment exists.
4. Deploys to `production`.
5. Moves the `jira-app-production` tag to the released commit.
6. Reads the production version Forge just numbered (`forge version list`) and publishes it as `launch/jira-app.json` in the public bucket (`{"version", "appId", "commit", "publishedAt"}`), through the `coderhelm-prod-jira-app-latest` role, which writes that one key only.
7. Lists production installations with `forge install list` and runs `forge install --upgrade --non-interactive` on each one that is behind. Site addresses are read at run time and are never printed. The repo is public, and so are its run logs.
8. Lists in the run summary each installation still behind, by number (never by address), with its major version and what that version lacks, and says the same in the warning.

Forge numbers versions itself on each deploy, so there is nothing to bump in `package.json`.

## Credentials

The job reads two repository secrets, which the Forge CLI picks up from the environment:

- `FORGE_EMAIL`: the email of the Atlassian account that owns the app.
- `FORGE_API_TOKEN`: an API token for that account, created at <https://id.atlassian.com/manage-profile/security/api-tokens>.

Set them with `gh secret set FORGE_EMAIL -R CoderHelm/coderhelm-platform` and `gh secret set FORGE_API_TOKEN -R CoderHelm/coderhelm-platform`. If either is missing, the run fails on its first step and says so.

## What needs a site admin

Forge makes two kinds of version:

- **Minor**: code changes, new events, and most module changes. Forge rolls these out to every site by itself. No one has to do anything.
- **Major**: the app asks for more access. Forge makes a major version when:
  - a scope is added or removed
  - egress (`permissions.external`) or content permissions are added or changed
  - a dynamic web trigger is added (web triggers are dynamic unless `response.type: static` is set)
  - remotes or providers change
  - licensing is turned on

  Each site keeps running its previous major version until its admin accepts the new one.

A major version still deploys. The run then:

- writes what changed to the run summary
- runs `forge install --upgrade` itself, which succeeds only on sites where the token's account is a site admin
- opens an issue titled "Jira app update needs a site admin" in this repo, or comments on the open one, so GitHub notifies the repo's watchers. The issue gives counts and the changed permissions, never site names.

On every other site, an Atlassian site admin must accept the update. Jira's own Manage apps page does not show it. They open **Atlassian Administration** (admin.atlassian.com), then **Apps**, **Sites**, choose the site, **Connected apps**, **Coderhelm**, **View app details** and **Update**. Or, faster, they open the app's installation link (developer console, the app, **Distribution**), choose the site and click **Update**: the release workflow publishes that link when the repository variable `FORGE_INSTALL_URL` holds it, and CoderHelm shows it on the Integrations page. CI cannot do this for them. When the CLI's own upgrade is refused ("insufficient permissions", or advice to install for another product), that is about the Forge token's account, not something the site lacks.

The app is Jira-only: it asks for `read:jira-work`, `write:jira-work`, `storage:app` and `read:attachment:jira`, and nothing in Confluence (`coderhelm-jira/test/manifest.test.js` holds it to that). Confluence pages for agents, Knowledge and release notes go through the Confluence part of the Atlassian connection in CoderHelm. A release that only changes code is a minor version, which Forge rolls out to every site by itself. When the update is accepted, the app's lifecycle trigger sends the site's web trigger URLs to CoderHelm again, so nobody has to re-save the settings page.

## Which version each site runs

The app sends its own version (Forge's `appVersion`) and app id with every Jira event and every trigger registration. The gateway keeps the latest seen per site in the team's `JIRA#config` (`app_version`, `app_version_seen_at`, `app_id`) and compares it with `launch/jira-app.json`, read without credentials and kept for ten minutes. A site is behind when its version is older, or when its events carry no version at all (builds from before the app reported one).

When a site is behind, the Integrations page's Jira row says which version it runs, the latest, what it lacks and how its admin accepts the update, with a link to Atlassian Administration and the installation link when one is published; a run on a ticket from that site says its comments may be incomplete. Both go away with the next event or registration from the updated app (accepting an update sends a registration at once).

What each version lacks comes from `services/common/src/jira_app_features.json`: the first released version with each feature, from the app's git history and the version each release run deployed. Add a row when a release adds something a team would miss on an older site. The run summary reads the same table.

These steps stay manual too:

- installing the app on a new site
- installing it for a new product on a site (for example, adding Confluence)

## Order with the platform deploy

The platform deploys on its own schedule (`deploy.yml`, every 10 minutes). The Jira app does not wait for it. If a change adds a web trigger that the gateway must store, merge the gateway change first, or in the same PR, and let the platform deploy before the next site update. The platform deploy has no hook that refreshes trigger URLs. They belong to each installation and are re-sent on install, on a major upgrade, and when an admin saves settings.

## Rolling back

Run the workflow again from an earlier commit:

- **Actions → Jira app release → Run workflow**, with `ref` set to the commit SHA to go back to.

Or deploy that commit by hand:

```sh
git checkout <earlier-commit>
cd coderhelm-jira
npm ci && (cd resources/admin && npm ci && npm run build)
forge deploy --environment production --non-interactive
```

Rolling back to code with the same permissions is a minor version and reaches every site at once. Rolling back permissions is itself a permission change: sites that never accepted the newer version stay where they were, and sites that did may need the admin step again.
