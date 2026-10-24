# Plugin examples

Plugins a team can install in CoderHelm. Each folder under `plugins/` is one
plugin: a `plugin.yaml` manifest plus the skills and agents it uses. This
repository is itself a plugin source: add `https://github.com/CoderHelm/coderhelm-platform`
on the Plugins page and the plugins below show up in the catalog.

- [`marketing`](marketing/): drafts launch posts, emails and blog updates
  from releases and briefs, for a person to approve before anything goes
  out.

Plugins are declarative. They are text plus the tools a team approves:
nothing here is run as code. To make one for your team, copy a folder into
your own repository (public, or private on your GitHub App installation),
change the text, and add that repository as a source.
