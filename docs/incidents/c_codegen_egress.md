# c. `c_codegen_egress`

**What happened:** The sandbox's network allowlist refused the CMS host. The run blamed the change: "its generated files were not regenerated".

**The guarantee:** Builds that run a generator may reach the hosts it needs. A host that is still refused is a tooling failure that names the host.
