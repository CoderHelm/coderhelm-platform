# bw. `bw_dependency_pr_review`

**What happened:** A product owner's review of a dependency pull request asked for the type check on the changed package when CI runs none, for sibling packages of one scope to move together, and for each major override to say which package pulls it in and why the bump is safe. The family check found siblings only from a registry record naming their source repository.

**The guarantee:** The verification set adds the type check to steps that lack one, and a failing type check takes the set out. When no registry record names a source repository, the scope's packages the base lockfile installs on one minor line are a family (never `@types/`), held together, and a head that splits them fails the set. Each major's section is built from its lockfile dependency paths, its route and the parents an override moves outside their range.
