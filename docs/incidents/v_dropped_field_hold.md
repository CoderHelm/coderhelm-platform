# v. `v_dropped_field_hold`

**What happened:** A run replaced two old checkboxes on its blocks with one shared setting, and shipped an expand migration and a separate contract migration that removes the old fields. The new-feature rules saw the schema drop both fields, found no staging CMS connection, and listed "remove the field from every document that still has it" for both as a check the change didn't pass. The pull request was held as a draft on work its own contract migration does, for fields that are inert anyway: the CMS ignores a field its schema no longer has, and the code stopped reading it.

**The guarantee:**

- A field a migration in the change removes or sets is never staging work for a person; a dropped field never holds the pull request.
- A line written the old way, listing only dropped fields, is settled: the pull request is not held on it and the line goes when it is marked ready.
- A planned file the coder, told to change it, says needs no change is listed for a person and ends the rounds.
- A branch already holding its stacked branch's head is not rebuilt.
