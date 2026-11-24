# cz. `cz_customer_details_in_pr_text`

**What happened:** Pull requests on CoderHelm's public repositories named details of an install CoderHelm serves, such as an organization's name, ticket keys and a host. The text guard in CI caught ids and email addresses, but it could not know names without a list, and a list in a public repository, even a hashed one, gives the names away.

**The guarantee:** CoderHelm checks every pull request of a public repository against terms it reads from its own records and keeps only in memory, and sets the "Customer details guard" check run. A hit fails the check with only the place. Fixing the text turns it green. A pull request from a fork is skipped with the same output whatever it says. CoderHelm never merges while the guard fails, and CI waits for the guard.
