# Parallel reviews

Use this workflow for independent investigations such as architecture, test
coverage, and performance. Keep a stable source baseline while reviewers run;
pause competing edits or give reviewers worktrees at a recorded commit when
reproducibility matters. Record relevant uncommitted changes separately from HEAD.

Choose complementary scopes. For example:

| Worker | Assignment | Expected evidence |
| --- | --- | --- |
| Architecture | Trace module boundaries and lifecycle ownership. | Concrete file/line findings and consequences. |
| Tests | Map important behavior to existing tests and identify gaps. | Test names, uncovered scenarios, suggested assertions. |
| Performance | Investigate a stated slow path. | Measured results when available; clearly labeled hypotheses otherwise. |

Give each worker the user's question, repository path/baseline, relevant context,
read-only project scope, and distinct final-result location. Clarify whether
running tests or benchmarks is in scope; parallel commands can still compete for
ports, databases, caches, or machine resources even when code is not edited.

Follow [launching workers](launching-workers.md). Start the independent sessions
before waiting. Record every ID and observe them in bounded intervals. Keep
results in separate files so collection does not depend on terminal scrollback.

For each finished finite worker, require an acceptable exit result and read its
fresh final message. Check its cited files and distinguish verified problems
from suggestions, missing evidence, and limitations. One worker's failure need
not cancel independent reviewers; report incomplete coverage explicitly.

Combine overlapping findings, resolve contradictions against the code/evidence,
and prioritize the useful conclusions. Stop interactive reviewers when follow-up
is finished unless the user asked to retain them. Keep result files and the run
manifest available for follow-up or recovery.
