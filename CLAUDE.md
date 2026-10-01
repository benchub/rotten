# Working agreements.

- **Test-driven development.** Every feature or fix starts with a failing test. Run it, watch it fail for the right reason, make it pass, then refactor. Infrastructure tasks start with a failing smoke check.
- **Backlog.** Pick work from `BACKLOG.md`, top down, and respect each task's "Needs" line. When a task is done, cut it from `BACKLOG.md` and paste it at the bottom of `BACKLOG-COMPLETE.md` with `Completed: <date>, <commit SHA>`. Don't read `BACKLOG-COMPLETE.md` unless you need history.
- **How each task gets built.** The main session coordinates. It doesn't write the code itself.
  1. **Build.** A builder agent works the task in its own git worktree, test first.
  2. **Review.** A separate reviewer agent reviews the builder's diff adversarially, looking for bugs, missing or weak tests, tests that pass for the wrong reason, and scope creep.
  3. **Iterate.** The builder addresses the review, and the reviewer checks again. That's at most two review-and-fix rounds.
  4. **Final fix.** The builder gets one last chance to fix anything still open.
  5. **Land.** The main session confirms `make test-all` is green and the diff is clean. Then it moves the task to `BACKLOG-COMPLETE.md` and merges the worktree branch into `master`.
  - **Squash merge** each task into `master` locally. Never push. The user pushes upstream.
  - **Unresolved review findings.** If the reviewer still has issues after two rounds, land what's solid and split the rest into new backlog tasks.
  - **Parallel tasks.** Run tasks at the same time only when they're unlikely to touch the same files. When in doubt, go one at a time to avoid merge conflicts.
  - **New findings.** Add bugs and follow-ups that turn up during a task to `BACKLOG.md` as new tasks.
  - **Dependencies.** Builders may add Go modules and gems as needed.
- **New tasks** get IDs in the form `YYYYMMDD-HHMMSS-N` (creation time plus a counter).
- **Design context** lives in `docs/plan.md`, including the decisions on scope: Postgres 14 through 18, no data retention, no CI.
- **Before calling anything done,** run `make test-all` (or `make test` until the UI exists). There's no CI, so this is the gate.
- **UI conventions (`ui/`).**
  - Rails 8.1 on Ruby 3.4.
  - RSpec, Capybara, FactoryBot, and Shoulda Matchers. Every user-facing flow gets a system spec.
  - Security specs live in `spec/security/`.
  - The dev image (with Chromium) is separate from the production image.
  - Keep org-specific auth values, like Okta settings, out of this repo. They come from env.
- **Tests use real Postgres in Docker** through `internal/testdb`. Don't mock the database.
