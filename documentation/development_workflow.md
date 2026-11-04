# Phase branch workflow

Each new phase starts from the current `develop` commit on a branch named
`feature/phase-XX-short-name`. Keep that branch limited to the phase's
agreed scope. Run the phase's validation commands, commit the reviewed
result, then merge the branch into `develop` with a merge commit. Keep
completed phase branches as named pointers to their validated results.
`main` receives release changes through the project's Gitflow process.

Example for Phase 2:

```bash
git switch develop
git switch -c feature/phase-02-ingestion
# Implement, validate and commit Phase 2 here.
git switch develop
git merge --no-ff feature/phase-02-ingestion
```

Phase 1 predates this branch convention. Its repository bootstrap was
committed on `main` when the new repository was created. The completed
Phase 1 branch preserves the subsequent FRED validation commit and this
workflow record, and is merged into `develop`. Existing commits are not
rewritten to claim they were originally created on a different branch.
