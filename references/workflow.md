# ClusterFlow workflow contract

Check capability status and the command guide before acting. The beta implements these core operations; real-cluster and scientific acceptance remain environment-specific.

## Setup and ownership

- The user-facing installation entry is a copyable AI prompt in README, not a list of commands the user must assemble. Detect supported installation mechanisms, inspect existing installations, preserve edits and verify discovery/explicit access plus CLI execution. Report each setup stage separately. Missing initialization is a stopping condition, not permission to write ad-hoc setup code or request passwords in chat. Exercise this complete prompt on a clean supported environment before calling onboarding ready.
- Keep portable skill/tool source separate from per-user connection configuration and system-managed secrets. A new OS user/device may need setup again; changing an AI account alone must not require a second codebase.
- Setup includes cluster alias, host/port/user, optional VPN/jump host, authentication and verified host fingerprint. Never silently trust a changed fingerprint or choose an insecure keyring backend.
- A project maps local source, remote source/staging, remote run root, local result root, execution environment and test commands. Validate Windows/WSL and remote POSIX paths without treating one syntax as the other.
- Treat project hooks as executable code: inspect and authorize them on first binding. Do not execute a downloaded configuration's arbitrary commands just because it parses.

## Preparation and snapshot

- Querying existing runs requires no source edits or test rerun. A new run uses local edits and inputs, with unresolved scientific choices clarified first.
- Test the exact tree and inputs that will be uploaded. Prevent concurrent edits from changing a prepared artifact; verify manifest hashes before submission.
- Snapshot source, input and resolved defaults with content hashes, Git commit plus uncommitted content when applicable, launch command, dependency/environment identifiers and seeds. A commit alone does not capture a dirty worktree. Exclude secrets, caches and bulk results.
- Preview additions/changes; detect remote edits relative to the last verified sync. No blanket bidirectional merge, overwrite or remote `--delete`. Stage and validate uploads before promoting them.
- Keep every execution's source/input artifact immutable, including while queued. Reuse verified content-addressed code artifacts when appropriate rather than copying complete environments and Git histories per run.

## Submission and observation

- Read site-configured resource preferences and inspect actual scheduler state. Never universalize one cluster's node names or unlimited walltime preference.
- Record an operation identity before submitting. Capture job ID and verify effective resources. If the response is lost, reconcile the operation against scheduler evidence; stop for resolution if ambiguity remains, rather than resubmitting.
- Separate scheduler state, application completion, exit status and output validation. No output is not proof of success. Report CSV/log disagreement rather than silently selecting stale data.
- Retrieval defaults to input/CSV/logs. Download to temporary paths, validate, then publish atomically; same size alone is not proof of identical content. Preserve useful partial data without treating it as complete.
- Records use append-only events plus derived summaries. Include operation/run/execution IDs, snapshot digest, input, resources, observed state, failures, transfer manifest and comparison outcome. Redact secrets and unnecessary personal data; never store command lines containing passwords.
- Resume the same physical problem under the scientific application's restart contract. Keep logical run ID stable and add a new execution/job identity; link each execution to its own immutable launch artifact. Do not overwrite old evidence or silently change physical parameters.
- Cancellation, destructive cleanup and resubmission need matching user intent. Failures have bounded retries for safe transfers; ambiguous submission and numerical failures stop instead of being swallowed.

## Release verification

Unit tests and a fake scheduler/transport first; live tests only with task authorization. Exercise wrong credentials, changed fingerprints, paths with spaces, upload/download interruption, response loss and duplicate prevention. A public release also requires an explicit license decision, secrets review, a clean-machine install check and one authorized realistic small-case workflow.

## Input-only remote code mode

Honor project code_mode=remote: prepare/upload only input and use existing remote_source. Require reviewed remote_check_command and application output paths. Never upload/seal solver code in this mode. Normal prepare/sync/submit/query/download commands apply; sync validates input remotely. Record Git evidence without claiming a frozen source; code updates require explicit user intent. See command guide for reproduction and mutable-code limitations. Profiles without code_mode preserve snapshot behavior.
