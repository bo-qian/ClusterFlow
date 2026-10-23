---
name: clusterflow
description: "Install and initialize ClusterFlow, bind local and remote project directories, prepare checked simulation snapshots, and submit/query/download Slurm jobs with traceable records. Use for cluster case workflows, including guarded cancellation and application-owned continuation."
---

# ClusterFlow

Keep the user's natural workflow: interpret requests such as “modify this case and submit it” as local preparation followed by validation and a traceable cluster run. Do not ask users to repeat an already configured project mapping.

## Capability gate

This is a beta implementation; automated tests do not prove the user's live cluster is configured. From this skill's directory, use the chosen Python environment:

```text
python scripts/clusterflow.py status --json
python scripts/clusterflow.py doctor --json
```

Read [commands and setup](references/commands.md) before installing, initializing or executing workflows. It is the canonical syntax and limitation guide. Do not invent flags or treat `remote_verified=false` as a test failure: no live cluster was verified by the release author. Initialization verifies each user's own connection separately. Status and doctor never access secrets or connect.

## Installation and onboarding requests

Follow the prompt in `README.md` and the command guide: inspect the checkout, use a private virtual environment for dependencies and the included full-skill installer at a verified harness-specific target. Do not merely copy `SKILL.md`; required source/support files must be deployed. Report installation, CLI, connection and project binding separately. Let the human enter a password through an interactive local terminal, never this chat or your captured stdin. Never auto-accept the fingerprint observed by the same probe. Missing supported credential storage stops password setup; offer an SSH agent rather than plaintext fallback. Installation permits only setup checks, not submitting/cancelling jobs.

## Workflow contract

For implementation or planning, read [the workflow contract](references/workflow.md). Before modifying simulation behavior or reproducing a historical case, also read [compatibility and reproduction](references/compatibility.md).

- Local source is authoritative for intended edits. Remote conflicts stop synchronization rather than being overwritten. No default remote deletion.
- Bind reviewed `command` and `check_command` arrays with explicit user intent. Configure the application's own output paths outside code and actual environment before claiming the mapping is usable; environment variables exported by ClusterFlow are not automatically honored by every solver.
- For a requested new case: edit local code/input, prepare and check the frozen copy, inspect sync preview, sync, inspect nodes, submit, then record the returned execution/job IDs in the research project's memory. Runtime records are local machine-readable evidence; a different AI still needs the research project's context.
- `--yes` expresses the current task's permission for the specific mutation, not a general approval to operate arbitrary jobs. Querying an old raw job ID uses `query --cluster ... --job ...` without creating a new run.
- Validate the actual code/input snapshot being submitted. Later local changes must not alter an existing run's code.
- Interpret authorization narrowly: querying is not submission, cancellation, deployment or cleanup. Clarify unresolved scientific choices, not previously configured routine paths.
- A submission timeout is ambiguous: reconcile job identity before retrying. Do not automatically cancel/requeue jobs or retry indefinitely.
- Use configured site resources, not hard-coded nodes, accounts, usernames, time limits or DI-specific assumptions.
- Passwords are entered through a local hidden prompt and held by an approved system credential store, never in chat, repository, arguments, configuration examples or logs. Do not fall back to plaintext storage.
- Report observed states and failures. Distinguish queued/running/completed scheduler state from the last valid numerical output row.
- Default downloads are lightweight inputs, CSV and logs. Images, checkpoints and large fields require task-specific intent.

## Development use

Before editing this repository, read `AGENTS.md`. Keep the commands, README and tests consistent. Do not claim untested live behavior as verified, publish, install globally or connect solely because this repository exists. Continuation uses explicit application restart input plus `prepare --resume ... --confirm-resume`; keep logical run ID and distinct execution IDs. Scientific compatibility remains the project's responsibility.

## Input-only remote code mode

Honor project code_mode=remote: prepare/upload only input and use existing remote_source. Require reviewed remote_check_command and application output paths. Never upload/seal solver code in this mode. Normal prepare/sync/submit/query/download commands apply; sync validates input remotely. Record Git evidence without claiming a frozen source; code updates require explicit user intent. See command guide for reproduction and mutable-code limitations. Profiles without code_mode preserve snapshot behavior.
