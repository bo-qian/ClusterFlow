# Command guide — 0.1.0b6

Run commands with the Python environment in which the reviewed checkout was installed. From its root use `python scripts/clusterflow.py ...`; `clusterflow ...` is the equivalent installed CLI. Output is JSON (also accepts `--json`). Global `--home PATH` comes **before** the command and chooses a private per-user configuration directory, not a directory inside this repository.

## Group site observations (offline)

```text
clusterflow site list
clusterflow site show group-cluster
```

The bundled site records the group's shared entry, heterogeneous CPU/GPU capacities, software paths, joblist conventions and unresolved memory limits. It is dated reference data, not live capacity, personal configuration or host-key trust. These commands never connect, read credentials, or create configuration. The JSON is included in wheels and full skill deployments. Read [the group operating profile](group-cluster.md) for context and refresh rules.

New group members may prefill only the common host/alias:

```text
clusterflow init --site group-cluster --user YOUR_USER --auth password
```

Run in the user's local terminal. Existing aliases still require deliberate replacement; explicit host/alias flags win. No username or credential is copied from the site. Trust/password handling below is unchanged; other clusters do not inherit this site implicitly.

## Install the full skill

From a reviewed clone:

```text
python -m venv /chosen/private/environment
/chosen/private/environment/python -m pip install /path/to/ClusterFlow
python scripts/install_skill.py --target /chosen/harness/skills/clusterflow
```

The environment interpreter path varies by OS (`Scripts/python.exe` on Windows, `bin/python` on Linux/macOS). The AI should choose an available supported interpreter, not copy the illustrative paths literally. Use the current harness's documented skill location. Deployment copies the complete tool/skill, not `.git`, tests, configs or credentials. It does not claim that every AI discovers skills the same way. Verify access to deployed `SKILL.md` and run the deployed `scripts/clusterflow.py doctor` with the chosen environment. A new chat/reload may be needed by the harness.

`--update` replaces only an unmodified managed deployment and leaves a timestamped backup. If local files changed, it stops. Update the reviewed checkout and its Python environment separately; never blindly pull over edits. Initial source install and later deployed copy must correspond to the same version.

## Initialize a cluster

```text
clusterflow init my-cluster --host cluster.example.invalid --user researcher --auth password
```

Run in a local terminal the human can operate. Setup displays the host, key type and fingerprint, then asks `Trust this host and save its key? [y/N]:` on the first connection. Enter `y` or `yes` to save the key; empty input, refusal or interruption stops setup before requesting a password. The next step requests the password with hidden terminal input. No administrator contact or full fingerprint entry is required by default. First-use trust does not independently verify server identity. Matching saved keys are reused without another trust prompt; a changed key stops setup before password entry, even with `--replace`.

The human must answer the trust and password prompts directly. An AI must not answer on their behalf, pipe `yes`, or supply the fingerprint it just observed. If the AI terminal cannot support direct human input, provide the command for the user's own local terminal. Unknown hosts without `--fingerprint` fail in noninteractive mode. The JSON result's `host_trust` distinguishes `first_use_confirmation`, `stored_key`, and `fingerprint_match`; connection success alone does not establish independent identity verification.

Optional strict verification uses a fingerprint independently obtained through a trusted channel:

```text
clusterflow init my-cluster --host cluster.example.invalid --user researcher --auth password --fingerprint SHA256:TRUSTED_FINGERPRINT
```

Replace the illustrative value with the separately verified fingerprint. A mismatch stops setup without falling back to first-use confirmation. This option skips only the first-use trust prompt; password authentication still requires a local hidden prompt. Never enter a real password in chat, CLI arguments, an environment variable or a JSON file. A password flag intentionally does not exist.

On Windows use Windows Credential Manager, on macOS Keychain, on Linux an available Secret Service. No plaintext or fallback backend is accepted. WSL and Windows are separate execution environments: prefer running the client under Windows for the Windows credential store, even when source is a WSL UNC path. Headless Linux without a secret service should use an SSH agent or explicitly selected key instead.

Alternative authentication:

```text
clusterflow init my-cluster --host cluster.example.invalid --user researcher --auth agent
clusterflow init my-cluster --host cluster.example.invalid --user researcher --auth key --key-file /explicit/key
```

Agent mode uses the configured SSH agent, not a scan through private key directories. Explicit key mode supports unencrypted keys; put encrypted keys in the agent. `--port` selects a non-default port. A jump host must first be initialized as its own profile, then select `--jump jump-profile`; VPN is established externally. Changed known host keys stop setup/connections and require trusted manual investigation; automatic replacement is intentionally unavailable.

`auth set my-cluster` verifies and replaces a stored password; `auth forget my-cluster --yes` removes it. `init --replace` updates a reviewed connection profile, not the host-key trust policy. Configuration is under local OS user config (`LOCALAPPDATA/clusterflow` on Windows, `XDG_CONFIG_HOME/clusterflow` or `~/.config/clusterflow` otherwise); actual secrets remain in the system store.

## Bind a project

Create a personal JSON file outside the checkout from `examples/project.example.json`. Review paths, `command` and `check_command`, then:

```text
clusterflow project add --file /private/project.json --trust-hooks
clusterflow project list
clusterflow project show example-simulation
```

Existing bindings need `--replace`. Source must exist; records/results must be outside source. Remote paths are explicit absolute POSIX paths. `command` and `check_command` are argument arrays, not implicit shell strings. Checks run on the frozen snapshot, with stdin disabled and the source snapshot as working directory. If checks need local dependencies, use their absolute interpreter. They must not modify source/input. Configure a genuine regression check, not a no-op just to pass the gate.

Supported substitutions in command/check/environment values:

| Token | Meaning |
|---|---|
| `{code}` | This execution's frozen code directory |
| `{input}` | Absolute path to the selected frozen input |
| `{run}` | Execution's run directory (local during checks, remote during launch) |
| `{tasks}` | Configured MPI/Slurm task count |

`environment` maps reviewed non-secret variable names to strings. Set interpreter/conda/module initialization in an explicit launch script or reviewed command. ClusterFlow does not install or guess the solver environment.

`resources` accepts `partition`, `account`, `tasks`, `nodes`, `cpus_per_task`, `memory_mb`, one literal `nodelist`, optional Slurm `time`, and whole-device `gpus_per_node` with optional `gpu_type`. Counts are positive integers; the type accepts letters, digits and underscores and requires a GPU count. These map to `--cpus-per-task` and `--gres=gpu[:TYPE]:COUNT`. CPU-only profiles remain CPU-only. Inspection checks configured CPU capacity including threads per task and typed GPU capacity on enough candidate nodes, not immediately idle resources. Slurm schedules and isolates devices. `memory_mb` is host memory, not VRAM; tracking/enforcement depends on site configuration.

Each task's CPUs must fit on one node. Inspection counts whole task slots on distinct eligible nodes within one partition, respecting an explicit node count and per-node memory/GPU requirements. Heterogeneous node capacities may combine; CPU fragments across nodes or capacities across partitions may not. Without `nodes`, the scheduler can select enough eligible nodes for the task count. These are configured-capacity checks, not a guarantee of current availability or partition/account access. See the [Slurm task and CPU options](https://slurm.schedmd.com/sbatch.html#OPT_cpus-per-task).

For the group, `partition=cpu`, `nodelist=node5`, `gpus_per_node=1`, `gpu_type=rtx_4090` requests one card. The [group GPU example](../examples/project.group-gpu.example.json) contains placeholders for personal paths and application-owned launch/check commands; review and replace them. Its GPU environment path is not an installed or verified FEniCSx environment.

No finite time limit is added by default; use `"time": "0"` only where the site permits unlimited time, which is checked before release.

**Application outputs must be directed outside the immutable code snapshot.** The wrapper exports `CLUSTERFLOW_RUN_DIR`, `CLUSTERFLOW_INPUT`, `CLUSTERFLOW_RUN_ID` and `CLUSTERFLOW_EXECUTION_ID`; the application does not automatically honor these. Configure its own output/input settings or launch wrapper accordingly. Use `{run}` in environment/command if supported by the application. No TOML or physical defaults are rewritten for you. Missing external meshes/data/environments must be staged separately and explicitly referenced; symlinks are not expanded into snapshots.

## Prepare, preview, sync and submit

```text
clusterflow prepare example-simulation --input inputs/case.toml --label MODEL_G1_240x160_dt0p01
clusterflow sync example-simulation EXECUTION
clusterflow sync example-simulation EXECUTION --yes
clusterflow nodes my-cluster
clusterflow submit example-simulation EXECUTION --yes
```

Replace `EXECUTION` with the returned `execution_id`. `prepare` copies real bytes (including uncommitted changes), freezes launch settings and runs the project's check command. File/aggregate limits are 64/512 MiB. It excludes secrets, VTK/HDF5/Exodus-like fields and common caches/results; extra `exclude` globs apply to files. No source tests or expected baselines are automatically modified. Failed checks retain local failure logs but produce no submit-ready record. Later local source edits do not affect this execution.

`--label` is a 1–64 character portable case description: ASCII letters, digits, underscores, hyphens and dots, starting with a letter or digit. Include scientific context such as model, case, mesh and `dt0p01`; do not collapse decimals to ambiguous `dt001`. If omitted, the label is derived from the project name and input stem. New scheduler names combine the label with a 12-character uniqueness suffix; both label and exact job name are frozen in the checked manifest and preview. The full UTC-based `execution_id` remains unchanged for tracking. Existing records without the new naming metadata retain their historical `cf_<execution_id>` job identity, including reconciliation and guarded release/cancellation.

These names never change application output IDs. Keep the scientific program's existing rules; for programs whose default is the first Slurm job number, leave that default in place rather than inserting a descriptive input `run_id`. Explicit user-requested IDs and application restart requirements take precedence. The tool does not parse/rewrite the input to change IDs, and an upgrade does not rename live jobs, inputs or output directories. See [naming examples](../examples/naming.md).

`sync` without `--yes` is an offline preview. With `--yes` it uploads to a fresh execution-specific subtree beneath `remote_source/clusterflow/`, verifies checksums and never updates the old remote working tree. Existing conflicting bytes stop the transfer; matching interrupted uploads can be continued. There is no remote delete command. `submit` rechecks the snapshot, inspects node capacity/states, submits held, records job ID, checks identity/time limit, then releases. Slurm decides scheduling; checking total capacity does not promise immediately idle resources or an instant start.

## Query, recover and cancel

```text
clusterflow query example-simulation EXECUTION
clusterflow query --cluster my-cluster --job 12345
clusterflow runs example-simulation
clusterflow reconcile example-simulation EXECUTION
clusterflow release example-simulation EXECUTION --yes
clusterflow cancel example-simulation EXECUTION --yes
```

A lost `sbatch` response leaves `SUBMIT_UNKNOWN`. Do not run a new submission to “try again”. Reconcile by the unique job name in queue/accounting. Zero/multiple matches remain blocked; do not reset the record by hand. A recovered held job needs explicit release. If Slurm accounting is unavailable, completed-job state may be `UNKNOWN`; that is not success. Cancellation verifies the recorded job name and reports a request, not already-confirmed cancellation.

## Retrieve results and inspect progress

```text
clusterflow download example-simulation EXECUTION
clusterflow progress example-simulation EXECUTION
clusterflow download example-simulation EXECUTION --remote-dir /explicit/application/output --groups data logs --max-mb 64
```

Default groups are directories `inputs`, `input`, `data`, `logs`, `log` beneath the execution run directory, capped at 64 MiB in total. Explicit `--groups` and size cap allow larger image/field directories. Symlinks and unsafe paths are rejected. Each file is read completely, sized/hashed, then atomically replaces its corresponding local result file; already completed files are retained if a later file fails. Concurrently written remote files can require another download. Source input is already retained locally in the snapshot even if the application did not create an `inputs/` output group.

`progress` reports the latest structurally complete row of each local CSV, including model-specific column names unchanged. It does not silently infer physical correctness from a row or replace scheduler status with file timestamps. Logs, NaNs and convergence failures still need interpretation by the AI/user.

## Continue a run

First review the application's own checkpoint compatibility and explicitly prepare the new input with its restart options. Then:

```text
clusterflow prepare example-simulation --input inputs/resume.toml --resume PARENT_EXECUTION --confirm-resume
```

Continue with the normal preview/sync/submit commands using the **new** execution ID. Logical `run_id` is preserved, each launch has its own snapshot, and the parent must have a confirmed terminal Slurm state before submission. The application owns checkpoint files, CSV reconciliation, physical-parameter validation and numerical continuation. ClusterFlow does not inject restart settings or claim all solvers support resume. Historical source reproduction uses its retained snapshot/environment, not whichever source happens to be current.

## Reproduce an old version

```text
clusterflow prepare example-simulation --input inputs/case.toml --from-execution OLD_EXECUTION
```

This verifies the retained old snapshot, copies its actual source/input and launch settings, reruns its checks, and creates a new logical run linked to the old execution. Current working-tree edits are not used. Continue with normal preview/sync/submit. External dependencies and environments must still be available and compatible; preserving old code is not a guarantee of identical results across changed libraries/hardware. Do not combine historical reproduction and continuation in one prepare request.

## Verification boundary

This beta has automated fake SSH/storage/Slurm tests, not production-cluster acceptance. First test deployment/doctor, then locally confirmed first-use trust (or optional independently verified fingerprint) and login, bind a small reviewed project, and submit a cheap smoke case only on the user's request. Cross-AI discovery, real credential access, site scheduling and scientific regression need verification in the target environment. Keep secrets out of shared diagnostic output.

## Existing remote code (input-only mode)

Set `"code_mode": "remote"` and a reviewed `remote_check_command` argument array in project JSON (see examples/project.remote.example.json). Existing profiles without code_mode retain snapshot behavior. Bind with project add --trust-hooks, adding --replace for reviewed updates.

Use the same prepare, sync preview, sync --yes, submit --yes, query, download, progress and guarded job controls. Prepare freezes only the selected regular input in local_source; it does not scan/copy solver code or run local check_command. Sync uploads input under the execution run directory and runs remote_check_command with {input}, {code}, {run}, {tasks} substitutions. Use absolute commands/paths or explicitly select the working directory. Remote check logs are saved locally; failed validation blocks submission. Launch runs in existing remote_source. Application output paths must explicitly target the execution run directory; environment variables alone are insufficient. Only input and per-job launch/log files are uploaded; existing remote code is neither overwritten nor sealed.

Configure `required_source_files` as a nonempty list of reviewed, relative code paths when the task selects a particular local version. Include output/rendering helpers as well as numerical modules. Prepare records their hashes in the checked manifest without uploading the code. Sync checks the actual remote bytes before input upload; submit checks again, so changed or missing files (including untracked helpers) block the run. Later local edits do not alter the recorded expected hashes. Existing profiles/records without this option retain their previous behavior; they do not acquire a claim that selected code versions match.

Resolve a mismatch through a separately authorized, reviewed code update, then recheck the execution. Input-only mode never silently deploys code or edits application inputs to work around a mismatch. Active Python processes can retain older imported modules after an on-disk update, so inspect the impact on running jobs before changing shared files. A read-only dashboard must not disguise an old runtime renderer as an updated calculation.

Git HEAD, status and tracked diff hash are recorded at sync and compared before submission. Unavailable Git evidence is recorded for non-Git code. Remote code remains mutable: untracked contents and changes after submission are not protected. Do not claim source reproduction. Remote mode rejects --resume and --from-execution; configure an application-reviewed restart as a new input instead. Explicitly request code updates separately.

For SinteringFEniCSx grouped VTX output, explicitly configure output.bp4_merge_executable with the verified cluster helper path. Missing output configuration can cause finalization failure after successful time stepping.
