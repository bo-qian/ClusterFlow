# 0.1.0b1 verification — 2026-10-23

This is an installation-test beta, not a claim of production or scientific acceptance.

| Environment/check | Observed result |
|---|---|
| Windows, Python 3.13.5, isolated virtual environment | 59 tests collected; 57 passed, 2 POSIX-only checks skipped |
| WSL/Debian, Python 3.11.2 | 59 tests collected; 58 passed, 1 Paramiko-dependent key test skipped because that source-only Linux environment lacks Paramiko |
| Windows dependencies | Paramiko 4.0.0, keyring 25.7.0 installed in the private test environment; pip dependency check passed |
| Complete skill deploy/update | Full source/support deployment, edited-install refusal and recoverable update backup tested; deployed CLI inspected |
| Skill syntax | Bundled skill validator passed using UTF-8 mode |

Tests exercise a fake Slurm service and a local fake SFTP filesystem, including workflow completion, failed checks, snapshot/launch tampering, old-source reproduction, remote conflicts, checksum failure, duplicate prevention, submission-response loss/reconciliation, bad resource state, download caps, partial CSV rows, continuation linkage and installer update protection. Password interfaces use an in-memory fake; no real credential-store entries were created or retrieved.

Not verified here: a real SSH server or Slurm cluster, real system-vault password access, site-specific scheduler policies, actual solver environments, large jobs, real checkpoints, scientific equivalence, or discovery in every AI harness. Windows and WSL do not share authentication stores automatically. Install and perform trusted read-only setup before requesting a small real smoke job.

Re-run from the reviewed checkout and its dependency environment:

```text
python -m unittest discover -s tests -v
python -m pip check
python scripts/clusterflow.py doctor --json
```


## 0.1.0b2 input-only mode

Windows: 67 automated tests, 65 passed / 2 POSIX-only skipped. Eight new tests cover input-only upload, source preservation, remote validation failure/recheck invalidation, conflicting/tampered input, changed Git evidence, duplicate submission, missing hooks and historical-operation restrictions. Existing snapshot tests remain passing. Live integration evidence is recorded separately after installation.

Live input-only CLI acceptance: prepare/sync preview included one 1592-byte input, sync validated on existing remote solver, submit/query reported job 8884 COMPLETED with exit 0:0. Download retrieved input/logs and explicitly requested application data; progress reported Step 2, Sim_Time 0.01. Grouped VTX finalization succeeded with an explicitly configured application helper. No solver code was uploaded. This is a short smoke test, not long-term numerical validation.

## 0.1.0b3 readable job naming — 2026-10-28

Windows/Python 3.13.5: 78 automated tests, 76 passed and 2 POSIX-only checks skipped. Eleven added naming tests cover explicit/default labels, bounds and unsafe values, distinct identities for identical labels, unchanged application input bytes, frozen naming metadata, lost submission responses, exact identity matching (including prefix collisions), and historical records without naming metadata. Existing input-only and snapshot workflow tests pass.

Scheduler names for new executions use readable case labels and a short uniqueness suffix. Older records retain their exact historical name derivation. Application run/output IDs are not rewritten. The new-name lifecycle and response recovery were tested against fake Slurm; these tests do not constitute a new live scientific submission.

## 0.1.0b4 selected remote-source gate — 2026-10-28

Windows/Python 3.13.5: 84 automated tests, 82 passed and 2 POSIX-only checks skipped. Six new tests verify matching selected source without code uploads, stale rendering code blocking before input upload, changed untracked output code blocking submit, missing local/remote files and unsafe configuration paths. Existing readable-name, legacy-record, snapshot and input-only tests remain passing. This checks intended selected file versions, not every mutable dependency or a source freeze after launch.

## 0.1.0b5 first-use host trust — 2026-11-05

Fix for [issue #1](https://github.com/bo-qian/ClusterFlow/issues/1): default initialization uses a human-operated local first-use confirmation instead of mandatory full fingerprint entry. Optional `--fingerprint` remains strict, matching saved keys are reused, and changed keys are rejected before password entry even with `--replace`. First-use trust is explicitly distinguished from independent server identity verification.

| Environment/check | Observed result |
|---|---|
| Windows/Python 3.13.5, Paramiko 4.0.0, keyring 25.7.0 | 98 tests; 96 passed, 2 POSIX-only checks skipped; dependency check passed |
| WSL/Debian, isolated Python 3.11.2, Paramiko 4.0.0, keyring 25.7.0 | All 98 tests passed; dependency check and offline doctor passed |
| Wheel built from a temporary source copy | 0.1.0b5 built and installed to an isolated target; installed status and doctor passed |
| Full skill deployment to a temporary target | Deployed source/support files and offline doctor passed |

Fourteen added tests exercise local confirmation before password entry, refusal/empty or invalid answers, EOF/interruption, noninteractive refusal, optional fingerprint match/mismatch, saved-key reuse without rewriting, explicit fingerprint checks on saved keys, changed key/algorithm rejection even with replacement and a matching new fingerprint, independent non-default ports, aliases, failed SSH/read-only checks, and unavailable credential storage. SSH and password storage are mocked; generated host keys and temporary known-host files are real. Tests neither read real credentials nor contact a cluster. Direct human onboarding, real system-vault access and actual cluster login remain environment-specific acceptance work. These checks deploy only to temporary targets and do not update the user's active installation.

## 0.1.0b6 group profile, GPU resources and monitor — 2026-11-05

Ubuntu/Python 3.14.10: all **119 offline tests passed**, including six site/onboarding/deployment tests, seven GPU resource tests and eight monitor parsing/presentation tests. Offline doctor, skill validation and diff whitespace checks passed. A wheel was built and installed into an isolated target; its packaged site data, offline site command and standalone monitor import were verified. No active user installation was replaced for this PR.

GPU submission tests use a fake transport: they verify typed/untyped GRES arguments, CPU threads per task, missing/wrong/insufficient/drained GPU capacity, invalid resource values and frozen requests after project edits. They do not prove a real scientific solver uses CUDA.

The group observations come from separately authorized live read-only checks and administrator setup on this date. Single-, concurrent single-, double- and four-GPU allocation/device-access checks succeeded. The maintained monitor source was separately deployed on node1 and exercised under its Python 3.6.8; a short double-GPU allocation validated physical card indices, per-job ownership and compute-process counts, then completed successfully. Its measured refresh was about 3.1 seconds (bounded by an SSH timeout); cached and Slurm-only reads about 0.06 seconds, compared with about 17.3 seconds for the former sequential monitor. Measurements are observations, not latency guarantees.

Interactive color and plain redirected/JSON output were checked. Follow-up display checks preserve one memory line per node and omit GPU placeholders/zero columns on CPU-only nodes. Existing scientific jobs continued running during those checks; no claim is made about numerical validation of those jobs.

Still not verified/configured: a group FEniCSx CUDA/PETSc environment, scientific GPU speedup/correctness, a shared project mapping, and consumable-memory scheduling. The site file intentionally stores none of the changing queue/usage/free-space values or members' credentials and private paths. Updating or publishing this source does not authorize new cluster mutations.
