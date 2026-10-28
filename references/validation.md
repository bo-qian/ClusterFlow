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
