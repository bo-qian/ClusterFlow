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
