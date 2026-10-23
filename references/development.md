# Development and maintenance

The public README is exclusively a user guide: installation prompt, initialization, natural-language usage, feature explanations and user-relevant limits. Do not add source architecture, release milestones, test counts, contributor notes or developer commands there. Keep necessary safety and live-cluster verification limits in plain user language.

## Local development

Use Python 3.11+ and an isolated environment. Install this reviewed checkout with `python -m pip install -e .`; this installs the CLI, not the AI skill registration. Paramiko and keyring are runtime dependencies. Do not mutate global environments as a side effect of ordinary use.

```text
python scripts/clusterflow.py --version
python scripts/clusterflow.py status --json
python scripts/clusterflow.py doctor --json
python -m unittest discover -s tests -v
python -m pip check
```

Build with `python -m build --wheel`. Deploy the full skill with `python scripts/install_skill.py --target /verified/harness/skills/clusterflow`, separately verifying the harness can discover or explicitly read the entry and that the selected Python environment can run the deployed tool. A deployment is not a second development source. Updates require an unmodified managed installation and preserve a backup.

## Source layout

| Location | Purpose |
|---|---|
| `SKILL.md` | AI operation entry; routes to mode-specific references |
| `AGENTS.md` | Development constraints |
| `src/clusterflow/` | Only implementation |
| `scripts/clusterflow.py` | Thin source-checkout CLI entry |
| `scripts/install_skill.py` | Complete deployment and guarded update entry |
| `references/commands.md` | Canonical operational syntax and limitations |
| `references/workflow.md` | Workflow constraints |
| `references/compatibility.md` | Scientific compatibility boundary |
| `references/validation.md` | Observed release verification, skips and unverified behavior |
| `examples/` | Fictional portable project mapping, not private user configuration |
| `tests/` | Offline/fake-transport/Slurm checks |
| `agents/openai.yaml` | Optional harness-specific UI metadata |

## Verification and releases

See [observed verification](validation.md), not the user README, for test counts and environment details. Use fake transports first. Verify build/install/deploy from a clean committed checkout. Never represent mocked keyring/Slurm tests as real credential access, SSH connection, cluster scheduling or scientific validation. Update skill instructions and command documentation with actual interface changes. User-specific credentials and experimental records never enter the public repository.
