# ClusterFlow development

- Read `README.md` and the relevant contract under `references/` before editing.
- `src/clusterflow/` is the only implementation. `scripts/clusterflow.py` is a thin source-checkout entry point; do not duplicate logic there.
- Use simple modules/functions and explicit data. Add features when their implementation is needed, not speculative plugin registries or scheduler abstractions.
- Preserve old CLI/config meanings unless a deliberate, documented change is requested. Add tests for every behavior implemented.
- Keep status/help/doctor offline and credential-free. Test remote operations using fakes; real cluster acceptance is separate. Installing dependencies or deploying a skill is explicit, never a side effect of a query.
- Test network behavior with fakes first; real cluster tests require explicit task authorization. Test artifacts belong outside source or in temporary directories.
- Secrets never enter this repository. `.gitignore` is defense in depth, not permission to keep secrets under the checkout.
- Snapshot code and inputs before submission; capture uncommitted changes without automatically committing/pushing the user's scientific repository. Do not mutate that repository just to create a snapshot.
- Project-specific numerical checks remain in the scientific project. A failed check blocks submission; never regenerate expected baselines automatically.
- Update `SKILL.md`, `README.md`, examples and tests together when public behavior changes. Keep planned capabilities distinct from implemented ones.
- Do not choose a license, configure a remote, commit, push or publish without the corresponding user direction.

Offline checks from the repository root:

```text
python scripts/clusterflow.py status --json
python scripts/clusterflow.py doctor --json
python -m unittest discover -s tests -v
```
