# Compatibility and scientific reproduction

## Two different promises

1. Historical reproduction: preserve the old source/input/environment specification and seeds, then rerun that version deliberately. Old checkpoints and dependency availability may limit what can actually be reproduced; report the limitation.
2. Latest-code compatibility: adding a feature must not unintentionally change an existing input's physical model, defaults, solver strategy or diagnostic definition. The scientific project owns and tests these semantics.

Use explicit input options for new behavior, normally off for old inputs. If a new option affects restart compatibility, validate it against the application's actual checkpoint contract. Do not add a universal config/schema migration framework or force a model-specific interface into all projects.

## Acceptance gate

- Bind reviewed project test commands, not DI-specific logic inside this generic skill.
- Run fast checks for relevant edits, small time-evolving regressions for numerical/diagnostic changes, and representative longer/MPI/restart checks for higher-risk changes.
- Initial-only diagnostics are not proof of time-evolution equivalence. A short smoke test also does not prove long-run stability.
- Record the baseline identity, exact tested snapshot, environment/MPI settings, compared quantities, tolerances, intervals and actual outcome. Keep compact metrics, not huge field trees, in regression repositories.
- Check schemas and identities strictly where meaningful. Compare numerical fields, conservation/energy measures and evolution curves with pre-agreed absolute/relative tolerances. Track pore counts, topology and closure/splitting events explicitly so an aggregate tolerance cannot hide a scientific change.
- Do not invent a universal tolerance or promise bitwise identity after changing MPI layout, libraries or hardware. Lock seeds and relevant environment settings where reproduction requires them.
- Failure blocks the affected formal submission. Do not weaken tolerances, rebaseline or label changed results as equivalent without an explained, approved scientific decision.
- A deliberate bug fix or algorithm change can legitimately alter output. Keep old snapshot evidence, document what changed and why, and obtain approval for changing the baseline. Do not reproduce known bugs in the new solver merely to make a comparison pass.

## Responsibility boundary

ClusterFlow prepares/calls checks and records evidence. It does not derive equations, decide scientific equivalence, replace the application's regression suite or copy scientific tests into this repository. Adding the tool must not mutate existing scientific source, inputs, historical outputs or checkpoints.
