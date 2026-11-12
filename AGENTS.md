# AGENTS.md

Guidance for AI agents and contributors working on Mastori. See [README.md](README.md) and the [design document](gpu-inference-benchmarking-platform.md) for project context.

## Testing

### Unit tests

Always aim for basic unit tests.

- Every new piece of logic (scheduler policies, queue handling, request validation, metrics calculations, provider abstractions, etc.) should ship with unit tests covering the happy path and the obvious edge and error cases.
- Keep unit tests fast, deterministic, and independent of external services (no real Redis, Kubernetes, GPUs, or cloud providers). Use fakes or mocks at the boundaries.
- Bug fixes should include a test that fails without the fix.
- If a change genuinely cannot be unit tested, say so explicitly and explain why.
- Unit tests must run in CI. Every package with unit tests needs a GitHub Actions job (under `.github/workflows/`) that runs them on push, scoped with `paths:` filters to the related code. When adding a new component with tests, add or extend its workflow in the same change.

### Integration tests

Before jumping into coding, discuss whether integration tests should be implemented for the change.

- Raise the question during planning, not after the implementation is done.
- Consider integration tests when the change crosses component boundaries (e.g. API → Redis → scheduler → worker), depends on real infrastructure behavior (Redis semantics, Kubernetes, Prometheus scraping), or touches a provider abstraction.
- Agree on scope (what is covered, what infrastructure is needed, how it runs locally and in CI) before writing code.
- If the decision is to skip them, note the reason briefly in the PR or task summary.

## Local ports

- All host ports for the local stacks (kind `extraPortMappings` and `docker compose`) must be in the range 7000–7999.
