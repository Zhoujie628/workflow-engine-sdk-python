# Contributing to workflow-engine-sdk-python

Thank you for contributing. Development requires Python 3.12 or newer.

## Setup

```bash
python -m venv .venv
python -m pip install -e ".[dev]"
python -m pytest -q
python -m build
```

Keep Java and Python protocol behavior aligned when changing workflow models,
events, routing, Task-T, or Negotiation-T. Add regression tests for both SDKs
when a change affects shared semantics.

Do not commit `.env` files, credentials, tokens, certificates, private
AgentCards, or production protocol payloads. INFO logs must contain lifecycle
metadata only; payload diagnostics are opt-in and must be redacted before they
are attached to an issue or pull request.

Use conventional commit messages and add a DCO sign-off:

```bash
git commit -s -m "fix: describe the change"
```

Pull requests should explain motivation, compatibility impact, tests, and any
required migration. Security reports must follow [SECURITY.md](SECURITY.md).
