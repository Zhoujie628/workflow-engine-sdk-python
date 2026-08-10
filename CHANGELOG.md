# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.3] - 2026-08-10

### Fixed
- **NegotiationTHandler metadata overwrite bug**: Use local metadata dict to prevent cross-handler contamination
- **Negotiation concern extraction**: Add fallback in `engine_client` for concern extraction when metadata is missing
- **needResponse=false handling**: Fix negotiation flow to properly handle terminal negotiation states

## [0.0.2] - 2026-08-06

### Fixed
- **Task-T prompt caching**: `TaskTHandler` now caches generated prompts by `message_text`, so identical task descriptions sent to multiple agents only call the LLM once (subsequent agents get cache hit, saving ~20s each)

### Added
- **Per-handler timing logs**: `_run_before_send_handlers` now logs each handler's execution time individually (e.g. `TaskTHandler.before_send for AgentX: 0.01s`)

## [0.0.1] - 2026-08-06

Initial release of `workflow-exec-engine` (renamed from internal `a2at-engine`).

### Features
- `A2ATransport`: shared wire layer with httpx client, auth manager, agent-card map, and SSE stream consumer
- `WorkflowEngineClient`: workflow send facade with Task-T prompt generation, Negotiation-T auto-loop, event callback
- `ExtensionSender`: one-shot pre-positioning facade for Authorization-T and Notification-T
- `ControlPoint`: flow decision interface (`on_task` / `on_self_task` / `on_route` / `on_negotiation`)
- `NegotiationStrategy`: pluggable clarification strategy
- `SELF_LOOP` step type for local task handling without A2A-T message
- `ANY_SUCCESS` step policy with early cancellation of remaining subtasks
- Parallel DAG step dispatch and context assembly (`ContextBuilder`)
- Agent authentication from AgentCard `securitySchemes` (Bearer, custom headers)
- SSE response normalization for non-standard server responses
- `RegistryClient` for fetching AgentCards and PSOP workflows