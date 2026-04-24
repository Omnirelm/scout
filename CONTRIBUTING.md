# Contributing to Weave

Thanks for your interest in improving Weave.

Weave is an agentic engineering platform focused on repeatable investigations, runbook execution, and tool-assisted workflows. High-value contributions usually improve one of these areas:

- Skills and prompts under `orchestrator/skills/`
- Integrations and tool wiring (local capabilities or MCP servers)
- API and orchestration behavior in `orchestrator/src/`
- Reliability, test coverage, and developer ergonomics

## Prerequisites

- Python `3.11+`
- [`uv`](https://docs.astral.sh/uv/)
- Docker (optional, for containerized local runs)

## Local Development Setup

1. Install dependencies:

```bash
cd orchestrator
uv sync
```

2. Configure environment:

```bash
cp .env.example .env
```

Set at minimum:

```bash
OPENAI_API_KEY=<your_openai_api_key>
```

3. Start the orchestrator API:

```bash
uv run orchestrator
```

Hot-reload alternative:

```bash
uv run uvicorn src.main:app --reload
```

4. Verify health:

```bash
curl http://localhost:8000/health
```

## Project Layout

```text
weave/
  docker-compose.yml
  orchestrator/
    config.yaml
    pyproject.toml
    skills/
      defaults/
      <tenant_id>/
    src/
      api/
      application/
      domain/
      infrastructure/
      config/
    tests/
```

## Common Contribution Paths

### Add or update a skill

Most contributions start by adding or refining a skill in `orchestrator/skills/defaults/`.

Required fields:

- `id`
- `name`
- `description`
- `kind`
- `instructions`
- `input_schema`

Common optional fields:

- `capabilities`
- `mcp_servers`
- `model`
- `steps`

Minimal example:

```yaml
id: my_new_skill
name: My New Skill
description: Summarize a production issue with key evidence.
kind: simple
instructions: |
  You are an SRE assistant. Analyze the input and return a concise, actionable summary.
capabilities:
  - opensearch_fetch_logs
model: gpt-5.1
input_schema:
  type: object
  properties:
    objective:
      type: string
  required: [objective]
```

### Add a local capability/tool

- Implement behavior in `orchestrator/src/infrastructure/` (or behind a protocol in `domain/` when appropriate).
- Wire dependencies through `orchestrator/src/bootstrap.py`.
- Expose/register the capability name so skills can invoke it.

### Add or update an MCP integration

- Add server config under top-level `mcp` in `orchestrator/config.yaml`.
- Supported transports: `stdio`, `sse`, `streamable_http`.
- Reference the configured server name from a skill's `mcp_servers` list.

## Coding Standards

Follow the service conventions in `orchestrator/CLAUD.md`:

- Keep dependency flow as `api` -> `application` -> `domain`.
- Keep `domain/` free of FastAPI/Starlette/I/O framework imports.
- Treat `infrastructure/` as adapter implementations of domain/application protocols.
- Add type hints for public functions and route handlers.
- Keep API handlers thin; delegate orchestration logic to `application/`.
- Prefer `create_app()` in `src/main.py` for test isolation.
- Use `ORCHESTRATOR_` prefixed env overrides for settings.
- Avoid introducing abstract/generic layers before a second concrete use case exists.

## Testing

From `orchestrator/`:

```bash
uv run pytest
```

Guidelines:

- Mirror feature structure under `orchestrator/tests/`.
- Use `fastapi.testclient.TestClient` (or `httpx` ASGI) for API tests.
- Mark GitHub integration tests with `integration_github`.
- Skip integration tests when needed:

```bash
uv run pytest -m "not integration_github"
```

## Docker and Compose

From repository root:

```bash
cp orchestrator/.env.example orchestrator/.env
docker compose up --build
```

Optional observability profile:

```bash
docker compose --profile observability up --build
```

With observability enabled, set:

```bash
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://jaeger:4317
```

Jaeger UI is available at `http://localhost:16686`.

## Pull Request Checklist

Before opening a PR:

- Include clear reproduction/validation steps.
- Include representative input/output examples for behavior changes.
- Add or update tests for new capabilities and logic changes.
- For skill changes, include a sample request (for example a `curl` payload to `POST /tasks/run`).
- Update docs when user-facing behavior or configuration changes.
- Do not commit secrets (`.env`, API keys, tokens) or bake secrets into images.
