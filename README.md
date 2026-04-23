# Weave

Weave is an agentic platform that takes boring, repetitive engineering work off your team, so humans can keep building what they love.

It can investigate incidents by analyzing logs and metrics, execute runbooks, and handle ad-hoc engineering requests such as:
- "Can you check if we already have a bug on our board for login failures?"
- "Can you inspect this repo and tell me whether feature flags gate payments?"

## Table of Contents
- [Why Weave](#why-Weave)
- [What Weave Can Do](#what-Weave-can-do)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
- [Example Requests](#example-requests)
- [Configuration](#configuration)
- [Project Layout](#project-layout)
- [Developing Locally](#developing-locally)
- [Contributing](#contributing)
- [License](#license)

## Why Weave

Engineering teams lose time on repetitive operational work:
- Triaging noisy alerts
- Correlating logs and metrics across systems
- Running the same investigation workflows repeatedly
- Handling "quick checks" across tools like issue trackers and code repos

Weave turns these tasks into agent workflows with reusable skills and tools, so investigations are faster, more consistent, and easier to scale.

## What Weave Can Do

- Incident triage using logs, metrics, and structured investigation steps
- Automated runbook execution for repeatable ops workflows
- Ad-hoc engineering requests across integrated systems
- Multi-step orchestration with planning, execution, and synthesis
- Local tool + MCP server composition in one skill run

## Core Concepts

- `skills`: reusable YAML-defined workflows (instructions/schemas/model, and optional steps)
- `tools` (`capabilities`): local callable functions used by skills
- `mcp_servers`: remote capability providers attached to skills via MCP

A skill can call both local `capabilities` and remote `mcp_servers` in the same execution.

## Quick Start

### Prerequisites
- Python `3.11+`
- [`uv`](https://docs.astral.sh/uv/)

### 1) Install dependencies
From `orchestrator/`:

```bash
uv sync
```

### 2) Configure environment
Create `orchestrator/.env`:

```bash
ORCHESTRATOR_DEBUG=true
OPENAI_API_KEY=<your_openai_api_key>
```

### 3) Configure integrations
Update `orchestrator/config.yaml` for your environment (MCP servers, log sources, auth headers, etc.).

### 4) Start the API

```bash
uv run orchestrator
```

Dev alternative:

```bash
uv run uvicorn src.main:app --reload
```

Server: `http://localhost:8000`

### 5) Verify health

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status":"ok","service":"orchestrator"}
```

## Example Requests

Weave orchestration runs through `POST /tasks/run`.

### Example: Repo investigation

```bash
curl -X POST http://localhost:8000/tasks/run \
  -H "Content-Type: application/json" \
  -d '{
    "skill_id": "git_inference",
    "task": "Check whether this repo has a payments service and whether feature flags control it.",
    "tenant_id": "default",
    "context": {},
    "input": {
      "repo": "https://github.com/open-telemetry/opentelemetry-demo",
      "question": "Check whether this repo has a payments service and whether feature flags control it."
    }
  }'
```

### Example: Incident-oriented request

```json
{
  "skill_id": "log_analysis",
  "task": "Investigate repeated login failures in production in the last 30 minutes.",
  "tenant_id": "default",
  "context": {
    "service": "auth-service",
    "environment": "prod"
  },
  "input": {
    "objective": "Find likely root cause and next action for login failures."
  }
}
```

Notes:
- `skill_id` lets you force a preferred skill before planner fallback.
- `input` must satisfy the selected skill's `input_schema`.
- Output includes synthesized summary, step-level execution details, and token cost metadata.

## Configuration

### Skills
Add skills in:
- `orchestrator/skills/defaults/` for versioned default skills
- `orchestrator/skills/<tenant_id>/` for tenant-specific overrides

Minimal skill example:

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

### MCP servers
Configure under `orchestrator/config.yaml` in `integrations.mcp.servers`:

```yaml
integrations:
  mcp:
    servers:
      - name: github
        enabled: true
        type: streamable_http
        url: https://api.githubcopilot.com/mcp/x/repos/readonly
        headers:
          Authorization: "Bearer <YOUR_PAT>"
        timeout: 15
        sse_read_timeout: 300
        cache_tools_list: true
```

Attach to a skill:

```yaml
mcp_servers:
  - github
```

Transport requirements:
- `type: stdio` requires `command` (optional `args` and `env`)
- `type: sse` and `type: streamable_http` require `url`

## Project Layout

```text
Weave/
  README.md
  orchestrator/
    config.yaml
    pyproject.toml
    skills/
      defaults/
      <tenant_id>/
    src/
      api/
      application/
      config/
      domain/
      infrastructure/
```

## Developing Locally

Run tests from `orchestrator/`:

```bash
uv run pytest
```

## Contributing

Contributions are welcome. A good first contribution path:
- Add or improve a skill in `orchestrator/skills/defaults/`
- Add integration capabilities through MCP server configuration
- Improve task orchestration behavior or API ergonomics

If you open a PR, include clear reproduction steps and example input/output for behavior changes.

## License

Add a `LICENSE` file at the repository root to define usage and distribution terms.
