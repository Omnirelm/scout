# Scout Orchestrator

Scout Orchestrator is an AI sidekick for engineering teams that turns open-ended work into coordinated, repeatable execution steps.
It sits between developer intent and tool execution, so teams can offload ad hoc operational tasks to Scout while keeping outcomes consistent and auditable.

## Capabilities

- Skill lifecycle APIs to register, update, list, and execute reusable skills.
- Multi-step task orchestration that can plan, invoke skills/tools, and synthesize results.
- Built-in day-2 operations fit for incident triage, log analysis, and follow-up automation.
- Integration-ready runtime for MCP servers and multiple logging backends via `config.yaml`.
- Execution trace and token-cost visibility for operational observability.

## Core Building Blocks

- `skills`: reusable workflow units defined in YAML (prompt instructions, schemas, model, and optional orchestration steps).
- `tools`: callable platform capabilities that skills can invoke (for example log query tools from configured logging sources).
- `mcp_servers`: external capability providers exposed through MCP and attached to skills by name.

In practice: a skill can call local tools (`capabilities`) and/or remote MCP tools (`mcp_servers`) in one run.

## Add a New Skill

You can add skills in two ways:

- File-based (recommended for versioned defaults): add a YAML file in `skills/defaults/`.
- Tenant-specific: add a YAML file in `skills/<tenant_id>/`.

Minimal example (`skills/defaults/my_new_skill.yaml`):

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

Notes:

- `kind: simple` requires `instructions`; `kind: composed` requires `steps`.
- If you omit `id` in file-based skills, the filename stem is used as the skill id.
- Built-in defaults are read-only via delete API; tenant skills can be created/updated/deleted.

## Add a New MCP Server

MCP servers are configured in `config.yaml` under `integrations.mcp.servers`.

Example `streamable_http` server:

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

To use it in a skill, reference the server by name:

```yaml
mcp_servers:
  - github
```

Transport requirements:

- `type: stdio` needs `command` (and optional `args` / `env`).
- `type: sse` or `type: streamable_http` needs `url`.

## Execute Skills

To execute a skill through task orchestration, call `POST /tasks/run`.

Example request:

```json
{
  "skill_id": "git_inference",
  "task": "check if this repo has a service called payments, and if so if its using any feature flags to control features",
  "tenant_id": "default",
  "context": {},
  "input": {
    "repo": "https://github.com/open-telemetry/opentelemetry-demo",
    "question": "check if this repo has a service called payments, and if so if its using any feature flags to control features"
  }
}
```

Example `curl`:

```bash
curl -X POST http://localhost:8000/tasks/run \
  -H "Content-Type: application/json" \
  -d '{
    "skill_id": "git_inference",
    "task": "check if this repo has a service called payments, and if so if its using any feature flags to control features",
    "tenant_id": "default",
    "context": {},
    "input": {
      "repo": "https://github.com/open-telemetry/opentelemetry-demo",
      "question": "check if this repo has a service called payments, and if so if its using any feature flags to control features"
    }
  }'
```

Sample response (shape):

```json
{
  "success": true,
  "summary": "{... synthesized summary JSON as string ...}",
  "steps_completed": [
    {
      "step_id": "plan_step_0",
      "objective": "Execute preferred skill 'git_inference' before planning.",
      "success": true,
      "output": "... evidence-backed analysis ...",
      "error": null
    }
  ],
  "reasoning": null,
  "error": null,
  "cost": {
    "label": "run_task",
    "total_tokens": 78108,
    "children": [
      {
        "label": "simple_skill:git_inference",
        "total_tokens": 75949,
        "children": []
      },
      {
        "label": "task_synthesizer",
        "total_tokens": 2159,
        "children": []
      }
    ]
  }
}
```

Notes:

- `skill_id` lets you force a preferred skill first, before any planner fallback.
- `input` must satisfy the selected skill's `input_schema`.
- `summary` may be a JSON string if the skill returns structured text output.


## Run Locally

### Prerequisites

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/)

### 1) Install dependencies

From the `orchestrator` directory:

```bash
uv sync
```

### 2) Configure environment

Create a `.env` file in `orchestrator/` (or export env vars in your shell):

```bash
ORCHESTRATOR_DEBUG=true
OPENAI_API_KEY=<your_openai_api_key>
```

You can also edit `config.yaml` to enable and configure integrations (MCP/log sources) for your local setup.

### 3) Start the API

```bash
uv run orchestrator
```

Alternative dev command:

```bash
uv run uvicorn src.main:app --reload
```

Server runs on `http://localhost:8000`.

### 4) Verify it is up

```bash
curl http://localhost:8000/health
```

Expected response shape:

```json
{"status":"ok","service":"orchestrator"}
```

### 5) Run tests (optional)

```bash
uv run pytest
```
