# Orchestrator — coding standards

This document sets **foundational** conventions for this Python service. Extend it as the codebase grows; avoid ceremony before there is a second use case.

## Stack

- Python 3.11+
- Dependency and env management: **uv** (`pyproject.toml`, `uv.lock`)
- Web: **FastAPI**, **Pydantic**, **`pydantic-settings`** for configuration
- Run locally: `uv run uvicorn src.main:app --reload` or `uv run orchestrator-api` (see `pyproject.toml` `[project.scripts]`)

## Layered layout

| Layer | Purpose |
| --- | --- |
| **`domain/`** | Entities, value objects, domain errors, **`typing.Protocol`** interfaces. **No** FastAPI, Starlette, or I/O imports. |
| **`application/`** | Use cases / application services: orchestrate domain rules and ports. No HTTP types. |
| **`infrastructure/`** | Adapters: databases, caches, external HTTP clients—implementations of `domain` protocols. |
| **`api/`** | HTTP surface: routers, request/response models, dependency wiring via `Depends`. Keep handlers thin; delegate to `application`. |
| **`config/`** | Centralized settings (env-backed). |

**Dependency direction:** `api` → `application` → `domain`; `infrastructure` implements `domain` protocols and is wired at the edge (e.g. FastAPI `Depends` or app factory).

## Design principles

- **SOLID (pragmatic):** Prefer small modules (**single responsibility**). Depend on **protocols** in `domain` or `application`, inject implementations from `infrastructure` (**dependency inversion**). Extend behavior by adding routes/use cases rather than growing god-objects (**open/closed**).
- **DRY:** One place for env/config (`config/settings.py`). Reuse Pydantic models under `api/schemas/` when the same shape appears in multiple routes—do not duplicate large model blobs.
- **YAGNI:** No generic repository layer until persistence is real. No abstract base classes until a **second** implementation exists.

## Python practices

- Type hints on public functions and route handlers.
- **`create_app()`** in `main.py` is the single factory for the FastAPI app; tests should prefer `create_app()` over importing the global `app` when isolation matters.
- Routers return clear Pydantic models or typed dicts; avoid leaking infrastructure exceptions—map to HTTP errors at the API boundary when you add error handling.

## Testing

- **`tests/`** mirrors features; use **`fastapi.testclient.TestClient`** (or `httpx` against ASGI) for API tests.
- Run: `uv run pytest` from the `orchestrator/` directory.

## Environment

- Settings use the prefix **`ORCHESTRATOR_`** (see `Settings` in `config/settings.py`). Example: `ORCHESTRATOR_DEBUG=true`.
