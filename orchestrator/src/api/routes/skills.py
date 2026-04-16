from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, Field

from src.core.skills import SkillDef, SkillRegistry, SkillResult, SkillRunner

router = APIRouter(prefix="/skills", tags=["skills"])


def _get_registry(request: Request) -> SkillRegistry:
    return request.app.state.skill_registry


def _get_runner(request: Request) -> SkillRunner:
    return request.app.state.skill_runner


class ExecuteSkillRequest(BaseModel):
    input: dict[str, Any]
    context: dict[str, Any] = Field(default_factory=dict)


@router.get("", response_model=list[SkillDef])
def list_skills(
    tenant_id: str = Query(default="default"),
    registry: SkillRegistry = Depends(_get_registry),
) -> list[SkillDef]:
    try:
        return registry.list_skills(tenant_id=tenant_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list skills: {exc}",
        ) from exc


@router.get("/{skill_id}", response_model=SkillDef)
def get_skill(
    skill_id: str,
    tenant_id: str = Query(default="default"),
    registry: SkillRegistry = Depends(_get_registry),
) -> SkillDef:
    try:
        return registry.get(skill_id=skill_id, tenant_id=tenant_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Skill not found: {skill_id}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get skill: {exc}",
        ) from exc


@router.post("", response_model=SkillDef)
def save_skill(
    skill: SkillDef,
    tenant_id: str = Query(default="default"),
    registry: SkillRegistry = Depends(_get_registry),
) -> SkillDef:
    try:
        return registry.save(skill_def=skill, tenant_id=tenant_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save skill: {exc}",
        ) from exc


@router.delete("/{skill_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_skill(
    skill_id: str,
    tenant_id: str = Query(default="default"),
    registry: SkillRegistry = Depends(_get_registry),
) -> Response:
    try:
        registry.delete(skill_id=skill_id, tenant_id=tenant_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Skill not found: {skill_id}",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete skill: {exc}",
        ) from exc


@router.post("/{skill_id}/execute", response_model=SkillResult)
async def execute_skill(
    skill_id: str,
    body: ExecuteSkillRequest,
    tenant_id: str = Query(default="default"),
    registry: SkillRegistry = Depends(_get_registry),
    runner: SkillRunner = Depends(_get_runner),
) -> SkillResult:
    # Distinguish unknown skill from execution failure.
    try:
        registry.get(skill_id=skill_id, tenant_id=tenant_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Skill not found: {skill_id}",
        ) from exc

    try:
        result = await runner.run_skill(
            skill_id=skill_id,
            input_payload=body.input,
            context=body.context,
            tenant_id=tenant_id,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute skill: {exc}",
        ) from exc

    if result.success:
        return result

    error_detail = result.error or "Skill execution failed"
    if "Missing required input fields per input_schema" in error_detail:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_detail,
        )

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_detail)
