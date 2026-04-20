"""Execute simple and composed skills via OpenAI Agents SDK."""

from __future__ import annotations

import json
import logging
from typing import Any

from agents import Agent, Runner, RunResult
from agents.mcp import MCPServerManager

from src.core.base import InvocationCost, extract_runner_cost
from src.core.skills import (
    SkillDef,
    SkillResult,
    SkillRunContext,
    SkillStep,
    StepResult,
)
from src.core.skills.registry import SkillRegistry
from src.core.tools.base import ToolNotFoundError, ToolRegistry
from src.core.mcp import McpServerRegistry

logger = logging.getLogger(__name__)

_MAX_SKILL_DEPTH = 20


class SkillRunner:
    """Runs skills loaded from SkillRegistry using tools from a ToolRegistry."""

    def __init__(
        self,
        skill_registry: SkillRegistry,
        tool_registry: ToolRegistry,
        *,
        mcp_registry: McpServerRegistry | None = None,
    ) -> None:
        self._skill_registry = skill_registry
        self._tool_registry = tool_registry
        self._mcp_registry = mcp_registry

    @property
    def tool_registry(self) -> ToolRegistry:
        """Platform tools used to resolve skill capabilities and plan invoke_tool steps."""
        return self._tool_registry

    async def run_skill(
        self,
        skill_id: str,
        input_payload: dict[str, Any],
        context: dict[str, Any],
        tenant_id: str,
        *,
        _depth: int = 0,
    ) -> SkillResult:
        if _depth > _MAX_SKILL_DEPTH:
            return SkillResult(
                success=False,
                error=f"Max skill nesting depth exceeded ({_MAX_SKILL_DEPTH})",
            )
        try:
            skill = self._skill_registry.get(skill_id, tenant_id)
        except KeyError as e:
            return SkillResult(success=False, error=str(e))

        try:
            self._validate_input(skill, input_payload)
        except ValueError as e:
            return SkillResult(success=False, error=str(e))

        try:
            if skill.kind == "simple":
                return await self._run_simple(skill, input_payload, context, _depth)
            return await self._run_composed(
                skill, input_payload, context, tenant_id, _depth
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Skill run failed: %s", skill_id)
            return SkillResult(success=False, error=str(exc))

    def _validate_input(self, skill: SkillDef, input_payload: dict[str, Any]) -> None:
        schema = skill.input_schema
        if not schema or not isinstance(schema, dict):
            return
        required = schema.get("required")
        if not required or not isinstance(required, list):
            return
        missing = [k for k in required if k not in input_payload]
        if missing:
            raise ValueError(
                f"Missing required input fields per input_schema: {missing!r}"
            )

    async def _run_simple(
        self,
        skill: SkillDef,
        input_payload: dict[str, Any],
        context: dict[str, Any],
        _depth: int,
    ) -> SkillResult:
        tools: list[Any] = []
        for tool_id in skill.capabilities:
            try:
                tool = self._tool_registry.resolve(tool_id, context)
                tools.append(tool.as_function_tool())
            except ToolNotFoundError as e:
                return SkillResult(success=False, error=str(e))

        mcp_instances = (
            self._mcp_registry.build_servers(skill.mcp_servers)
            if self._mcp_registry is not None
            else []
        )
        if mcp_instances:
            async with MCPServerManager(
                mcp_instances,
                strict=False,
                drop_failed_servers=True,
            ) as mgr:
                agent = Agent(
                    name=skill.name,
                    model=skill.model,
                    instructions=skill.instructions,
                    tools=tools,
                    mcp_servers=mgr.active_servers,
                    output_type=None,
                )
                result: RunResult = await Runner.run(
                    starting_agent=agent,
                    input=json.dumps(input_payload),
                )
        else:
            agent = Agent(
                name=skill.name,
                model=skill.model,
                instructions=skill.instructions,
                tools=tools,
                mcp_servers=[],
                output_type=None,
            )
            result = await Runner.run(
                starting_agent=agent,
                input=json.dumps(input_payload),
            )
        cost = extract_runner_cost(result, f"simple_skill:{skill.id}")
        return SkillResult(
            success=True,
            output=result.final_output,
            cost=cost,
        )

    async def _run_composed(
        self,
        skill: SkillDef,
        input_payload: dict[str, Any],
        context: dict[str, Any],
        tenant_id: str,
        _depth: int,
    ) -> SkillResult:
        run_context = SkillRunContext(original_input=dict(input_payload))
        children: list[InvocationCost] = []

        for step in skill.steps:
            step_result, step_cost = await self._execute_step(
                step=step,
                run_context=run_context,
                context=context,
                tenant_id=tenant_id,
                parent_model=skill.model,
                _depth=_depth,
            )
            run_context.steps_completed.append(step_result)
            if step_cost is not None:
                children.append(step_cost)

            if not step_result.success:
                total = sum(c.total_tokens for c in children)
                return SkillResult(
                    success=False,
                    output=run_context.model_dump(),
                    error=step_result.error or "Step failed",
                    cost=InvocationCost(
                        label=f"composed_skill:{skill.id}",
                        children=children,
                        total_tokens=total,
                    ),
                )

        total = sum(c.total_tokens for c in children)
        return SkillResult(
            success=True,
            output=run_context.model_dump(),
            cost=InvocationCost(
                label=f"composed_skill:{skill.id}",
                children=children,
                total_tokens=total,
            ),
        )

    async def _execute_step(
        self,
        step: SkillStep,
        run_context: SkillRunContext,
        context: dict[str, Any],
        tenant_id: str,
        parent_model: str,
        _depth: int,
    ) -> tuple[StepResult, InvocationCost | None]:
        if step.type == "invoke_skill":
            assert step.skill_id is not None
            sub = await self.run_skill(
                step.skill_id,
                run_context.model_dump(),
                context,
                tenant_id,
                _depth=_depth + 1,
            )
            return (
                StepResult(
                    step_id=step.id,
                    objective=step.objective,
                    success=sub.success,
                    output=sub.output,
                    error=sub.error,
                ),
                sub.cost,
            )

        if step.type == "invoke_tool":
            assert step.tool_id is not None
            try:
                tool = self._tool_registry.resolve(step.tool_id, context)
                out = tool.execute(**(step.params or {}))
            except Exception as exc:  # noqa: BLE001
                return (
                    StepResult(
                        step_id=step.id,
                        objective=step.objective,
                        success=False,
                        output=None,
                        error=str(exc),
                    ),
                    InvocationCost(
                        label=f"tool:{step.tool_id}",
                        children=[],
                        total_tokens=0,
                    ),
                )
            return (
                StepResult(
                    step_id=step.id,
                    objective=step.objective,
                    success=True,
                    output=out,
                    error=None,
                ),
                InvocationCost(
                    label=f"tool:{step.tool_id}",
                    children=[],
                    total_tokens=0,
                ),
            )

        # synthesize
        instructions = (
            f"{step.objective}\n\n"
            "You are executing a synthesis step in a composed skill workflow.\n"
            "The input JSON contains original_input and steps_completed from prior steps.\n"
            "Use that context to produce a concise result that fulfills the objective."
        )
        agent = Agent(
            name=f"synthesize_{step.id}",
            model=parent_model,
            instructions=instructions,
            tools=[],
            output_type=None,
        )
        result: RunResult = await Runner.run(
            starting_agent=agent,
            input=json.dumps(run_context.model_dump()),
        )
        cost = extract_runner_cost(result, f"synthesize:{step.id}")
        return (
            StepResult(
                step_id=step.id,
                objective=step.objective,
                success=True,
                output=result.final_output,
                error=None,
            ),
            cost,
        )
