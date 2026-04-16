"""Task planner endpoint: plan then execute steps (skills, tools, synthesize)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from agents import Agent, AgentOutputSchema, Runner, RunResult
from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.agent_factories.instructions import (
    get_agent_instructions,
    get_agent_model,
    get_agent_name,
)
from src.core.base import InvocationCost, extract_runner_cost
from src.core.skills import SkillRegistry, SkillRunner, StepResult
from src.core.tools.base import ToolNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])

_MAX_REPLANS = 2


class InvocationCostJSON(BaseModel):
    """JSON-serializable cost tree for API responses."""

    label: str
    total_tokens: int = 0
    children: list[InvocationCostJSON] = Field(default_factory=list)

    @classmethod
    def from_cost(cls, c: InvocationCost | None) -> InvocationCostJSON | None:
        if c is None:
            return None
        return cls(
            label=c.label,
            total_tokens=c.total_tokens,
            children=[cls.from_cost(ch) for ch in c.children],
        )


class PlanStep(BaseModel):
    """Planner step; YAML / LLM use camelCase (stepType, skillId, toolId)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    step_type: Literal["invoke_skill", "invoke_tool", "synthesize"] = Field(
        ..., alias="stepType"
    )
    skill_id: str | None = Field(default=None, alias="skillId")
    tool_id: str | None = Field(default=None, alias="toolId")
    objective: str
    params: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _ids_for_type(self) -> PlanStep:
        if self.step_type == "invoke_skill" and not self.skill_id:
            raise ValueError("invoke_skill plan step requires skillId")
        if self.step_type == "invoke_tool" and not self.tool_id:
            raise ValueError("invoke_tool plan step requires toolId")
        return self


class ExecutionPlan(BaseModel):
    steps: list[PlanStep]
    reasoning: str


class RunTaskRequest(BaseModel):
    task: str
    tenant_id: str = "default"
    context: dict[str, Any] = Field(default_factory=dict)
    skill_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)


class RunTaskResponse(BaseModel):
    success: bool
    summary: str | None = None
    steps_completed: list[StepResult] = Field(default_factory=list)
    reasoning: str | None = None
    error: str | None = None
    cost: InvocationCostJSON | None = None


@dataclass
class TaskRunState:
    steps_completed: list[StepResult] = field(default_factory=list)
    completed_steps_payload: list[dict[str, Any]] = field(default_factory=list)
    cost_children: list[InvocationCost] = field(default_factory=list)
    last_reasoning: str | None = None
    last_error: str | None = None


def _plan_step_to_action(step: PlanStep) -> dict[str, Any]:
    return step.model_dump(mode="json", by_alias=True)


def _record_step(
    *,
    step: PlanStep,
    step_result: StepResult,
    steps_completed: list[StepResult],
    completed_steps_payload: list[dict[str, Any]],
) -> None:
    steps_completed.append(step_result)
    completed_steps_payload.append(
        {
            "action": _plan_step_to_action(step),
            "result": {
                "success": step_result.success,
                "payload": step_result.output,
                "error": step_result.error,
            },
        }
    )


def _direct_skill_input_payload(task: RunTaskRequest) -> dict[str, Any]:
    payload = dict(task.input)
    payload.setdefault("objective", task.task)
    payload.setdefault("task", task.task)
    payload.setdefault("prior_steps", [])
    return payload


def _response_with_cost(
    *,
    success: bool,
    state: TaskRunState,
    summary: str | None = None,
    error: str | None = None,
) -> RunTaskResponse:
    total_tokens = sum(c.total_tokens for c in state.cost_children)
    return RunTaskResponse(
        success=success,
        summary=summary,
        steps_completed=state.steps_completed,
        reasoning=state.last_reasoning,
        error=error,
        cost=InvocationCostJSON.from_cost(
            InvocationCost(
                label="run_task",
                children=state.cost_children,
                total_tokens=total_tokens,
            )
        ),
    )


async def _finalize_success(task: str, state: TaskRunState) -> RunTaskResponse:
    summary, synth_cost = await _final_summary(task, state.steps_completed)
    if synth_cost is not None:
        state.cost_children.append(synth_cost)
    return _response_with_cost(success=True, state=state, summary=summary)


async def _run_direct_skill_if_requested(
    task: RunTaskRequest, runner: SkillRunner, state: TaskRunState
) -> Literal["not_requested", "succeeded", "failed"]:
    """Run preferred skill before planning and return explicit execution status."""
    if not task.skill_id:
        return "not_requested"

    direct_step = PlanStep(
        stepType="invoke_skill",
        skillId=task.skill_id,
        objective=f"Execute preferred skill '{task.skill_id}' before planning.",
    )
    direct_result = await runner.run_skill(
        task.skill_id,
        _direct_skill_input_payload(task),
        task.context,
        task.tenant_id,
    )
    if direct_result.cost is not None:
        state.cost_children.append(direct_result.cost)

    step_result = StepResult(
        step_id="plan_step_0",
        objective=direct_step.objective,
        success=direct_result.success,
        output=direct_result.output,
        error=direct_result.error,
    )
    _record_step(
        step=direct_step,
        step_result=step_result,
        steps_completed=state.steps_completed,
        completed_steps_payload=state.completed_steps_payload,
    )
    if direct_result.success:
        return "succeeded"

    state.last_error = direct_result.error or "Preferred skill failed"
    return "failed"


async def _run_planned_iteration(
    *,
    task: RunTaskRequest,
    plan: ExecutionPlan,
    runner: SkillRunner,
    state: TaskRunState,
) -> bool:
    """Execute one generated plan. Returns True when plan failed."""
    plan_failed = False
    start_index = len(state.steps_completed)

    for idx, step in enumerate(plan.steps):
        global_idx = start_index + idx
        sr, step_costs = await _execute_plan_step(
            step,
            global_idx,
            task=task,
            runner=runner,
            prior_steps=list(state.steps_completed),
        )
        state.cost_children.extend(step_costs)
        _record_step(
            step=step,
            step_result=sr,
            steps_completed=state.steps_completed,
            completed_steps_payload=state.completed_steps_payload,
        )
        if not sr.success:
            plan_failed = True
            state.last_error = sr.error or "Step failed"
            break

    return plan_failed


async def _run_planner(
    *,
    task: RunTaskRequest,
    registry: SkillRegistry,
    runner: SkillRunner,
    completed_steps: list[dict[str, Any]],
    replan_reason: str | None,
) -> tuple[ExecutionPlan, InvocationCost]:
    agent_key = "task_planner"
    instructions = get_agent_instructions(agent_key)
    model = get_agent_model(agent_key)
    name = get_agent_name(agent_key)

    skills = registry.list_skills(task.tenant_id)
    available_skills = [
        {
            "id": s.id,
            "name": s.name,
            "description": s.description,
            "whenToUse": s.description,
            "input_schema": s.input_schema,
        }
        for s in skills
    ]
    tools = runner.tool_registry.list_tools()
    available_tools = [
        {"id": t.name, "description": t.description} for t in tools
    ]

    payload: dict[str, Any] = {
        "task": {
            "prompt": task.task,
            "tenantId": task.tenant_id,
            "skillId": task.skill_id,
            "input": task.input,
        },
        "availableSkills": available_skills,
        "availableTools": available_tools,
        "completedSteps": completed_steps,
    }
    if replan_reason:
        payload["replanReason"] = replan_reason

    agent = Agent(
        name=name,
        model=model,
        instructions=instructions,
        tools=[],
        output_type=AgentOutputSchema(ExecutionPlan, strict_json_schema=False),
    )
    result: RunResult = await Runner.run(
        starting_agent=agent,
        input=json.dumps(payload),
    )
    plan = result.final_output_as(ExecutionPlan, True)
    cost = extract_runner_cost(result, "task_planner")
    return plan, cost


async def _execute_plan_step(
    step: PlanStep,
    step_index: int,
    *,
    task: RunTaskRequest,
    runner: SkillRunner,
    prior_steps: list[StepResult],
) -> tuple[StepResult, list[InvocationCost]]:
    step_id = f"plan_step_{step_index}"
    if step.step_type == "invoke_skill":
        assert step.skill_id is not None
        input_payload: dict[str, Any] = {
            "objective": step.objective,
            "task": task.task,
            "prior_steps": [s.model_dump() for s in prior_steps],
        }
        sr = await runner.run_skill(
            step.skill_id,
            input_payload,
            task.context,
            task.tenant_id,
        )
        extra_costs = [sr.cost] if sr.cost is not None else []
        return (
            StepResult(
                step_id=step_id,
                objective=step.objective,
                success=sr.success,
                output=sr.output,
                error=sr.error,
            ),
            extra_costs,
        )

    if step.step_type == "invoke_tool":
        assert step.tool_id is not None
        try:
            tool = runner.tool_registry.resolve(step.tool_id, task.context)
            out = tool.execute(**(step.params or {}))
        except (ToolNotFoundError, TypeError, ValueError) as exc:
            return (
                StepResult(
                    step_id=step_id,
                    objective=step.objective,
                    success=False,
                    output=None,
                    error=str(exc),
                ),
                [
                    InvocationCost(
                        label=f"tool:{step.tool_id}",
                        children=[],
                        total_tokens=0,
                    )
                ],
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("invoke_tool failed")
            return (
                StepResult(
                    step_id=step_id,
                    objective=step.objective,
                    success=False,
                    output=None,
                    error=str(exc),
                ),
                [],
            )
        return (
            StepResult(
                step_id=step_id,
                objective=step.objective,
                success=True,
                output=out,
                error=None,
            ),
            [
                InvocationCost(
                    label=f"tool:{step.tool_id}",
                    children=[],
                    total_tokens=0,
                )
            ],
        )

    # synthesize
    instructions = (
        f"{step.objective}\n\n"
        "You synthesize a concise answer from the JSON input: it contains the user task "
        "and prior_steps (orchestration results). Be factual."
    )
    agent = Agent(
        name="task_inline_synthesize",
        model=get_agent_model("task_synthesizer"),
        instructions=instructions,
        tools=[],
        output_type=None,
    )
    synth_input = {
        "task": task.task,
        "prior_steps": [s.model_dump() for s in prior_steps],
    }
    result: RunResult = await Runner.run(
        starting_agent=agent,
        input=json.dumps(synth_input),
    )
    synth_cost = extract_runner_cost(result, f"plan_synthesize:{step_id}")
    return (
        StepResult(
            step_id=step_id,
            objective=step.objective,
            success=True,
            output=result.final_output,
            error=None,
        ),
        [synth_cost],
    )


async def _final_summary(
    task: str, steps_completed: list[StepResult]
) -> tuple[str | None, InvocationCost | None]:
    agent = Agent(
        name=get_agent_name("task_synthesizer"),
        model=get_agent_model("task_synthesizer"),
        instructions=get_agent_instructions("task_synthesizer"),
        tools=[],
        output_type=None,
    )
    result: RunResult = await Runner.run(
        starting_agent=agent,
        input=json.dumps(
            {"task": task, "steps_completed": [s.model_dump() for s in steps_completed]}
        ),
    )
    out = result.final_output
    summary = out if isinstance(out, str) else str(out)
    return summary, extract_runner_cost(result, "task_synthesizer")


@router.post("/run", response_model=RunTaskResponse)
async def run_task(body: RunTaskRequest, request: Request) -> RunTaskResponse:
    registry: SkillRegistry = request.app.state.skill_registry
    runner: SkillRunner = request.app.state.skill_runner
    state = TaskRunState()

    direct_skill_status = await _run_direct_skill_if_requested(body, runner, state)
    if direct_skill_status == "succeeded":
        return await _finalize_success(body.task, state)
    if direct_skill_status == "failed":
        return _response_with_cost(
            success=False,
            state=state,
            error=state.last_error,
        )

    for replan_idx in range(_MAX_REPLANS + 1):
        replan_reason: str | None = None
        if replan_idx > 0 and state.last_error:
            replan_reason = f"Execution failed: {state.last_error}. Revise the plan."

        try:
            plan, planner_cost = await _run_planner(
                task=body,
                registry=registry,
                runner=runner,
                completed_steps=state.completed_steps_payload,
                replan_reason=replan_reason,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("task_planner failed")
            state.cost_children.append(
                InvocationCost(label="task_planner_error", children=[], total_tokens=0)
            )
            return _response_with_cost(success=False, state=state, error=str(exc))

        state.cost_children.append(planner_cost)
        state.last_reasoning = plan.reasoning
        plan_failed = await _run_planned_iteration(
            task=body,
            plan=plan,
            runner=runner,
            state=state,
        )

        if not plan_failed:
            return await _finalize_success(body.task, state)

        if replan_idx >= _MAX_REPLANS:
            return _response_with_cost(
                success=False,
                state=state,
                error=state.last_error,
            )

    raise RuntimeError("run_task: exhausted replan loop without returning")
