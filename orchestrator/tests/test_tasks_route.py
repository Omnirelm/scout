from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.api.routes import tasks
from src.core.base import InvocationCost
from src.core.skills import SkillResult, StepResult


class _DummyRunner:
    def __init__(self, skill_result: SkillResult) -> None:
        self._skill_result = skill_result
        self.calls: list[tuple[str, dict, dict, str]] = []
        self.tool_registry = SimpleNamespace(list_tools=lambda: [])

    async def run_skill(
        self,
        skill_id: str,
        input_payload: dict,
        context: dict,
        tenant_id: str,
    ) -> SkillResult:
        self.calls.append((skill_id, input_payload, context, tenant_id))
        return self._skill_result


def _request_with_runner(runner: _DummyRunner) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(skill_registry=SimpleNamespace(), skill_runner=runner)
        )
    )


@pytest.mark.asyncio
async def test_run_task_planner_only_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    planner_called = {"value": False}

    async def fake_run_planner(**_: object) -> tuple[tasks.ExecutionPlan, InvocationCost]:
        planner_called["value"] = True
        return (
            tasks.ExecutionPlan(
                steps=[tasks.PlanStep(stepType="synthesize", objective="Summarize")],
                reasoning="plan",
            ),
            InvocationCost(label="planner", total_tokens=5),
        )

    async def fake_execute_plan_step(
        step: tasks.PlanStep,
        step_index: int,
        **_: object,
    ) -> tuple[StepResult, list[InvocationCost]]:
        return (
            StepResult(
                step_id=f"plan_step_{step_index}",
                objective=step.objective,
                success=True,
                output={"ok": True},
                error=None,
            ),
            [],
        )

    async def fake_final_summary(
        task: str, steps_completed: list[StepResult]
    ) -> tuple[str, InvocationCost]:
        return (f"summary:{task}:{len(steps_completed)}", InvocationCost(label="synth"))

    monkeypatch.setattr(tasks, "_run_planner", fake_run_planner)
    monkeypatch.setattr(tasks, "_execute_plan_step", fake_execute_plan_step)
    monkeypatch.setattr(tasks, "_final_summary", fake_final_summary)

    runner = _DummyRunner(SkillResult(success=True, output={"ignored": True}))
    req = _request_with_runner(runner)
    body = tasks.RunTaskRequest(task="do thing")

    response = await tasks.run_task(body, req)

    assert planner_called["value"] is True
    assert len(runner.calls) == 0
    assert response.success is True
    assert response.summary == "summary:do thing:1"


@pytest.mark.asyncio
async def test_run_task_direct_skill_success_skips_planner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planner_called = {"value": False}

    async def fake_run_planner(**_: object) -> tuple[tasks.ExecutionPlan, InvocationCost]:
        planner_called["value"] = True
        raise AssertionError("Planner should not run on direct skill success")

    async def fake_final_summary(
        task: str, steps_completed: list[StepResult]
    ) -> tuple[str, InvocationCost]:
        return (f"summary:{task}:{len(steps_completed)}", InvocationCost(label="synth"))

    monkeypatch.setattr(tasks, "_run_planner", fake_run_planner)
    monkeypatch.setattr(tasks, "_final_summary", fake_final_summary)

    runner = _DummyRunner(SkillResult(success=True, output={"direct": True}))
    req = _request_with_runner(runner)
    body = tasks.RunTaskRequest(
        task="investigate",
        skill_id="log_analysis",
        input={"alert_id": "a-1"},
        context={"service": "checkout"},
    )

    response = await tasks.run_task(body, req)

    assert planner_called["value"] is False
    assert response.success is True
    assert len(response.steps_completed) == 1
    first_call = runner.calls[0]
    assert first_call[0] == "log_analysis"
    assert first_call[1]["alert_id"] == "a-1"
    assert first_call[1]["objective"] == "investigate"
    assert first_call[2] == {"service": "checkout"}


@pytest.mark.asyncio
async def test_run_task_direct_skill_failure_returns_error_without_planner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planner_called = {"value": False}

    async def fake_run_planner(**_: object) -> tuple[tasks.ExecutionPlan, InvocationCost]:
        planner_called["value"] = True
        raise AssertionError("Planner should not run on direct skill failure")

    monkeypatch.setattr(tasks, "_run_planner", fake_run_planner)

    runner = _DummyRunner(SkillResult(success=False, error="boom"))
    req = _request_with_runner(runner)
    body = tasks.RunTaskRequest(task="investigate", skill_id="log_analysis")

    response = await tasks.run_task(body, req)

    assert planner_called["value"] is False
    assert response.success is False
    assert response.error == "boom"
    assert len(response.steps_completed) == 1
    assert response.steps_completed[0].success is False
    assert response.steps_completed[0].error == "boom"


@pytest.mark.asyncio
async def test_run_planner_includes_skill_hint_and_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeRunResult:
        def final_output_as(
            self, _model: type[tasks.ExecutionPlan], _strict: bool
        ) -> tasks.ExecutionPlan:
            return tasks.ExecutionPlan(steps=[], reasoning="ok")

    async def fake_runner_run(*, starting_agent: object, input: str) -> _FakeRunResult:
        del starting_agent
        captured["payload"] = json.loads(input)
        return _FakeRunResult()

    monkeypatch.setattr(tasks.Runner, "run", staticmethod(fake_runner_run))
    monkeypatch.setattr(
        tasks,
        "extract_runner_cost",
        lambda *_: InvocationCost(label="planner", total_tokens=0),
    )
    monkeypatch.setattr(tasks, "get_agent_instructions", lambda _k: "instructions")
    monkeypatch.setattr(tasks, "get_agent_model", lambda _k: "gpt-5-mini")
    monkeypatch.setattr(tasks, "get_agent_name", lambda _k: "planner")

    registry = SimpleNamespace(list_skills=lambda _tenant: [])
    runner = SimpleNamespace(tool_registry=SimpleNamespace(list_tools=lambda: []))
    task = tasks.RunTaskRequest(
        task="investigate",
        tenant_id="default",
        skill_id="log_analysis",
        input={"alert_id": "a-1"},
    )

    await tasks._run_planner(
        task=task,
        registry=registry,
        runner=runner,
        completed_steps=[],
        replan_reason=None,
    )

    planner_task = captured["payload"]["task"]
    assert planner_task["prompt"] == "investigate"
    assert planner_task["tenantId"] == "default"
    assert planner_task["skillId"] == "log_analysis"
    assert planner_task["input"] == {"alert_id": "a-1"}
