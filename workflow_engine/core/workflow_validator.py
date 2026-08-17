# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Validation for workflow definitions before any task is dispatched."""

from collections import deque

from workflow_engine.core.models import Workflow


TERMINAL_TARGETS = frozenset({"end", "retry", "endNode"})


def validate_workflow(workflow: Workflow) -> None:
    """Raise ``ValueError`` when a workflow cannot be executed safely."""
    errors = find_workflow_errors(workflow)
    if errors:
        raise ValueError("Invalid workflow: " + "; ".join(errors))


def find_workflow_errors(workflow: Workflow) -> list[str]:
    """Return deterministic validation errors without dispatching work."""
    if workflow is None:
        return ["workflow is null"]
    if not workflow.steps:
        return ["workflow has no steps"]

    errors: list[str] = []
    steps = {}
    for step in workflow.steps:
        if step is None or not step.name or not step.name.strip():
            errors.append("step name is blank")
            continue
        if step.name in steps:
            errors.append(f"duplicate step '{step.name}'")
        else:
            steps[step.name] = step
        for task in step.subtasks or []:
            if task is None:
                errors.append(f"step '{step.name}' has a null subtask")
            elif not task.agent or not task.agent.strip():
                errors.append(f"step '{step.name}' has a subtask with blank agent")

    graph: dict[str, list[str]] = {name: [] for name in steps}
    indegree: dict[str, int] = {name: 0 for name in steps}
    for step in steps.values():
        for jump in step.next or []:
            if jump is None:
                errors.append(f"step '{step.name}' has a null next condition")
                continue
            target = jump.step
            if not target or not target.strip():
                errors.append(f"step '{step.name}' has a blank next target")
            elif target not in TERMINAL_TARGETS:
                if target not in steps:
                    errors.append(
                        f"step '{step.name}' references unknown step '{target}'"
                    )
                elif target == step.name:
                    errors.append(f"step '{step.name}' contains a cycle")
                else:
                    graph[step.name].append(target)
                    indegree[target] += 1
        for context_ref in step.context_from or []:
            if context_ref != "*" and context_ref not in steps:
                errors.append(
                    f"step '{step.name}' references unknown context step "
                    f"'{context_ref}'"
                )

    if errors:
        return errors

    roots = deque(name for name, degree in indegree.items() if degree == 0)
    if not roots:
        return ["workflow has no root step"]
    for root in roots:
        if steps[root].layer != 0:
            errors.append(f"root step '{root}' must have layer 0")

    visited: set[str] = set()
    reachable = deque(roots)
    while reachable:
        current = reachable.popleft()
        if current in visited:
            continue
        visited.add(current)
        reachable.extend(graph[current])
    if len(visited) != len(graph):
        unreachable = sorted(set(graph) - visited)
        errors.append(f"cyclic or unreachable steps {unreachable}")

    remaining = dict(indegree)
    topological = deque(roots)
    processed = 0
    while topological:
        current = topological.popleft()
        processed += 1
        for target in graph[current]:
            remaining[target] -= 1
            if remaining[target] == 0:
                topological.append(target)
    if processed != len(graph) and len(visited) == len(graph):
        errors.append("workflow contains a cycle")
    return errors
