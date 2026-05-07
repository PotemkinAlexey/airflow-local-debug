from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from airflow_local_debug.mocks import (
    TaskMockRule,
    load_task_mock_rules,
    local_task_mocks,
    task_matches_mock_rule,
    task_mock_rules_from_payload,
)


@dataclass
class FakeTask:
    task_id: str
    task_type: str = "FakeOperator"

    def execute(self, context: dict[str, Any]) -> str:
        return "real"


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]


class FakeTI:
    def __init__(self) -> None:
        self.pushed: dict[str, Any] = {}

    def xcom_push(self, *, key: str, value: Any) -> None:
        self.pushed[key] = value


def test_task_mock_rules_from_payload_accepts_mocks_object() -> None:
    rules = task_mock_rules_from_payload(
        {
            "mocks": [
                {
                    "name": "warehouse load",
                    "task_id": "load_to_snowflake",
                    "xcom": {"return_value": {"rows_loaded": 12}},
                }
            ]
        }
    )

    assert rules == [
        TaskMockRule(
            task_id="load_to_snowflake",
            mode="success",
            xcom={"return_value": {"rows_loaded": 12}},
            name="warehouse load",
        )
    ]


def test_task_mock_rules_reject_unscoped_or_unsupported_rules() -> None:
    with pytest.raises(ValueError, match="must define task_id"):
        task_mock_rules_from_payload([{"xcom": {}}])
    with pytest.raises(ValueError, match="unsupported mode"):
        task_mock_rules_from_payload([{"task_id": "a", "mode": "fail"}])


def test_load_task_mock_rules_reads_json_file(tmp_path) -> None:
    mock_file = tmp_path / "mocks.json"
    mock_file.write_text('[{"task_id_glob": "load_*", "return_value": "ok"}]', encoding="utf-8")

    rules = load_task_mock_rules(mock_file)

    assert len(rules) == 1
    assert rules[0].task_id_glob == "load_*"
    assert rules[0].xcom == {"return_value": "ok"}


def test_task_matches_mock_rule_by_task_and_operator() -> None:
    task = FakeTask("load_to_snowflake")

    assert task_matches_mock_rule(task, TaskMockRule(task_id="load_to_snowflake"))
    assert task_matches_mock_rule(task, TaskMockRule(task_id_glob="load_*"))
    assert task_matches_mock_rule(task, TaskMockRule(operator="FakeOperator"))
    assert task_matches_mock_rule(task, TaskMockRule(operator_glob="*FakeTask"))
    assert not task_matches_mock_rule(task, TaskMockRule(task_id="other"))


def test_local_task_mocks_patch_execute_and_push_xcom() -> None:
    task = FakeTask("load")
    dag = FakeDag(task_dict={"load": task})
    ti = FakeTI()
    notes: list[str] = []

    with local_task_mocks(
        dag,
        [TaskMockRule(task_id="load", name="local load", xcom={"return_value": {"rows": 3}, "meta": "fixture"})],
        notes=notes,
    ) as registry:
        assert task.execute({"ti": ti}) == {"rows": 3}
        assert ti.pushed == {"return_value": {"rows": 3}, "meta": "fixture"}
        assert registry.mocked_task_ids == {"load"}
        assert registry.mock_infos[0].rule_name == "local load"

    assert task.execute({}) == "real"
    assert notes == []


def test_local_task_mocks_require_matching_rules() -> None:
    task = FakeTask("extract")
    dag = FakeDag(task_dict={"extract": task})

    with pytest.raises(ValueError, match="matched no tasks"):
        with local_task_mocks(dag, [TaskMockRule(task_id="missing")]):
            pass

    assert task.execute({}) == "real"
