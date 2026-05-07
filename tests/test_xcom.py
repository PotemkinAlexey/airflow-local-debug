from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from airflow_local_debug.execution.xcom import (
    extract_xcoms,
    fallback_return_xcoms,
    json_safe,
    task_xcom_label,
)

# --- json_safe -----------------------------------------------------------


def test_json_safe_passes_through_primitives() -> None:
    assert json_safe(None) is None
    assert json_safe(1) == 1
    assert json_safe(1.5) == 1.5
    assert json_safe("hi") == "hi"
    assert json_safe(True) is True


def test_json_safe_recurses_into_dict_with_str_keys() -> None:
    assert json_safe({"a": 1, 2: "x"}) == {"a": 1, "2": "x"}


def test_json_safe_normalises_list_tuple_set() -> None:
    assert json_safe([1, 2]) == [1, 2]
    assert json_safe((3, 4)) == [3, 4]
    out = json_safe({5, 6})
    assert sorted(out) == [5, 6]


def test_json_safe_serializes_datetime_via_isoformat() -> None:
    dt = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert json_safe(dt) == "2026-01-02T03:04:05+00:00"


def test_json_safe_falls_back_to_str_for_unknown_objects() -> None:
    class _Custom:
        def __repr__(self) -> str:
            return "<custom-repr>"

    assert json_safe(_Custom()) == "<custom-repr>"


def test_json_safe_handles_isoformat_attribute_that_raises() -> None:
    class _BadIso:
        def isoformat(self) -> str:
            raise RuntimeError("nope")

        def __repr__(self) -> str:
            return "<bad-iso>"

    assert json_safe(_BadIso()) == "<bad-iso>"


def test_json_safe_keeps_json_serialisable_dataclass_via_str() -> None:
    @dataclass
    class _Point:
        x: int
        y: int

    out = json_safe(_Point(1, 2))
    assert "_Point" in out or "x=1" in out


# --- task_xcom_label ------------------------------------------------------


def test_task_xcom_label_returns_plain_id_for_unmapped_task() -> None:
    assert task_xcom_label("extract", None) == "extract"
    assert task_xcom_label("extract", -1) == "extract"


def test_task_xcom_label_includes_map_index_for_mapped_task() -> None:
    assert task_xcom_label("transform", 0) == "transform[0]"
    assert task_xcom_label("transform", 7) == "transform[7]"


# --- fallback_return_xcoms ------------------------------------------------


@dataclass
class _FakeTI:
    task_id: str
    map_index: int | None = -1
    return_value: Any = None

    def xcom_pull(self, *, task_ids: str, key: str = "return_value", **kwargs: Any) -> Any:
        return self.return_value


class _FakeDagrun:
    def __init__(self, tis: list[_FakeTI]) -> None:
        self._tis = tis

    def get_task_instances(self) -> list[_FakeTI]:
        return list(self._tis)


def test_fallback_return_xcoms_collects_per_ti_return_values() -> None:
    dr = _FakeDagrun(
        [
            _FakeTI(task_id="extract", return_value={"rows": 3}),
            _FakeTI(task_id="load", return_value="ok"),
        ]
    )

    snapshot = fallback_return_xcoms(dr)

    assert snapshot == {
        "extract": {"return_value": {"rows": 3}},
        "load": {"return_value": "ok"},
    }


def test_fallback_return_xcoms_skips_labels_present_in_skip_set() -> None:
    dr = _FakeDagrun(
        [
            _FakeTI(task_id="extract", return_value="from_pull"),
            _FakeTI(task_id="load", return_value="from_pull"),
        ]
    )

    snapshot = fallback_return_xcoms(dr, skip_labels={"extract"})

    assert snapshot == {"load": {"return_value": "from_pull"}}


def test_fallback_return_xcoms_handles_mapped_indices() -> None:
    dr = _FakeDagrun(
        [
            _FakeTI(task_id="t", map_index=0, return_value=10),
            _FakeTI(task_id="t", map_index=1, return_value=20),
        ]
    )

    snapshot = fallback_return_xcoms(dr)

    assert snapshot == {
        "t[0]": {"return_value": 10},
        "t[1]": {"return_value": 20},
    }


def test_fallback_return_xcoms_omits_tasks_without_return_value() -> None:
    dr = _FakeDagrun(
        [
            _FakeTI(task_id="empty", return_value=None),
            _FakeTI(task_id="full", return_value=42),
        ]
    )

    snapshot = fallback_return_xcoms(dr)

    assert "empty" not in snapshot
    assert snapshot["full"] == {"return_value": 42}


def test_fallback_return_xcoms_returns_empty_for_dagrun_without_iterator() -> None:
    class _NoIter:
        pass

    assert fallback_return_xcoms(_NoIter()) == {}


# --- extract_xcoms (orchestration) ----------------------------------------


def test_extract_xcoms_returns_empty_when_dagrun_is_none() -> None:
    assert extract_xcoms(None, object()) == {}


def test_extract_xcoms_merges_query_and_fallback(monkeypatch) -> None:
    from airflow_local_debug.execution import xcom as xcom_module

    monkeypatch.setattr(
        xcom_module,
        "query_xcoms",
        lambda dagrun, dag: {
            "extract": {"return_value": "from_db", "extra_key": "side"},
        },
    )
    monkeypatch.setattr(
        xcom_module,
        "fallback_return_xcoms",
        lambda dagrun, *, skip_labels=None: {
            "load": {"return_value": "from_pull"},
        },
    )

    result = xcom_module.extract_xcoms(object(), object())

    assert result == {
        "extract": {"return_value": "from_db", "extra_key": "side"},
        "load": {"return_value": "from_pull"},
    }


def test_extract_xcoms_does_not_overwrite_db_value_with_fallback(monkeypatch) -> None:
    """If both sources have the same key for the same label, query wins."""
    from airflow_local_debug.execution import xcom as xcom_module

    monkeypatch.setattr(
        xcom_module,
        "query_xcoms",
        lambda dagrun, dag: {"task_a": {"return_value": "from_db"}},
    )
    # Fallback gets `skip_labels={"task_a"}` per the optimization, so it
    # would not return that label. But even if it did, the merge logic
    # should keep the db value. Simulate the buggy case:
    monkeypatch.setattr(
        xcom_module,
        "fallback_return_xcoms",
        lambda dagrun, *, skip_labels=None: {"task_a": {"return_value": "from_pull"}},
    )

    result = xcom_module.extract_xcoms(object(), object())

    assert result == {"task_a": {"return_value": "from_db"}}
