from __future__ import annotations

from airflow_local_debug.traceback_utils import (
    _classify_problem,
    format_pretty_exception,
    safe_repr,
    shrink,
)


def test_classify_problem_timeout() -> None:
    category, _, _ = _classify_problem(message="request timed out after 30s")
    assert category == "timeout"


def test_classify_problem_http_status() -> None:
    category, _, _ = _classify_problem(status=500, message="internal server error")
    assert category == "http"


def test_classify_problem_auth_by_status() -> None:
    category, _, _ = _classify_problem(status=401, message="something went wrong")
    assert category == "auth"


def test_classify_problem_auth_by_keyword() -> None:
    category, _, _ = _classify_problem(message="invalid credentials")
    assert category == "auth"


def test_classify_problem_does_not_match_loose_token_keyword() -> None:
    # After the keyword tightening, a stray "token" reference is no longer
    # auto-classified as auth.
    category, _, _ = _classify_problem(message="KeyError: 'token'")
    assert category != "auth"


def test_classify_problem_network() -> None:
    category, _, _ = _classify_problem(message="connection refused")
    assert category == "network"


def test_classify_problem_io() -> None:
    category, _, _ = _classify_problem(message="No such file or directory")
    assert category == "io"


def test_classify_problem_unknown() -> None:
    category, _, _ = _classify_problem(message="something went wrong without category hints")
    assert category == "unknown"


def test_format_pretty_exception_renders_task_id_and_class() -> None:
    try:
        raise ValueError("boom")
    except ValueError as exc:
        rendered = format_pretty_exception(exc, task_id="my_task", enable_colors=False)

    assert "my_task" in rendered
    assert "ValueError" in rendered
    assert "boom" in rendered


def test_safe_repr_truncates_long_values() -> None:
    text = safe_repr("x" * 2000, limit=50)
    assert len(text) <= 80  # 50 + ellipsis marker
    assert "trimmed" in text


def test_shrink_handles_dict_and_list() -> None:
    assert shrink({"a": 1, "b": 2}) == {"a": "1", "b": "2"}
    assert shrink([1, 2, 3]) == ["1", "2", "3"]
    assert "trimmed" not in shrink("short")
