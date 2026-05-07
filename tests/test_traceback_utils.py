from __future__ import annotations

from airflow_local_debug.reporting.traceback_utils import (
    _classify_problem,
    format_pretty_exception,
    safe_repr,
    shrink,
)


def test_classify_problem_timeout() -> None:
    category, _, _ = _classify_problem(message="request timed out after 30s")
    assert category == "timeout"


def test_classify_problem_timeout_by_class_name() -> None:
    category, _, _ = _classify_problem(class_name="ReadTimeout", message="")
    assert category == "timeout"


def test_classify_problem_does_not_match_sql_timeout_column_name() -> None:
    category, _, _ = _classify_problem(
        class_name="OperationalError",
        message="no such table: task_instance SELECT trigger_timeout FROM task_instance",
    )

    assert category != "timeout"


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


# --- _redact_url ----------------------------------------------------------


def test_redact_url_masks_sensitive_query_param_values() -> None:
    from airflow_local_debug.reporting.traceback_utils import _redact_url

    redacted = _redact_url("https://api.example.com/v1?token=ABCDEF&user=bob")
    assert "ABCDEF" not in redacted
    assert "user=bob" in redacted


def test_redact_url_leaves_non_sensitive_query_params_alone() -> None:
    from airflow_local_debug.reporting.traceback_utils import _redact_url

    redacted = _redact_url("https://api.example.com/v1?page=2&size=20")
    assert redacted == "https://api.example.com/v1?page=2&size=20"


def test_redact_url_returns_input_unchanged_for_non_url_strings() -> None:
    from airflow_local_debug.reporting.traceback_utils import _redact_url

    assert _redact_url("plain text without scheme") == "plain text without scheme"


# --- _shorten_path --------------------------------------------------------


def test_shorten_path_returns_relative_for_files_under_repo_root() -> None:
    from pathlib import Path

    from airflow_local_debug.reporting.traceback_utils import _repo_root, _shorten_path

    sample = str(_repo_root() / "src" / "airflow_local_debug" / "runner.py")
    assert _shorten_path(sample) == str(Path("src") / "airflow_local_debug" / "runner.py")


def test_shorten_path_keeps_absolute_when_outside_repo() -> None:
    from airflow_local_debug.reporting.traceback_utils import _shorten_path

    assert _shorten_path("/usr/lib/python3/site-packages/foo.py") == "/usr/lib/python3/site-packages/foo.py"


# --- _format_frame --------------------------------------------------------


def test_format_frame_includes_file_line_and_name() -> None:
    import traceback

    from airflow_local_debug.reporting.traceback_utils import _format_frame

    frame = traceback.FrameSummary("/abs/path/dag.py", 42, "extract", line="x = 1")
    rendered = _format_frame(frame, enable_colors=False)
    assert "dag.py" in rendered
    assert "42" in rendered
    assert "extract" in rendered


# --- _truncate_middle / _indent_block ------------------------------------


def test_truncate_middle_keeps_ends_when_string_is_too_long() -> None:
    from airflow_local_debug.reporting.traceback_utils import _truncate_middle

    out = _truncate_middle("a" * 100 + "b" * 100, max_len=20)
    assert "..." in out
    assert out.startswith("a")
    assert out.endswith("b")


def test_truncate_middle_passthrough_for_short_strings() -> None:
    from airflow_local_debug.reporting.traceback_utils import _truncate_middle

    assert _truncate_middle("short", max_len=10) == "short"


def test_indent_block_indents_each_line() -> None:
    from airflow_local_debug.reporting.traceback_utils import _indent_block

    out = _indent_block("a\nb\nc", n=2)
    assert out == "  a\n  b\n  c"


# --- format_pretty_log_record --------------------------------------------


def test_format_pretty_log_record_renders_warning_with_object_label() -> None:
    import logging

    from airflow_local_debug.reporting.traceback_utils import format_pretty_log_record

    record = logging.LogRecord(
        name="airflow.task",
        level=logging.WARNING,
        pathname="/abs/dag.py",
        lineno=10,
        msg="something fishy %s",
        args=("happened",),
        exc_info=None,
    )
    out = format_pretty_log_record(record, object_label="extract", enable_colors=False)
    assert "extract" in out
    assert "something fishy happened" in out


# --- StepTracer ----------------------------------------------------------


def test_step_tracer_records_event_and_completes_clean() -> None:
    import io

    from airflow_local_debug.reporting.traceback_utils import StepTracer, StepTracerOptions

    buf = io.StringIO()
    tracer = StepTracer(
        "extract",
        "PythonOperator",
        run_id="run-1",
        map_index=None,
        options=StepTracerOptions(stream=buf, enable_colors=False),
    )
    with tracer:
        tracer.event("step", "doing stuff")

    output = buf.getvalue()
    assert "extract" in output
    assert "step" in output


def test_step_tracer_renders_error_block_on_exception() -> None:
    import io

    from airflow_local_debug.reporting.traceback_utils import StepTracer, StepTracerOptions

    buf = io.StringIO()
    tracer = StepTracer(
        "transform",
        "PythonOperator",
        run_id="run-1",
        map_index=None,
        options=StepTracerOptions(stream=buf, enable_colors=False),
    )
    try:
        with tracer:
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    output = buf.getvalue()
    assert "transform" in output
    assert "boom" in output or "RuntimeError" in output
