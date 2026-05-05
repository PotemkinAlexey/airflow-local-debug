"""
Shared formatting utilities for local debug output.

This module is the single place for:
- problem classification (`auth`, `network`, `timeout`, `permission`, etc.)
- pretty traceback rendering
- pretty warning/error log rendering
- lightweight live step tracing helpers (`StepTracer`)

Keeping these pieces together ensures that live task logs and final exceptions
look consistent and follow the same classification rules.
"""

from __future__ import annotations

import logging
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, TextIO, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

RESET = "\033[0m"
DIM = "\033[2m"
RED = "\033[91m"
BLUE = "\033[94m"
YELLOW = "\033[33m"
CYAN = "\033[36m"

ERROR_STYLES = {
    "auth": ("🔒", "Auth"),
    "http": ("🧱", "HTTP"),
    "timeout": ("⏱️", "Timeout"),
    "network": ("🌐", "Network"),
    "json": ("🧩", "JSON"),
    "io": ("📁", "I/O"),
    "perm": ("🚫", "Permission"),
    "key": ("🗝️", "Key"),
    "value": ("⚙️", "Value"),
    "airflow": ("🪁", "Airflow"),
    "unknown": ("💀", "Error"),
}

DOT = "•"
START_DEFAULT = "🚗"
FINISH_DEFAULT = "🏁"


def _colorize(text: str, color: str, enabled: bool) -> str:
    return f"{color}{text}{RESET}" if enabled else text


def _truncate_middle(text: str, max_len: int = 140, sep: str = "...") -> str:
    if not isinstance(text, str) or len(text) <= max_len:
        return text
    keep = (max_len - len(sep)) // 2
    return text[:keep] + sep + text[-keep:]


def _indent_block(text: str, n: int = 4) -> str:
    pad = " " * n
    lines = (text or "").splitlines() or [""]
    return "\n".join(pad + line for line in lines)


def _redact_url(
    value: str,
    keys: tuple[str, ...] = ("sig", "token", "sas", "x-ms-signature", "authorization"),
) -> str:
    try:
        parsed = urlparse(value)
        query = []
        for key, query_value in parse_qsl(parsed.query, keep_blank_values=True):
            query.append((key, "****" if key.lower() in keys else query_value))
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query), parsed.fragment))
    except Exception:
        return value


def _preview_text(value: Any, limit: int = 300) -> str:
    try:
        text = str(value) if value is not None else ""
    except Exception:
        return "<unstringable>"
    return text[:limit] + ("...<trimmed>" if len(text) > limit else "")


def _collect_error_context(exc: BaseException) -> dict[str, Any]:
    ctx: dict[str, Any] = {}

    fields = (
        "status",
        "status_code",
        "code",
        "errno",
        "reason",
        "url",
        "method",
        "path",
        "filename",
        "lineno",
        "colno",
        "pos",
        "request_id",
        "trace_id",
        "error_code",
    )
    for field in fields:
        if hasattr(exc, field):
            try:
                ctx[field] = getattr(exc, field)
            except Exception:
                continue

    if getattr(exc, "args", None):
        try:
            first = str(exc.args[0])
            if first:
                ctx.setdefault("message", _preview_text(first))
        except Exception:
            pass

    response = getattr(exc, "response", None)
    if response is not None:
        try:
            for field in ("status", "status_code", "reason"):
                value = getattr(response, field, None)
                if value is not None:
                    ctx.setdefault(field, value)

            headers = getattr(response, "headers", None)
            if headers:
                try:
                    request_id = headers.get("x-ms-request-id") or headers.get("x-request-id")
                    if request_id:
                        ctx.setdefault("request_id", request_id)
                except Exception:
                    pass

            body_text = None
            try:
                text_value = getattr(response, "text", None)
                if callable(text_value):
                    body_text = _preview_text(text_value())
                elif isinstance(text_value, str):
                    body_text = _preview_text(text_value)
                elif hasattr(response, "content"):
                    content = getattr(response, "content") or b""
                    if isinstance(content, (bytes, bytearray)):
                        body_text = _preview_text(content.decode("utf-8", "replace"))
            except Exception:
                body_text = None
            if body_text:
                ctx.setdefault("body_preview", body_text)

            request = getattr(response, "request", None)
            if request is not None:
                method = getattr(request, "method", None)
                url = getattr(request, "url", None)
                if method:
                    ctx.setdefault("method", method)
                if url:
                    ctx.setdefault("url", url)
        except Exception:
            pass

    request = getattr(exc, "request", None)
    if request is not None:
        try:
            method = getattr(request, "method", None)
            url = getattr(request, "url", None)
            if method:
                ctx.setdefault("method", method)
            if url:
                ctx.setdefault("url", url)
        except Exception:
            pass

    return ctx


def _maybe_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _has_any(text: str, *parts: str) -> bool:
    return any(part in text for part in parts)


def _has_word_any(text: str, *parts: str) -> bool:
    return any(re.search(rf"(?<![a-z0-9_]){re.escape(part)}(?![a-z0-9_])", text) for part in parts)


def _classify_exception(exc: BaseException, summary: dict[str, Any] | None = None) -> tuple[str, str, str]:
    module_name = str(getattr(exc.__class__, "__module__", "") or "")
    class_name = str(getattr(exc.__class__, "__name__", "") or "")
    message = str(exc or "")

    status = None
    if isinstance(summary, dict):
        status = summary.get("status_code") or summary.get("status")

    return _classify_problem(
        module_name=module_name,
        class_name=class_name,
        message=message,
        status=status,
    )


def _classify_problem(
    *,
    module_name: str = "",
    class_name: str = "",
    message: str = "",
    status: Any = None,
) -> tuple[str, str, str]:
    """
    Classify an error into a single problem category.

    Priority order (first match wins, top to bottom):
        timeout > rate-limit-http > network > auth > json > io > perm
        > generic 4xx/5xx http > airflow > key > value > unknown

    The order favors transport-level diagnostics (timeout/network) over
    application-level ones (auth/json) so that, for example, a "timeout
    while authenticating" is reported as a timeout problem rather than auth.
    """
    module_name = str(module_name or "").lower()
    class_name = str(class_name or "").lower()
    message = str(message or "").lower()
    status = _maybe_int(status)

    if (status in (408, 504, 524)) or _has_any(
        message, "timeout", "timed out", "deadline exceeded", "request timed out"
    ):
        return ("timeout",) + ERROR_STYLES["timeout"]
    if (status == 429) or _has_any(message, "rate limit", "too many requests", "throttl"):
        return ("http",) + ERROR_STYLES["http"]
    if _has_any(
        message,
        "dns",
        "resolve",
        "connection reset",
        "connection refused",
        "connection aborted",
        "ssl",
        "tls",
        "host unreachable",
        "name or service not known",
        "temporary failure in name resolution",
        "network is unreachable",
        "no response from",
        "failed to establish a new connection",
    ):
        return ("network",) + ERROR_STYLES["network"]
    if (status in (401, 403)) or _has_any(
        message,
        "unauthorized",
        "forbidden",
        "authentication failed",
        "authenticationfailed",
        "authentication unavailable",
        "failed to retrieve a token",
        "invalid_client",
        "invalid grant",
        "credentials",
        "oauth",
        "access token",
        "bearer token",
        "aadsts",
        "managed identity",
    ):
        return ("auth",) + ERROR_STYLES["auth"]
    if _has_any(message, "jsondecode", "json decode", "invalid json", "malformed json", "expecting value"):
        return ("json",) + ERROR_STYLES["json"]
    if _has_any(
        message,
        "no such file",
        "enoent",
        "disk full",
        "no space left",
        "input/output error",
        "i/o error",
        "broken pipe",
        "file not found",
        "directory not found",
    ):
        return ("io",) + ERROR_STYLES["io"]
    if _has_any(
        message,
        "permission denied",
        "operation not permitted",
        "eacces",
        "eperm",
        "access denied",
        "read-only file system",
    ):
        return ("perm",) + ERROR_STYLES["perm"]
    if status is not None and status >= 400:
        return ("http",) + ERROR_STYLES["http"]
    if "airflow" in module_name or "airflow" in class_name:
        return ("airflow",) + ERROR_STYLES["airflow"]
    if "keyerror" in class_name or "keyerror" in message:
        return ("key",) + ERROR_STYLES["key"]
    if "valueerror" in class_name or "typeerror" in class_name or "invalid value" in message:
        return ("value",) + ERROR_STYLES["value"]
    return ("unknown",) + ERROR_STYLES["unknown"]


def _repo_root() -> Path:
    path = Path(__file__).resolve()
    markers = ("pyproject.toml", ".git", "requirements.txt")
    for parent in path.parents:
        if any((parent / marker).exists() for marker in markers):
            return parent
    return path.parent


def _shorten_path(filename: str) -> str:
    try:
        path = Path(filename).resolve()
    except Exception:
        return filename

    root = _repo_root()
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def _format_frame(frame: traceback.FrameSummary, *, enable_colors: bool) -> str:
    location = f"{_shorten_path(frame.filename)}:{frame.lineno}"
    header = f"  {location} in {frame.name}"
    if frame.line:
        return "\n".join(
            [
                _colorize(header, BLUE, enable_colors),
                f"    {_colorize(frame.line.strip(), DIM, enable_colors)}",
            ]
        )
    return _colorize(header, BLUE, enable_colors)


def _now_ms() -> float:
    return time.time() * 1000.0


def _fmt_dur(ms: float) -> str:
    if ms < 1000:
        return f"{ms:.1f} ms"
    seconds = ms / 1000.0
    return f"{seconds:.2f} s" if seconds < 60 else f"{int(seconds // 60)}m {seconds % 60:.1f}s"


def safe_repr(obj: Any, limit: int = 800) -> str:
    """Safe and bounded repr() for debug logging."""
    try:
        text = repr(obj)
    except Exception as exc:
        text = f"<unreprable: {exc}>"
    return text if len(text) <= limit else text[:limit] + "...<trimmed>"


def shrink(obj: Any, limit: int = 800) -> Union[str, list[Any], dict[str, str]]:
    """
    Compact pretty-printer for logging common payloads.
    - dict => {k: safe_repr(v, 200)} for up to 50 keys
    - list/tuple => [safe_repr(v, 200)] for up to 50 items
    - everything else => safe_repr(obj, limit)
    """
    try:
        if isinstance(obj, Mapping):
            out: dict[str, str] = {}
            for key, value in list(obj.items())[:50]:
                out[str(key)] = safe_repr(value, limit=200)
            return out
        if isinstance(obj, (list, tuple)):
            return [safe_repr(value, limit=200) for value in obj[:50]]
        return safe_repr(obj, limit=limit)
    except Exception as exc:
        return f"<shrink-error: {exc}>"


def _select_frames(tb_exc: traceback.TracebackException, max_frames: int = 12) -> tuple[list[traceback.FrameSummary], int]:
    frames = list(tb_exc.stack)
    if len(frames) <= max_frames:
        return frames, 0

    repo_root = str(_repo_root())
    app_frames = [frame for frame in frames if repo_root in str(frame.filename)]
    if app_frames:
        selected = app_frames[-max_frames:]
        hidden = len(frames) - len(selected)
        return selected, hidden

    return frames[-max_frames:], len(frames) - max_frames


def format_pretty_exception(
    exc: BaseException,
    *,
    task_id: str | None = None,
    enable_colors: bool = True,
) -> str:
    ctx = _collect_error_context(exc)
    if isinstance(ctx.get("url"), str):
        ctx["url"] = _truncate_middle(_redact_url(ctx["url"]))
    if "message" not in ctx:
        ctx["message"] = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__

    category, icon, label = _classify_exception(exc, ctx)
    status = ctx.get("status_code") or ctx.get("status")
    error_code = ctx.get("error_code")

    title_bits = [label]
    if error_code:
        title_bits.append(str(error_code))
    if status:
        title_bits.append(str(status))
    title = " - ".join(title_bits)

    object_label = task_id or "<dag>"
    parts = [f"{icon} [{object_label}] {_colorize(title, RED, enable_colors)}"]

    if "message" in ctx and ctx["message"] not in (None, "", {}):
        parts.append(_colorize(f"↳ message: {ctx['message']}", RED, enable_colors))
    for key in ("method", "url", "request_id", "error_code", "filename", "lineno"):
        if key in ctx and ctx[key] not in (None, "", {}):
            parts.append(_colorize(f"↳ {key}: {ctx[key]}", BLUE, enable_colors))

    if isinstance(ctx.get("body_preview"), str) and ctx["body_preview"].strip():
        body = ctx["body_preview"].strip()
        if len(body) > 800:
            body = body[:800] + "...<trimmed>"
        parts.append(_colorize("≡ body:", DIM, enable_colors))
        parts.append(_indent_block(body, 4))

    tb_exc = traceback.TracebackException.from_exception(exc, capture_locals=False)
    frames, hidden_count = _select_frames(tb_exc)
    if frames:
        parts.append(_colorize("Traceback:", YELLOW, enable_colors))
        if hidden_count > 0:
            parts.append(_colorize(f"  ... {hidden_count} frame(s) hidden ...", DIM, enable_colors))
        for frame in frames:
            parts.append(_format_frame(frame, enable_colors=enable_colors))

    parts.append(
        _colorize(
            f"{exc.__class__.__name__}: {str(exc) or exc.__class__.__name__}",
            RED,
            enable_colors,
        )
    )

    return "\n".join(part for part in parts if part)


def format_pretty_log_record(
    record: logging.LogRecord,
    *,
    object_label: str | None = None,
    enable_colors: bool = True,
) -> str:
    """Render a warning/error log record as a categorized problem block."""
    object_label = object_label or (
        getattr(record, "task_id", None)
        or getattr(record, "dag_id", None)
        or getattr(record, "name", None)
        or "<log>"
    )

    exc = None
    if record.exc_info and len(record.exc_info) >= 2:
        exc = record.exc_info[1]

    if isinstance(exc, BaseException):
        block = format_pretty_exception(exc, task_id=str(object_label), enable_colors=enable_colors)
        lines = block.splitlines()
        insert_at = 1 if lines else 0
        extras: list[str] = []
        if record.name:
            extras.append(_colorize(f"↳ logger: {record.name}", BLUE, enable_colors))
        log_message = record.getMessage()
        if log_message and log_message not in str(exc):
            extras.append(_colorize(f"↳ log: {log_message}", BLUE, enable_colors))
        if extras:
            lines[insert_at:insert_at] = extras
        return "\n".join(lines)

    message = str(record.getMessage() or "").strip()
    lines = [line.rstrip() for line in message.splitlines()]
    headline = next((line.strip() for line in lines if line.strip()), record.levelname.title())
    details = [line for line in lines[1:] if line.strip()]

    category, icon, label = _classify_problem(
        module_name=record.name,
        class_name="",
        message=message,
        status=getattr(record, "status_code", None) or getattr(record, "status", None),
    )

    severity = "Error" if record.levelno >= logging.ERROR else "Warning"
    severity_color = RED if record.levelno >= logging.ERROR else YELLOW
    title = f"{label} {severity}" if label != "Error" else severity

    parts = [f"{icon} [{object_label}] {_colorize(title, severity_color, enable_colors)}"]
    if record.name:
        parts.append(_colorize(f"↳ logger: {record.name}", BLUE, enable_colors))
    parts.append(_colorize(f"↳ message: {headline}", severity_color, enable_colors))
    if getattr(record, "pathname", None):
        location = f"{_shorten_path(record.pathname)}:{getattr(record, 'lineno', '?')}"
        parts.append(_colorize(f"↳ source: {location}", BLUE, enable_colors))
    if details:
        parts.append(_colorize("≡ details:", DIM, enable_colors))
        parts.append(_indent_block("\n".join(details), 4))
    return "\n".join(part for part in parts if part)


@dataclass
class StepTracerOptions:
    """Options controlling how `StepTracer` prints to the console."""

    stream: TextIO = sys.stdout
    enable_colors: bool = True
    inline_event_limit: int = 500
    show_meta: bool = True
    start_icon: Optional[str] = None
    finish_icon: Optional[str] = None
    error_icon: Optional[str] = None


class StepTracer:
    """
    Minimal context manager for live per-task logging.

    It prints:
    - task start
    - structured events like `resolved_kwargs` / `result`
    - pretty categorized error blocks
    - task finish duration
    """

    def __init__(
        self,
        task_id: str,
        operator: str,
        run_id: Optional[str],
        map_index: Optional[int],
        options: Optional[StepTracerOptions] = None,
    ) -> None:
        self.task_id = task_id
        self.operator = operator
        self.run_id = run_id
        self.map_index = map_index
        self.opts = options or StepTracerOptions()
        self._t0_ms: float = 0.0

    def event(self, kind: str, data: Any = "") -> None:
        """Print a structured event line inside the step lifetime."""
        if isinstance(data, str):
            limit = self.opts.inline_event_limit
            text = data if len(data) <= limit else data[:limit] + "...<trimmed>"
        else:
            text = safe_repr(data, limit=self.opts.inline_event_limit)
        self._println(DOT, f"{_colorize(kind, CYAN, self.opts.enable_colors)}: {text}")

    def __enter__(self) -> "StepTracer":
        self._t0_ms = _now_ms()
        start_icon = self.opts.start_icon or START_DEFAULT
        self._println(start_icon, _colorize("start", YELLOW, self.opts.enable_colors))
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        duration = _fmt_dur(_now_ms() - self._t0_ms)
        if exc_type is not None and exc is not None:
            block = format_pretty_exception(
                exc,
                task_id=self.task_id,
                enable_colors=self.opts.enable_colors,
            )
            meta = self._meta_suffix()
            lines = block.splitlines()
            if lines:
                lines[0] = lines[0] + meta
            self.opts.stream.write("\n".join(lines) + "\n")
            self.opts.stream.flush()
            self._println(self.opts.error_icon or ERROR_STYLES["unknown"][0], f"finish {duration}")
            return False

        self._println(self.opts.finish_icon or FINISH_DEFAULT, f"finish {duration}")
        return True

    def _meta_suffix(self) -> str:
        if not self.opts.show_meta:
            return ""
        parts = []
        if self.operator:
            parts.append(f"op={self.operator}")
        if self.map_index not in (None, -1):
            parts.append(f"idx={self.map_index}")
        if self.run_id:
            parts.append(f"run={self.run_id}")
        return f" {_colorize('| ' + ', '.join(parts), DIM, self.opts.enable_colors)}" if parts else ""

    def _println(self, icon: str, msg: str) -> None:
        self.opts.stream.write(f"{icon} [{self.task_id}] {msg}{self._meta_suffix()}\n")
        self.opts.stream.flush()
