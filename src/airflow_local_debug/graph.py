from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import textwrap
from typing import Any

from airflow_local_debug.topology import topological_task_ids as _topological_task_ids

RESET = "\033[0m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
DIM = "\033[2m"
YELLOW = "\033[33m"
GREEN = "\033[32m"


def _colorize(text: str, color: str, enabled: bool) -> str:
    return f"{color}{text}{RESET}" if enabled else text


def _task_group_path(task: Any) -> str | None:
    task_group = getattr(task, "task_group", None)
    if task_group is None:
        return None

    group_id = getattr(task_group, "group_id", None)
    if not group_id:
        return None
    if group_id in {"root", None}:
        return None
    return str(group_id)


def _task_depths(tasks: list[Any]) -> dict[str, int]:
    task_dict = {task.task_id: task for task in tasks}
    depths: dict[str, int] = {}
    for task_id in _topological_task_ids(tasks):
        task = task_dict[task_id]
        upstream_ids = [up_id for up_id in sorted(getattr(task, "upstream_task_ids", set()) or set()) if up_id in task_dict]
        if not upstream_ids:
            depths[task_id] = 0
        else:
            depths[task_id] = max(depths.get(up_id, 0) for up_id in upstream_ids) + 1
    return depths


def _lane_key(task: Any) -> str:
    return _task_group_path(task) or f"__task__:{task.task_id}"


def _lane_title(lane_key: str) -> str:
    if lane_key.startswith("__task__:"):
        return lane_key[len("__task__:") :]
    return lane_key


def _wrap_label(text: str, width: int = 24, max_lines: int = 3) -> list[str]:
    wrapped = textwrap.wrap(text, width=width) or [text]
    if len(wrapped) <= max_lines:
        return wrapped
    trimmed = wrapped[: max_lines - 1]
    tail = wrapped[max_lines - 1]
    trimmed.append((tail[: max(0, width - 3)] + "...") if len(tail) > width else tail)
    return trimmed


def _display_task_label(task: Any, *, max_width: int = 32) -> str:
    task_id = str(getattr(task, "task_id", "<unknown>"))
    group_path = _task_group_path(task)
    if group_path and task_id.startswith(f"{group_path}."):
        task_id = task_id[len(group_path) + 1 :]
    if len(task_id) <= max_width:
        return task_id
    return task_id[: max(0, max_width - 3)] + "..."


def _format_tree_console_graph(
    dag: Any,
    tasks: list[Any],
    task_dict: dict[str, Any],
    *,
    enable_colors: bool,
) -> str:
    lines = [f"✨ DAG Structure: {getattr(dag, 'dag_id', '<unknown>')}", ""]
    downstream_map: dict[str, list[str]] = defaultdict(list)
    upstream_map: dict[str, list[str]] = defaultdict(list)

    for task in tasks:
        task_id = task.task_id
        for child_id in sorted(getattr(task, "downstream_task_ids", set()) or set()):
            if child_id not in task_dict:
                continue
            downstream_map[task_id].append(child_id)
            upstream_map[child_id].append(task_id)

    roots = sorted(
        [task_id for task_id in task_dict if not sorted(getattr(task_dict[task_id], "upstream_task_ids", set()) or set())]
    )
    drawn: set[str] = set()

    def group_label(group_path: str) -> str:
        return _colorize(group_path, MAGENTA, enable_colors)

    def task_label(task_id: str, *, current_group: str | None = None) -> str:
        task = task_dict[task_id]
        group_path = _task_group_path(task)
        task_name = _display_task_label(task) if current_group and current_group == group_path else task.task_id
        return _colorize(task_name, CYAN, enable_colors)

    def render_branch(task_id: str, prefix: str = "", is_last: bool = True, current_group: str | None = None) -> None:
        task = task_dict[task_id]
        group_path = _task_group_path(task)
        connector = "└── " if is_last else "├── "

        if group_path and group_path != current_group:
            lines.append(f"{prefix}{connector}{group_label(group_path)}")
            new_prefix = prefix + ("    " if is_last else "│   ")
            render_branch(task_id, new_prefix, True, current_group=group_path)
            return

        if task_id in drawn:
            repeat = _colorize("[...]", GREEN, enable_colors)
            lines.append(f"{prefix}{connector}{task_label(task_id, current_group=current_group)} {repeat}")
            return

        lines.append(f"{prefix}{connector}{task_label(task_id, current_group=current_group)}")
        drawn.add(task_id)

        children = sorted(downstream_map.get(task_id, []))
        new_prefix = prefix + ("    " if is_last else "│   ")
        for index, child_id in enumerate(children):
            render_branch(child_id, new_prefix, index == len(children) - 1, current_group=current_group)

    if not roots:
        roots = _topological_task_ids(tasks)[:1]

    if len(roots) == 1:
        root_id = roots[0]
        root_group = _task_group_path(task_dict[root_id])
        if root_group:
            lines.append(f"🚀 {group_label(root_group)}")
            render_branch(root_id, "", True, current_group=root_group)
        else:
            lines.append(f"🚀 {task_label(root_id)}")
            drawn.add(root_id)
            children = sorted(downstream_map.get(root_id, []))
            for index, child_id in enumerate(children):
                render_branch(child_id, "", index == len(children) - 1)
        return "\n".join(lines)

    root_labels = " + ".join(_colorize(root_id, CYAN, enable_colors) for root_id in roots)
    lines.append(f"🚀 {_colorize('ROOTS', YELLOW, enable_colors)}: {root_labels}")

    first_layer = sorted(
        {
            child_id
            for root_id in roots
            for child_id in downstream_map.get(root_id, [])
            if child_id in task_dict and set(upstream_map.get(child_id, [])) <= set(roots)
        }
    )

    if first_layer:
        for index, task_id in enumerate(first_layer):
            render_branch(task_id, "", index == len(first_layer) - 1)
    else:
        for index, root_id in enumerate(roots):
            render_branch(root_id, "", index == len(roots) - 1)

    leftovers = [task_id for task_id in _topological_task_ids(tasks) if task_id not in drawn and task_id not in roots]
    if leftovers:
        lines.append("")
        lines.append(_colorize("Detached", YELLOW, enable_colors))
        for task_id in leftovers:
            render_branch(task_id, "", True)

    return "\n".join(lines)


def format_dag_graph(dag: Any, *, enable_colors: bool = True) -> str:
    tasks = list(getattr(dag, "task_dict", {}).values()) or list(getattr(dag, "tasks", []) or [])
    if not tasks:
        return f"✨ DAG Structure: {getattr(dag, 'dag_id', '<unknown>')}\n\n<empty>"

    task_dict = {task.task_id: task for task in tasks}

    return _format_tree_console_graph(
        dag,
        tasks,
        task_dict,
        enable_colors=enable_colors,
    )


def print_dag_graph(dag: Any, *, enable_colors: bool = True) -> None:
    print(format_dag_graph(dag, enable_colors=enable_colors))


SVG_MAX_TASKS = 200


def render_dag_svg(dag: Any) -> str:
    tasks = list(getattr(dag, "task_dict", {}).values()) or list(getattr(dag, "tasks", []) or [])
    if not tasks:
        dag_id = escape(str(getattr(dag, "dag_id", "<unknown>")))
        return (
            '<svg xmlns="http://www.w3.org/2000/svg" width="600" height="120">'
            '<rect width="100%" height="100%" fill="#f8fafc"/>'
            f'<text x="24" y="40" font-size="20" font-family="Helvetica, Arial, sans-serif" fill="#0f172a">{dag_id}</text>'
            '<text x="24" y="76" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#475569">&lt;empty dag&gt;</text>'
            "</svg>"
        )

    if len(tasks) > SVG_MAX_TASKS:
        dag_id = escape(str(getattr(dag, "dag_id", "<unknown>")))
        return (
            '<svg xmlns="http://www.w3.org/2000/svg" width="720" height="160">'
            '<rect width="100%" height="100%" fill="#f8fafc"/>'
            f'<text x="24" y="44" font-size="20" font-weight="700" font-family="Helvetica, Arial, sans-serif" fill="#0f172a">{dag_id}</text>'
            f'<text x="24" y="84" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#475569">'
            f'DAG has {len(tasks)} tasks; SVG rendering disabled above {SVG_MAX_TASKS}.'
            '</text>'
            '<text x="24" y="116" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#64748b">'
            'Use the ASCII graph (print_dag_graph) for an overview of large DAGs.'
            '</text>'
            "</svg>"
        )

    topo_order = _topological_task_ids(tasks)
    depths = _task_depths(tasks)
    task_dict = {task.task_id: task for task in tasks}

    lane_keys: list[str] = []
    for task_id in topo_order:
        lane = _lane_key(task_dict[task_id])
        if lane not in lane_keys:
            lane_keys.append(lane)
    lane_index = {lane: idx for idx, lane in enumerate(lane_keys)}

    box_w = 240
    box_h = 72
    col_gap = 90
    lane_gap = 56
    left_pad = 160
    top_pad = 90
    row_step = box_h + lane_gap

    positions: dict[str, tuple[int, int]] = {}
    for task_id in topo_order:
        task = task_dict[task_id]
        lane = _lane_key(task)
        x = left_pad + depths.get(task_id, 0) * (box_w + col_gap)
        y = top_pad + lane_index[lane] * row_step
        positions[task_id] = (x, y)

    max_depth = max(depths.values()) if depths else 0
    width = left_pad + (max_depth + 1) * (box_w + col_gap) + 180
    height = top_pad + len(lane_keys) * row_step + 120

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        """
<defs>
  <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
    <polygon points="0 0, 10 3.5, 0 7" fill="#64748b" />
  </marker>
  <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
    <feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#94a3b8" flood-opacity="0.25"/>
  </filter>
</defs>
        """.strip(),
        f'<rect width="{width}" height="{height}" fill="#f8fafc"/>',
        f'<text x="32" y="42" font-size="22" font-weight="700" font-family="Helvetica, Arial, sans-serif" fill="#0f172a">{escape(str(getattr(dag, "dag_id", "<unknown>")))}</text>',
        '<text x="32" y="66" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#475569">Local DAG graph preview</text>',
    ]

    real_groups = [lane for lane in lane_keys if not lane.startswith("__task__:")]
    for group_id in real_groups:
        group_task_ids = [task_id for task_id in topo_order if _task_group_path(task_dict[task_id]) == group_id]
        if not group_task_ids:
            continue
        xs = [positions[task_id][0] for task_id in group_task_ids]
        ys = [positions[task_id][1] for task_id in group_task_ids]
        box_x = min(xs) - 24
        box_y = min(ys) - 32
        box_width = max(xs) - min(xs) + box_w + 48
        box_height = max(ys) - min(ys) + box_h + 56
        parts.append(
            f'<rect x="{box_x}" y="{box_y}" width="{box_width}" height="{box_height}" '
            'rx="18" ry="18" fill="#eef2ff" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="6 4"/>'
        )
        parts.append(
            f'<text x="{box_x + 16}" y="{box_y + 24}" font-size="14" font-weight="700" '
            f'font-family="Helvetica, Arial, sans-serif" fill="#334155">{escape(group_id)}</text>'
        )

    for lane in lane_keys:
        if lane.startswith("__task__:"):
            label = _lane_title(lane)
            lane_y = top_pad + lane_index[lane] * row_step + box_h / 2 + 6
            parts.append(
                f'<text x="32" y="{lane_y}" font-size="13" font-weight="600" '
                f'font-family="Helvetica, Arial, sans-serif" fill="#475569">{escape(label)}</text>'
            )

    for task_id in topo_order:
        task = task_dict[task_id]
        downstream_ids = sorted(getattr(task, "downstream_task_ids", set()) or set())
        for child_id in downstream_ids:
            if child_id not in positions:
                continue
            x1, y1 = positions[task_id]
            x2, y2 = positions[child_id]
            start_x = x1 + box_w
            start_y = y1 + box_h / 2
            end_x = x2
            end_y = y2 + box_h / 2
            dx = max(40, (end_x - start_x) / 2)
            parts.append(
                f'<path d="M {start_x} {start_y} C {start_x + dx} {start_y}, {end_x - dx} {end_y}, {end_x} {end_y}" '
                'fill="none" stroke="#64748b" stroke-width="2.2" marker-end="url(#arrowhead)"/>'
            )

    for task_id in topo_order:
        task = task_dict[task_id]
        x, y = positions[task_id]
        group_id = _task_group_path(task)
        fill = "#ffffff" if group_id is None else "#fefce8"
        stroke = "#cbd5e1" if group_id is None else "#f59e0b"
        parts.append(
            f'<rect x="{x}" y="{y}" width="{box_w}" height="{box_h}" rx="14" ry="14" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="2" filter="url(#shadow)"/>'
        )

        title_lines = _wrap_label(task.task_id, width=24, max_lines=2)
        for line_index, line in enumerate(title_lines):
            parts.append(
                f'<text x="{x + 16}" y="{y + 24 + line_index * 17}" font-size="14" font-weight="700" '
                f'font-family="Helvetica, Arial, sans-serif" fill="#0f172a">{escape(line)}</text>'
            )

        operator_name = getattr(task, "task_type", None) or task.__class__.__name__
        parts.append(
            f'<text x="{x + 16}" y="{y + 58}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#475569">'
            f'{escape(f"[{operator_name}]")}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def write_dag_svg(dag: Any, output_path: str | None = None) -> str:
    dag_id = str(getattr(dag, "dag_id", "dag"))
    safe_dag_id = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in dag_id)
    if output_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = str(Path("/tmp/airflow_debug_graphs") / f"{safe_dag_id}_{timestamp}.svg")

    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_dag_svg(dag), encoding="utf-8")
    return str(path)
