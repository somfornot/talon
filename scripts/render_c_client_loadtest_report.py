#!/usr/bin/env python3
"""Render a self-contained Chinese HTML report for the native C client benchmark."""

from __future__ import annotations

import argparse
import html
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Union


SIZE_LABELS = {
    4096: "4 KiB",
    65536: "64 KiB",
    1048576: "1 MiB",
}
SCENARIO_ORDER = [
    "hot-direct",
    "latency-0ms",
    "latency-1ms",
    "latency-5ms",
    "latency-10ms",
]
SCENARIO_LABELS = {
    "hot-direct": "直连热 L2",
    "latency-0ms": "代理 0 ms",
    "latency-1ms": "代理 +1 ms",
    "latency-5ms": "代理 +5 ms",
    "latency-10ms": "代理 +10 ms",
}
COLORS = ["#2563eb", "#059669", "#d97706", "#7c3aed", "#dc2626"]


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_number}: invalid JSON: {error}") from error
        if isinstance(value, dict):
            rows.append(value)
    return rows


def median(rows: list[dict[str, Any]], field: str) -> float:
    return float(statistics.median(float(row.get(field, 0)) for row in rows))


def aggregate(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, int, int], dict[str, Any]]:
    summaries: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    stacks: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        scenario = str(row.get("scenario", ""))
        if scenario not in SCENARIO_ORDER:
            continue
        if row.get("type") == "summary":
            key = (
                scenario,
                int(row["request_bytes"]),
                int(row["requested_concurrency"]),
            )
            summaries[key].append(row)
        elif row.get("type") == "stack_metrics":
            key = (
                scenario,
                int(row["request_bytes"]),
                int(row["concurrency"]),
            )
            stacks[key].append(row)

    output: dict[tuple[str, int, int], dict[str, Any]] = {}
    for key, samples in summaries.items():
        stack = stacks.get(key, [])
        output[key] = {
            "scenario": key[0],
            "request_bytes": key[1],
            "concurrency": key[2],
            "rounds": len(samples),
            "qps": median(samples, "success_qps"),
            "attempt_qps": median(samples, "attempt_qps"),
            "mibps": median(samples, "mib_per_second"),
            "p50_ms": median(samples, "p50_ms"),
            "p95_ms": median(samples, "p95_ms"),
            "worst_p99_ms": max(float(row["p99_ms"]) for row in samples),
            "worst_p999_ms": max(float(row["p999_ms"]) for row in samples),
            "max_ms": max(float(row["max_ms"]) for row in samples),
            "logical_errors": sum(int(row["logical_errors"]) for row in samples),
            "submission_errors": sum(int(row["submission_errors"]) for row in samples),
            "dropped_samples": sum(int(row["dropped_samples"]) for row in samples),
            "single_block": all(
                row.get("all_requests_below_block_size") is True
                and row.get("all_requests_single_block") is True
                for row in samples
            ),
            "actual_concurrency": max(int(row["actual_concurrency"]) for row in samples),
            "worker_cpu": median(stack, "worker_cpu_cores") if stack else 0.0,
            "worker_rss_kib": max(
                (int(row["worker_rss_kib"]) for row in stack), default=0
            ),
            "l2_misses": sum(int(row["l2_cache_misses"]) for row in stack),
            "backend_fetches": sum(int(row["backend_fetches"]) for row in stack),
        }
    return output


def peak_and_knee(
    aggregates: dict[tuple[str, int, int], dict[str, Any]],
    scenario: str,
    request_bytes: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    points = sorted(
        (
            value
            for key, value in aggregates.items()
            if key[0] == scenario and key[1] == request_bytes
        ),
        key=lambda value: value["concurrency"],
    )
    if not points:
        raise SystemExit(f"missing data for {scenario}, request_bytes={request_bytes}")
    peak = max(points, key=lambda value: value["qps"])
    threshold = peak["qps"] * 0.95
    knee = next(point for point in points if point["qps"] >= threshold)
    return peak, knee


def fmt_int(value: Union[float, int]) -> str:
    return f"{value:,.0f}"


def fmt_ms(value: float) -> str:
    if value < 1:
        return f"{value:.3f} ms"
    return f"{value:.2f} ms"


def td(value: str, class_name: str = "") -> str:
    class_attr = f' class="{class_name}"' if class_name else ""
    return f"<td{class_attr}>{value}</td>"


def tr(cells: list[str], class_name: str = "") -> str:
    class_attr = f' class="{class_name}"' if class_name else ""
    return f"<tr{class_attr}>{''.join(cells)}</tr>"


def table(headers: list[str], rows: list[str], class_name: str = "") -> str:
    classes = f"data-table {class_name}".strip()
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    return (
        f'<div class="table-scroll"><table class="{classes}">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def line_chart(
    series: list[dict[str, Any]],
    x_values: list[int],
    x_label: str,
    title: str,
    log_y: bool = False,
) -> str:
    width, height = 980, 370
    left, right, top, bottom = 78, 24, 34, 62
    plot_w = width - left - right
    plot_h = height - top - bottom
    values = [
        float(point["y"])
        for item in series
        for point in item["points"]
        if float(point["y"]) > 0
    ]
    max_y = max(values)
    min_y = min(values)

    if log_y:
        log_min = math.floor(math.log10(min_y))
        log_max = math.ceil(math.log10(max_y))
        if log_min == log_max:
            log_max += 1

        def y_pos(value: float) -> float:
            ratio = (math.log10(max(value, 10**log_min)) - log_min) / (
                log_max - log_min
            )
            return top + plot_h * (1 - ratio)

        y_ticks = [10**power for power in range(log_min, log_max + 1)]
    else:
        ceiling = max_y * 1.08

        def y_pos(value: float) -> float:
            return top + plot_h * (1 - value / ceiling)

        y_ticks = [ceiling * index / 4 for index in range(5)]

    def x_pos(value: int) -> float:
        if len(x_values) == 1:
            return left + plot_w / 2
        return left + plot_w * x_values.index(value) / (len(x_values) - 1)

    parts = [
        f'<figure class="chart"><figcaption>{html.escape(title)}</figcaption>',
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="{html.escape(title)}">',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" '
        'fill="#ffffff" rx="8"/>',
    ]
    for tick in y_ticks:
        y = y_pos(float(tick))
        label = (
            f"{tick / 1000:g}K"
            if tick >= 1000 and tick < 1_000_000
            else f"{tick:,.0f}"
        )
        parts.append(
            f'<line x1="{left}" x2="{width-right}" y1="{y:.1f}" y2="{y:.1f}" '
            'stroke="#dbe3ef" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left-10}" y="{y+4:.1f}" text-anchor="end" '
            f'class="axis-label">{html.escape(label)}</text>'
        )
    for value in x_values:
        x = x_pos(value)
        parts.append(
            f'<text x="{x:.1f}" y="{height-bottom+25}" text-anchor="middle" '
            f'class="axis-label">{value}</text>'
        )
    parts.append(
        f'<text x="{left+plot_w/2:.1f}" y="{height-10}" text-anchor="middle" '
        f'class="axis-title">{html.escape(x_label)}</text>'
    )
    parts.append(
        f'<text x="17" y="{top+plot_h/2:.1f}" text-anchor="middle" '
        f'transform="rotate(-90 17 {top+plot_h/2:.1f})" '
        'class="axis-title">成功 QPS</text>'
    )

    legend_x = left
    for index, item in enumerate(series):
        color = COLORS[index % len(COLORS)]
        legend_item_x = legend_x + index * 180
        parts.append(
            f'<line x1="{legend_item_x}" x2="{legend_item_x+22}" y1="17" y2="17" '
            f'stroke="{color}" stroke-width="4"/>'
        )
        parts.append(
            f'<text x="{legend_item_x+29}" y="21" class="legend-label">'
            f'{html.escape(item["name"])}</text>'
        )
        points = sorted(item["points"], key=lambda point: x_values.index(point["x"]))
        coords = " ".join(
            f'{x_pos(int(point["x"])):.1f},{y_pos(float(point["y"])):.1f}'
            for point in points
        )
        parts.append(
            f'<polyline points="{coords}" fill="none" stroke="{color}" '
            'stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>'
        )
        for point in points:
            x = x_pos(int(point["x"]))
            y = y_pos(float(point["y"]))
            tooltip = (
                f'{item["name"]}；{x_label} {point["x"]}；'
                f'成功 QPS {fmt_int(float(point["y"]))}'
            )
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}">'
                f"<title>{html.escape(tooltip)}</title></circle>"
            )
    parts.extend(["</svg>", "</figure>"])
    return "".join(parts)


def membership_chart(buckets: list[dict[str, Any]]) -> str:
    series = [
        {
            "name": "成功 QPS",
            "points": [
                {"x": index + 1, "y": float(bucket["success_qps"])}
                for index, bucket in enumerate(buckets)
            ],
        },
        {
            "name": "逻辑错误/秒",
            "points": [
                {"x": index + 1, "y": float(bucket["logical_errors"])}
                for index, bucket in enumerate(buckets)
            ],
        },
    ]
    return line_chart(
        series,
        list(range(1, len(buckets) + 1)),
        "测量时间桶（每桶 1 秒）",
        "Membership 故障前、故障窗口与恢复后的每秒结果",
    )


def render(hot_path: Path, membership_path: Path) -> str:
    rows = load_jsonl(hot_path)
    membership_rows = load_jsonl(membership_path)
    environment = next(row for row in rows if row.get("type") == "environment")
    aggregates = aggregate(rows)

    relevant_summaries = [
        row
        for row in rows
        if row.get("type") == "summary" and row.get("scenario") in SCENARIO_ORDER
    ]
    relevant_stacks = [
        row
        for row in rows
        if row.get("type") == "stack_metrics"
        and row.get("scenario") in SCENARIO_ORDER
    ]
    expected_rows = 5 * (8 + 8 + 6) * 3
    excluded_partial_summaries = sum(
        row.get("type") == "summary"
        and row.get("scenario") not in SCENARIO_ORDER
        for row in rows
    )
    nominal_hot_latency_seconds = expected_rows * (
        int(environment["warmup_seconds"]) + int(environment["measure_seconds"])
    )
    integrity = {
        "summaries": len(relevant_summaries),
        "stacks": len(relevant_stacks),
        "expected": expected_rows,
        "logical_errors": sum(int(row["logical_errors"]) for row in relevant_summaries),
        "submission_errors": sum(
            int(row["submission_errors"]) for row in relevant_summaries
        ),
        "dropped_samples": sum(
            int(row["dropped_samples"]) for row in relevant_summaries
        ),
        "bad_blocks": sum(
            not (
                row.get("all_requests_below_block_size") is True
                and row.get("all_requests_single_block") is True
            )
            for row in relevant_summaries
        ),
        "bad_concurrency": sum(
            int(row["actual_concurrency"]) != int(row["requested_concurrency"])
            for row in relevant_summaries
        ),
    }

    latency_stacks = [
        row for row in relevant_stacks if str(row["scenario"]).startswith("latency-")
    ]
    proxy_counter_mismatches = sum(
        int(row["proxy_attempts"]) != int(row["logical_attempts"])
        or int(row["worker_attempts"]) != int(row["proxy_forwarded"])
        for row in latency_stacks
    )

    direct_summary_rows: list[str] = []
    direct_cards: list[str] = []
    hot_chart_series: list[dict[str, Any]] = []
    for request_bytes in SIZE_LABELS:
        peak, knee = peak_and_knee(aggregates, "hot-direct", request_bytes)
        direct_summary_rows.append(
            tr(
                [
                    td(SIZE_LABELS[request_bytes]),
                    td(fmt_int(knee["concurrency"]), "number"),
                    td(fmt_int(knee["qps"]), "number strong"),
                    td(
                        f'{fmt_int(peak["qps"])} @ concurrency '
                        f'{fmt_int(peak["concurrency"])}',
                        "number",
                    ),
                    td(fmt_int(peak["mibps"]), "number"),
                    td(fmt_ms(knee["worst_p99_ms"]), "number"),
                    td(fmt_ms(knee["worst_p999_ms"]), "number"),
                    td(f'{knee["worker_cpu"]:.2f} cores', "number"),
                    td(f'{knee["worker_rss_kib"] / 1024:.1f} MiB', "number"),
                ]
            )
        )
        direct_cards.append(
            '<article class="metric-card">'
            f'<div class="metric-label">{SIZE_LABELS[request_bytes]} 峰值中位 QPS</div>'
            f'<div class="metric-value">{fmt_int(peak["qps"])}</div>'
            f'<div class="metric-note">95% 拐点 concurrency={knee["concurrency"]}</div>'
            "</article>"
        )
        points = [
            value
            for key, value in sorted(
                aggregates.items(), key=lambda item: item[0][2]
            )
            if key[0] == "hot-direct" and key[1] == request_bytes
        ]
        hot_chart_series.append(
            {
                "name": SIZE_LABELS[request_bytes],
                "points": [
                    {"x": point["concurrency"], "y": point["qps"]}
                    for point in points
                ],
            }
        )

    direct_sweep_rows: list[str] = []
    all_concurrencies = [1, 8, 32, 64, 128, 256, 512, 1024]
    for concurrency in all_concurrencies:
        cells = [td(fmt_int(concurrency), "number")]
        for request_bytes in SIZE_LABELS:
            point = aggregates.get(("hot-direct", request_bytes, concurrency))
            if point is None:
                cells.append(td("—", "muted number"))
            else:
                cells.append(
                    td(
                        f'<strong>{fmt_int(point["qps"])}</strong>'
                        f'<small>p99 {fmt_ms(point["worst_p99_ms"])}</small>',
                        "number",
                    )
                )
        direct_sweep_rows.append(tr(cells))

    latency_rows: list[str] = []
    latency_chart_series: list[dict[str, Any]] = []
    for request_bytes in SIZE_LABELS:
        points: list[dict[str, Any]] = []
        for delay, scenario in [
            (0, "latency-0ms"),
            (1, "latency-1ms"),
            (5, "latency-5ms"),
            (10, "latency-10ms"),
        ]:
            peak, knee = peak_and_knee(aggregates, scenario, request_bytes)
            latency_rows.append(
                tr(
                    [
                        td(f"{delay} ms", "number"),
                        td(SIZE_LABELS[request_bytes]),
                        td(fmt_int(knee["concurrency"]), "number"),
                        td(fmt_int(knee["qps"]), "number strong"),
                        td(
                            f'{fmt_int(peak["qps"])} @ {peak["concurrency"]}',
                            "number",
                        ),
                        td(fmt_ms(knee["p50_ms"]), "number"),
                        td(fmt_ms(knee["worst_p99_ms"]), "number"),
                        td(fmt_ms(knee["worst_p999_ms"]), "number"),
                    ]
                )
            )
            points.append({"x": delay, "y": peak["qps"]})
        latency_chart_series.append(
            {"name": SIZE_LABELS[request_bytes], "points": points}
        )

    proxy_overhead_rows: list[str] = []
    for request_bytes in SIZE_LABELS:
        direct_peak, _ = peak_and_knee(aggregates, "hot-direct", request_bytes)
        proxy_peak, _ = peak_and_knee(aggregates, "latency-0ms", request_bytes)
        delta = (proxy_peak["qps"] / direct_peak["qps"] - 1) * 100
        proxy_overhead_rows.append(
            tr(
                [
                    td(SIZE_LABELS[request_bytes]),
                    td(fmt_int(direct_peak["qps"]), "number"),
                    td(fmt_int(proxy_peak["qps"]), "number"),
                    td(f"{delta:.1f}%", "number danger-text"),
                ]
            )
        )

    outlier_rows: list[str] = []
    for scenario in SCENARIO_ORDER:
        selected = [
            row for row in relevant_summaries if row["scenario"] == scenario
        ]
        over_one_second = sum(float(row["max_ms"]) >= 1000 for row in selected)
        worst = max(selected, key=lambda row: float(row["max_ms"]))
        outlier_rows.append(
            tr(
                [
                    td(SCENARIO_LABELS[scenario]),
                    td(f"{over_one_second} / {len(selected)}", "number"),
                    td(fmt_ms(float(worst["max_ms"])), "number danger-text"),
                    td(SIZE_LABELS[int(worst["request_bytes"])]),
                    td(fmt_int(int(worst["requested_concurrency"])), "number"),
                    td(fmt_ms(float(worst["p999_ms"])), "number"),
                ]
            )
        )

    complete_aggregate_rows: list[str] = []
    for scenario in SCENARIO_ORDER:
        for request_bytes in SIZE_LABELS:
            points = sorted(
                (
                    value
                    for key, value in aggregates.items()
                    if key[0] == scenario and key[1] == request_bytes
                ),
                key=lambda value: value["concurrency"],
            )
            for point in points:
                complete_aggregate_rows.append(
                    tr(
                        [
                            td(SCENARIO_LABELS[scenario]),
                            td(SIZE_LABELS[request_bytes]),
                            td(fmt_int(point["concurrency"]), "number"),
                            td(fmt_int(point["rounds"]), "number"),
                            td(fmt_int(point["qps"]), "number strong"),
                            td(fmt_int(point["attempt_qps"]), "number"),
                            td(fmt_int(point["mibps"]), "number"),
                            td(fmt_ms(point["p50_ms"]), "number"),
                            td(fmt_ms(point["p95_ms"]), "number"),
                            td(fmt_ms(point["worst_p99_ms"]), "number"),
                            td(fmt_ms(point["worst_p999_ms"]), "number"),
                            td(fmt_ms(point["max_ms"]), "number danger-text"),
                            td(f'{point["worker_cpu"]:.2f}', "number"),
                            td(f'{point["worker_rss_kib"] / 1024:.1f}', "number"),
                            td(fmt_int(point["l2_misses"]), "number"),
                            td(fmt_int(point["backend_fetches"]), "number"),
                            td(
                                f'{point["submission_errors"]} / '
                                f'{point["logical_errors"]} / '
                                f'{point["dropped_samples"]}',
                                "number",
                            ),
                            td(fmt_int(point["actual_concurrency"]), "number"),
                            td("是" if point["single_block"] else "否"),
                        ],
                        "failure-row" if point["max_ms"] >= 1000 else "",
                    )
                )

    membership_event = next(
        row for row in membership_rows if row.get("type") == "membership_event"
    )
    membership_summary = next(
        row
        for row in membership_rows
        if row.get("type") == "summary" and row.get("scenario") == "membership"
    )
    buckets = sorted(
        (
            row
            for row in membership_rows
            if row.get("type") == "bucket" and row.get("scenario") == "membership"
        ),
        key=lambda row: int(row["begin_ms"]),
    )
    kill_at = int(membership_event["kill_after_measure_start_ms"])
    recovery_at = kill_at + int(
        membership_event["survivor_first_request_after_kill_ms"]
    )
    membership_table_rows: list[str] = []
    phase_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, bucket in enumerate(buckets, 1):
        begin = int(bucket["begin_ms"])
        end = int(bucket["end_ms"])
        if end <= kill_at:
            phase = "故障前"
            phase_key = "before"
        elif begin < recovery_at:
            phase = "故障窗口"
            phase_key = "failure"
        else:
            phase = "恢复后"
            phase_key = "after"
        phase_buckets[phase_key].append(bucket)
        membership_table_rows.append(
            tr(
                [
                    td(str(index), "number"),
                    td(f"{begin / 1000:.0f}–{end / 1000:.0f} s", "number"),
                    td(phase),
                    td(fmt_int(float(bucket["success_qps"])), "number strong"),
                    td(fmt_int(int(bucket["logical_errors"])), "number"),
                ],
                "failure-row" if phase_key == "failure" else "",
            )
        )

    def phase_stats(key: str) -> tuple[float, int]:
        selected = phase_buckets[key]
        return (
            float(statistics.median(float(row["success_qps"]) for row in selected)),
            sum(int(row["logical_errors"]) for row in selected),
        )

    before_qps, before_errors = phase_stats("before")
    failure_qps, failure_errors = phase_stats("failure")
    after_qps, after_errors = phase_stats("after")
    warmup_errors = int(membership_summary["total_logical_errors"]) - int(
        membership_summary["logical_errors"]
    )

    parameter_rows = [
        (
            "git_commit",
            str(environment["git_commit"]),
            "被测代码基线。git_dirty=true 表示同时包含尚未提交的本地改动。",
        ),
        (
            "block_size",
            "256 MiB",
            "Talon 对对象进行逻辑切块的单位。它不是 L2 page，也不是单次请求大小。",
        ),
        (
            "object_size",
            "64 MiB",
            "测试对象总大小。对象只占 block #0 的前 64 MiB；它不会为了测试而制造现实中罕见的超大跨块请求。",
        ),
        (
            "object_count",
            "1",
            "hot、latency 与 membership 都只随机读取同一个 64 MiB 对象；已停止的 backend 矩阵不在报告中。",
        ),
        (
            "request_bytes",
            "4 KiB / 64 KiB / 1 MiB",
            "每次 talon_read_async 要读取的逻辑数据量，与 block_size 独立。",
        ),
        (
            "offset 生成",
            "对象范围内伪随机；向下 4 KiB 对齐",
            "每个闭环 slot 有独立 RNG。offset 保证 offset + request_bytes 不超过对象结尾；1 MiB 请求不保证按 L2 page 对齐。",
        ),
        (
            "l1_capacity_bytes",
            "0",
            "关闭 worker 进程内的 L1 数据缓存；结果不是 L1 命中性能。",
        ),
        (
            "l2_page_size_bytes",
            "1 MiB",
            "L2 以 1 MiB page 管理数据。随机且仅 4 KiB 对齐的 1 MiB 请求通常跨两个 L2 pages。",
        ),
        (
            "l2_capacity_bytes",
            "256 MiB",
            "Worker 对本地 L2 文件实施淘汰时使用的驻留字节上限，等于 256 个 1 MiB pages；不是进程 RSS，也不限制 Linux page cache。",
        ),
        (
            "L2 热缓存",
            "64 pages 已填充",
            "每个场景只在首轮 warmup 观察到 64 次 miss/fetch；容量足以保留整个 64 MiB 对象。测量的是热 paged-L2 文件及其内核页缓存路径，不是冷 NVMe/backend。",
        ),
        (
            "backend_delay_ms / backend_jitter_ms",
            "0 / 0",
            "hot、latency 与 membership 的 origin fetch 没有人为 backend 延迟或抖动；延迟实验只在 worker 响应返回途中由代理增加固定 sleep。",
        ),
        (
            "worker paged_miss_run_concurrency",
            "8（默认值）",
            "一个 worker 读取发生 L2 miss 时，最多并行拉取 8 个独立缺页区间。热缓存正式测量阶段没有缺页，它不等于 client concurrency，也不决定这里的热读 QPS。",
        ),
        (
            "version / size",
            "0x8LOADTEST / 64 MiB",
            "启动前 stat 一次，之后每次读取传入 exact version 与 size，避免每次 read 再做 metadata RPC，并禁止静默读取新版本。",
        ),
        (
            "concurrency",
            "1–1024",
            "闭环维持的独立 talon_read_async 数量。一个 callback 完成后，同一 slot 立即提交下一次读。",
        ),
        (
            "每个 slot 的 buffer",
            "独立且大小为 request_bytes",
            "并发 256 × 1 MiB 至少需要约 256 MiB client 数据 buffer；因此 1 MiB 组没有继续测 512/1024 并发。",
        ),
        (
            "actual_concurrency",
            "等于 requested_concurrency",
            "测量期间实际观察到的最大活跃逻辑请求数；所有结果均达到配置值。",
        ),
        (
            "单读内部窗口",
            "8 block requests",
            "只保护罕见的跨 block 巨大读取。本报告每个逻辑请求只涉及一个 block，因此它不限制这里的 QPS。",
        ),
        (
            "aggregate limit",
            "1024 worker requests",
            "一个 Rust Client 及其 clones 共享的 worker 请求总额度。它约束跨大量独立读取的资源占用。",
        ),
        (
            "单 block 校验",
            "每次提交前强制检查",
            "检查 request_bytes < 256 MiB，并验证 floor(offset/block_size) == floor((offset+len-1)/block_size)；不满足就停止该 slot，而不是把请求算进结果。",
        ),
        (
            "warmup_seconds",
            "3 s",
            "正式计数前先运行三秒，用于建立连接、membership 与 L2 热状态。",
        ),
        (
            "measure_seconds",
            "10 s",
            "hot/latency 每一轮的计数窗口；membership 使用 12 s 以覆盖 kill 与恢复。",
        ),
        (
            "repeats",
            "3",
            "hot/latency 每个参数点重复三轮，QPS 报告三轮中位数，p99 报告三轮最差值。",
        ),
        (
            "max_samples",
            "5,000,000 / 轮",
            "每轮最多保存五百万条完成样本用于百分位。这里 dropped_samples 全部为 0，所以延迟分布没有被采样上限截断。",
        ),
        (
            "normal membership timing",
            "heartbeat 250 ms / unhealthy 1000 ms / lease 3000 ms",
            "hot 与 latency 的 coordinator/worker 常规本地测试时序；只影响 membership 维护，不是读请求 timeout。",
        ),
        (
            "worker_rust_log",
            "warn",
            "关闭每次 cache hit 的 INFO 日志，避免日志写盘主导压测；WARN/ERROR 仍保留。",
        ),
        (
            "worker data plane",
            "默认选择；未设置 FORCE_TOKIO",
            "编排没有强制 Tokio fallback。原始结果没有记录最终选择，因此报告不把这一点进一步推断成已证明的 io_uring 结果。",
        ),
        (
            "success_qps",
            "成功逻辑读数 / 秒",
            "在测量提交窗口内发起并最终成功完成的 talon_read_async 数量除以窗口秒数。",
        ),
        (
            "attempt_qps",
            "全部逻辑尝试数 / 秒",
            "成功和最终失败的逻辑调用都计入；没有错误时等于 success_qps。",
        ),
        (
            "MiB/s",
            "成功字节数 / 秒",
            "逻辑响应数据吞吐，不等于后端对象存储或物理 NVMe 吞吐。",
        ),
        (
            "p50 / p95 / p99 / p999",
            "延迟百分位",
            "例如 p99=0.1 ms 表示 99% 的成功请求不慢于 0.1 ms；它不描述最慢的那几个请求。",
        ),
        (
            "max",
            "单轮最慢成功请求",
            "对极少数异常最敏感。本次存在秒级 max，因此不能仅依据 p99 宣称尾延迟完全健康。",
        ),
        (
            "95% 拐点",
            "达到峰值中位 QPS 95% 的最小 concurrency",
            "用来寻找“继续加并发收益已经很小”的位置，不是协议或代码中的硬限制。",
        ),
        (
            "logical attempt",
            "一次 talon_read_async 调用",
            "loadgen 视角的业务请求；total_attempts 还包含 warmup，而 attempts 只包含测量窗口。",
        ),
        (
            "proxy_attempt",
            "代理观察到的一帧 GetVersionedRange",
            "不含启动 stat；若 client 重试，同一个逻辑请求会产生额外 proxy attempts。",
        ),
        (
            "worker_attempt",
            "真实 worker 收到的一次读请求",
            "代理注入的失败不会到达 worker；worker_requests_including_stat 则保留启动 stat。",
        ),
        (
            "proxy 0 ms",
            "转发但不主动 sleep",
            "用于测量代理自身的解析、复制和调度开销，绝不能当作直连 worker 上限。",
        ),
        (
            "proxy delay_ms",
            "0 / 1 / 5 / 10 ms",
            "每次从真实 worker 收到完整响应后、向 C SDK 写回响应前 sleep 的固定时间；它不是 backend 延迟，也不模拟网络带宽限制。",
        ),
        (
            "membership heartbeat",
            "worker/coordinator 100 ms",
            "仅故障演练使用；unhealthy=500 ms、lease=1500 ms，决定 owner 何时从 membership 中被摘除。",
        ),
    ]
    parameter_table_rows = [
        tr([td(html.escape(name)), td(html.escape(value)), td(html.escape(meaning))])
        for name, value, meaning in parameter_rows
    ]

    matrix_rows = [
        tr(
            [
                td("hot-direct"),
                td("C SDK → coordinator → 真实 worker"),
                td("4 KiB / 64 KiB"),
                td("1, 8, 32, 64, 128, 256, 512, 1024", "number"),
                td("8 × 2 × 3 = 48", "number"),
            ]
        ),
        tr(
            [
                td("hot-direct"),
                td("C SDK → coordinator → 真实 worker"),
                td("1 MiB"),
                td("1, 8, 32, 64, 128, 256", "number"),
                td("6 × 1 × 3 = 18", "number"),
            ]
        ),
        tr(
            [
                td("latency-0/1/5/10ms"),
                td("C SDK → benchmark proxy → 真实 worker"),
                td("与 hot 相同"),
                td("每个 delay 均为 22 个参数点"),
                td("22 × 3 × 4 = 264", "number"),
            ]
        ),
        tr(
            [
                td("membership"),
                td("C SDK → coordinator → 两个真实 worker"),
                td("64 KiB"),
                td("256；第约 5 秒 kill owner"),
                td("1", "number"),
            ]
        ),
    ]
    matrix_table = table(
        ["Scenario", "数据路径", "请求大小", "Concurrency / 操作", "完整 summary 数"],
        matrix_rows,
    )

    field_definitions = [
        ("通用", "type", "枚举字符串", "JSONL 记录类型：environment、measurement_start、summary、stack_metrics、membership_event 或 bucket。"),
        ("环境", "mode", "字符串", "编排脚本启动模式。主文件记录为 all，但运行被停止，因此不能据此认为所有矩阵均完成。"),
        ("环境", "git_commit", "Git SHA", "启动压测时仓库 HEAD；本次为 dd93ab3…。"),
        ("环境", "git_dirty", "布尔值", "启动时工作树是否包含未提交改动；true 表示结果不只对应裸 commit。"),
        ("环境/summary", "block_size", "bytes", "Client 将逻辑对象映射到 worker block 的大小；本次 268,435,456 bytes。"),
        ("环境/summary", "object_size", "bytes", "stat 返回的单个对象长度；本次 67,108,864 bytes。"),
        ("环境", "l1_capacity_bytes", "bytes", "Worker 内存 L1 容量；0 明确关闭 L1。"),
        ("环境", "l2_capacity_bytes", "bytes", "本地 L2 缓存文件受淘汰策略约束的驻留字节预算；不是 RSS 或 page-cache 上限。"),
        ("环境", "l2_page_size_bytes", "bytes", "Paged L2 的落盘与命中判断粒度；本次 1,048,576 bytes。"),
        ("环境", "measure_seconds", "秒", "环境摘要声称的默认正式测量时长；membership 旧首行误写 10，实际以 summary.seconds=12 为准。"),
        ("环境", "output", "路径", "编排脚本写入的原始 JSONL 路径。"),
        ("环境", "repeats", "次数", "环境摘要声称的重复数；hot/latency 为 3，membership 旧首行误写 3，实际只有 round=1。"),
        ("环境/summary", "warmup_seconds", "秒", "该轮正式测量前的闭环预热时长；warmup 计入 total_attempts，但不计入 success_qps。"),
        ("环境", "worker_rust_log", "日志级别", "Worker 的 RUST_LOG；warn 表示不写每个 cache-hit INFO。"),
        ("标识", "scenario", "字符串", "数据路径/故障条件标签，如 hot-direct、latency-5ms 或 membership。"),
        ("标识", "round", "正整数", "同一参数点的重复轮编号；hot/latency 为 1–3。"),
        ("边界", "unix_ms", "Unix epoch ms", "measurement_start 的墙钟时间；只供外部脚本把 kill 时刻对齐到正式测量起点，不参与延迟计算。"),
        ("summary", "coordinator", "host:port", "C Client 使用的 coordinator 地址；本次为本机 loopback。"),
        ("summary", "uri", "URI", "读取对象的逻辑 URI。"),
        ("summary", "version", "opaque 字符串", "启动 stat 得到并在之后每个 read 传入 worker 的精确对象版本；Client 不解释其内容。"),
        ("summary", "object_count", "个", "该轮随机访问的 URI 数；纳入报告的场景均为 1。"),
        ("summary", "request_bytes", "bytes/request", "一次公开 talon_read_async 要写入调用方 buffer 的逻辑长度。"),
        ("summary", "requested_concurrency", "请求数", "命令行要求闭环同时维持的独立异步逻辑读 slot 数。"),
        ("summary", "actual_concurrency", "请求数", "atomic active 计数器观察到的峰值；用于确认 loadgen 真正把请求推到目标并发。"),
        ("summary", "seconds", "秒", "该 summary 实际用于 QPS 分母的正式测量窗口。"),
        ("summary", "attempts", "次", "提交时间落在正式窗口内的逻辑读尝试数；同步提交失败也仍是一次 attempt。"),
        ("summary", "total_attempts", "次", "warmup 与正式窗口合计的逻辑读尝试数。"),
        ("summary/bucket", "successes", "次", "正式窗口内 callback 成功、长度正确且首尾 payload 校验通过的逻辑读数；bucket 行只统计提交时间落入该桶的请求。"),
        ("summary/bucket", "success_qps", "requests/s", "summary 中为 successes / seconds；bucket 中为 successes / ((end_ms-begin_ms)/1000)。"),
        ("summary", "attempt_qps", "attempts/s", "attempts / seconds；失败场景下它可能高于 success_qps。"),
        ("summary", "mib_per_second", "MiB/s", "正式窗口成功返回的逻辑字节数 / 2^20 / seconds；不是物理 backend 带宽。"),
        ("summary", "p50_ms", "ms", "成功请求 latency 的第 50 百分位（中位延迟）。"),
        ("summary", "p95_ms", "ms", "成功请求 latency 的第 95 百分位。"),
        ("summary", "p99_ms", "ms", "成功请求 latency 的第 99 百分位。"),
        ("summary", "p999_ms", "ms", "成功请求 latency 的第 99.9 百分位。"),
        ("summary", "max_ms", "ms", "该轮成功样本中的最大 latency，即 100 百分位；最容易暴露极少数卡住请求。"),
        ("summary", "submission_errors", "次", "talon_read_async 在提交阶段同步返回失败，或 loadgen 的单-block 安全检查失败；这种请求没有 callback。"),
        ("summary/bucket", "logical_errors", "次", "正式窗口内 callback 返回失败、短读或 payload 校验失败的次数；不包含同步 submission_errors。bucket 行按请求提交时间归桶。"),
        ("summary", "total_logical_errors", "次", "warmup 加正式窗口的 callback 逻辑错误总数；与 logical_errors 的差是 warmup 错误。"),
        ("summary", "sample_count", "条", "真正进入百分位计算的成功 latency 样本数；失败样本不进入 latency 百分位。"),
        ("summary", "dropped_samples", "条", "完成事件超过 max_samples 数组容量后未保存的样本数；QPS 计数仍保留。"),
        ("summary", "all_requests_below_block_size", "布尔值", "请求长度均严格小于 256 MiB 的运行不变量。"),
        ("summary", "all_requests_single_block", "布尔值", "每次提交的 offset 与最后一个 byte 属于同一 block 的运行不变量。"),
        ("summary", "first_error", "字符串/null", "正式测量期观察到的第一条错误文本；无错误为 null。"),
        ("stack", "concurrency", "请求数", "stack_metrics 对应的 requested_concurrency，名称较短但语义相同。"),
        ("stack", "observation_seconds", "秒", "用于 stack QPS 分母的名义 warmup+measurement 时长（本次通常 13）；不含完成 outstanding 请求的额外 drain 时间。"),
        ("stack", "logical_attempts", "次", "从 summary.total_attempts 复制的 warmup+measurement 逻辑尝试数。"),
        ("stack", "worker_requests_including_stat", "次", "Worker requests_total 的该轮增量，包括 loadgen 启动时恰好一次 stat。"),
        ("stack", "worker_attempts", "次", "worker_requests_including_stat - 1；表示到达真实 worker 的读取帧数，包含可能的 client 重试。"),
        ("stack", "worker_attempt_qps", "frames/s", "worker_attempts / observation_seconds；口径含 warmup，不能直接与仅正式窗口的 success_qps 相减。"),
        ("stack", "l2_cache_misses", "page misses", "Worker L2 miss counter 的该轮增量；paged 模式下按被访问且缺失的 page 计数。"),
        ("stack", "l2_cache_miss_qps", "misses/s", "l2_cache_misses / observation_seconds。"),
        ("stack", "backend_fetches", "次", "Worker backend fetch duration histogram count 的增量；相关场景中对应首次填充缺页。"),
        ("stack", "backend_fetch_qps", "fetches/s", "backend_fetches / observation_seconds；不是成功逻辑读 QPS。"),
        ("stack", "membership_queries", "次", "Coordinator membership_query control counter 的该轮增量。"),
        ("stack", "worker_cpu_cores", "CPU cores", "Worker-1 的 user+system CPU 秒 / 实际命令墙钟秒；1.0 约等于占满一个逻辑 CPU，不含 client/proxy/coordinator CPU。"),
        ("stack", "worker_rss_kib", "KiB", "该轮结束后 Worker-1 的 VmRSS 快照；不是全栈内存，也不是全程峰值。"),
        ("stack", "proxy_attempts", "read frames", "代理看到的 GetVersionedRange/GetVersionedRangeTenant 帧数；不含 stat，client 重试会新增一帧。"),
        ("stack", "proxy_attempt_qps", "frames/s", "proxy_attempts / observation_seconds。"),
        ("stack", "proxy_forwarded", "read frames", "代理已转给 upstream 且收到匹配 request_id 响应的读取帧数；注入失败不会增加它。"),
        ("stack", "proxy_injected_unavailable", "次", "代理直接回复 typed Unavailable 的次数；本报告 latency 场景均为 0。"),
        ("stack", "retry_attempts", "估算次数", "max(proxy_attempts - logical_attempts, 0)；由跨层计数推导，不是 Client 内部直接暴露的 retry counter。"),
        ("stack", "retry_succeeded", "估算次数", "max(retry_attempts - total_logical_errors, 0)；仅用于故障实验粗略拆分，本报告不据此下结论。"),
        ("membership", "owner", "node id", "owner-probe 观察到当时为对象服务、随后被 SIGKILL 的 worker。"),
        ("membership", "kill_after_measure_start_ms", "ms", "kill owner 相对 measurement_start 的实际时间；目标 5 秒，本次 5,032 ms。"),
        ("membership", "coordinator_excluded_after_kill_ms", "ms", "从 kill 到 owner 不再出现在 coordinator worker 列表的观测时间。"),
        ("membership", "survivor_first_request_after_kill_ms", "ms", "从 kill 到 survivor 的 bytes_served_total 首次增长的观测时间。"),
        ("membership", "heartbeat_ms", "ms", "演练中 coordinator reconciliation 与 worker heartbeat 的配置间隔；均为 100 ms。"),
        ("membership", "unhealthy_ms", "ms", "Coordinator 将失联节点判为 unhealthy 的配置阈值；本次 500 ms。"),
        ("membership", "lease_ms", "ms", "Worker membership lease TTL；本次 1,500 ms。"),
        ("bucket", "begin_ms", "ms", "该一秒桶相对正式测量起点的左边界；请求按 submitted_ns 而不是 callback 完成时刻归桶。"),
        ("bucket", "end_ms", "ms", "该一秒桶相对正式测量起点的右边界。"),
    ]
    documented_fields = {field for _, field, _, _ in field_definitions}
    observed_fields = {
        field
        for row in rows + membership_rows
        for field in row
    }
    undocumented_fields = sorted(observed_fields - documented_fields)
    if undocumented_fields:
        raise SystemExit(
            "undocumented JSONL fields in HTML report: "
            + ", ".join(undocumented_fields)
        )
    field_dictionary_rows = [
        tr(
            [
                td(html.escape(group)),
                td(f"<code>{html.escape(field)}</code>"),
                td(html.escape(unit)),
                td(html.escape(meaning)),
            ]
        )
        for group, field, unit, meaning in field_definitions
    ]
    field_dictionary_table = table(
        ["记录组", "原始字段", "单位 / 类型", "精确定义"],
        field_dictionary_rows,
    )

    aggregation_rows = [
        tr([td("单轮 QPS"), td("successes / seconds"), td("只使用正式测量窗口；warmup 不进入分子。")]),
        tr([td("三轮中位 QPS"), td("median(round 1, 2, 3)"), td("本报告容量/延迟表的默认 QPS 口径，降低偶然抖动影响。")]),
        tr([td("最差 p99/p999"), td("max(三轮各自 percentile)"), td("不是把三轮样本合并后重新算百分位，而是保守选三轮中最差的一轮。")]),
        tr([td("95% 拐点"), td("min concurrency where median QPS ≥ 0.95 × peak"), td("从测试过的离散并发点中选择，不是拟合出的连续阈值。")]),
        tr([td("Membership phase QPS"), td("median(该阶段的一秒桶 QPS)"), td("跨越 kill/recovery 边界的一秒桶整体归入故障窗口，因此阶段边界精度为一秒桶级。")]),
    ]
    aggregation_table = table(
        ["报告派生值", "计算方式", "解释"], aggregation_rows
    )

    hot_chart = line_chart(
        hot_chart_series,
        all_concurrencies,
        "独立逻辑请求并发数",
        "直连热 paged-L2：并发增加时的成功 QPS",
    )
    latency_chart = line_chart(
        latency_chart_series,
        [0, 1, 5, 10],
        "代理注入响应延迟（ms）",
        "代理链路：各请求大小的峰值中位 QPS（对数纵轴）",
        log_y=True,
    )

    generated_at = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    direct_summary_table = table(
        [
            "请求大小",
            "95% 拐点",
            "拐点中位 QPS",
            "峰值中位 QPS @ 并发",
            "峰值逻辑 MiB/s",
            "拐点最差 p99",
            "拐点最差 p999",
            "Worker CPU",
            "Worker RSS 上限",
        ],
        direct_summary_rows,
    )
    direct_sweep_table = table(
        ["Concurrency", "4 KiB：中位 QPS / 最差 p99", "64 KiB：中位 QPS / 最差 p99", "1 MiB：中位 QPS / 最差 p99"],
        direct_sweep_rows,
    )
    latency_table = table(
        [
            "注入延迟",
            "请求大小",
            "95% 拐点",
            "拐点中位 QPS",
            "峰值中位 QPS @ 并发",
            "拐点 p50",
            "拐点最差 p99",
            "拐点最差 p999",
        ],
        latency_rows,
    )
    proxy_table = table(
        ["请求大小", "直连峰值中位 QPS", "0 ms 代理峰值中位 QPS", "峰值变化"],
        proxy_overhead_rows,
    )
    outlier_table = table(
        ["场景", "max ≥ 1 s 的轮数", "最坏 max", "请求大小", "并发", "同轮 p999"],
        outlier_rows,
    )
    complete_aggregate_table = table(
        [
            "场景",
            "请求",
            "并发",
            "轮数",
            "中位成功 QPS",
            "中位 attempt QPS",
            "中位 MiB/s",
            "中位 p50",
            "中位 p95",
            "最差 p99",
            "最差 p999",
            "最坏 max",
            "Worker CPU cores",
            "Worker RSS MiB",
            "L2 misses 合计",
            "backend fetches 合计",
            "提交错 / 逻辑错 / 丢样本",
            "实际并发",
            "单 block",
        ],
        complete_aggregate_rows,
    )
    membership_bucket_table = table(
        ["秒桶", "测量区间", "阶段", "成功 QPS", "逻辑错误"],
        membership_table_rows,
    )
    parameters_table = table(["参数 / 指标", "本次值", "具体含义"], parameter_table_rows)

    template = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Talon C SDK → Worker 性能报告</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #172033;
      --muted: #5f6b7a;
      --line: #dbe3ef;
      --soft: #f5f8fc;
      --blue: #2563eb;
      --blue-soft: #eaf1ff;
      --green: #047857;
      --green-soft: #e8f8f1;
      --amber: #b45309;
      --amber-soft: #fff4df;
      --red: #b42318;
      --red-soft: #fff0ee;
      --card: #ffffff;
      --shadow: 0 10px 30px rgba(26, 43, 73, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background: linear-gradient(180deg, #edf4ff 0, #f8fafc 320px, #f8fafc 100%);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      line-height: 1.62;
    }
    main { max-width: 1240px; margin: 0 auto; padding: 44px 26px 80px; }
    header {
      padding: 36px;
      color: #fff;
      background: linear-gradient(135deg, #153a75, #2563eb 62%, #38bdf8);
      border-radius: 22px;
      box-shadow: var(--shadow);
    }
    h1 { margin: 0 0 10px; font-size: clamp(30px, 4vw, 48px); line-height: 1.18; }
    header p { margin: 6px 0; max-width: 900px; color: #e6f0ff; }
    .meta { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 20px; }
    .pill {
      display: inline-flex; align-items: center; padding: 5px 11px;
      border-radius: 999px; background: rgba(255,255,255,.15);
      border: 1px solid rgba(255,255,255,.26); font-size: 13px;
    }
    .print-button {
      margin-top: 18px; padding: 9px 15px; border: 0; border-radius: 9px;
      color: #153a75; background: #fff; font-weight: 700; cursor: pointer;
    }
    .toc {
      display: flex; flex-wrap: wrap; gap: 8px; margin-top: 18px;
      padding: 14px; border: 1px solid #d9e4f4; border-radius: 13px;
      background: rgba(255,255,255,.72);
    }
    .toc a {
      padding: 5px 10px; border-radius: 8px; color: #17427d;
      background: #fff; text-decoration: none; font-size: 13px; font-weight: 700;
    }
    section {
      margin-top: 26px; padding: 28px; background: var(--card);
      border: 1px solid #e3e9f2; border-radius: 18px; box-shadow: var(--shadow);
    }
    h2 { margin: 0 0 16px; font-size: 26px; line-height: 1.25; }
    h3 { margin: 26px 0 12px; font-size: 20px; }
    p { margin: 10px 0; }
    code {
      padding: 2px 6px; border-radius: 5px; color: #193765;
      background: #edf3fb; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .callout {
      margin: 16px 0; padding: 15px 17px; border-left: 5px solid var(--blue);
      border-radius: 9px; background: var(--blue-soft);
    }
    .callout.good { border-color: var(--green); background: var(--green-soft); }
    .callout.warn { border-color: var(--amber); background: var(--amber-soft); }
    .callout.danger { border-color: var(--red); background: var(--red-soft); }
    .metrics {
      display: grid; grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px; margin: 18px 0;
    }
    .metric-card {
      padding: 18px; border: 1px solid var(--line); border-radius: 13px;
      background: linear-gradient(145deg, #fff, #f5f9ff);
    }
    .metric-label { color: var(--muted); font-size: 13px; font-weight: 650; }
    .metric-value { margin: 4px 0; font-size: 29px; line-height: 1.2; font-weight: 800; }
    .metric-note { color: var(--muted); font-size: 13px; }
    .flow {
      display: flex; align-items: stretch; gap: 8px; overflow-x: auto;
      padding: 12px 2px 18px;
    }
    .node {
      min-width: 145px; padding: 13px; text-align: center; border: 1px solid #cdd9eb;
      border-radius: 11px; background: #f7faff; font-weight: 750;
    }
    .node small { display: block; color: var(--muted); font-weight: 500; }
    .node.off { color: #8a2830; background: #fff2f2; border-style: dashed; }
    .arrow { align-self: center; color: var(--blue); font-size: 24px; font-weight: 800; }
    .table-scroll { overflow-x: auto; margin: 14px 0; }
    .data-table { width: 100%; border-collapse: collapse; font-size: 14px; }
    .data-table th {
      position: sticky; top: 0; padding: 10px 12px; color: #344054;
      text-align: left; white-space: nowrap; background: #edf3fb;
      border-bottom: 2px solid #cdd9e9;
    }
    .data-table td { padding: 10px 12px; border-bottom: 1px solid #e7ecf3; vertical-align: top; }
    .data-table tbody tr:hover { background: #f8fbff; }
    .data-table small { display: block; color: var(--muted); white-space: nowrap; }
    .data-table .number { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
    .strong, strong { font-weight: 800; }
    .muted { color: var(--muted); }
    .danger-text { color: var(--red); font-weight: 750; }
    .failure-row { background: #fff3f1; }
    .chart { margin: 20px 0; padding: 12px; border: 1px solid var(--line); border-radius: 13px; background: var(--soft); }
    .chart figcaption { margin: 4px 6px 10px; font-weight: 750; }
    .chart svg { display: block; width: 100%; height: auto; }
    .axis-label, .legend-label { fill: #526071; font: 12px ui-sans-serif, system-ui, sans-serif; }
    .axis-title { fill: #344054; font: 600 13px ui-sans-serif, system-ui, sans-serif; }
    .two-col { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
    details { margin: 16px 0; }
    summary { cursor: pointer; color: #193765; }
    ul { padding-left: 22px; }
    a { color: #175cd3; }
    footer { margin-top: 24px; color: var(--muted); font-size: 13px; text-align: center; }
    @media (max-width: 800px) {
      main { padding: 20px 12px 50px; }
      header, section { padding: 20px; border-radius: 14px; }
      .metrics, .two-col { grid-template-columns: 1fr; }
    }
    @media print {
      body { background: #fff; }
      main { max-width: none; padding: 0; }
      header { color: #111; background: #fff; box-shadow: none; border: 2px solid #2458a6; }
      header p { color: #333; }
      .pill { border-color: #bbb; }
      section { break-inside: avoid; box-shadow: none; }
      .print-button { display: none; }
    }
  </style>
</head>
<body>
<main>
  <header>
    <h1>Talon C SDK → Worker 性能报告</h1>
    <p>重点回答：关闭 L1、开启 1 MiB paged-L2 后，单机真实 C ABI 全链路能达到多少 QPS；增加响应延迟后如何变化；owner worker 失效时多久恢复。</p>
    <div class="meta">
      <span class="pill">基线 __COMMIT__</span>
      <span class="pill">未提交工作树</span>
      <span class="pill">Intel Xeon Gold 6338 · 64 logical CPUs</span>
      <span class="pill">125.4 GiB RAM · Linux 5.15</span>
      <span class="pill">生成时间 __GENERATED_AT__</span>
    </div>
    <button class="print-button" onclick="window.print()">打印 / 导出 PDF</button>
  </header>

  <nav class="toc" aria-label="报告目录">
    <a href="#executive-summary">结论</a><a href="#paths">链路</a>
    <a href="#matrix">测试矩阵</a><a href="#parameters">参数字典</a>
    <a href="#direct-results">直连结果</a><a href="#latency-results">延迟结果</a>
    <a href="#membership-results">Membership</a><a href="#tail-risk">尾延迟</a>
    <a href="#full-results">完整聚合表</a><a href="#integrity">完整性</a>
    <a href="#scope">边界</a><a href="#artifacts">原始数据</a>
  </nav>

  <section id="executive-summary">
    <h2>1. 先看结论</h2>
    <div class="metrics">__DIRECT_CARDS__</div>
    <div class="callout good">
      <strong>直连、热 paged-L2 容量场景达到 10K QPS 观察目标。</strong>
      4 KiB 和 64 KiB 在 concurrency=1 时就分别达到约 52.7K 和 31.3K QPS；
      1 MiB 在 concurrency=8 时超过 10K。常规小请求性能来自大量独立
      <code>talon_read_async</code>，不是一个超大请求跨 256 MiB blocks 并发。
    </div>
    <div class="callout warn">
      <strong>容量与 SLO 要分开看。</strong>
      QPS、p99 和 p999 很高且稳定，但 330 个十秒参数轮次中有 50 个出现过至少一个
      ≥1 秒的成功请求，最坏接近 10 秒。因此当前数据能说明总体吞吐能力，
      不能证明 max latency 或极端公平性已经健康。
    </div>
    <div class="callout">
      本报告只纳入完整跑完的 <strong>hot、latency 和 membership</strong>。
      按要求停止后，backend miss 与 1% failure injection 不进入结论，也没有用不完整数据补表。
    </div>
  </section>

  <section id="paths">
    <h2>2. 到底测试了哪条链路</h2>
    <h3>直连热 L2：用于回答本矩阵观察到的单机容量</h3>
    <div class="flow">
      <div class="node">原生 C loadgen<small>闭环独立请求</small></div><div class="arrow">→</div>
      <div class="node">公开 C ABI<small>talon_read_async</small></div><div class="arrow">→</div>
      <div class="node">Rust Client<small>placement + pool</small></div><div class="arrow">→</div>
      <div class="node">真实 Worker<small>默认 data plane</small></div><div class="arrow">→</div>
      <div class="node off">L1 关闭<small>capacity = 0</small></div><div class="arrow">→</div>
      <div class="node">Paged L2<small>1 MiB pages</small></div>
    </div>
    <p>启动前通过 coordinator stat 一次，之后每次读取都携带 exact version 和 size。每个新启动的 worker cache 只在第一轮 warmup 通过 origin 填充 64 个 1 MiB pages；随后正式测量是热 L2 路径。这里的“容量”是这台主机、这套 loadgen 和测试过的离散并发点共同观察到的值，不是对任意硬件的理论上限。</p>

    <h3>延迟代理：只用于控制变量，不代表生产架构</h3>
    <div class="flow">
      <div class="node">C SDK</div><div class="arrow">→</div>
      <div class="node">Benchmark proxy<small>转发 Talon frame<br>响应前 sleep</small></div><div class="arrow">→</div>
      <div class="node">真实 Worker</div><div class="arrow">→</div>
      <div class="node">Paged L2</div>
    </div>
    <p>代理是独立 benchmark binary，不在生产 worker 中。之所以引入它，是因为这里要控制“worker 已完成读取后，响应再晚多少毫秒回到 client”，同时又不在生产 worker 路径加入测试分支。0 ms 组量化代理自身开销，1/5/10 ms 组观察固定响应延迟下的变化；只有直连结果用于讨论本矩阵的容量。</p>

    <h3>Membership 故障演练</h3>
    <div class="flow">
      <div class="node">C SDK<small>concurrency 256</small></div><div class="arrow">→</div>
      <div class="node">Coordinator<small>membership</small></div><div class="arrow">→</div>
      <div class="node">Owner worker<small>第 5 秒 kill</small></div>
      <div class="arrow">⇢</div><div class="node">Survivor worker<small>重新 placement</small></div>
    </div>
  </section>

  <section id="matrix">
    <h2>3. 实际跑了哪些参数、为什么耗时较长</h2>
    __MATRIX_TABLE__
    <div class="callout warn">
      完整 hot/latency 共 <strong>330</strong> 个十秒参数轮次；每轮还有 3 秒 warmup，
      所以仅名义负载时间就是 <strong>__NOMINAL_MINUTES__ 分钟</strong>
      （330 × 13 秒），还不含编译、组件重启、ready/membership 等待和 outstanding
      请求 drain。Membership 另有 3 秒 warmup、12 秒测量和一次 owner probe。
      这就是此前整套测试耗时长的直接原因。
    </div>
    <p>4 KiB/64 KiB 各有 8 个并发点；1 MiB 只有 6 个并发点，是为了让 client 独立 buffer 在最高点约为 256 MiB，而不是额外制造 512 MiB/1 GiB buffer 压力。报告使用每个参数点三轮中位 QPS，并保守展示三轮最差 p99/p999。</p>
    <div class="callout">
      主 JSONL 里还有中止前留下的 <strong>__EXCLUDED_PARTIAL__</strong> 条不完整 backend summary；
      它们全部被过滤。Failure injection 没有进入本次结果。两种测试能力及代码都保留，
      没有删除；以后若要执行任何类似规模的矩阵，会先给出轮数、名义时长和资源量并等待明确批准。
    </div>
  </section>

  <section id="parameters">
    <h2>4. 每个参数和指标是什么意思</h2>
    <p>下面的“本次值”是这次真实运行的配置，不是泛泛定义。</p>
    <div class="callout">
      <strong>最容易混淆的三个单位：</strong>256 MiB <code>block_size</code> 决定对象到 worker/cache key 的逻辑分块；
      1 MiB <code>l2_page_size_bytes</code> 决定 paged-L2 的缺页与落盘粒度；
      4 KiB/64 KiB/1 MiB <code>request_bytes</code> 才是一次业务读取长度。
      64 MiB 对象完整落在 block #0，但成千上万个独立单-block 请求仍然可以并发。
    </div>
    __PARAMETERS_TABLE__
    <h3>报告中的派生值如何计算</h3>
    __AGGREGATION_TABLE__
    <details open>
      <summary><strong>完整 JSONL 字段字典（原始文件出现的每一个字段）</strong></summary>
      <p>这一表由生成器与输入字段集合自动核对；出现新字段但没有解释时，报告生成会直接失败。</p>
      __FIELD_DICTIONARY_TABLE__
    </details>
  </section>

  <section id="direct-results">
    <h2>5. 直连热 paged-L2 结果</h2>
    <p><strong>95% 拐点</strong>是达到该请求大小峰值中位 QPS 95% 的最小并发。它比“峰值在哪个偶然点”更适合作为实际并发选择。</p>
    __DIRECT_SUMMARY_TABLE__
    <p class="muted">峰值逻辑 MiB/s 是 client 收到的字节吞吐。热 L2、loopback 和 Linux page cache 会让该值远高于实际对象存储或单块 NVMe 带宽，所以不同 request size 之间不能只比较 QPS，也不能把该列当成 backend 吞吐。</p>
    __HOT_CHART__
    <details open>
      <summary><strong>完整并发扫描（每格为三轮中位 QPS / 三轮最差 p99）</strong></summary>
      __DIRECT_SWEEP_TABLE__
    </details>
    <div class="callout">
      4 KiB 在 concurrency=32 已达到峰值的 97%；64 KiB 在 64 达到 98.8%；
      1 MiB 在 32 达到 98.1%。继续提高到 256/512 带来的吞吐增益很小，
      高并发不应被理解为越大越好。
    </div>
  </section>

  <section id="latency-results">
    <h2>6. 可控响应延迟结果</h2>
    <div class="callout warn">
      代理要完整读取并重新写出 Talon frame，尤其会复制大 payload。
      因此下面反映“经代理且增加延迟的链路”，不是直连 worker 的性能。
    </div>
    __PROXY_TABLE__
    __LATENCY_CHART__
    __LATENCY_TABLE__
    <p>所有 264 个 latency 轮次中，<code>proxy_attempts == logical_attempts</code> 且 <code>worker_attempts == proxy_forwarded</code>，没有注入失败、重试或最终逻辑错误。这里的 attempt 数差异定义仍然保留，是为了将来有重试时不会把一帧重试误算成新的业务请求。</p>
    <div class="callout danger">
      0 ms 代理相对直连峰值已经下降约 50%–69%，说明代理不是透明组件。
      因此 1/5/10 ms 数据只能回答“这套代理控制变量下吞吐如何变化”，不能精确外推真实跨机网络延迟下的 QPS。
    </div>
  </section>

  <section id="membership-results">
    <h2>7. Membership 失效与恢复</h2>
    <div class="metrics">
      <article class="metric-card"><div class="metric-label">Coordinator 摘除 owner</div><div class="metric-value">__EXCLUDED_MS__ ms</div><div class="metric-note">从 kill 时刻起</div></article>
      <article class="metric-card"><div class="metric-label">Survivor 首次成功</div><div class="metric-value">__SURVIVOR_MS__ ms</div><div class="metric-note">从 kill 时刻起</div></article>
      <article class="metric-card"><div class="metric-label">恢复后中位 QPS</div><div class="metric-value">__AFTER_QPS__</div><div class="metric-note">恢复后 5 个一秒桶</div></article>
    </div>
    <p>本轮使用 64 KiB 请求、concurrency=256、两个真实 worker；owner 为 <code>__OWNER__</code>，在正式测量开始后 __KILL_AT_MS__ ms 被 SIGKILL。全 12 秒窗口成功 QPS 为 __MEMBERSHIP_QPS__，正式窗口逻辑错误 __MEMBERSHIP_ERRORS__ 个；第一条错误为 <code>__MEMBERSHIP_FIRST_ERROR__</code>。</p>
    <div class="two-col">
      <div class="callout good"><strong>故障前：</strong>中位 __BEFORE_QPS__ QPS，测量窗口内 __BEFORE_ERRORS__ 个错误。</div>
      <div class="callout danger"><strong>故障窗口：</strong>中位 __FAILURE_QPS__ QPS，共 __FAILURE_ERRORS__ 个逻辑错误。</div>
    </div>
    <div class="callout good"><strong>恢复后：</strong>中位 __AFTER_QPS__ QPS，__AFTER_ERRORS__ 个错误。恢复吞吐不低于故障前。</div>
    __MEMBERSHIP_CHART__
    __MEMBERSHIP_TABLE__
    <div class="callout warn">
      Membership warmup 期间另有 __WARMUP_ERRORS__ 个逻辑错误；故障前五个正式秒桶也有
      __BEFORE_ERRORS__ 个零星错误。它们没有改变吞吐结论，但意味着健康启动阶段也不是严格零错误，
      应作为后续独立问题定位，不能藏在 failure-window 总数里。
    </div>
  </section>

  <section id="tail-risk">
    <h2>8. 为什么 p99 很好，但 max 很差</h2>
    <p>每轮可能完成数百万请求。p999 只要求 99.9% 请求足够快；即使有几个请求等待数秒，也可能完全看不见。<code>max</code> 专门暴露这种极少数异常。</p>
    __OUTLIER_TABLE__
    <div class="callout danger">
      <strong>结论：</strong>这些秒级值通过每个 slot 的 monotonic 提交/完成时间计算，
      warmup 请求不会进入正式样本。当前只能确认“极少数已提交请求长时间未完成”，
      还不能把原因确定为 client scheduler、connection pool、worker data plane 或宿主机调度。
      在没有单独批准诊断矩阵前，本报告不做无证据归因。
    </div>
  </section>

  <section id="full-results">
    <h2>9. 全部 110 个 hot/latency 聚合参数点</h2>
    <p>下面没有只挑峰值：每个 scenario × request size × concurrency 都在表中。每行聚合三轮；QPS、MiB/s、p50、p95 和 Worker CPU 取三轮中位数，p99/p999/max/RSS 取三轮最坏值，miss/fetch/error 取三轮合计。红底表示该参数点至少有一轮 max ≥ 1 秒。</p>
    <details>
      <summary><strong>展开完整聚合结果</strong></summary>
      __COMPLETE_AGGREGATE_TABLE__
    </details>
  </section>

  <section id="integrity">
    <h2>10. 数据完整性与验证</h2>
    <div class="metrics">
      <article class="metric-card"><div class="metric-label">完整 hot/latency summary</div><div class="metric-value">__SUMMARY_COUNT__ / __EXPECTED_COUNT__</div><div class="metric-note">对应 stack metrics：__STACK_COUNT__</div></article>
      <article class="metric-card"><div class="metric-label">提交 / 逻辑错误</div><div class="metric-value">__SUBMISSION_ERRORS__ / __LOGICAL_ERRORS__</div><div class="metric-note">hot + latency</div></article>
      <article class="metric-card"><div class="metric-label">丢弃样本 / 非单 block</div><div class="metric-value">__DROPPED__ / __BAD_BLOCKS__</div><div class="metric-note">实际并发不匹配：__BAD_CONCURRENCY__</div></article>
    </div>
    <ul>
      <li>每个请求都小于 256 MiB，并通过 offset/end 检查确认只落在一个 Talon block。</li>
      <li>每个场景只在首次 warmup 行累计 64 次 L2 misses/backend fetches，恰好对应 64 MiB 对象的 64 个 1 MiB pages。</li>
      <li>相关 hot/latency worker 与 proxy 日志没有 WARN/ERROR；coordinator 只有本地 benchmark 未启用管理认证的预期警告。</li>
      <li>Workspace build/test/clippy/rustdoc 已通过；最终 cargo check、Rust fmt、C11 严格编译链接、shell/Python 语法及 git diff check 通过。</li>
      <li>代理计数不一致的轮数：__PROXY_MISMATCHES__。</li>
    </ul>
  </section>

  <section id="scope">
    <h2>11. 这份报告不能说明什么</h2>
    <ul>
      <li><strong>不是 backend miss 性能：</strong>该矩阵已停止并从报告排除。</li>
      <li><strong>不是失败注入性能：</strong>1% typed Unavailable 模式仍完整保留在工具中，但本报告不运行、不引用；没有删除这项能力。</li>
      <li><strong>不是跨机器网络结果：</strong>client、coordinator、proxy 和 worker 都在同一台 64 logical CPU 主机，通过 loopback 通信。</li>
      <li><strong>不是 L1 性能：</strong>L1 capacity 明确为 0。</li>
      <li><strong>不是多 block fan-out：</strong>64 MiB 对象小于 256 MiB block，内部单读窗口 8 与本报告 QPS 无关。</li>
      <li><strong>不是冷物理盘上限：</strong>热 L2 文件会进入 Linux page cache；MiB/s 是逻辑响应带宽。</li>
      <li><strong>不是 CI 门槛：</strong>绝对值依赖主机；10K QPS 是观察目标，不是硬编码通过条件。</li>
    </ul>
  </section>

  <section id="artifacts">
    <h2>12. 原始数据与源码</h2>
    <ul>
      <li><a href="__HOT_FILE__">hot/latency 原始 JSONL</a>：hot 和四组 latency 完整；同一文件后部存在中断前产生的 backend 行，本报告明确过滤。</li>
      <li><a href="__MEMBERSHIP_FILE__">membership 原始 JSONL</a>：summary 中实际 warmup=3 s、measurement=12 s、round=1。</li>
      <li><a href="../../scripts/c_client_loadtest.sh">真实栈编排脚本</a></li>
      <li><a href="../../clients/c/examples/loadgen.c">原生 C loadgen</a></li>
      <li><a href="../../clients/rust/src/client.rs">Rust Client 并发实现</a></li>
      <li><a href="../../scripts/render_c_client_loadtest_report.py">本 HTML 报告生成器</a></li>
    </ul>
    <p class="muted">注意：membership 原始文件首行 environment 由旧脚本误写为通用 10 秒/3 轮；实际 summary 与本报告使用的是 12 秒/1 轮。脚本已经修正，未为修复展示字段而重跑负载。</p>
  </section>

  <footer>Generated from existing JSONL only. No additional benchmark was executed while producing this report.</footer>
</main>
</body>
</html>
"""

    replacements = {
        "__COMMIT__": html.escape(str(environment["git_commit"])[:12]),
        "__GENERATED_AT__": html.escape(generated_at),
        "__DIRECT_CARDS__": "".join(direct_cards),
        "__MATRIX_TABLE__": matrix_table,
        "__NOMINAL_MINUTES__": f"{nominal_hot_latency_seconds / 60:.1f}",
        "__EXCLUDED_PARTIAL__": fmt_int(excluded_partial_summaries),
        "__PARAMETERS_TABLE__": parameters_table,
        "__AGGREGATION_TABLE__": aggregation_table,
        "__FIELD_DICTIONARY_TABLE__": field_dictionary_table,
        "__DIRECT_SUMMARY_TABLE__": direct_summary_table,
        "__HOT_CHART__": hot_chart,
        "__DIRECT_SWEEP_TABLE__": direct_sweep_table,
        "__PROXY_TABLE__": proxy_table,
        "__LATENCY_CHART__": latency_chart,
        "__LATENCY_TABLE__": latency_table,
        "__EXCLUDED_MS__": fmt_int(
            int(membership_event["coordinator_excluded_after_kill_ms"])
        ),
        "__SURVIVOR_MS__": fmt_int(
            int(membership_event["survivor_first_request_after_kill_ms"])
        ),
        "__OWNER__": html.escape(str(membership_event["owner"])),
        "__KILL_AT_MS__": fmt_int(kill_at),
        "__MEMBERSHIP_QPS__": fmt_int(float(membership_summary["success_qps"])),
        "__MEMBERSHIP_ERRORS__": fmt_int(int(membership_summary["logical_errors"])),
        "__MEMBERSHIP_FIRST_ERROR__": html.escape(
            str(membership_summary["first_error"])
        ),
        "__BEFORE_QPS__": fmt_int(before_qps),
        "__FAILURE_QPS__": fmt_int(failure_qps),
        "__AFTER_QPS__": fmt_int(after_qps),
        "__BEFORE_ERRORS__": fmt_int(before_errors),
        "__FAILURE_ERRORS__": fmt_int(failure_errors),
        "__AFTER_ERRORS__": fmt_int(after_errors),
        "__WARMUP_ERRORS__": fmt_int(warmup_errors),
        "__MEMBERSHIP_CHART__": membership_chart(buckets),
        "__MEMBERSHIP_TABLE__": membership_bucket_table,
        "__OUTLIER_TABLE__": outlier_table,
        "__COMPLETE_AGGREGATE_TABLE__": complete_aggregate_table,
        "__SUMMARY_COUNT__": fmt_int(integrity["summaries"]),
        "__STACK_COUNT__": fmt_int(integrity["stacks"]),
        "__EXPECTED_COUNT__": fmt_int(integrity["expected"]),
        "__SUBMISSION_ERRORS__": fmt_int(integrity["submission_errors"]),
        "__LOGICAL_ERRORS__": fmt_int(integrity["logical_errors"]),
        "__DROPPED__": fmt_int(integrity["dropped_samples"]),
        "__BAD_BLOCKS__": fmt_int(integrity["bad_blocks"]),
        "__BAD_CONCURRENCY__": fmt_int(integrity["bad_concurrency"]),
        "__PROXY_MISMATCHES__": fmt_int(proxy_counter_mismatches),
        "__HOT_FILE__": html.escape(hot_path.name),
        "__MEMBERSHIP_FILE__": html.escape(membership_path.name),
    }
    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)
    unresolved = sorted(
        word for word in set(template.split()) if word.startswith("__") and word.endswith("__")
    )
    if unresolved:
        raise SystemExit(f"unresolved report placeholders: {unresolved}")
    return template


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("hot_latency_jsonl", type=Path)
    parser.add_argument("membership_jsonl", type=Path)
    parser.add_argument("output_html", type=Path)
    args = parser.parse_args()
    document = render(args.hot_latency_jsonl, args.membership_jsonl)
    args.output_html.parent.mkdir(parents=True, exist_ok=True)
    args.output_html.write_text(document, encoding="utf-8")
    print(args.output_html)


if __name__ == "__main__":
    main()
