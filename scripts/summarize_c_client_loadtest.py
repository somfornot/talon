#!/usr/bin/env python3
"""Summarize JSONL emitted by c_client_loadtest.sh without third-party deps."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def load(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, raw in enumerate(path.read_text().splitlines(), 1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_number}: invalid JSON: {error}") from error
        if isinstance(row, dict):
            rows.append(row)
    return rows


def median(rows: list[dict[str, Any]], key: str) -> float:
    return float(statistics.median(float(row.get(key, 0)) for row in rows))


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("type") != "summary":
            continue
        groups[
            (
                str(row.get("scenario", "unknown")),
                int(row.get("request_bytes", 0)),
                int(row.get("requested_concurrency", 0)),
            )
        ].append(row)

    output: list[dict[str, Any]] = []
    for (scenario, request_bytes, concurrency), samples in sorted(groups.items()):
        output.append(
            {
                "type": "aggregate",
                "scenario": scenario,
                "request_bytes": request_bytes,
                "concurrency": concurrency,
                "rounds": len(samples),
                "median_success_qps": round(median(samples, "success_qps"), 3),
                "median_attempt_qps": round(median(samples, "attempt_qps"), 3),
                "median_mib_per_second": round(median(samples, "mib_per_second"), 3),
                "worst_p99_ms": round(max(float(row.get("p99_ms", 0)) for row in samples), 3),
                "worst_p999_ms": round(max(float(row.get("p999_ms", 0)) for row in samples), 3),
                "logical_errors": sum(int(row.get("logical_errors", 0)) for row in samples),
                "submission_errors": sum(
                    int(row.get("submission_errors", 0)) for row in samples
                ),
                "peak_actual_concurrency": max(
                    int(row.get("actual_concurrency", 0)) for row in samples
                ),
                "single_block_verified": all(
                    row.get("all_requests_below_block_size") is True
                    and row.get("all_requests_single_block") is True
                    for row in samples
                ),
            }
        )
    return output


def proxy_overhead(aggregates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keyed = {
        (row["scenario"], row["request_bytes"], row["concurrency"]): row
        for row in aggregates
    }
    output: list[dict[str, Any]] = []
    for (scenario, request_bytes, concurrency), proxy in keyed.items():
        if scenario not in {"latency-0ms", "proxy-0ms"}:
            continue
        direct = keyed.get(("hot-direct", request_bytes, concurrency))
        if direct is None or direct["median_success_qps"] == 0:
            continue
        qps_delta = (
            (proxy["median_success_qps"] - direct["median_success_qps"])
            / direct["median_success_qps"]
            * 100.0
        )
        output.append(
            {
                "type": "proxy_overhead",
                "request_bytes": request_bytes,
                "concurrency": concurrency,
                "direct_median_qps": direct["median_success_qps"],
                "proxy_0ms_median_qps": proxy["median_success_qps"],
                "qps_delta_percent": round(qps_delta, 3),
            }
        )
    return output


def stack_aggregates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("type") != "stack_metrics":
            continue
        groups[
            (
                str(row.get("scenario", "unknown")),
                int(row.get("request_bytes", 0)),
                int(row.get("concurrency", 0)),
            )
        ].append(row)
    output: list[dict[str, Any]] = []
    for (scenario, request_bytes, concurrency), samples in sorted(groups.items()):
        output.append(
            {
                "type": "stack_aggregate",
                "scenario": scenario,
                "request_bytes": request_bytes,
                "concurrency": concurrency,
                "rounds": len(samples),
                "median_worker_attempt_qps": round(
                    median(samples, "worker_attempt_qps"), 3
                ),
                "median_proxy_attempt_qps": round(
                    median(samples, "proxy_attempt_qps"), 3
                ),
                "median_worker_cpu_cores": round(
                    median(samples, "worker_cpu_cores"), 3
                ),
                "max_worker_rss_kib": max(
                    int(row.get("worker_rss_kib", 0)) for row in samples
                ),
                "l2_cache_misses": sum(
                    int(row.get("l2_cache_misses", 0)) for row in samples
                ),
                "median_l2_cache_miss_qps": round(
                    median(samples, "l2_cache_miss_qps"), 3
                ),
                "backend_fetches": sum(
                    int(row.get("backend_fetches", 0)) for row in samples
                ),
                "median_backend_fetch_qps": round(
                    median(samples, "backend_fetch_qps"), 3
                ),
                "membership_queries": sum(
                    int(row.get("membership_queries", 0)) for row in samples
                ),
                "proxy_injected_unavailable": sum(
                    int(row.get("proxy_injected_unavailable", 0)) for row in samples
                ),
                "retry_attempts": sum(
                    int(row.get("retry_attempts", 0)) for row in samples
                ),
                "retry_succeeded": sum(
                    int(row.get("retry_succeeded", 0)) for row in samples
                ),
            }
        )
    return output


def failure_impact(aggregates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keyed = {
        (row["scenario"], row["request_bytes"], row["concurrency"]): row
        for row in aggregates
    }
    output: list[dict[str, Any]] = []
    for (scenario, request_bytes, concurrency), failed in keyed.items():
        if scenario != "failure-1pct":
            continue
        baseline = keyed.get(("failure-baseline", request_bytes, concurrency))
        if baseline is None or baseline["median_success_qps"] == 0:
            continue
        drop = 100.0 * (
            1.0 - failed["median_success_qps"] / baseline["median_success_qps"]
        )
        output.append(
            {
                "type": "failure_impact",
                "request_bytes": request_bytes,
                "concurrency": concurrency,
                "baseline_median_qps": baseline["median_success_qps"],
                "failure_median_qps": failed["median_success_qps"],
                "throughput_drop_percent": round(drop, 3),
                "logical_errors": failed["logical_errors"],
            }
        )
    return output


def membership_phases(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = [row for row in rows if row.get("type") == "membership_event"]
    buckets = [
        row
        for row in rows
        if row.get("type") == "bucket" and row.get("scenario") == "membership"
    ]
    if not events or not buckets:
        return []
    event = events[-1]
    kill_ms = int(event["kill_after_measure_start_ms"])
    survivor_after = event.get("survivor_first_request_after_kill_ms")
    if survivor_after is None:
        return []
    recovery_ms = kill_ms + int(survivor_after)

    phases: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for bucket in buckets:
        begin = int(bucket.get("begin_ms", 0))
        end = int(bucket.get("end_ms", begin))
        if end <= kill_ms:
            phase = "before_failure"
        elif begin < recovery_ms:
            phase = "failure_window"
        else:
            phase = "after_recovery"
        phases[phase].append(bucket)

    output: list[dict[str, Any]] = []
    for phase in ("before_failure", "failure_window", "after_recovery"):
        selected = phases.get(phase, [])
        if not selected:
            continue
        output.append(
            {
                "type": "membership_phase",
                "phase": phase,
                "buckets": len(selected),
                "median_success_qps": round(
                    statistics.median(float(row["success_qps"]) for row in selected), 3
                ),
                "logical_errors": sum(
                    int(row.get("logical_errors", 0)) for row in selected
                ),
            }
        )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path)
    args = parser.parse_args()
    rows = load(args.jsonl)
    aggregates = aggregate(rows)
    derived = (
        aggregates
        + stack_aggregates(rows)
        + proxy_overhead(aggregates)
        + failure_impact(aggregates)
        + membership_phases(rows)
    )
    for row in derived:
        print(json.dumps(row, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
