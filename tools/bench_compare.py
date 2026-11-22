"""优化前后对比基准：同一批数据分别跑基线引擎（逐笔扫描历史行）与现行引擎。

用法::

    python3 -I tools/bench_compare.py

数据特征：拨款日各异（约 2900 个不同日期）、归属/归口有效期大量重叠，
用于观察“底表行检查次数”和耗时的前后差异。基线引擎为优化前的原始实现，
放在 tools/baseline_reconcile.py，仅用于对比。
"""

from __future__ import annotations

import importlib.util
import os
import sys
import time
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from app.reconcile import RunStats, reconcile as new_reconcile  # noqa: E402


def _load_baseline():
    path = os.path.join(HERE, "baseline_reconcile.py")
    spec = importlib.util.spec_from_file_location("baseline_reconcile", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["baseline_reconcile"] = mod
    spec.loader.exec_module(mod)
    return mod


def csv_bytes(rows):
    return ("\n".join(",".join(str(c) for c in r) for r in rows) + "\n").encode()


def build_dataset():
    base = date(2016, 1, 1)
    grants = [["单号", "项目号", "拨款日", "金额_分"]]
    attrs = [["行号", "项目号", "学院码", "起日", "止日"]]
    jurs = [["行号", "学院码", "统计单位", "起日", "止日"]]

    n_projects, n_colleges = 60, 20
    line = 1
    for p in range(n_projects):
        for k in range(80):
            start = base + timedelta(days=(k * 31 + p * 11) % 3300)
            end = start + timedelta(days=1825 + (k % 9) * 90)
            attrs.append([line, f"P{p}", f"C{(p + k) % n_colleges}",
                          start.isoformat(), end.isoformat()])
            line += 1
    line = 100000
    for c in range(n_colleges):
        for k in range(60):
            start = base + timedelta(days=(k * 47 + c * 13) % 3300)
            end = start + timedelta(days=1825 + (k % 6) * 120)
            jurs.append([line, f"C{c}", f"U{(c * 5 + k) % 40}",
                         start.isoformat(), end.isoformat()])
            line += 1

    gid = 1
    for d in range(2900):
        day = base + timedelta(days=d)
        for k in range(3):
            if (d + k) % 11 == 0:
                continue
            p = (d * 7 + k * 17) % n_projects
            grants.append([f"G{gid:06d}", f"P{p}", day.isoformat(), 1000 + gid])
            gid += 1
    return csv_bytes(grants), csv_bytes(attrs), csv_bytes(jurs)


def _fingerprint(report):
    return (
        report.total_grants, report.normal_grants, report.grand_total,
        sorted(report.unit_totals.items()),
        sorted(report.unit_path_counts.items()),
        [(m.grant_no, m.broken_layer, m.attribution_hits) for m in report.missed],
        [(o.grant_no, o.path_count, sorted(o.unit_counts.items()))
         for o in report.overcounted],
        [(e.grant_no, e.path_count, e.status) for e in report.entries],
    )


def main():
    baseline = _load_baseline()
    grants, attrs, jurs = build_dataset()

    orig_active = baseline._active
    scanned = {"rows": 0, "calls": 0}

    def counting_active(rows, day):
        scanned["rows"] += len(rows)
        scanned["calls"] += 1
        return orig_active(rows, day)

    baseline._active = counting_active
    started = time.perf_counter()
    old = baseline.reconcile(grants, attrs, jurs)
    old_seconds = time.perf_counter() - started

    stats = RunStats()
    started = time.perf_counter()
    new = new_reconcile(grants, attrs, jurs, stats=stats)
    new_seconds = time.perf_counter() - started

    print(f"输入：拨款 {new.total_grants} 笔 / 归属 {stats.attribution_rows_indexed} 行 / "
          f"归口 {stats.jurisdiction_rows_indexed} 行 / 拨款日各异")
    print("报表一致:", _fingerprint(old) == _fingerprint(new))
    print(f"优化前：{old_seconds:6.2f}s，底表行检查 {scanned['rows']:,} 次"
          f"（逐笔扫描 {scanned['calls']:,} 次）")
    print(f"优化后：{new_seconds:6.2f}s，底表行检查 {stats.base_row_checks:,} 次"
          f"（归属 {stats.attribution_sweep_steps:,} + 归口 {stats.jurisdiction_sweep_steps:,}）")
    print(f"降幅：行检查 {scanned['rows'] / stats.base_row_checks:.0f}x，"
          f"耗时 {old_seconds / new_seconds:.1f}x")


if __name__ == "__main__":
    main()
