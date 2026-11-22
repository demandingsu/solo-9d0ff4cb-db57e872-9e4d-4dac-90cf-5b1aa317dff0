"""拨款核对核心引擎。

数据流（按拨款日做两层内连接）::

    拨款表 grant ──(项目号, 拨款日 ∈ 归属有效期)──> 归属表 attribution
                 ──(学院码, 拨款日 ∈ 归口有效期)──> 归口表 jurisdiction

一条完整匹配路径 = (一条归属行, 一条归口行)。
- 路径数为 0：漏计，标明断在归属层还是归口层；
- 路径数 > 1：多计，实际入表次数即路径数；
- 同一统计单位的不同路径分别计入（菱形连接也要按路径数倍乘金额）。

关键约束：计算合计与次数时**不枚举全部匹配路径**。
归属有效期为 [起日, 止日)，归口同理（含起日、不含止日）。

批量核对按拨款日升序做一次“有效期扫描”：
- 底表每行只在起日进入活动集、在止日退出活动集各一次，整批复用；
- 活动集按“项目号 -> 学院 -> 条数”“学院 -> 统计单位 -> 条数”聚合，
  单笔拨款只需读取当日命中的学院及其统计单位；
- 同一拨款日、同一项目只计算一次学院/单位分布，供该日多笔拨款复用；
- 只有多计单需要展示示例路径时，才对至多 2 条路径做有界取样。
"""

from __future__ import annotations

import csv
import heapq
import io
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

# 各文件期望列名（顺序固定，首行表头必须完全一致）
GRANT_COLUMNS = ["单号", "项目号", "拨款日", "金额_分"]
ATTRIBUTION_COLUMNS = ["行号", "项目号", "学院码", "起日", "止日"]
JURISDICTION_COLUMNS = ["行号", "学院码", "统计单位", "起日", "止日"]

GRANT_FILE = "拨款表"
ATTRIBUTION_FILE = "归属表"
JURISDICTION_FILE = "归口表"


class InputError(Exception):
    """输入数据错误，errors 中每项为 (文件标识, 行号或 None, 说明)。"""

    def __init__(self, errors: List[Tuple[str, Optional[int], str]]):
        self.errors = errors
        super().__init__("; ".join(self.format(e) for e in errors))

    @staticmethod
    def format(item: Tuple[str, Optional[int], str]) -> str:
        label, line, msg = item
        return f"{label} 第{line}行: {msg}" if line else f"{label}: {msg}"


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AttributionRow:
    line_no: int            # 数据中的“行号”（业务行号，非 CSV 物理行）
    project: str
    college: str
    start: date
    end: date


@dataclass(frozen=True)
class JurisdictionRow:
    line_no: int
    college: str
    unit: str
    start: date
    end: date


@dataclass(frozen=True)
class Grant:
    grant_no: str
    project: str
    grant_date: date
    amount: int             # 分
    csv_line: int           # CSV 物理行号（含表头偏移），用于报错定位


@dataclass(frozen=True)
class MatchPath:
    """一条匹配路径：归属行 + 归口行。"""

    attribution_line: int
    jurisdiction_line: int
    college: str
    unit: str


@dataclass
class MissedGrant:
    grant_no: str
    project: str
    grant_date: date
    amount: int
    broken_layer: str       # "归属层" | "归口层"
    attribution_hits: int   # 命中的归属行数（归口层断裂时 > 0）


@dataclass
class OvercountedGrant:
    grant_no: str
    project: str
    grant_date: date
    amount: int
    path_count: int                       # 实际入表次数（完整路径总数）
    sample_paths: List[MatchPath]         # 至多 2 条示例路径
    unit_counts: Dict[str, int]           # 各统计单位的路径数


@dataclass
class GrantEntry:
    """每笔拨款的实际入表情况。"""

    grant_no: str
    project: str
    grant_date: date
    amount: int
    path_count: int
    status: str                           # 正常 / 漏计 / 多计


@dataclass
class RunStats:
    """核对过程中的底表访问统计，用于核对“整批复用底表”的实测口径。"""

    attribution_rows_indexed: int = 0
    jurisdiction_rows_indexed: int = 0
    attribution_sweep_steps: int = 0
    jurisdiction_sweep_steps: int = 0
    slice_reuses: int = 0
    sample_row_reads: int = 0

    @property
    def base_row_checks(self) -> int:
        """本次核对实际查看的底表行事件数（替代逐笔重扫历史行）。"""
        return self.attribution_sweep_steps + self.jurisdiction_sweep_steps


@dataclass
class UnitSlice:
    """某项目在某个拨款日命中的学院与统计单位分布（同日同项目可复用）。"""

    college_counts: Dict[str, int]        # 学院 -> 当日有效归属行数
    unit_counts: Dict[str, int]           # 统计单位 -> 路径数
    # 多计示例路径按需生成一次（该日该项目所有多计单共用）
    sample_paths: Optional[List["MatchPath"]] = None


@dataclass
class Report:
    total_grants: int
    normal_grants: int
    unit_totals: Dict[str, int]           # 统计单位 -> 实际合计（分）
    unit_path_counts: Dict[str, int]      # 统计单位 -> 实际入表次数
    missed: List[MissedGrant]
    overcounted: List[OvercountedGrant]
    entries: List[GrantEntry]
    grand_total: int                      # 所有统计单位合计（分）
    stats: RunStats = field(default_factory=RunStats)


# --------------------------------------------------------------------------- #
# CSV 读取与校验
# --------------------------------------------------------------------------- #
def _decode(raw: bytes, label: str) -> str:
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InputError([(label, None, f"文件不是合法的 UTF-8 编码（{exc.reason}）")])


def _parse_csv(raw: bytes, label: str, expected: List[str]) -> List[Dict[str, str]]:
    """通用 CSV 解析：校验表头、列数，返回带物理行号的字典列表。"""
    text = _decode(raw, label)
    if "\x00" in text:
        raise InputError([(label, None, "文件包含 NUL 字节，疑似不是 CSV 文本")])
    reader = csv.reader(io.StringIO(text))
    try:
        rows = list(reader)
    except csv.Error as exc:
        raise InputError([(label, None, f"CSV 解析失败: {exc}")])

    if not rows or all(not any(cell.strip() for cell in r) for r in rows):
        raise InputError([(label, None, "文件为空或没有数据行")])

    header = [c.strip() for c in rows[0]]
    if header != expected:
        raise InputError(
            [(label, 1, f"表头应为 {','.join(expected)}，实际为 {','.join(header) or '(空)'}")]
        )

    records: List[Dict[str, str]] = []
    for physical, row in enumerate(rows[1:], start=2):
        if not any(cell.strip() for cell in row):
            continue  # 跳过纯空行
        if len(row) != len(expected):
            raise InputError(
                [(label, physical, f"列数应为 {len(expected)}，实际为 {len(row)}")]
            )
        record = {col: row[i].strip() for i, col in enumerate(expected)}
        record["_csv_line"] = physical  # type: ignore[assignment]
        records.append(record)
    if not records:
        raise InputError([(label, None, "只有表头，没有数据行")])
    return records


def _parse_date(value: str, label: str, line: int, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise InputError(
            [(label, line, f"{field_name} 日期格式应为 YYYY-MM-DD，实际为 '{value}'")]
        )


def _parse_int(value: str, label: str, line: int, field_name: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise InputError(
            [(label, line, f"{field_name} 应为整数，实际为 '{value}'")]
        )


def _require_text(value: str, label: str, line: int, field_name: str) -> str:
    if value == "":
        raise InputError([(label, line, f"{field_name} 不能为空")])
    return value


def parse_grants(raw: bytes, label: str = GRANT_FILE) -> List[Grant]:
    records = _parse_csv(raw, label, GRANT_COLUMNS)
    grants: List[Grant] = []
    seen: Dict[str, int] = {}
    errors: List[Tuple[str, Optional[int], str]] = []
    for rec in records:
        line = rec["_csv_line"]  # type: ignore[assignment]
        try:
            grant_no = _require_text(rec["单号"], label, line, "单号")
            project = _require_text(rec["项目号"], label, line, "项目号")
            grant_date = _parse_date(rec["拨款日"], label, line, "拨款日")
            amount = _parse_int(rec["金额_分"], label, line, "金额_分")
            if amount < 0:
                errors.append((label, line, f"金额_分不能为负数，实际为 {amount}"))
                continue
            if grant_no in seen:
                errors.append(
                    (label, line, f"单号 '{grant_no}' 重复，首次出现在第 {seen[grant_no]} 行")
                )
                continue
            seen[grant_no] = line
            grants.append(Grant(grant_no, project, grant_date, amount, line))
        except InputError as exc:
            errors.extend(exc.errors)
    if errors:
        raise InputError(errors)
    return grants


def _parse_interval_table(
    raw: bytes,
    label: str,
    columns: List[str],
) -> Tuple[List[dict], List[Tuple[str, Optional[int], str]]]:
    """解析归属/归口表，返回 (记录, 错误)，行号唯一性由调用方按表校验。"""
    records = _parse_csv(raw, label, columns)
    errors: List[Tuple[str, Optional[int], str]] = []
    parsed: List[dict] = []
    seen: Dict[int, int] = {}
    for rec in records:
        line = rec["_csv_line"]  # type: ignore[assignment]
        try:
            line_no = _parse_int(rec["行号"], label, line, "行号")
            if line_no in seen:
                errors.append(
                    (label, line, f"行号 {line_no} 重复，首次出现在第 {seen[line_no]} 行")
                )
                continue
            seen[line_no] = line
            start = _parse_date(rec["起日"], label, line, "起日")
            end = _parse_date(rec["止日"], label, line, "止日")
            if not start < end:
                errors.append(
                    (label, line, f"有效期非法：起日 {start} 必须早于止日 {end}（含起不含止）")
                )
                continue
            parsed.append({"rec": rec, "line": line, "line_no": line_no,
                           "start": start, "end": end})
        except InputError as exc:
            errors.extend(exc.errors)
    return parsed, errors


def parse_attributions(raw: bytes, label: str = ATTRIBUTION_FILE) -> List[AttributionRow]:
    parsed, errors = _parse_interval_table(raw, label, ATTRIBUTION_COLUMNS)
    rows: List[AttributionRow] = []
    for item in parsed:
        rec, line = item["rec"], item["line"]
        try:
            project = _require_text(rec["项目号"], label, line, "项目号")
            college = _require_text(rec["学院码"], label, line, "学院码")
            rows.append(
                AttributionRow(item["line_no"], project, college, item["start"], item["end"])
            )
        except InputError as exc:
            errors.extend(exc.errors)
    if errors:
        raise InputError(errors)
    return rows


def parse_jurisdictions(raw: bytes, label: str = JURISDICTION_FILE) -> List[JurisdictionRow]:
    parsed, errors = _parse_interval_table(raw, label, JURISDICTION_COLUMNS)
    rows: List[JurisdictionRow] = []
    for item in parsed:
        rec, line = item["rec"], item["line"]
        try:
            college = _require_text(rec["学院码"], label, line, "学院码")
            unit = _require_text(rec["统计单位"], label, line, "统计单位")
            rows.append(
                JurisdictionRow(item["line_no"], college, unit, item["start"], item["end"])
            )
        except InputError as exc:
            errors.extend(exc.errors)
    if errors:
        raise InputError(errors)
    return rows


# --------------------------------------------------------------------------- #
# 有效期扫描与两层连接
# --------------------------------------------------------------------------- #
def _sweep_events(
    rows: Iterable[Tuple[date, date, int]],
) -> List[Tuple[date, int, int]]:
    """把区间行转成 (日期, 序号, +1/-1) 事件并按日期排序。

    每行只在起日加入、止日退出各一次，整批拨款共用这一次扫描。
    """
    events: List[Tuple[date, int, int]] = []
    for start, end, seq in rows:
        events.append((start, seq, 1))
        events.append((end, seq, -1))
    events.sort(key=lambda e: (e[0], e[1]))
    return events


class _ActiveRows:
    """当日有效行的上传序号，按需按上传行序惰性遍历。

    加入/移除均为 O(log n)/O(1)；失效序号留在堆里，遍历时顺手丢弃。
    只有需要展示示例路径时才遍历，取够两条即停。

    遍历是一个上下文管理器：提前 break 时也会立即把已弹出但未消费的
    有效序号放回，避免依赖垃圾回收时机。
    """

    __slots__ = ("_heap", "_alive", "_pending")

    def __init__(self) -> None:
        self._heap: List[int] = []
        self._alive: set = set()
        self._pending: List[int] = []

    def add(self, seq: int) -> None:
        self._alive.add(seq)
        heapq.heappush(self._heap, seq)

    def remove(self, seq: int) -> None:
        self._alive.discard(seq)

    def __enter__(self) -> "_ActiveRows":
        return self

    def __exit__(self, *_exc) -> None:
        for seq in self._pending:
            heapq.heappush(self._heap, seq)
        self._pending.clear()

    def __iter__(self):
        """按上传行序产出仍有效的序号；须在 with 块中使用。"""
        while self._heap:
            seq = heapq.heappop(self._heap)
            if seq in self._alive:
                self._pending.append(seq)
                yield seq


def reconcile(
    grants_raw: bytes,
    attributions_raw: bytes,
    jurisdictions_raw: bytes,
    stats: Optional[RunStats] = None,
) -> Report:
    """执行两层内连接核对（按拨款日批量扫描有效期）。

    底表每行只在起日/止日各处理一次，活动集按项目→学院→条数、
    学院→统计单位→条数聚合；单笔拨款只读取当日命中的学院与统计单位，
    不逐笔重扫历史行，也不展开归属×归口的完整匹配路径。

    ``stats`` 可传入 RunStats 收集底表访问次数（用于前后对比与自测）。
    """
    # 三个文件一起解析，汇总全部错误后一次性退回
    errors: List[Tuple[str, Optional[int], str]] = []
    grants: List[Grant] = []
    attributions: List[AttributionRow] = []
    jurisdictions: List[JurisdictionRow] = []
    for raw, parser, sink in (
        (grants_raw, parse_grants, grants),
        (attributions_raw, parse_attributions, attributions),
        (jurisdictions_raw, parse_jurisdictions, jurisdictions),
    ):
        try:
            sink.extend(parser(raw))
        except InputError as exc:
            errors.extend(exc.errors)
    if errors:
        raise InputError(errors)

    stats = stats if stats is not None else RunStats()
    stats.attribution_rows_indexed = len(attributions)
    stats.jurisdiction_rows_indexed = len(jurisdictions)

    # 上传行序 = 解析顺序；示例路径必须按此顺序取前两条
    attr_events = _sweep_events(
        (row.start, row.end, seq) for seq, row in enumerate(attributions)
    )
    jur_events = _sweep_events(
        (row.start, row.end, seq) for seq, row in enumerate(jurisdictions)
    )

    # 活动集聚合：项目 -> 学院 -> 条数；学院 -> 统计单位 -> 条数
    attr_active: Dict[str, Dict[str, int]] = {}
    attr_active_rows: Dict[str, _ActiveRows] = {}   # 项目 -> 当日有效归属行
    jur_active: Dict[str, Dict[str, int]] = {}
    jur_active_rows: Dict[str, _ActiveRows] = {}    # 学院 -> 当日有效归口行

    def add_attr(seq: int) -> None:
        row = attributions[seq]
        colleges = attr_active.setdefault(row.project, {})
        colleges[row.college] = colleges.get(row.college, 0) + 1
        active = attr_active_rows.get(row.project)
        if active is None:
            active = attr_active_rows[row.project] = _ActiveRows()
        active.add(seq)

    def remove_attr(seq: int) -> None:
        row = attributions[seq]
        colleges = attr_active[row.project]
        left = colleges[row.college] - 1
        if left:
            colleges[row.college] = left
        else:
            del colleges[row.college]
        attr_active_rows[row.project].remove(seq)

    def add_jur(seq: int) -> None:
        row = jurisdictions[seq]
        units = jur_active.setdefault(row.college, {})
        units[row.unit] = units.get(row.unit, 0) + 1
        active = jur_active_rows.get(row.college)
        if active is None:
            active = jur_active_rows[row.college] = _ActiveRows()
        active.add(seq)

    def remove_jur(seq: int) -> None:
        row = jurisdictions[seq]
        units = jur_active[row.college]
        left = units[row.unit] - 1
        if left:
            units[row.unit] = left
        else:
            del units[row.unit]
        jur_active_rows[row.college].remove(seq)

    unit_totals: Dict[str, int] = {}
    unit_path_counts: Dict[str, int] = {}
    missed: List[MissedGrant] = []
    overcounted: List[OvercountedGrant] = []
    entries: List[GrantEntry] = []
    normal = 0

    # 按拨款日升序扫描；同一拨款日只推进一次事件指针
    ordered = sorted(grants, key=lambda g: (g.grant_date, g.grant_no))
    attr_pos = jur_pos = 0
    current_day: Optional[date] = None
    slices: Dict[str, UnitSlice] = {}   # 当日 项目号 -> 命中分布

    for grant in ordered:
        day = grant.grant_date
        if day != current_day:
            current_day = day
            slices.clear()
            while attr_pos < len(attr_events) and attr_events[attr_pos][0] <= day:
                _, seq, delta = attr_events[attr_pos]
                stats.attribution_sweep_steps += 1
                if delta == 1:
                    add_attr(seq)
                else:
                    remove_attr(seq)
                attr_pos += 1
            while jur_pos < len(jur_events) and jur_events[jur_pos][0] <= day:
                _, seq, delta = jur_events[jur_pos]
                stats.jurisdiction_sweep_steps += 1
                if delta == 1:
                    add_jur(seq)
                else:
                    remove_jur(seq)
                jur_pos += 1

        slice_ = slices.get(grant.project)
        if slice_ is None:
            college_counts = dict(attr_active.get(grant.project, {}))
            unit_counts: Dict[str, int] = {}
            for college, n_attr in college_counts.items():
                for unit, n_jur in jur_active.get(college, {}).items():
                    unit_counts[unit] = unit_counts.get(unit, 0) + n_attr * n_jur
            slice_ = UnitSlice(college_counts, unit_counts)
            slices[grant.project] = slice_
        else:
            stats.slice_reuses += 1

        if not slice_.college_counts:
            missed.append(
                MissedGrant(
                    grant.grant_no, grant.project, grant.grant_date,
                    grant.amount, "归属层", 0,
                )
            )
            entries.append(
                GrantEntry(
                    grant.grant_no, grant.project, grant.grant_date,
                    grant.amount, 0, "漏计",
                )
            )
            continue

        total_paths = sum(slice_.unit_counts.values())
        if total_paths == 0:
            missed.append(
                MissedGrant(
                    grant.grant_no, grant.project, grant.grant_date,
                    grant.amount, "归口层", sum(slice_.college_counts.values()),
                )
            )
            entries.append(
                GrantEntry(
                    grant.grant_no, grant.project, grant.grant_date,
                    grant.amount, 0, "漏计",
                )
            )
            continue

        per_unit = slice_.unit_counts
        for unit, paths in per_unit.items():
            unit_path_counts[unit] = unit_path_counts.get(unit, 0) + paths
            unit_totals[unit] = unit_totals.get(unit, 0) + paths * grant.amount

        entries.append(
            GrantEntry(
                grant.grant_no, grant.project, grant.grant_date,
                grant.amount, total_paths,
                "正常" if total_paths == 1 else "多计",
            )
        )
        if total_paths == 1:
            normal += 1
            continue

        # 多计：按上传行序取至多 2 条示例路径（有界取样，该日该项目只取一次）
        if slice_.sample_paths is None:
            sample: List[MatchPath] = []
            active_attrs = attr_active_rows.get(grant.project)
            if active_attrs is not None:
                with active_attrs:
                    for attr_seq in active_attrs:
                        if len(sample) >= 2:
                            break
                        stats.sample_row_reads += 1
                        attr = attributions[attr_seq]
                        active_jurs = jur_active_rows.get(attr.college)
                        if active_jurs is None:
                            continue
                        with active_jurs:
                            for jur_seq in active_jurs:
                                if len(sample) >= 2:
                                    break
                                stats.sample_row_reads += 1
                                jur = jurisdictions[jur_seq]
                                sample.append(
                                    MatchPath(attr.line_no, jur.line_no,
                                              attr.college, jur.unit)
                                )
            slice_.sample_paths = sample

        overcounted.append(
            OvercountedGrant(
                grant.grant_no, grant.project, grant.grant_date,
                grant.amount, total_paths, list(slice_.sample_paths),
                dict(sorted(per_unit.items())),
            )
        )

    missed.sort(key=lambda m: m.grant_no)
    overcounted.sort(key=lambda o: o.grant_no)
    entries.sort(key=lambda e: e.grant_no)
    return Report(
        total_grants=len(grants),
        normal_grants=normal,
        unit_totals=dict(sorted(unit_totals.items())),
        unit_path_counts=dict(sorted(unit_path_counts.items())),
        missed=missed,
        overcounted=overcounted,
        entries=entries,
        grand_total=sum(unit_totals.values()),
        stats=stats,
    )
