"""
Venue 名称补全脚本

扫描 DBLP NT.gz 文件，为 nodes_venue.csv 中 name 为空的记录填充名称。

名称来源优先级（从高到低）:
  1. NT 文件 /streams/ 节点的 rdfsLabel / title（最权威，全称）
  2. NT 文件 /rec/ publication 的 publishedIn / publishedInBook（聚合最常出现值）
  3. venue_key 末段大写作为缩写兜底（如 conf/cvpr → CVPR）

用法:
    python data/scripts/patch_venue_names.py
    python data/scripts/patch_venue_names.py \\
        --input  data/raw/dblp-2026-04-01.nt.gz \\
        --venue  data/processed/neo4j/nodes_venue.csv
"""

import argparse
import csv
import gzip
import re
import sys
import time
from collections import Counter
from pathlib import Path

# ---------------------------------------------------------------------------
# 路径常量
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_NT   = REPO_ROOT / "data" / "raw" / "dblp-2026-04-01.nt.gz"
DEFAULT_CSV  = REPO_ROOT / "data" / "processed" / "neo4j" / "nodes_venue.csv"

SEP = "|"

# ---------------------------------------------------------------------------
# 正则
# ---------------------------------------------------------------------------

# venue_key 提取
_STREAM_RE  = re.compile(r"dblp\.org/streams/((conf|journals)/[^/\s>]+)")
_TOC_RE     = re.compile(r"dblp\.org/db/((conf|journals)/[^/\s>]+)")
_STREAM_SUBJ_RE = re.compile(r"<https://dblp\.org/streams/((conf|journals)/[^/\s>]+)>")

# 谓词 URI 快速检测
_RDFS_LABEL  = "http://www.w3.org/2000/01/rdf-schema#label"
_FOAF_NAME   = "http://xmlns.com/foaf/0.1/name"
_DBLP_TITLE  = "https://dblp.org/rdf/schema#title"
_PUB_IN      = "https://dblp.org/rdf/schema#publishedIn"
_PUB_IN_BOOK = "https://dblp.org/rdf/schema#publishedInBook"
_PUB_STREAM  = "https://dblp.org/rdf/schema#publishedInStream"
_TOC_PAGE    = "https://dblp.org/rdf/schema#listedOnTocPage"

# 字面值提取
_LIT_RE = re.compile(r'^"((?:[^"\\]|\\.)*)"')


def _extract_literal(obj_str: str) -> str:
    """从 NT 对象字符串中提取字面值（去掉引号和类型标注）。"""
    obj_str = obj_str.strip()
    m = _LIT_RE.match(obj_str)
    if not m:
        return ""
    raw = m.group(1)
    return raw.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", " ").replace("\\t", " ")


def _extract_uri(obj_str: str) -> str:
    """从 NT 对象字符串中提取 URI（去掉尖括号）。"""
    obj_str = obj_str.strip()
    if obj_str.startswith("<") and obj_str.endswith(">"):
        return obj_str[1:-1]
    return ""


def _venue_key_from_stream_uri(uri: str) -> str | None:
    """从 stream URI 提取 venue_key（conf/xxx 或 journals/xxx）。"""
    m = _STREAM_SUBJ_RE.match(f"<{uri}>")
    if m:
        return m.group(1)
    m = _STREAM_RE.search(uri)
    return m.group(1) if m else None


def _venue_key_from_toc(uri: str) -> str | None:
    """从 listedOnTocPage URI 提取 venue_key（取前两段）。"""
    m = _TOC_RE.search(uri)
    if not m:
        return None
    # conf/cvpr/cvpr2023 → conf/cvpr
    parts = m.group(1).split("/")
    return f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else None


def _abbrev_from_key(venue_key: str) -> str:
    """从 venue_key 生成缩写名（最后一段大写，保留连字符）。

    示例:
        conf/cvpr     → CVPR
        journals/tpami → TPAMI
        conf/acl-dmtw → ACL-DMTW
    """
    last = venue_key.split("/")[-1]
    return last.upper()


# ---------------------------------------------------------------------------
# 两遍扫描：收集 venue 名称
# ---------------------------------------------------------------------------

def build_venue_name_map(nt_gz_path: Path) -> dict[str, str]:
    """扫描 NT.gz，构建 {venue_key: best_name} 映射。

    策略:
      - stream_names[vk]   : 来自 /streams/ rdfsLabel / title（最权威）
      - pub_name_cnt[vk]   : 来自 publication publishedIn（Counter 取最多）

    两遍扫描确保 stream 名（文件尾 565M 行）能覆盖 publication 名。

    Args:
        nt_gz_path: DBLP NT.gz 文件路径。

    Returns:
        {venue_key: best_name} 字典。
    """
    stream_names: dict[str, str] = {}
    pub_name_cnt: dict[str, Counter] = {}

    t0 = time.time()
    print("[scan] 开始扫描 NT 文件（单遍，同时收集 publication 名和 stream 名）…", flush=True)

    # 简单状态机：跟踪当前 subject 类型和属性
    cur_subj = ""
    cur_is_stream = False
    cur_venue_key = ""      # 当前 stream 的 venue_key
    cur_stream_name = ""    # 当前 stream 的名称候选

    # publication 侧：venue_key + publishedIn 需要在同一 pub subject 里同时出现
    cur_pub_venue_key = ""  # 当前 pub 收集到的 venue_key
    cur_pub_name = ""       # 当前 pub 收集到的 publishedIn 名

    cnt_lines = 0

    with gzip.open(nt_gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for raw in f:
            cnt_lines += 1
            if cnt_lines % 10_000_000 == 0:
                elapsed = time.time() - t0
                print(
                    f"[scan] {cnt_lines // 1_000_000}M 行 | "
                    f"stream 名 {len(stream_names):,} | "
                    f"pub 名 {len(pub_name_cnt):,} | "
                    f"{elapsed:.0f}s",
                    flush=True,
                )

            line = raw.rstrip()
            if not line or line.startswith("#"):
                continue

            # 三元组结构：<subj> <pred> obj .
            if not line.endswith(" .") and not line.endswith("\t."):
                # 兜底：末尾直接是 .
                if not line.endswith("."):
                    continue
            line_stripped = line[:-1].rstrip()  # 去掉 ' .'

            # 快速分割 subject
            if not line_stripped.startswith("<"):
                continue
            try:
                gt1 = line_stripped.index(">")
            except ValueError:
                continue
            subj = line_stripped[1:gt1]

            # -------------------------------------------------------
            # 当 subject 切换时，flush 上一个 subject
            # -------------------------------------------------------
            if subj != cur_subj:
                # flush stream
                if cur_is_stream and cur_venue_key and cur_stream_name:
                    if cur_venue_key not in stream_names:
                        stream_names[cur_venue_key] = cur_stream_name
                # flush pub
                if cur_pub_venue_key and cur_pub_name:
                    pub_name_cnt.setdefault(cur_pub_venue_key, Counter())[cur_pub_name] += 1

                cur_subj = subj
                if "dblp.org/streams/" in subj:
                    cur_is_stream = True
                    cur_venue_key = _venue_key_from_stream_uri(subj) or ""
                    cur_stream_name = ""
                else:
                    cur_is_stream = False
                    cur_venue_key = ""
                cur_pub_venue_key = ""
                cur_pub_name = ""

            rest = line_stripped[gt1 + 1:].lstrip()
            if not rest.startswith("<"):
                continue
            try:
                gt2 = rest.index(">")
            except ValueError:
                continue
            pred = rest[1:gt2]
            obj_str = rest[gt2 + 1:].strip()

            # -------------------------------------------------------
            # Stream 节点：收集 rdfsLabel / title
            # -------------------------------------------------------
            if cur_is_stream and cur_venue_key:
                if pred in (_RDFS_LABEL, _DBLP_TITLE, _FOAF_NAME):
                    name = _extract_literal(obj_str)
                    if name and not cur_stream_name:
                        cur_stream_name = name

            # -------------------------------------------------------
            # Publication 节点：收集 publishedIn + venue_key
            # -------------------------------------------------------
            elif "dblp.org/rec/" in subj:
                if pred == _PUB_IN or pred == _PUB_IN_BOOK:
                    name = _extract_literal(obj_str)
                    if name and not cur_pub_name:
                        cur_pub_name = name
                elif pred == _PUB_STREAM:
                    uri = _extract_uri(obj_str)
                    # publishedInStream URI 格式: https://dblp.org/streams/conf/cvpr
                    m = re.search(r"dblp\.org/streams/((conf|journals)/[^/\s>]+)", uri)
                    if m:
                        cur_pub_venue_key = m.group(1)
                elif pred == _TOC_PAGE:
                    uri = _extract_uri(obj_str)
                    vk = _venue_key_from_toc(uri)
                    if vk and not cur_pub_venue_key:
                        cur_pub_venue_key = vk

    # 处理最后一个 subject
    if cur_is_stream and cur_venue_key and cur_stream_name:
        stream_names.setdefault(cur_venue_key, cur_stream_name)
    if cur_pub_venue_key and cur_pub_name:
        pub_name_cnt.setdefault(cur_pub_venue_key, Counter())[cur_pub_name] += 1

    elapsed = time.time() - t0
    print(
        f"[scan] 完成: {cnt_lines:,} 行 | "
        f"stream 名 {len(stream_names):,} | "
        f"pub 名 {len(pub_name_cnt):,} | 耗时 {elapsed:.1f}s",
        flush=True,
    )

    # 合并：stream 名优先，其次 pub 最常出现名
    merged: dict[str, str] = {}
    for vk, cnt_map in pub_name_cnt.items():
        merged[vk] = cnt_map.most_common(1)[0][0]
    for vk, name in stream_names.items():
        merged[vk] = name  # stream 名覆盖 pub 名

    print(f"[scan] 合并后共 {len(merged):,} 个 venue 有名称（stream 优先）")
    return merged


# ---------------------------------------------------------------------------
# Patch CSV
# ---------------------------------------------------------------------------

def patch_csv(csv_path: Path, name_map: dict[str, str]) -> None:
    """读取 venue CSV，补全 name 列，写回原文件。

    补全策略:
      1. name_map 中有对应 venue_key → 使用该名称
      2. 否则 → venue_key 末段大写作为缩写（如 conf/cvpr → CVPR）

    Args:
        csv_path: nodes_venue.csv 路径。
        name_map: {venue_key: name} 映射。
    """
    rows: list[list[str]] = []
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=SEP)
        header = next(reader)
        for row in reader:
            rows.append(row)

    # 确定各列索引
    try:
        name_idx = header.index("name")
        key_idx  = header.index("venue_key")
    except ValueError:
        print("[patch] 错误: CSV 缺少 name 或 venue_key 列，退出。")
        sys.exit(1)

    filled_from_map = 0
    filled_from_abbrev = 0

    for row in rows:
        if len(row) <= name_idx:
            row.extend([""] * (name_idx + 1 - len(row)))

        if row[name_idx].strip():
            continue  # 已有名称，跳过

        venue_key = row[key_idx].strip() if key_idx < len(row) else ""
        if not venue_key:
            continue

        # 优先使用扫描到的名称
        if venue_key in name_map:
            row[name_idx] = name_map[venue_key]
            filled_from_map += 1
        else:
            # 兜底：缩写
            row[name_idx] = _abbrev_from_key(venue_key)
            filled_from_abbrev += 1

    # 写回
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=SEP, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        writer.writerows(rows)

    print(
        f"[patch] 完成: {filled_from_map:,} 条使用扫描名称, "
        f"{filled_from_abbrev:,} 条使用缩写兜底。"
    )
    print(f"[patch] CSV 已更新: {csv_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="补全 nodes_venue.csv 的 name 列",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--input",  default=str(DEFAULT_NT),  help="DBLP NT.gz 路径")
    parser.add_argument("--venue",  default=str(DEFAULT_CSV), help="nodes_venue.csv 路径")
    parser.add_argument(
        "--abbrev-only",
        action="store_true",
        help="跳过 NT 文件扫描，仅用缩写兜底（极快，但名称不完整）",
    )
    args = parser.parse_args()

    csv_path = Path(args.venue)
    if not csv_path.exists():
        print(f"错误: 找不到 {csv_path}")
        sys.exit(1)

    if args.abbrev_only:
        print("[patch] 仅使用缩写模式（--abbrev-only）")
        patch_csv(csv_path, {})
        return

    nt_path = Path(args.input)
    if not nt_path.exists():
        print(f"错误: 找不到 NT 文件 {nt_path}")
        sys.exit(1)

    name_map = build_venue_name_map(nt_path)
    patch_csv(csv_path, name_map)


if __name__ == "__main__":
    main()
