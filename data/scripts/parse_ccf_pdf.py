"""
CCF 第七版 PDF 解析脚本

直接从官方 PDF 中提取 DBLP URL，无需调用任何外部 API，
生成 {dblp_key_prefix: ccf_rank} 映射表供 ingest_dblp.py 使用。

解析原理:
  PDF 每行携带 DBLP URL（如 dblp.uni-trier.de/db/conf/cvpr/），
  通过正则直接提取 conf/xxx 或 journals/xxx 前缀，
  配合段落标题识别当前评级（A/B/C）和类型（会议/期刊）。

用法:
    # 解析并生成映射表（默认输出到 data/raw/ccf_catalogue.json）
    python parse_ccf_pdf.py

    # 指定 PDF 路径和输出路径
    python parse_ccf_pdf.py --pdf /path/to/ccf.pdf --output /path/to/out.json

    # 诊断模式：打印所有解析到的条目（不写文件）
    python parse_ccf_pdf.py --dry-run

    # 验证与 ES 对齐率
    python parse_ccf_pdf.py --verify --es-host http://49.52.27.139:9200
"""

import argparse
import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------

REPO_ROOT   = Path(__file__).resolve().parents[2]
DEFAULT_PDF = REPO_ROOT / "data" / "raw" / "第七版中国计算机学会推荐国际学术会议和期刊目录（正式版）.pdf"
DEFAULT_OUT = REPO_ROOT / "data" / "raw" / "ccf_catalogue.json"

# ---------------------------------------------------------------------------
# 正则：从 DBLP URL 中提取 conf/xxx 或 journals/xxx 前缀
# 示例:
#   http://dblp.uni-trier.de/db/conf/cvpr/        -> conf/cvpr
#   http://dblp.uni-trier.de/db/journals/tpami/   -> journals/tpami
#   https://dblp.uni-trier.de/db/conf/nips/index.html -> conf/nips
# ---------------------------------------------------------------------------
DBLP_URL_RE = re.compile(
    r"dblp\.uni-trier\.de/db/((?:conf|journals)/[\w-]+)"
)

# 评级段落标题识别（容忍空格变体）
RANK_RE = re.compile(r"[一二三四五]、\s*([ABC])\s*类")

# 类型页面标题识别
TYPE_JOURNAL_RE  = re.compile(r"推荐国际学术期刊")
TYPE_CONF_RE     = re.compile(r"推荐国际学术会议")

# ---------------------------------------------------------------------------
# DBLP 已知历史别名/重命名补丁
# 原因：官方 PDF 列出旧 DBLP URL，但 DBLP 后来为该会议/期刊建了新路径
# key=PDF 中提取的前缀, value=list[等价的新前缀]
# ---------------------------------------------------------------------------
DBLP_ALIAS_PATCH: dict[str, list[str]] = {
    # NeurIPS: 2018 年前论文在 conf/nips，2018 后新建 conf/neurips
    "conf/nips":    ["conf/neurips"],
    # VLDB Journal: DBLP 同时维护 journals/vldb 和 journals/pvldb
    "journals/vldb": ["journals/pvldb"],
    # ECML/PKDD: 会议和期刊论文分别在两个路径
    "conf/pkdd":    ["conf/ecml"],
    "conf/ecml":    ["conf/pkdd"],
    # USENIX Security: 部分年份在 conf/uss，部分在 conf/usenixsec
    "conf/uss":     ["conf/usenixsec"],
    # USENIX ATC: 部分年份路径不同
    "conf/usenix":  ["conf/atc"],
}

# ---------------------------------------------------------------------------
# 核心解析
# ---------------------------------------------------------------------------


def parse_pdf(pdf_path: Path) -> dict[str, str]:
    """解析 CCF PDF，返回 {dblp_prefix: rank} 映射。

    Args:
        pdf_path: CCF PDF 文件路径。

    Returns:
        映射字典，key 如 "conf/cvpr"，value 为 "A"/"B"/"C"。
    """
    try:
        import pdfplumber
    except ImportError:
        print("❌ 需要安装 pdfplumber: uv add pdfplumber")
        sys.exit(1)

    ccf_map: dict[str, str] = {}
    conflicts: list[str] = []

    # 全局状态：跨页持久
    current_rank: str | None = None        # "A" / "B" / "C"
    current_type: str | None = None        # "journal" / "conference"

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"[parse] 共 {total_pages} 页，开始解析...")

        for page_idx, page in enumerate(pdf.pages):
            raw_text = page.extract_text() or ""
            lines = raw_text.splitlines()

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # --- 检测类型切换（期刊 / 会议）---
                if TYPE_JOURNAL_RE.search(line):
                    current_type = "journal"
                    continue
                if TYPE_CONF_RE.search(line):
                    current_type = "conference"
                    continue

                # --- 检测评级切换（A / B / C）---
                rank_m = RANK_RE.search(line)
                if rank_m:
                    current_rank = rank_m.group(1)   # "A" / "B" / "C"
                    continue

                # --- 提取 DBLP URL 并解析前缀 ---
                url_m = DBLP_URL_RE.search(line)
                if url_m and current_rank and current_type:
                    prefix = url_m.group(1).rstrip("/")  # e.g. "conf/cvpr"

                    # 类型一致性校验
                    if current_type == "journal" and not prefix.startswith("journals"):
                        print(f"  ⚠️  类型不符（期刊节但前缀为 {prefix}），已跳过")
                        continue
                    if current_type == "conference" and not prefix.startswith("conf"):
                        print(f"  ⚠️  类型不符（会议节但前缀为 {prefix}），已跳过")
                        continue

                    # 冲突检测
                    if prefix in ccf_map:
                        existing = ccf_map[prefix]
                        if existing != current_rank:
                            conflicts.append(
                                f"{prefix}: 已有 {existing}，新值 {current_rank}（保留已有）"
                            )
                        continue  # 以先出现的评级为准

                    ccf_map[prefix] = current_rank

    # 报告
    print(f"\n[parse] 解析完成，共提取 {len(ccf_map)} 条有效条目")
    if conflicts:
        print(f"[parse] ⚠️  冲突条目（保留首次出现值）: {len(conflicts)} 条")
        for c in conflicts:
            print(f"    {c}")

    # 展开 DBLP 路径别名补丁（补充历史重命名路径）
    alias_added = 0
    for src, aliases in DBLP_ALIAS_PATCH.items():
        if src in ccf_map:
            rank = ccf_map[src]
            for alias in aliases:
                if alias not in ccf_map:
                    ccf_map[alias] = rank
                    alias_added += 1
                    print(f"[patch] 别名补充: {alias} -> {rank}（来自 {src}）")
    if alias_added:
        print(f"[parse] 别名补丁共新增 {alias_added} 条")

    return ccf_map


# ---------------------------------------------------------------------------
# 诊断：打印所有条目
# ---------------------------------------------------------------------------


def dry_run(ccf_map: dict[str, str]) -> None:
    """以表格形式打印所有解析结果。"""
    from collections import Counter
    rank_counts = Counter(ccf_map.values())

    print(f"\n{'='*60}")
    print(f"  CCF 映射表解析结果（共 {len(ccf_map)} 条）")
    print(f"  A 类: {rank_counts['A']}  B 类: {rank_counts['B']}  C 类: {rank_counts['C']}")
    print(f"{'='*60}")

    for rank in ["A", "B", "C"]:
        entries = sorted(k for k, v in ccf_map.items() if v == rank)
        print(f"\n  ── CCF-{rank} ({len(entries)} 条) ──")
        for e in entries:
            ptype = "期刊" if e.startswith("journals") else "会议"
            print(f"    [{ptype}] {e}")


# ---------------------------------------------------------------------------
# 验证与 ES 对齐率
# ---------------------------------------------------------------------------


def verify_alignment(ccf_map: dict[str, str], es_host: str) -> None:
    """查询 ES，统计有 CCF 评级的论文覆盖率。

    Args:
        ccf_map: 生成的 CCF 映射表。
        es_host: ES 地址。
    """
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        print("❌ 需要安装 elasticsearch: uv add elasticsearch")
        return

    alias = "dblp_search"
    es = Elasticsearch(hosts=[es_host], request_timeout=30)

    try:
        # 检查别名是否存在
        try:
            es.indices.get_alias(name=alias)
        except Exception:
            print(f"⚠️  ES 别名 '{alias}' 不存在，请先完成灌库。")
            return

        total  = es.count(index=alias)["count"]
        rated  = es.count(index=alias, query={"exists": {"field": "ccf_rating"}})["count"]
        a_cnt  = es.count(index=alias, query={"term": {"ccf_rating": "A"}})["count"]
        b_cnt  = es.count(index=alias, query={"term": {"ccf_rating": "B"}})["count"]
        c_cnt  = es.count(index=alias, query={"term": {"ccf_rating": "C"}})["count"]

        print(f"\n{'='*55}")
        print(f"  CCF 评级 × ES 对齐验证  ({alias})")
        print(f"{'='*55}")
        print(f"  总论文数:           {total:>10,}")
        print(f"  有 CCF 评级:        {rated:>10,}  ({rated/total*100:.1f}%)")
        print(f"    CCF-A:            {a_cnt:>10,}")
        print(f"    CCF-B:            {b_cnt:>10,}")
        print(f"    CCF-C:            {c_cnt:>10,}")
        print(f"{'='*55}")

        # 抽样无评级条目，供补充参考
        sample = es.search(
            index=alias,
            query={"bool": {"must_not": {"exists": {"field": "ccf_rating"}}}},
            _source=["dblp_key", "venue"],
            size=15,
        )
        hits = sample["hits"]["hits"]
        if hits:
            print("\n  无评级抽样（高频前缀可补充进映射表）:")
            prefix_freq: dict[str, int] = {}
            for h in hits:
                k = h["_source"].get("dblp_key", "")
                parts = k.split("/")
                if len(parts) >= 2:
                    p = f"{parts[0]}/{parts[1]}"
                    prefix_freq[p] = prefix_freq.get(p, 0) + 1
            for p, cnt in sorted(prefix_freq.items(), key=lambda x: -x[1]):
                print(f"    {p:<35} (样本中出现 {cnt} 次)")

    except Exception as e:
        print(f"❌ 验证失败: {e}")
    finally:
        es.close()


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="CCF 第七版 PDF → DBLP 映射表生成脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--pdf",
        default=str(DEFAULT_PDF),
        help=f"CCF PDF 文件路径（默认 {DEFAULT_PDF.name}）",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUT),
        help=f"输出 JSON 路径（默认 {DEFAULT_OUT}）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印解析结果，不写入文件",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="生成后验证与 ES 的对齐率",
    )
    parser.add_argument(
        "--es-host",
        default="http://49.52.27.139:9200",
        help="ES 地址（--verify 时使用）",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        print(f"❌ PDF 文件不存在: {pdf_path}")
        sys.exit(1)

    print(f"[main] PDF: {pdf_path.name}")
    ccf_map = parse_pdf(pdf_path)

    if not ccf_map:
        print("❌ 未解析到任何条目，请检查 PDF 格式")
        sys.exit(1)

    # 统计
    from collections import Counter
    rank_counts = Counter(ccf_map.values())
    conf_cnt    = sum(1 for k in ccf_map if k.startswith("conf/"))
    jour_cnt    = sum(1 for k in ccf_map if k.startswith("journals/"))

    if args.dry_run:
        dry_run(ccf_map)
        return

    # 写入
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(ccf_map, f, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"\n{'='*55}")
    print(f"  ✅ CCF 映射表已写入: {out_path}")
    print(f"  总条目: {len(ccf_map)}  （会议 {conf_cnt}，期刊 {jour_cnt}）")
    print(f"  CCF-A: {rank_counts['A']}  CCF-B: {rank_counts['B']}  CCF-C: {rank_counts['C']}")
    print(f"{'='*55}")

    if args.verify:
        verify_alignment(ccf_map, args.es_host)


if __name__ == "__main__":
    main()
