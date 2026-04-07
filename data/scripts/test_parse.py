"""
DBLP XML 解析测试脚本 - 验证数据结构与解析结果

用法:
    python data/scripts/test_parse.py
    python data/scripts/test_parse.py --input data/raw/dblp.xml.gz --sample 5
"""

import argparse
import gzip
import html
import json
import re
from pathlib import Path

try:
    from lxml import etree
except ImportError:
    print("❌ 请先安装: pip install lxml  或  uv run --project backend python test_parse.py")
    raise

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
REPO_ROOT   = Path(__file__).resolve().parents[2]
DEFAULT_XML = REPO_ROOT / "data" / "raw" / "dblp.xml.gz"

CS_KEY_PREFIXES = ("conf/", "journals/")
TARGET_TAGS     = ["article", "inproceedings"]

# 统计用：各 tag 计数
TAG_COUNTER: dict[str, int] = {}

# ---------------------------------------------------------------------------
# 流式实体替换包装器（与 ingest.py 保持一致）
# ---------------------------------------------------------------------------

_ENTITY_RE = re.compile(rb"&([a-zA-Z][a-zA-Z0-9]*);")

_XML_BUILTIN_ENTITIES: frozenset[str] = frozenset({"amp", "lt", "gt", "quot", "apos"})


class _EntityFixStream:
    def __init__(self, fileobj):
        self._f = fileobj
        self._buf = b""

    def read(self, size: int = 65536) -> bytes:
        chunk = self._f.read(size)
        data = self._buf + chunk
        if not chunk:
            result, self._buf = data, b""
            return self._replace(result)
        last_amp = data.rfind(b"&")
        if last_amp != -1 and last_amp > len(data) - 32:
            self._buf = data[last_amp:]
            data = data[:last_amp]
        else:
            self._buf = b""
        return self._replace(data)

    @staticmethod
    def _replace(data: bytes) -> bytes:
        def _sub(m: re.Match) -> bytes:
            name = m.group(1).decode("ascii", errors="ignore")
            if name in _XML_BUILTIN_ENTITIES:
                return m.group(0)
            replaced = html.unescape(f"&{name};")
            if replaced != f"&{name};":
                return "".join(f"&#{ord(c)};" for c in replaced).encode("ascii")
            return m.group(0)
        return _ENTITY_RE.sub(_sub, data)


# ---------------------------------------------------------------------------
# 工具函数（与 ingest 脚本保持一致）
# ---------------------------------------------------------------------------

def _text(elem, tag: str) -> str | None:
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _all_texts(elem, tag: str) -> list[str]:
    return [c.text.strip() for c in elem.findall(tag) if c.text and c.text.strip()]


def _elem_to_dict(elem) -> dict:
    """将一个 DBLP XML 元素转为字典，尽量提取所有子字段"""
    year_str = _text(elem, "year")
    return {
        "dblp_key":  elem.get("key"),
        "pub_type":  elem.tag,
        "title":     _text(elem, "title"),
        "authors":   _all_texts(elem, "author"),
        "year":      int(year_str) if year_str and year_str.isdigit() else None,
        "venue":     _text(elem, "journal") or _text(elem, "booktitle"),
        "journal":   _text(elem, "journal"),
        "booktitle": _text(elem, "booktitle"),
        "volume":    _text(elem, "volume"),
        "number":    _text(elem, "number"),
        "pages":     _text(elem, "pages"),
        "url":       _text(elem, "url"),
        "ee":        _text(elem, "ee"),
        "publisher": _text(elem, "publisher"),
    }


# ---------------------------------------------------------------------------
# 解析 & 采样
# ---------------------------------------------------------------------------

def run_test(xml_gz_path: str, sample: int = 5, scan_limit: int = 50_000) -> None:
    path = Path(xml_gz_path)
    print(f"\n{'='*60}")
    print(f"  DBLP XML 解析测试")
    print(f"  文件: {path}")
    print(f"  文件大小: {path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"  采样数量: {sample} 条 CS 论文")
    print(f"  扫描上限: {scan_limit} 条记录（任意类型）")
    print(f"{'='*60}\n")

    samples: list[dict] = []      # 已采集的 CS 论文样本
    cs_count    = 0               # CS 论文总计数
    total_count = 0               # 所有记录计数
    skipped_no_title = 0          # 无 title 跳过数

    with gzip.open(xml_gz_path, "rb") as f:
        context = etree.iterparse(
            _EntityFixStream(f),
            events=("end",),
            tag=TARGET_TAGS,
            recover=True,
            load_dtd=False,
            resolve_entities=False,
        )

        for _event, elem in context:
            total_count += 1
            TAG_COUNTER[elem.tag] = TAG_COUNTER.get(elem.tag, 0) + 1

            key = elem.get("key", "")
            is_cs = key.startswith(CS_KEY_PREFIXES)

            if is_cs:
                cs_count += 1
                title = _text(elem, "title")
                if not title:
                    skipped_no_title += 1
                elif len(samples) < sample:
                    samples.append(_elem_to_dict(elem))

            # 释放内存
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if total_count >= scan_limit:
                print(f"[test] 已扫描 {scan_limit} 条，停止（可用 --scan-limit 调大）")
                break

    # ---------------------------------------------------------------------------
    # 打印统计
    # ---------------------------------------------------------------------------
    print(f"【统计结果（扫描了 {total_count} 条 article/inproceedings）】")
    print(f"  其中 CS 领域（conf/ + journals/ 前缀）: {cs_count} 条")
    print(f"  无标题跳过: {skipped_no_title} 条")
    print(f"  各类型分布: {TAG_COUNTER}")
    cs_ratio = cs_count / total_count * 100 if total_count else 0
    print(f"  CS 占比: {cs_ratio:.1f}%")

    print(f"\n【字段完整性分析（基于 {len(samples)} 条样本）】")
    field_names = ["title", "authors", "year", "venue", "journal",
                   "booktitle", "pages", "url", "ee", "publisher"]
    for field in field_names:
        filled = sum(1 for s in samples if s.get(field))
        print(f"  {field:<12}: {filled}/{len(samples)} 有值")

    print(f"\n{'='*60}")
    print(f"  采样详情（前 {len(samples)} 条 CS 论文）")
    print(f"{'='*60}")
    for i, paper in enumerate(samples, 1):
        print(f"\n── 样本 {i} ──────────────────────────────────────")
        print(json.dumps(paper, ensure_ascii=False, indent=2))

    print(f"\n{'='*60}")
    print("✅ 解析测试完成！")
    if cs_count >= 1000:
        print(f"✅ 可导入 1000 条（已发现 {cs_count} 条 CS 论文）")
    else:
        print(f"⚠️  扫描范围内仅发现 {cs_count} 条 CS 论文，"
              f"请增大 --scan-limit（当前 {scan_limit}）再试")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DBLP XML 解析测试")
    parser.add_argument("--input",      default=str(DEFAULT_XML), help="dblp.xml.gz 路径")
    parser.add_argument("--sample",     type=int, default=5,      help="打印样本数（默认 5）")
    parser.add_argument("--scan-limit", type=int, default=50_000, help="扫描记录上限（默认 50000）")
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"❌ 文件不存在: {args.input}")
        return

    run_test(args.input, sample=args.sample, scan_limit=args.scan_limit)


if __name__ == "__main__":
    main()
