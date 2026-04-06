"""
DBLP XML 解析 & Elasticsearch 写入脚本

用法:
    python data/scripts/ingest_dblp.py [--limit 1000] [--input data/raw/dblp.xml.gz]

功能:
    1. 流式解析 dblp.xml.gz（iterparse，内存友好）
    2. 提取 inproceedings / article 记录的元数据：title / authors / year / venue / url / ee
    3. 批量写入 Elasticsearch（同步客户端，默认每批 200 条）
"""

import argparse
import gzip
import html
import os
import re
import sys
from pathlib import Path

# 将 backend 目录加入路径，以便复用 app.core.config
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from lxml import etree  

# 可按需覆盖环境变量（ES 地址等）
os.environ.setdefault("ES_HOST", "http://localhost:9200")

from app.core.config import settings  

# 导入脚本专用: 使用同步客户端，避免 WSL2 下 aiohttp 连接超时问题
from elasticsearch import Elasticsearch 
from elasticsearch.helpers import bulk  

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

TARGET_TAGS: frozenset[str] = frozenset({"article", "inproceedings"})

# ---------------------------------------------------------------------------
# 流式实体替换包装器
# ---------------------------------------------------------------------------

_ENTITY_RE = re.compile(rb"&([a-zA-Z][a-zA-Z0-9]*);")


class _EntityFixStream:
    """将 DBLP XML 中的 HTML 实体引用（&eacute; &ouml; 等）实时替换为 Unicode。

    同时将 XML 声明中的 ISO-8859-1 改为 UTF-8，避免乱码。
    """

    def __init__(self, fileobj):
        self._f = fileobj
        self._buf = b""
        self._first = True   # 第一个 chunk 需要修改编码声明

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
        if self._first:
            # 将 XML 声明的编码改为 UTF-8，防止 lxml 按 ISO-8859-1 解读替换后的 UTF-8 字节
            data = data.replace(b'encoding="ISO-8859-1"', b'encoding="UTF-8"')
            data = data.replace(b"encoding='ISO-8859-1'", b"encoding='UTF-8'")
            self._first = False
        return self._replace(data)

    @staticmethod
    def _replace(data: bytes) -> bytes:
        def _sub(m: re.Match) -> bytes:
            name = m.group(1).decode("ascii", errors="ignore")
            replaced = html.unescape(f"&{name};")
            if replaced != f"&{name};":
                return replaced.encode("utf-8")
            return m.group(0)  # 未知实体保原样
        return _ENTITY_RE.sub(_sub, data)

PAPERS_MAPPING: dict = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "dblp_key":  {"type": "keyword"},
            "title":     {"type": "text", "analyzer": "standard"},
            "authors":   {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "year":      {"type": "integer"},
            "venue":     {"type": "keyword"},
            "url":       {"type": "keyword"},
            "ee":        {"type": "keyword"},
            "pub_type":  {"type": "keyword"},
        }
    },
}


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _text(elem, tag: str) -> str | None:
    """提取子元素的文本内容（第一个匹配项）"""
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _all_texts(elem, tag: str) -> list[str]:
    """提取所有同名子元素的文本（如多个 author）"""
    return [
        child.text.strip()
        for child in elem.findall(tag)
        if child.text and child.text.strip()
    ]


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def parse_dblp(xml_gz_path: str, limit: int = 1000) -> list[dict]:
    """流式解析 dblp.xml.gz，返回不超过 limit 条论文元数据字典列表。

    Args:
        xml_gz_path: dblp.xml.gz 文件路径。
        limit:       最多提取的论文数量。

    Returns:
        dict 列表（可直接用于 ES bulk 写入）。
    """
    papers: list[dict] = []
    count = 0

    print(f"[ingest] 开始解析: {xml_gz_path}（目标 {limit} 条）")

    with gzip.open(xml_gz_path, "rb") as f:
        context = etree.iterparse(
            _EntityFixStream(f),
            events=("end",),
            tag=list(TARGET_TAGS),
            recover=True,
            load_dtd=False,
            resolve_entities=False,
        )

        for _event, elem in context:
            key: str = elem.get("key", "")

            title = _text(elem, "title")
            if not title:
                elem.clear()
                continue

            authors = _all_texts(elem, "author")
            year_str = _text(elem, "year")
            year: int | None = int(year_str) if year_str and year_str.isdigit() else None
            venue = _text(elem, "journal") or _text(elem, "booktitle")
            url = _text(elem, "url")
            ee = _text(elem, "ee")
            pub_type = elem.tag

            papers.append({
                "dblp_key": key,
                "title": title,
                "authors": authors,
                "year": year,
                "venue": venue,
                "url": url,
                "ee": ee,
                "pub_type": pub_type,
            })
            count += 1
            if count % 100 == 0:
                print(f"[ingest]   已提取 {count} 条 ...", flush=True)

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if count >= limit:
                print(f"[ingest] 已达目标数量 {limit}，停止解析。")
                break

    print(f"[ingest] 解析完成，共提取 {len(papers)} 条记录。")
    return papers


# ---------------------------------------------------------------------------
# 导入
# ---------------------------------------------------------------------------

def ingest(xml_gz_path: str, limit: int = 1000, batch_size: int = 200) -> None:
    """解析 DBLP XML 并写入 Elasticsearch（同步客户端）。

    Args:
        xml_gz_path: dblp.xml.gz 文件路径。
        limit:       最多写入的论文数量。
        batch_size:  每次批量写入的条数。
    """
    index = settings.ES_INDEX_PAPERS
    print(f"[ingest] ES 地址: {settings.ES_HOST}，索引: {index}")

    es = Elasticsearch(hosts=[settings.ES_HOST], request_timeout=30)

    # 确保索引存在
    if not es.indices.exists(index=index):
        es.indices.create(
            index=index,
            settings=PAPERS_MAPPING["settings"],
            mappings=PAPERS_MAPPING["mappings"],
        )
        print("[ingest] Elasticsearch 索引已创建。")
    else:
        print("[ingest] Elasticsearch 索引已存在。")

    papers = parse_dblp(xml_gz_path, limit=limit)
    if not papers:
        print("[ingest] 未解析到任何记录，退出。")
        return

    total_ok = 0
    total_err: list = []

    for i in range(0, len(papers), batch_size):
        batch = papers[i : i + batch_size]
        actions = [
            {"_index": index, "_id": doc["dblp_key"], "_source": doc}
            for doc in batch
        ]
        ok, errors = bulk(es, actions, raise_on_error=False, raise_on_exception=False)
        total_ok += ok
        total_err.extend(errors)
        print(f"[ingest] 批次 {i // batch_size + 1}: 写入 {ok} 条，失败 {len(errors)} 条")

    es.close()
    print(f"\n[ingest] ✅ 完成！总计写入 {total_ok} 条，失败 {len(total_err)} 条。")
    if total_err:
        print("[ingest] 前 5 条错误:")
        for err in total_err[:5]:
            print(f"  {err}")


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> None:
    """命令行入口"""
    default_input = str(REPO_ROOT / "data" / "raw" / "dblp.xml.gz")

    parser = argparse.ArgumentParser(description="将 DBLP XML 数据导入 Elasticsearch")
    parser.add_argument("--input", default=default_input, help="dblp.xml.gz 文件路径")
    parser.add_argument("--limit", type=int, default=1000, help="最多导入的论文数量（默认 1000）")
    parser.add_argument("--batch-size", type=int, default=200, help="批量写入大小（默认 200）")
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"[ingest] ❌ 文件不存在: {args.input}")
        sys.exit(1)

    ingest(args.input, limit=args.limit, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
