"""
DBLP XML -> Elasticsearch 独立导入脚本（无需项目依赖）

服务器上运行前先安装依赖:
    pip install elasticsearch lxml

用法:
    python ingest_standalone.py \
        --input /path/to/dblp.xml.gz \
        --es-host http://localhost:9200 \
        --index dblp_papers \
        --limit 1000
"""

import argparse
import gzip
import sys
from pathlib import Path

try:
    from lxml import etree
except ImportError:
    print("请先安装: pip install lxml")
    sys.exit(1)

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
except ImportError:
    print("请先安装: pip install elasticsearch")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 索引 Mapping
# ---------------------------------------------------------------------------

PAPERS_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "dblp_key": {"type": "keyword"},
            "title":    {"type": "text", "analyzer": "standard"},
            "authors":  {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "year":     {"type": "integer"},
            "venue":    {"type": "keyword"},
            "url":      {"type": "keyword"},
            "ee":       {"type": "keyword"},
            "pub_type": {"type": "keyword"},
        }
    },
}

CS_KEY_PREFIXES = ("conf/", "journals/")
TARGET_TAGS = ["article", "inproceedings"]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _text(elem, tag):
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _all_texts(elem, tag):
    return [c.text.strip() for c in elem.findall(tag) if c.text and c.text.strip()]


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def parse_dblp(xml_gz_path: str, limit: int = 1000) -> list:
    papers = []
    count = 0
    print(f"[ingest] 开始解析: {xml_gz_path}（目标 {limit} 条）")

    with gzip.open(xml_gz_path, "rb") as f:
        context = etree.iterparse(
            f, events=("end",), tag=TARGET_TAGS,
            recover=True, load_dtd=False, resolve_entities=False,
        )
        for _event, elem in context:
            key = elem.get("key", "")
            if not key.startswith(CS_KEY_PREFIXES):
                elem.clear()
                continue

            title = _text(elem, "title")
            if not title:
                elem.clear()
                continue

            year_str = _text(elem, "year")
            year = int(year_str) if year_str and year_str.isdigit() else None

            papers.append({
                "dblp_key": key,
                "title":    title,
                "authors":  _all_texts(elem, "author"),
                "year":     year,
                "venue":    _text(elem, "journal") or _text(elem, "booktitle"),
                "url":      _text(elem, "url"),
                "ee":       _text(elem, "ee"),
                "pub_type": elem.tag,
            })
            count += 1
            if count % 100 == 0:
                print(f"[ingest]   已提取 {count} 条 ...", flush=True)

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if count >= limit:
                print(f"[ingest] 已达目标 {limit} 条，停止解析。")
                break

    print(f"[ingest] 解析完成，共 {len(papers)} 条。")
    return papers


# ---------------------------------------------------------------------------
# 写入 ES
# ---------------------------------------------------------------------------

def ingest(es_host: str, index: str, xml_gz_path: str, limit: int, batch_size: int):
    print(f"[ingest] ES: {es_host}  索引: {index}")
    es = Elasticsearch(hosts=[es_host], request_timeout=30)

    # 确保索引存在
    try:
        from elasticsearch import NotFoundError
        try:
            es.indices.get(index=index)
            print("[ingest] 索引已存在，跳过创建。")
        except NotFoundError:
            es.indices.create(
                index=index,
                settings=PAPERS_MAPPING["settings"],
                mappings=PAPERS_MAPPING["mappings"],
            )
            print("[ingest] 索引已创建。")
    except Exception as e:
        print(f"[ingest] 索引操作失败: {e}")
        sys.exit(1)

    papers = parse_dblp(xml_gz_path, limit=limit)
    if not papers:
        print("[ingest] 无数据，退出。")
        return

    total_ok = 0
    total_err = []
    for i in range(0, len(papers), batch_size):
        batch = papers[i: i + batch_size]
        actions = [{"_index": index, "_id": d["dblp_key"], "_source": d} for d in batch]
        ok, errors = bulk(es, actions, raise_on_error=False, raise_on_exception=False)
        total_ok += ok
        total_err.extend(errors)
        print(f"[ingest] 批次 {i // batch_size + 1}: 写入 {ok} 条，失败 {len(errors)} 条")

    es.close()
    print(f"\n[ingest] ✅ 完成！写入 {total_ok} 条，失败 {len(total_err)} 条。")
    if total_err:
        for e in total_err[:5]:
            print(f"  {e}")


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    default="dblp.xml.gz")
    parser.add_argument("--es-host",  default="http://localhost:9200")
    parser.add_argument("--index",    default="dblp_papers")
    parser.add_argument("--limit",    type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"[ingest] ❌ 文件不存在: {args.input}")
        sys.exit(1)

    ingest(args.es_host, args.index, args.input, args.limit, args.batch_size)


if __name__ == "__main__":
    main()
