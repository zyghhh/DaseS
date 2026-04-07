"""
DBLP XML 蓝绿发布入库脚本

基于 Elasticsearch 别名（alias）实现零停机数据更新：
  1. 下载 dblp.xml.gz + dblp.dtd（可选，脚本内自动判断）
  2. 创建带日期后缀的"绿库"（新索引），如 dblp_index_20260401
  3. 流式解析 XML 并批量灌入绿库（保留完整原始 XML 片段）
  4. 数据校验：文档总数 & 测试查询
  5. 原子化别名切换 dblp_search → 绿库
  6. 垃圾回收：保留 N-1 版本，删除更旧的索引

用法:
    # 全量导入（默认不限制条数）
    python data/scripts/ingest_dblp.py

    # 测试模式：仅导入 1000 条
    python data/scripts/ingest_dblp.py --limit 1000

    # 指定日期后缀 & 跳过下载
    python data/scripts/ingest_dblp.py --date 20260401 --skip-download

    # 仅执行别名切换（跳过导入）
    python data/scripts/ingest_dblp.py --switch-only --index dblp_index_20260401

    # 回滚到上一个版本
    python data/scripts/ingest_dblp.py --rollback
"""

import argparse
import gc
import gzip
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# 将 backend 目录加入路径，以便复用 app.core.config
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from lxml import etree

from app.core.config import settings

# 脚本专用: 使用同步客户端，避免 WSL2 下 aiohttp 连接超时问题
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

DBLP_XML_URL = "https://dblp.org/xml/dblp.xml.gz"
DBLP_DTD_URL = "https://dblp.org/xml/dblp.dtd"

ALIAS_NAME: str = settings.ES_ALIAS_PAPERS      # dblp_search
INDEX_PREFIX: str = settings.ES_INDEX_PREFIX     # dblp_index

TARGET_TAGS: frozenset[str] = frozenset({"article", "inproceedings"})

# 最低文档数量阈值（低于此值认为灌库异常）
MIN_DOC_COUNT: int = 7_000_000

# ---------------------------------------------------------------------------
# 索引 Mapping（与 es_service.py 保持一致 + raw_xml 字段）
# ---------------------------------------------------------------------------

PAPERS_MAPPING: dict = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "-1",          # 灌库期间关闭自动刷新，提升写入速度
        "similarity": {
            "paper_bm25": {
                "type": "BM25",
                "k1": 1.2,
                "b": 0.5,
            }
        },
        "analysis": {
            "analyzer": {
                "default": {"type": "english"},
                "title_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "english_stemmer"],
                },
            },
            "filter": {
                "english_stop":    {"type": "stop",   "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
            },
        },
    },
    "mappings": {
        "properties": {
            "dblp_key": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "title_analyzer",
                "similarity": "paper_bm25",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "raw": {"type": "text", "analyzer": "standard"},
                },
            },
            "authors": {
                "type": "text",
                "analyzer": "standard",
                "similarity": "paper_bm25",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "year":     {"type": "integer"},
            "venue":    {"type": "keyword"},
            "url":      {"type": "keyword"},
            "ee":       {"type": "keyword"},
            "pub_type": {"type": "keyword"},
            "raw_xml":  {"type": "text", "index": False},  # 纯存储，不索引不分词
            # --- 扩展结构化字段（高频过滤/展示）---
            "pages":       {"type": "keyword"},
            "volume":      {"type": "keyword"},
            "mdate":       {"type": "date", "format": "yyyy-MM-dd"},
            "author_pids": {"type": "keyword"},
            "ee_links":    {"type": "keyword"},
            "school":      {"type": "keyword"},
            "publisher":   {"type": "keyword"},
            # --- 跨源关联标识符（数据融合桥梁）---
            "doi":         {"type": "keyword"},    # DOI，如 10.1016/j.artint.2023.103
            "arxiv_id":    {"type": "keyword"},    # arXiv ID，如 2301.12345
            # --- 预留：外部 API 补全字段 ---
            "abstract":       {"type": "text", "index": False},   # 论文摘要，外部数据源补全
            "abstract_source": {"type": "keyword"},                 # 摘要来源：S2AG | ArXiv | None
            "ccf_rating":      {"type": "keyword"},                 # CCF 评级: A, B, C
        }
    },
}

# 灌库完成后恢复的运行时 settings
RUNTIME_SETTINGS: dict = {
    "refresh_interval": "1s",
    "number_of_replicas": 0,
}


# ---------------------------------------------------------------------------
# 流式实体替换包装器
# ---------------------------------------------------------------------------

_ENTITY_RE = re.compile(rb"&([a-zA-Z][a-zA-Z0-9]*);")

# 标准 XML 内建实体，不需要替换
_XML_BUILTIN_ENTITIES: frozenset[str] = frozenset({"amp", "lt", "gt", "quot", "apos"})


class _EntityFixStream:
    """将 DBLP XML 中的 HTML 具名实体（&eacute; &ouml; 等）实时替换为 XML 数字字符引用（&#NNN;）。

    保持 XML 编码声明为 ISO-8859-1，让 lxml 按原始编码解析；
    避免将声明改为 UTF-8 后字节流与声明不一致导致解析中断。
    """

    def __init__(self, fileobj):
        self._f = fileobj
        self._buf = b""

    def read(self, size: int = 65536) -> bytes:
        chunk = self._f.read(size)
        data = self._buf + chunk
        if not chunk:
            result, self._buf = data, b""
            return self._replace(result)
        # 保留末尾可能被截断的实体引用
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
            # 跳过 XML 内建实体，lxml 可直接处理
            if name in _XML_BUILTIN_ENTITIES:
                return m.group(0)
            replaced = html.unescape(f"&{name};")
            if replaced != f"&{name};":
                # 替换为 XML 数字字符引用（纯 ASCII，任意编码下均安全）
                return "".join(f"&#{ord(c)};" for c in replaced).encode("ascii")
            # 未知实体，原样保留（lxml recover 模式会跳过）
            return m.group(0)
        return _ENTITY_RE.sub(_sub, data)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _text(elem: etree._Element, tag: str) -> str | None:
    """提取子元素的完整文本内容（含内联子元素如 <i>、<sub> 等）"""
    child = elem.find(tag)
    if child is not None:
        text = "".join(child.itertext()).strip()
        return text if text else None
    return None


def _all_texts(elem: etree._Element, tag: str) -> list[str]:
    """提取所有同名子元素的完整文本（含内联子元素）"""
    results: list[str] = []
    for child in elem.findall(tag):
        text = "".join(child.itertext()).strip()
        if text:
            results.append(text)
    return results


def _all_attr(elem: etree._Element, tag: str, attr: str) -> list[str]:
    """提取所有同名子元素的指定属性值（过滤空字符串）"""
    return [
        v for child in elem.findall(tag)
        if (v := child.get(attr, "").strip())
    ]


def _elem_to_raw_xml(elem: etree._Element) -> str:
    """将 lxml Element 序列化为完整的 XML 字符串（含子元素）"""
    return etree.tostring(elem, encoding="unicode", method="xml")


_DOI_RE = re.compile(r"https?://(?:dx\.)?doi\.org/(.+)")
_ARXIV_RE = re.compile(r"https?://arxiv\.org/abs/([\w.]+)")


def _extract_doi(ee_links: list[str]) -> str | None:
    """从 ee_links 中提取 DOI（去掉 https://doi.org/ 前缀）"""
    for link in ee_links:
        m = _DOI_RE.match(link)
        if m:
            return m.group(1).rstrip("/")
    return None


def _extract_arxiv_id(ee_links: list[str]) -> str | None:
    """从 ee_links 中提取 arXiv ID（如 2301.12345）"""
    for link in ee_links:
        m = _ARXIV_RE.match(link)
        if m:
            return m.group(1)
    return None


def _today_suffix() -> str:
    """返回当天日期后缀，如 20260401"""
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d")


def _load_ccf_map() -> dict[str, str]:
    """从本地 JSON 加载 CCF 评级映射"""
    path = REPO_ROOT / "data" / "raw" / "ccf_catalogue.json"
    if not path.exists():
        print(f"[ccf] ⚠️ 警告: 未找到 CCF 映射文件 {path}，论文将不带评级。")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_ccf_rating(dblp_key: str, ccf_map: dict[str, str]) -> str | None:
    """根据 dblp_key 匹配 CCF 评级"""
    if not dblp_key or not ccf_map:
        return None
    # "conf/cvpr/2023/1" -> "conf/cvpr"
    parts = dblp_key.split("/")
    if len(parts) >= 2:
        prefix = f"{parts[0]}/{parts[1]}"
        return ccf_map.get(prefix)
    return None


# ---------------------------------------------------------------------------
# Step 0: 数据下载
# ---------------------------------------------------------------------------

def download_data(output_dir: Path) -> tuple[Path, Path]:
    """下载 dblp.xml.gz 和 dblp.dtd 到指定目录。

    Args:
        output_dir: 输出目录。

    Returns:
        (xml_gz_path, dtd_path)
    """
    import httpx

    output_dir.mkdir(parents=True, exist_ok=True)
    xml_gz_path = output_dir / "dblp.xml.gz"
    dtd_path = output_dir / "dblp.dtd"

    for url, dest in [(DBLP_XML_URL, xml_gz_path), (DBLP_DTD_URL, dtd_path)]:
        if dest.exists():
            print(f"[download] 文件已存在，跳过: {dest}")
            continue
        print(f"[download] 正在下载: {url} -> {dest}")
        tmp_dest = dest.with_suffix(dest.suffix + ".tmp")
        with httpx.stream("GET", url, follow_redirects=True, timeout=600) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(tmp_dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1024 * 256):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = downloaded / total * 100
                        print(f"\r[download]   {pct:.1f}% ({downloaded // 1024 // 1024}MB / {total // 1024 // 1024}MB)", end="", flush=True)
            print()
        tmp_dest.rename(dest)
        print(f"[download] 完成: {dest}")

    return xml_gz_path, dtd_path


# ---------------------------------------------------------------------------
# Step 1: 创建绿库（新索引）
# ---------------------------------------------------------------------------

def create_green_index(es: Elasticsearch, index_name: str) -> None:
    """创建带日期后缀的新索引（绿库）。

    Args:
        es: Elasticsearch 同步客户端。
        index_name: 新索引名称，如 dblp_index_20260401。
    """
    if es.indices.exists(index=index_name):
        # 检查该索引是否正被别名引用（即正在服务）
        try:
            alias_info = es.indices.get_alias(index=index_name, name=ALIAS_NAME)
            if alias_info:
                print(f"[green] ❌ 索引 {index_name} 当前正被别名 '{ALIAS_NAME}' 引用，"
                      f"不能删除！请使用不同的 --date 后缀。")
                sys.exit(1)
        except NotFoundError:
            pass  # 未被别名引用，可以安全删除
        print(f"[green] 索引 {index_name} 已存在（未被别名引用），将删除后重建。")
        es.indices.delete(index=index_name)

    es.indices.create(
        index=index_name,
        settings=PAPERS_MAPPING["settings"],
        mappings=PAPERS_MAPPING["mappings"],
    )
    print(f"[green] 绿库已创建: {index_name}")


# ---------------------------------------------------------------------------
# Step 2: 流式解析 + 批量灌库（含完整 raw_xml）
# ---------------------------------------------------------------------------

def stream_ingest(
    es: Elasticsearch,
    index_name: str,
    xml_gz_path: str,
    limit: int = 0,
    batch_size: int = 500,
    ccf_map: dict[str, str] | None = None,
) -> tuple[int, int]:
    """流式解析 DBLP XML 并批量写入 ES（内存友好，不全量加载）。

    Args:
        es: Elasticsearch 同步客户端。
        index_name: 目标索引名称。
        xml_gz_path: dblp.xml.gz 文件路径。
        limit: 最多导入条数，0 表示不限制（全量）。
        batch_size: 每批写入的文档数。

    Returns:
        (成功总数, 失败总数)
    """
    total_ok = 0
    total_err = 0
    batch: list[dict] = []
    count = 0
    start_time = time.time()

    if ccf_map is None:
        ccf_map = {}
    limit_str = f"{limit} 条" if limit > 0 else "全量（不限制）"
    print(f"[ingest] 开始解析: {xml_gz_path}（目标: {limit_str}）")

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

            # 提取完整原始 XML 片段（在 elem.clear() 之前）
            raw_xml = _elem_to_raw_xml(elem)

            authors    = _all_texts(elem, "author")
            author_pids = _all_attr(elem, "author", "pid")    # <author pid="281/0400">
            year_str   = _text(elem, "year")
            year: int | None = int(year_str) if year_str and year_str.isdigit() else None
            venue      = _text(elem, "journal") or _text(elem, "booktitle")
            url        = _text(elem, "url")
            ee_links   = _all_texts(elem, "ee")               # 所有 <ee> 链接
            ee         = ee_links[0] if ee_links else None    # 向后展示兼容
            pub_type   = elem.tag
            pages      = _text(elem, "pages")
            volume     = _text(elem, "volume")
            mdate      = elem.get("mdate", "") or None        # 属性: mdate="2023-01-15"
            school     = _text(elem, "school")               # 学位论文学校
            publisher  = _text(elem, "publisher")            # 出版社
            doi        = _extract_doi(ee_links)              # 从 ee_links 提取 DOI
            arxiv_id   = _extract_arxiv_id(ee_links)         # 从 ee_links 提取 arXiv ID
            ccf_rating = _get_ccf_rating(key, ccf_map)       # 匹配 CCF 评级

            doc = {
                "dblp_key":   key,
                "title":      title,
                "authors":    authors,
                "year":       year,
                "venue":      venue,
                "url":        url,
                "ee":         ee,
                "pub_type":   pub_type,
                "raw_xml":    raw_xml,
                "pages":      pages,
                "volume":     volume,
                "mdate":      mdate,
                "author_pids": author_pids,
                "ee_links":   ee_links,
                "school":     school,
                "publisher":  publisher,
                "doi":        doi,
                "arxiv_id":   arxiv_id,
                "ccf_rating": ccf_rating,
            }

            batch.append({
                "_index": index_name,
                "_id": key,
                "_source": doc,
            })
            count += 1

            # 批量写入
            if len(batch) >= batch_size:
                ok, errors = bulk(es, batch, raise_on_error=False, raise_on_exception=False)
                total_ok += ok
                total_err += len(errors)
                if errors:
                    for err in errors[:2]:
                        print(f"[ingest]   错误示例: {err}")
                batch.clear()

                if count % 50_000 == 0:
                    elapsed = time.time() - start_time
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"[ingest]   已处理 {count:,} 条 | 速率 {rate:,.0f} 条/秒 | "
                          f"成功 {total_ok:,} | 失败 {total_err:,}", flush=True)

            # 释放内存
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if count % 200_000 == 0:
                gc.collect()

            if 0 < limit <= count:
                print(f"[ingest] 已达目标数量 {limit:,}，停止解析。")
                break

    # 最后一批
    if batch:
        ok, errors = bulk(es, batch, raise_on_error=False, raise_on_exception=False)
        total_ok += ok
        total_err += len(errors)
        batch.clear()

    elapsed = time.time() - start_time
    print(f"\n[ingest] 解析+灌库完成！")
    print(f"[ingest]   总记录: {count:,} | 成功: {total_ok:,} | 失败: {total_err:,}")
    print(f"[ingest]   耗时: {elapsed:.1f}s ({elapsed / 60:.1f}min)")

    return total_ok, total_err


# ---------------------------------------------------------------------------
# Step 2.5: 灌库后恢复运行时 settings
# ---------------------------------------------------------------------------

def finalize_index(es: Elasticsearch, index_name: str) -> None:
    """灌库完成后恢复 refresh_interval、执行 force_merge 优化。

    Args:
        es: Elasticsearch 同步客户端。
        index_name: 目标索引名称。
    """
    print(f"[finalize] 恢复 refresh_interval=1s ...")
    es.indices.put_settings(index=index_name, settings=RUNTIME_SETTINGS)

    print(f"[finalize] 执行 refresh ...")
    es.indices.refresh(index=index_name)

    print(f"[finalize] 执行 force_merge (max_num_segments=1) ...")
    es.indices.forcemerge(index=index_name, max_num_segments=1, request_timeout=600)
    print(f"[finalize] 索引优化完成: {index_name}")


# ---------------------------------------------------------------------------
# Step 3: 数据校验
# ---------------------------------------------------------------------------

def validate_index(es: Elasticsearch, index_name: str, min_count: int = MIN_DOC_COUNT) -> bool:
    """校验新索引的数据完整性。

    Args:
        es: Elasticsearch 同步客户端。
        index_name: 待校验的索引名称。
        min_count: 最小文档数量阈值。

    Returns:
        校验是否通过。
    """
    print(f"\n[validate] 开始校验索引: {index_name}")

    # 1. 文档总数
    es.indices.refresh(index=index_name)
    count_resp = es.count(index=index_name)
    doc_count = count_resp["count"]
    print(f"[validate]   文档总数: {doc_count:,}")

    if doc_count < min_count:
        print(f"[validate] ❌ 文档数量 {doc_count:,} 低于阈值 {min_count:,}，校验失败！")
        return False

    # 2. 测试查询：标题搜索
    test_resp = es.search(
        index=index_name,
        query={"match": {"title": "deep learning"}},
        size=3,
    )
    hits = test_resp["hits"]["total"]["value"]
    print(f"[validate]   测试查询 'deep learning': 命中 {hits:,} 条")

    if hits == 0:
        print(f"[validate] ❌ 测试查询无结果，校验失败！")
        return False

    # 3. 抽样检查 raw_xml 字段
    sample = test_resp["hits"]["hits"][0]["_source"]
    has_raw_xml = bool(sample.get("raw_xml"))
    print(f"[validate]   raw_xml 字段: {'✅ 存在' if has_raw_xml else '❌ 缺失'}")

    if not has_raw_xml:
        print(f"[validate] ❌ raw_xml 字段缺失，校验失败！")
        return False

    print(f"[validate] ✅ 校验通过！")
    return True


# ---------------------------------------------------------------------------
# Step 4: 原子化别名切换
# ---------------------------------------------------------------------------

def switch_alias(es: Elasticsearch, new_index: str, alias: str = ALIAS_NAME) -> str | None:
    """原子化切换别名到新索引，返回被移除的旧索引名。

    Args:
        es: Elasticsearch 同步客户端。
        new_index: 新索引名称。
        alias: 别名名称。

    Returns:
        被移除别名的旧索引名（若有），否则 None。
    """
    print(f"\n[switch] 正在切换别名 '{alias}' -> {new_index}")

    # 查找当前别名指向的索引
    old_indices: list[str] = []
    try:
        alias_info = es.indices.get_alias(name=alias)
        old_indices = list(alias_info.keys())
        print(f"[switch]   当前别名指向: {old_indices}")
    except NotFoundError:
        print(f"[switch]   别名 '{alias}' 不存在，将直接创建。")

    # 构建原子操作 actions
    actions: list[dict] = []
    for old_idx in old_indices:
        actions.append({"remove": {"index": old_idx, "alias": alias}})
    actions.append({"add": {"index": new_index, "alias": alias}})

    es.indices.update_aliases(actions=actions)
    print(f"[switch] ✅ 别名切换完成: {alias} -> {new_index}")

    return old_indices[0] if old_indices else None


# ---------------------------------------------------------------------------
# Step 5: 垃圾回收（保留 N-1 版本）
# ---------------------------------------------------------------------------

def garbage_collect(es: Elasticsearch, current_index: str, keep_previous: str | None = None) -> None:
    """删除比 N-1 更旧的索引，释放磁盘空间。

    Args:
        es: Elasticsearch 同步客户端。
        current_index: 当前正在使用的索引（刚切换到的绿库）。
        keep_previous: 需要保留的上一版本索引（用于回滚）。
    """
    print(f"\n[gc] 开始垃圾回收 ...")

    # 列出所有 dblp_index_* 索引
    try:
        all_indices = list(es.indices.get(index=f"{INDEX_PREFIX}_*").keys())
    except NotFoundError:
        print(f"[gc] 无 {INDEX_PREFIX}_* 索引，跳过。")
        return

    keep_set = {current_index}
    if keep_previous:
        keep_set.add(keep_previous)

    to_delete = sorted(idx for idx in all_indices if idx not in keep_set)

    if not to_delete:
        print(f"[gc] 无需清理。当前保留: {sorted(keep_set)}")
        return

    for idx in to_delete:
        print(f"[gc]   删除旧索引: {idx}")
        es.indices.delete(index=idx)

    print(f"[gc] ✅ 已清理 {len(to_delete)} 个旧索引。保留: {sorted(keep_set)}")


# ---------------------------------------------------------------------------
# 回滚命令
# ---------------------------------------------------------------------------

def rollback(es: Elasticsearch, alias: str = ALIAS_NAME) -> None:
    """回滚到上一个版本：找到 N-1 版本索引，切换别名回去。

    Args:
        es: Elasticsearch 同步客户端。
        alias: 别名名称。
    """
    print(f"\n[rollback] 正在查找可回滚的索引 ...")

    # 当前别名指向
    try:
        alias_info = es.indices.get_alias(name=alias)
        current_indices = list(alias_info.keys())
    except NotFoundError:
        print(f"[rollback] ❌ 别名 '{alias}' 不存在，无法回滚。")
        return

    # 列出所有带前缀的索引
    try:
        all_indices = sorted(es.indices.get(index=f"{INDEX_PREFIX}_*").keys())
    except NotFoundError:
        print(f"[rollback] ❌ 无 {INDEX_PREFIX}_* 索引。")
        return

    # 排除当前索引，找到最近的一个
    candidates = [idx for idx in all_indices if idx not in current_indices]
    if not candidates:
        print(f"[rollback] ❌ 无可用的回滚目标（仅有当前索引: {current_indices}）")
        return

    target = candidates[-1]  # 按字典序最新的即为 N-1
    print(f"[rollback] 回滚目标: {target}（当前: {current_indices}）")

    switch_alias(es, target, alias)
    print(f"[rollback] ✅ 已回滚到: {target}")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def run_full_pipeline(
    xml_gz_path: str,
    date_suffix: str | None = None,
    limit: int = 0,
    batch_size: int = 500,
    skip_download: bool = False,
    skip_validate_threshold: int = MIN_DOC_COUNT,
) -> None:
    """执行完整的蓝绿发布流水线。

    Args:
        xml_gz_path: dblp.xml.gz 文件路径。
        date_suffix: 日期后缀，默认使用当天日期。
        limit: 最多导入条数，0 表示全量。
        batch_size: 批量写入大小。
        skip_download: 是否跳过下载步骤。
        skip_validate_threshold: 校验文档数阈值。
    """
    suffix = date_suffix or _today_suffix()
    new_index = f"{INDEX_PREFIX}_{suffix}"

    print("=" * 60)
    print(f"  DBLP 蓝绿发布入库流水线")
    print(f"  ES 地址:     {settings.ES_HOST}")
    print(f"  别名:        {ALIAS_NAME}")
    print(f"  新索引:      {new_index}")
    print(f"  数据文件:    {xml_gz_path}")
    print(f"  导入限制:    {'全量' if limit == 0 else f'{limit:,} 条'}")
    print("=" * 60)

    es = Elasticsearch(hosts=[settings.ES_HOST], request_timeout=60)

    # --- Step 0: 下载数据 ---
    if not skip_download:
        raw_dir = Path(xml_gz_path).parent
        xml_gz_path_obj, _ = download_data(raw_dir)
        xml_gz_path = str(xml_gz_path_obj)

    if not Path(xml_gz_path).exists():
        print(f"[pipeline] ❌ 文件不存在: {xml_gz_path}")
        sys.exit(1)

    # --- Step 1: 创建绿库 ---
    create_green_index(es, new_index)

    # --- Step 2: 流式灌库 ---
    ccf_map = _load_ccf_map()
    total_ok, total_err = stream_ingest(es, new_index, xml_gz_path, limit=limit, batch_size=batch_size, ccf_map=ccf_map)

    if total_ok == 0:
        print(f"[pipeline] ❌ 无数据写入成功，中止流水线。")
        es.indices.delete(index=new_index, ignore=[404])
        es.close()
        sys.exit(1)

    # --- Step 2.5: 恢复 settings & 优化 ---
    finalize_index(es, new_index)

    # --- Step 3: 数据校验 ---
    # 若为测试模式（limit > 0），降低阈值
    actual_threshold = min(skip_validate_threshold, total_ok) if limit > 0 else skip_validate_threshold
    passed = validate_index(es, new_index, min_count=actual_threshold)

    if not passed:
        print(f"\n[pipeline] ❌ 校验未通过，绿库 {new_index} 保留但不切换别名。")
        print(f"[pipeline]   请人工检查后手动执行切换或删除：")
        print(f"[pipeline]     切换: python {__file__} --switch-only --index {new_index}")
        print(f"[pipeline]     删除: curl -XDELETE '{settings.ES_HOST}/{new_index}'")
        es.close()
        sys.exit(1)

    # --- Step 4: 原子化别名切换 ---
    old_index = switch_alias(es, new_index, ALIAS_NAME)

    # --- Step 5: 垃圾回收 ---
    garbage_collect(es, new_index, keep_previous=old_index)

    es.close()

    print("\n" + "=" * 60)
    print(f"  ✅ 蓝绿发布完成！")
    print(f"  别名 '{ALIAS_NAME}' -> {new_index}")
    if old_index:
        print(f"  上一版本已保留: {old_index}（可用 --rollback 秒级回滚）")
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
    """命令行入口"""
    default_input = str(REPO_ROOT / "data" / "raw" / "dblp.xml.gz")

    parser = argparse.ArgumentParser(
        description="DBLP 蓝绿发布入库脚本 - 零停机更新 Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", default=default_input, help="dblp.xml.gz 文件路径")
    parser.add_argument("--limit", type=int, default=0, help="最多导入条数（0=全量，默认全量）")
    parser.add_argument("--batch-size", type=int, default=500, help="批量写入大小（默认 500）")
    parser.add_argument("--date", default=None, help="日期后缀（默认当天，如 20260401）")
    parser.add_argument("--skip-download", action="store_true", help="跳过下载步骤")
    parser.add_argument("--min-docs", type=int, default=MIN_DOC_COUNT, help=f"校验最低文档数（默认 {MIN_DOC_COUNT:,}）")

    # 特殊操作模式
    parser.add_argument("--switch-only", action="store_true", help="仅执行别名切换（跳过导入）")
    parser.add_argument("--index", default=None, help="--switch-only 时指定目标索引名")
    parser.add_argument("--rollback", action="store_true", help="回滚到上一个版本")

    args = parser.parse_args()

    es = Elasticsearch(hosts=[settings.ES_HOST], request_timeout=60)

    # --- 回滚模式 ---
    if args.rollback:
        rollback(es)
        es.close()
        return

    # --- 仅切换别名模式 ---
    if args.switch_only:
        if not args.index:
            print("❌ --switch-only 需要配合 --index 指定目标索引")
            sys.exit(1)
        if not es.indices.exists(index=args.index):
            print(f"❌ 索引 {args.index} 不存在")
            sys.exit(1)
        old = switch_alias(es, args.index, ALIAS_NAME)
        if old:
            print(f"上一版本: {old}（已保留）")
        es.close()
        return

    # --- 完整流水线 ---
    run_full_pipeline(
        xml_gz_path=args.input,
        date_suffix=args.date,
        limit=args.limit,
        batch_size=args.batch_size,
        skip_download=args.skip_download,
        skip_validate_threshold=args.min_docs,
    )


if __name__ == "__main__":
    main()

