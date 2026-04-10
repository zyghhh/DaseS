"""
S2AG + ArXiv + OpenAlex 增量摘要补全脚本

从 Elasticsearch 中筛选「有 DOI 但无摘要」的记录，按优先级补充摘要：
  1. S2AG Batch API（每次 500 条 DOI，1 次/秒限速）
  2. ArXiv API 兜底（仅针对有 arxiv_id 的记录，3 秒/次限速）
  3. OpenAlex API 兜底（按 DOI 查询，倒排索引还原摘要，约 10 次/秒）

与 ingest_s2ag_abstracts.py 的区别：
  - ingest_s2ag_abstracts.py：下载 S2AG 全量分片文件，本地解析后按 DOI 匹配 ES
  - 本脚本：从 ES 反向查缺，直接调 S2AG/ArXiv/OpenAlex API 在线补充

适用场景：
  - S2AG 全量分片已跑完，仍有大量无摘要记录（DOI 覆盖不足）
  - 需要针对 ES 中有 DOI 的记录精准补全

用法:
    # 全量补全（S2AG + ArXiv + OpenAlex）
    python data/scripts/supplement_abstracts.py

    # 仅 S2AG，跳过 ArXiv 和 OpenAlex
    python data/scripts/supplement_abstracts.py --no-arxiv --no-openalex

    # 仅 ArXiv 兜底（跳过 S2AG 和 OpenAlex）
    python data/scripts/supplement_abstracts.py --no-s2ag --no-openalex

    # 仅 OpenAlex 兜底（跳过 S2AG 和 ArXiv）
    python data/scripts/supplement_abstracts.py --no-s2ag --no-arxiv --openalex-email you@example.com

    # 测试模式：只处理前 1000 条
    python data/scripts/supplement_abstracts.py --limit 1000

    # 强制覆盖已有摘要
    python data/scripts/supplement_abstracts.py --overwrite

    # 自定义 ES 和 API Key
    python data/scripts/supplement_abstracts.py --es-host http://localhost:9200 --api-key YOUR_KEY



    # 终端1：仅 S2AG（状态写入默认目录 data/s2ag_abstract/）
    cd /mnt/d/vDesktop/DaseS/backend
    uv run python ../data/scripts/supplement_abstracts.py \
    --no-arxiv --no-openalex

    # 终端2：仅 ArXiv（状态写入独立目录，避免冲突）
    cd /mnt/d/vDesktop/DaseS/backend
    uv run python ../data/scripts/supplement_abstracts.py \
    --no-s2ag --no-openalex \
    --output-dir ../data/s2ag_abstract/state_arxiv

    # 终端3：仅 OpenAlex（状态写入独立目录）
    cd /mnt/d/vDesktop/DaseS/backend
    uv run python ../data/scripts/supplement_abstracts.py \
    --no-s2ag --no-arxiv \
    --output-dir ../data/s2ag_abstract/state_openalex
"""

import argparse
import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
import urllib.request
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

# 自动加载 backend/.env
_SCRIPT_DIR = Path(__file__).resolve().parent
_ENV_PATH = _SCRIPT_DIR.parent.parent / "backend" / ".env"
if _ENV_PATH.exists():
    load_dotenv(_ENV_PATH)

# ---------------------------------------------------------------------------
# 默认配置
# ---------------------------------------------------------------------------

DEFAULT_S2AG_API_KEY: str = os.environ.get("S2AG_API_KEY", "")
DEFAULT_ES_HOST: str = os.environ.get("ES_HOST", "http://49.52.27.139:9200")
DEFAULT_ES_ALIAS: str = os.environ.get("ES_ALIAS", "dblp_search")
DEFAULT_OPENALEX_EMAIL: str = os.environ.get("OPENALEX_EMAIL", "")

DEFAULT_OUTPUT_DIR: str = os.environ.get(
    "S2AG_OUTPUT_DIR",
    str(_SCRIPT_DIR.parent / "s2ag_abstract"),
)

# S2AG Batch API
S2AG_BATCH_URL = (
    "https://api.semanticscholar.org/graph/v1/paper/batch"
    "?fields=externalIds,abstract"
)

# ArXiv API
ARXIV_API_URL = "http://export.arxiv.org/api/query?id_list={arxiv_id}"
ARXIV_NAMESPACE = {"atom": "http://www.w3.org/2005/Atom"}

# OpenAlex API
OPENALEX_API_URL = "https://api.openalex.org/works/{doi}"
OPENALEX_MAILTO_PARAM = "mailto"

# 批量参数
S2AG_BATCH_SIZE: int = 500       # S2AG 每批 DOI/ArXiv ID 数量
ARXIV_BATCH_SIZE: int = 100       # ArXiv API 每批 ID 数量（降低触发限速概率）
ES_BULK_SIZE: int = 500          # ES bulk 写入批量大小
ES_SCROLL_SIZE: int = 5000       # ES scroll 每页大小
ES_SCROLL_TIMEOUT: str = "5m"    # scroll 保持时间

# 限速参数
S2AG_RATE_LIMIT_SECS: float = 1.0    # 有 API Key 时 1 次/秒
ARXIV_RATE_LIMIT_SECS: float = 3.0   # ArXiv 批次间隔（基础值，429 时指数退避）
ARXIV_RETRY_MAX: int = 4             # ArXiv 429/连接失败最大重试次数
ARXIV_RETRY_BASE_WAIT: float = 60.0  # 第一次 429 等待时间（秒），后续翻倍
OPENALEX_RATE_LIMIT_SECS: float = 3  # OpenAlex 礼貌池约 10 次/秒；保守设 0.2s
OPENALEX_RETRY_MAX: int = 3             # OpenAlex 429 最大重试次数
OPENALEX_RETRY_BASE_WAIT: float = 30.0  # OpenAlex 429 首次等待秒数


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _now_str() -> str:
    """返回当前 UTC 时间字符串。"""
    return datetime.now(tz=timezone.utc).strftime("%H:%M:%S")


def _log(tag: str, msg: str) -> None:
    """统一日志格式。"""
    print(f"[{_now_str()}][{tag}] {msg}", flush=True)


import re

# ArXiv ID 合法格式：
# 新格式：YYMM.NNNNN 或 YYMM.NNNNNvN（如 2301.01234 或 2301.01234v2）
# 旧格式：category/YYMMNNN（如 cs/0011010、hep-th/9901001、cs.AI/0301001）
_ARXIV_ID_RE = re.compile(
    r"^(?:\d{4}\.\d{4,5}(?:v\d+)?"
    r"|[a-z][a-zA-Z0-9-]*(?:\.[A-Z]{2})?/\d{4,7})"
    r"$"
)


def is_valid_arxiv_id(arxiv_id: str) -> bool:
    """检查 arxiv_id 是否为合法的 ArXiv 论文 ID。

    过滤掉 DBLP 中常见的分类前缀（如 'cs'、'math'、'cs.AI'），
    只保留真正的论文 ID（如 '2301.01234' 或 'math.GT/0309136'）。

    Args:
        arxiv_id: 待检查的字符串。

    Returns:
        True 表示合法的 ArXiv 论文 ID。
    """
    if not arxiv_id:
        return False
    return bool(_ARXIV_ID_RE.match(arxiv_id))


# ---------------------------------------------------------------------------
# Step 1: 从 ES scroll 查询有 DOI 但无摘要的记录
# ---------------------------------------------------------------------------


def iter_docs_without_abstract(
    es: Elasticsearch,
    alias: str,
    overwrite: bool = False,
    limit: int = 0,
) -> Iterator[dict]:
    """从 ES 中 scroll 查询有 DOI 但无摘要的文档。

    Args:
        es: Elasticsearch 客户端。
        alias: ES 别名/索引名。
        overwrite: 是否包含已有摘要的记录（强制覆盖模式）。
        limit: 最多返回多少条记录（0=不限）。

    Yields:
        文档字典，包含 _id, doi, arxiv_id, dblp_key 等字段。
    """
    must_clauses: list[dict] = [
        {"exists": {"field": "doi"}},  # 必须有 DOI
    ]
    if not overwrite:
        must_clauses.append(
            {"bool": {"must_not": {"exists": {"field": "abstract_source"}}}}
        )

    query: dict = {"bool": {"must": must_clauses}}

    _log("scroll", "开始 scroll 查询有 DOI 无摘要的记录...")

    try:
        resp = es.search(
            index=alias,
            query=query,
            _source=["doi", "arxiv_id", "dblp_key"],
            size=ES_SCROLL_SIZE,
            scroll=ES_SCROLL_TIMEOUT,
        )
    except Exception as e:
        _log("scroll", f"❌ 初始 scroll 查询失败: {e}")
        return

    scroll_id: str = resp.get("_scroll_id", "")
    total_hits: int = resp["hits"]["total"]["value"]
    _log("scroll", f"匹配到 {total_hits:,} 条记录")

    count = 0
    while True:
        hits = resp["hits"]["hits"]
        if not hits:
            break

        for hit in hits:
            src = hit["_source"]
            doc = {
                "_id": hit["_id"],
                "doi": src.get("doi", ""),
                "arxiv_id": src.get("arxiv_id") or "",
                "dblp_key": src.get("dblp_key", ""),
            }
            yield doc
            count += 1
            if 0 < limit <= count:
                break

        if 0 < limit <= count:
            break

        # 继续翻页
        try:
            resp = es.scroll(scroll_id=scroll_id, scroll=ES_SCROLL_TIMEOUT)
            scroll_id = resp.get("_scroll_id", scroll_id)
        except Exception as e:
            _log("scroll", f"⚠️  scroll 翻页失败: {e}")
            break

    # 清除 scroll 上下文
    try:
        es.clear_scroll(scroll_id=scroll_id)
    except Exception:
        pass

    _log("scroll", f"共遍历 {count:,} 条记录")


# ---------------------------------------------------------------------------
# Step 2: S2AG Batch API 批量抓取摘要
# ---------------------------------------------------------------------------


def fetch_abstracts_from_s2ag_batch(
    docs: list[dict],
    api_key: str,
) -> dict[str, str]:
    """调用 S2AG Batch API 批量获取摘要（按 DOI 查询）。

    将 docs 列表按 S2AG_BATCH_SIZE 分块，每块发送一次 POST 请求。
    使用 API Key 时须遵守 1 次/秒限速。

    Args:
        docs: 文档列表，每项须含 'doi' 字段。
        api_key: Semantic Scholar API 密钥。

    Returns:
        {doi: abstract} 字典，仅包含成功获取到摘要的结果。
    """
    if not docs:
        return {}

    headers = {"x-api-key": api_key}
    results: dict[str, str] = {}
    total_chunks = (len(docs) + S2AG_BATCH_SIZE - 1) // S2AG_BATCH_SIZE

    for chunk_idx in range(total_chunks):
        start = chunk_idx * S2AG_BATCH_SIZE
        end = min(start + S2AG_BATCH_SIZE, len(docs))
        chunk = docs[start:end]

        # 组装 S2AG 请求格式："DOI:10.xxx"
        ids_payload = [f"DOI:{doc['doi']}" for doc in chunk]

        # 限速
        if chunk_idx > 0:
            time.sleep(S2AG_RATE_LIMIT_SECS)

        _log(
            "s2ag",
            f"[{chunk_idx + 1}/{total_chunks}] 请求 {len(ids_payload)} 条 DOI...",
        )

        try:
            resp = requests.post(
                S2AG_BATCH_URL,
                headers=headers,
                json={"ids": ids_payload},
                timeout=60,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            _log("s2ag", f"❌ 请求失败: {e}")
            continue

        papers = resp.json()
        found = 0
        for paper in papers:
            if paper and paper.get("abstract"):
                doi = paper.get("externalIds", {}).get("DOI")
                if doi:
                    results[doi] = paper["abstract"].strip()
                    found += 1

        _log("s2ag", f"  本批获取 {found} 条摘要")

    return results


def fetch_abstracts_from_s2ag_arxiv_batch(
    docs: list[dict],
    api_key: str,
) -> dict[str, str]:
    """调用 S2AG Batch API 批量获取摘要（按 ArXiv ID 查询）。

    专为有 arxiv_id 但无 DOI 的记录设计（Phase 1.5）。
    S2AG 支持 "ARXIV:xxx" 格式，速率与 DOI 批量查询相同。

    Args:
        docs: 文档列表，每项须含 'arxiv_id' 字段。
        api_key: Semantic Scholar API 密钥。

    Returns:
        {normalized_arxiv_id: abstract} 字典。
    """
    if not docs:
        return {}

    headers = {"x-api-key": api_key}
    results: dict[str, str] = {}
    total_chunks = (len(docs) + S2AG_BATCH_SIZE - 1) // S2AG_BATCH_SIZE

    for chunk_idx in range(total_chunks):
        start = chunk_idx * S2AG_BATCH_SIZE
        end = min(start + S2AG_BATCH_SIZE, len(docs))
        chunk = docs[start:end]

        # 组装 S2AG 请求格式："ARXIV:cs.SE/0010035"
        ids_payload = [f"ARXIV:{_normalize_arxiv_id(doc['arxiv_id'])}" for doc in chunk]

        if chunk_idx > 0:
            time.sleep(S2AG_RATE_LIMIT_SECS)

        _log(
            "s2ag_arxiv",
            f"[{chunk_idx + 1}/{total_chunks}] 请求 {len(ids_payload)} 条 ArXiv ID...",
        )

        try:
            resp = requests.post(
                S2AG_BATCH_URL,
                headers=headers,
                json={"ids": ids_payload},
                params={"fields": "abstract,externalIds"},
                timeout=60,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            _log("s2ag_arxiv", f"❌ 请求失败: {e}")
            continue

        papers = resp.json()
        found = 0
        for paper in papers:
            if not paper or not paper.get("abstract"):
                continue
            # S2AG 返回的 externalIds.ArXiv 即为规范化 ID（无版本号）
            arxiv_id = paper.get("externalIds", {}).get("ArXiv", "")
            if arxiv_id:
                results[_normalize_arxiv_id(arxiv_id)] = paper["abstract"].strip()
                found += 1

        _log("s2ag_arxiv", f"  本批获取 {found} 条摘要")

    return results



# ---------------------------------------------------------------------------
# OpenAlex 倒排索引还原
# ---------------------------------------------------------------------------


def reconstruct_abstract(inverted_index: dict[str, list[int]]) -> str | None:
    """将 OpenAlex 的倒排索引 (abstract_inverted_index) 还原为完整摘要文本。

    Args:
        inverted_index: OpenAlex 返回的倒排索引，格式为 {"word": [pos1, pos2, ...]}。

    Returns:
        还原后的摘要文本；若倒排索引为空则返回 None。
    """
    if not inverted_index:
        return None

    # 找出摘要的总长度（位置索引最大值）
    max_index = 0
    for positions in inverted_index.values():
        if positions:
            max_index = max(max_index, max(positions))

    # 初始化占位列表，将单词填入对应位置
    abstract_words = [""] * (max_index + 1)
    for word, positions in inverted_index.items():
        for pos in positions:
            abstract_words[pos] = word

    return " ".join(abstract_words).strip()


def fetch_abstract_from_openalex(
    doi: str,
    email: str = "",
) -> str | None:
    """从 OpenAlex API 获取单篇论文摘要。

    通过 DOI 查询 OpenAlex Works API，将倒排索引还原为可读摘要。
    建议提供 email 以进入 Polite Pool（更快的响应速度和更宽松的限额）。

    Args:
        doi: 论文 DOI（如 "10.1016/j.artint.2023.103"）。
        email: 可选，提供给 OpenAlex 的邮箱，进入礼貌池。

    Returns:
        摘要文本，获取失败或无摘要则返回 None。
    """
    if not doi:
        return None

    # 拼接完整 DOI URL 格式（OpenAlex API 要求）
    if not doi.startswith("https://doi.org/"):
        doi_url = f"https://doi.org/{doi}"
    else:
        doi_url = doi

    url = f"https://api.openalex.org/works/{doi_url}"
    params: dict[str, str] = {}
    if email:
        params[OPENALEX_MAILTO_PARAM] = email

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()

        inverted_index = data.get("abstract_inverted_index")
        return reconstruct_abstract(inverted_index)
    except requests.RequestException as e:
        _log("openalex", f"⚠️  获取 DOI:{doi} 失败: {e}")
        return None


def fetch_abstracts_from_openalex_batch(
    docs: list[dict],
    email: str = "",
) -> dict[str, str]:
    """对一批有 DOI 的文档逐个调用 OpenAlex API 获取摘要。

    OpenAlex 暂不支持批量 POST 查询（与 S2AG 不同），因此逐条请求。
    提供 email 可进入 Polite Pool，限速约 10 次/秒。

    Args:
        docs: 文档列表，每项须含 'doi' 字段。
        email: 提供给 OpenAlex 的邮箱，进入礼貌池。

    Returns:
        {doi: abstract} 字典，仅包含成功获取到摘要的结果。
    """
    results: dict[str, str] = {}
    total = len(docs)

    for idx, doc in enumerate(docs, start=1):
        doi = doc.get("doi", "")
        if not doi:
            continue

        # 限速
        if idx > 1:
            time.sleep(OPENALEX_RATE_LIMIT_SECS)

        if idx % 100 == 0 or idx == total:
            _log("openalex", f"[{idx}/{total}] 正在查询 DOI:{doi}...")

        abstract = fetch_abstract_from_openalex(doi, email)
        if abstract:
            results[doi] = abstract

    _log("openalex", f"OpenAlex 获取 {len(results)}/{total} 条摘要")
    return results


# ---------------------------------------------------------------------------
# Step 3: ArXiv API 增量兜底
# ---------------------------------------------------------------------------


def _normalize_arxiv_id(arxiv_id: str) -> str:
    """去除 ArXiv ID 末尾的版本号后缀（如 v1、v2）。"""
    return re.sub(r'v\d+$', '', arxiv_id)


def fetch_abstracts_from_arxiv_batch(
    arxiv_ids: list[str],
    retry_wait: float = ARXIV_RETRY_BASE_WAIT,
) -> dict[str, str]:
    """批量调用 ArXiv API 获取摘要，含指数退避重试。

    Args:
        arxiv_ids: ArXiv ID 列表（可含版本号后缀，如 2301.01234v1）。
        retry_wait: 首次 429 等待秒数，后续每次翻倍。

    Returns:
        {normalized_arxiv_id: abstract} 字典，key 已去除版本号后缀。
    """
    if not arxiv_ids:
        return {}

    id_list_str = ",".join(arxiv_ids)
    url = (
        f"http://export.arxiv.org/api/query"
        f"?id_list={id_list_str}"
        f"&max_results={len(arxiv_ids)}"
    )

    results: dict[str, str] = {}
    wait = retry_wait
    for attempt in range(ARXIV_RETRY_MAX + 1):
        try:
            response = urllib.request.urlopen(url, timeout=90).read()
            root = ET.fromstring(response)

            for entry in root.findall("atom:entry", ARXIV_NAMESPACE):
                id_elem = entry.find("atom:id", ARXIV_NAMESPACE)
                if id_elem is None or not id_elem.text:
                    continue
                raw_id = id_elem.text.strip().split("/abs/")[-1]
                normalized = _normalize_arxiv_id(raw_id)
                summary = entry.find("atom:summary", ARXIV_NAMESPACE)
                if summary is not None and summary.text:
                    results[normalized] = summary.text.strip().replace("\n", " ")
            return results  # 成功则直接返回

        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt < ARXIV_RETRY_MAX:
                    _log(
                        "arxiv",
                        f"⚠️  429 限速（第 {attempt + 1}/{ARXIV_RETRY_MAX} 次重试），"
                        f"等待 {wait:.0f}s...",
                    )
                    time.sleep(wait)
                    wait = min(wait * 2, 300.0)  # 指数退避，上限 5 分钟
                else:
                    _log("arxiv", f"⚠️  429 已达最大重试次数，放弃本批 {len(arxiv_ids)} 条")
            else:
                _log("arxiv", f"⚠️  HTTP {e.code}，放弃本批: {e}")
                break
        except Exception as e:
            if attempt < ARXIV_RETRY_MAX:
                _log(
                    "arxiv",
                    f"⚠️  连接失败（第 {attempt + 1}/{ARXIV_RETRY_MAX} 次重试），"
                    f"等待 {wait:.0f}s: {e}",
                )
                time.sleep(wait)
                wait = min(wait * 2, 300.0)
            else:
                _log("arxiv", f"⚠️  批量查询失败（{len(arxiv_ids)} 条 ID）: {e}")
                break

    return results


def fetch_abstract_from_arxiv(arxiv_id: str) -> str | None:
    """从 ArXiv API 获取单篇论文摘要（内部调用批量接口）。

    Args:
        arxiv_id: ArXiv 论文 ID（如 "2301.01234" 或 "2301.01234v1"）。

    Returns:
        摘要文本，获取失败返回 None。
    """
    results = fetch_abstracts_from_arxiv_batch([arxiv_id])
    normalized = _normalize_arxiv_id(arxiv_id)
    return results.get(normalized) or results.get(arxiv_id)


# ---------------------------------------------------------------------------
# Step 4: 批量更新 ES 摘要字段
# ---------------------------------------------------------------------------


def update_es_abstracts(
    es: Elasticsearch,
    alias: str,
    updates: list[dict],
) -> tuple[int, int]:
    """批量更新 ES 文档的摘要字段。

    Args:
        es: Elasticsearch 客户端。
        alias: ES 别名/索引名。
        updates: 更新列表，每项含 _id, abstract, abstract_source。

    Returns:
        (成功数, 失败数)
    """
    if not updates:
        return 0, 0

    actions: list[dict] = []
    for item in updates:
        actions.append({
            "_op_type": "update",
            "_index": alias,
            "_id": item["_id"],
            "doc": {
                "abstract": item["abstract"],
                "abstract_source": item["abstract_source"],
            },
        })

    ok, errors = bulk(
        es,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
    )
    err_count = len(errors) if isinstance(errors, list) else 0
    if errors and err_count > 0:
        for err in errors[:3]:
            _log("bulk", f"  错误示例: {err}")

    return ok, err_count


# ---------------------------------------------------------------------------
# Step 5: 断点续传状态管理
# ---------------------------------------------------------------------------


def _state_path(output_dir: Path) -> Path:
    """返回状态文件路径。"""
    return output_dir / "supplement_state.json"


def load_state(output_dir: Path) -> dict:
    """加载断点续传状态。

    Returns:
        状态字典，包含：
        - s2ag_processed_dois: 已通过 S2AG 处理过的 DOI 集合
        - arxiv_processed_ids: 已通过 ArXiv 处理过的 arxiv_id 集合
        - openalex_processed_dois: 已通过 OpenAlex 处理过的 DOI 集合
        - total_s2ag_ok: S2AG 累计更新成功数
        - total_arxiv_ok: ArXiv 累计更新成功数
        - total_openalex_ok: OpenAlex 累计更新成功数
    """
    sp = _state_path(output_dir)
    if sp.exists():
        try:
            data = json.loads(sp.read_text())
            _log("state", f"断点续传：S2AG 已处理 {len(data.get('s2ag_processed_dois', []))} DOI，"
                 f"ArXiv 已处理 {len(data.get('arxiv_processed_ids', []))} ID，"
                 f"OpenAlex 已处理 {len(data.get('openalex_processed_dois', []))} DOI")
            return data
        except Exception:
            pass
    return {
        "s2ag_processed_dois": [],
        "arxiv_processed_ids": [],
        "openalex_processed_dois": [],
        "total_s2ag_ok": 0,
        "total_arxiv_ok": 0,
        "total_openalex_ok": 0,
    }


def save_state(output_dir: Path, state: dict) -> None:
    """保存断点续传状态。"""
    sp = _state_path(output_dir)
    state["updated_at"] = _now_str()
    sp.write_text(json.dumps(state, ensure_ascii=False))


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def run(
    es_host: str,
    es_alias: str,
    api_key: str,
    output_dir: Path,
    no_s2ag: bool = False,
    no_arxiv: bool = False,
    no_openalex: bool = False,
    openalex_email: str = "",
    overwrite: bool = False,
    limit: int = 0,
) -> None:
    """执行增量摘要补全流水线。

    优先级：S2AG → ArXiv → OpenAlex，前者获取成功后后者不再重复。

    Args:
        es_host: Elasticsearch 地址。
        es_alias: ES 别名/索引名。
        api_key: Semantic Scholar API 密钥。
        output_dir: 状态文件存储目录。
        no_s2ag: 跳过 S2AG API 补全。
        no_arxiv: 跳过 ArXiv API 兜底。
        no_openalex: 跳过 OpenAlex API 兜底。
        openalex_email: 提供给 OpenAlex 的邮箱（进入礼貌池）。
        overwrite: 强制覆盖已有摘要。
        limit: 最多处理记录数（0=不限）。
    """
    print("=" * 65)
    print("  S2AG + ArXiv + OpenAlex 增量摘要补全")
    print(f"  ES 地址:      {es_host}")
    print(f"  ES 别名:      {es_alias}")
    print(f"  S2AG 补全:    {'禁用' if no_s2ag else '启用'}")
    print(f"  ArXiv 兜底:   {'禁用' if no_arxiv else '启用'}")
    print(f"  OpenAlex 兜底:{'禁用' if no_openalex else '启用'}")
    print(f"  覆盖模式:     {'是' if overwrite else '否（仅填充空摘要）'}")
    print(f"  处理上限:     {'全量' if limit == 0 else f'{limit:,} 条'}")
    if not no_s2ag:
        print(f"  API Key:      {api_key[:8]}...{api_key[-4:]}")
    if not no_openalex:
        print(f"  OpenAlex邮箱: {openalex_email or '未设置（建议设置以进入礼貌池）'}")
    print("=" * 65)

    pipeline_start = time.time()

    # --- 连接 ES ---
    _log("run", f"连接 Elasticsearch: {es_host}")
    es = Elasticsearch(hosts=[es_host], request_timeout=60)
    try:
        info = es.info()
        _log("run", f"ES 连接成功，版本: {info['version']['number']}")
    except Exception as e:
        _log("run", f"❌ ES 连接失败: {e}")
        sys.exit(1)

    # --- 加载断点状态 ---
    output_dir.mkdir(parents=True, exist_ok=True)
    state = load_state(output_dir)
    s2ag_processed_dois: set[str] = set(state.get("s2ag_processed_dois", []))
    arxiv_processed_ids: set[str] = set(state.get("arxiv_processed_ids", []))
    openalex_processed_dois: set[str] = set(state.get("openalex_processed_dois", []))
    total_s2ag_ok: int = state.get("total_s2ag_ok", 0)
    total_arxiv_ok: int = state.get("total_arxiv_ok", 0)
    total_openalex_ok: int = state.get("total_openalex_ok", 0)

    # --- Phase 1: S2AG Batch API ---
    if not no_s2ag:
        _log("run", "=" * 40)
        _log("run", "Phase 1: S2AG Batch API 补全")
        _log("run", "=" * 40)

        # 流式处理：边 scroll 边积累，满 S2AG_BATCH_SIZE 条立即发请求并写 ES
        s2ag_buf: list[dict] = []      # 当前积累的 doc 缓冲区
        batch_updates: list[dict] = [] # 待写入 ES 的更新列表
        batch_idx = 0                  # 批次计数
        total_scrolled = 0             # scroll 遍历总数（含已跳过的）

        def _flush_s2ag_buf() -> None:
            """将 s2ag_buf 中的文档发送到 S2AG API，结果追加到 batch_updates。"""
            nonlocal batch_idx, total_s2ag_ok
            if not s2ag_buf:
                return

            batch_idx += 1
            _log(
                "s2ag",
                f"[批次 {batch_idx}] 处理 {len(s2ag_buf)} 条 DOI "
                f"(累计已处理 {len(s2ag_processed_dois):,} / scroll 遍历 {total_scrolled:,})",
            )

            doi_to_abstract = fetch_abstracts_from_s2ag_batch(s2ag_buf, api_key)

            for doc in s2ag_buf:
                doi = doc["doi"]
                if doi in doi_to_abstract:
                    batch_updates.append({
                        "_id": doc["_id"],
                        "abstract": doi_to_abstract[doi],
                        "abstract_source": "S2AG_API",
                    })
                s2ag_processed_dois.add(doi)

            s2ag_buf.clear()

            # 每批立即写入 ES（不等攒够 ES_BULK_SIZE，避免数据积压丢失）
            if batch_updates:
                ok, err = update_es_abstracts(es, es_alias, batch_updates)
                total_s2ag_ok += ok
                if err > 0:
                    _log("s2ag", f"⚠️  ES 写入失败 {err} 条")
                _log("s2ag", f"  本批写入 ES {ok} 条")
                batch_updates.clear()

            # 每批保存一次断点状态
            state.update({
                "s2ag_processed_dois": sorted(s2ag_processed_dois),
                "total_s2ag_ok": total_s2ag_ok,
            })
            save_state(output_dir, state)

        for doc in iter_docs_without_abstract(es, es_alias, overwrite, limit):
            total_scrolled += 1
            if doc["doi"] in s2ag_processed_dois:
                continue  # 断点续传：跳过已处理的 DOI
            s2ag_buf.append(doc)
            if len(s2ag_buf) >= S2AG_BATCH_SIZE:
                _flush_s2ag_buf()

        # 处理尾部不足一批的剩余文档
        _flush_s2ag_buf()

        # 写入剩余未达 ES_BULK_SIZE 的更新
        if batch_updates:
            ok, err = update_es_abstracts(es, es_alias, batch_updates)
            total_s2ag_ok += ok
            batch_updates.clear()

        state["total_s2ag_ok"] = total_s2ag_ok
        save_state(output_dir, state)

        _log("run", f"Phase 1 完成: S2AG 累计更新 {total_s2ag_ok:,} 条")

    # --- Phase 1.5: S2AG ArXiv 批量补全（arxiv_id 有但 DOI 缺失的记录）---
    if not no_s2ag:
        _log("run", "=" * 40)
        _log("run", "Phase 1.5: S2AG ArXiv ID 批量补全")
        _log("run", "=" * 40)

        s2ag_arxiv_processed_ids: set[str] = set(
            state.get("s2ag_arxiv_processed_ids", [])
        )
        total_s2ag_arxiv_ok: int = state.get("total_s2ag_arxiv_ok", 0)

        # 查询有 arxiv_id 但无摘要的记录（无论是否有 DOI）
        arxiv_pending_query: dict = {"bool": {"must": [
            {"exists": {"field": "arxiv_id"}},
        ]}}
        if not overwrite:
            arxiv_pending_query["bool"]["must"].append(
                {"bool": {"must_not": {"exists": {"field": "abstract_source"}}}}
            )

        s2ag_arxiv_buf: list[dict] = []
        s2ag_arxiv_updates: list[dict] = []
        s2ag_arxiv_batch_idx = 0
        total_scrolled_s2ag_arxiv = 0

        def _flush_s2ag_arxiv_buf() -> None:
            """将 s2ag_arxiv_buf 通过 S2AG ARXIV 批量接口处理。"""
            nonlocal s2ag_arxiv_batch_idx, total_s2ag_arxiv_ok
            if not s2ag_arxiv_buf:
                return

            if s2ag_arxiv_batch_idx > 0:
                time.sleep(S2AG_RATE_LIMIT_SECS)

            s2ag_arxiv_batch_idx += 1
            _log(
                "s2ag_arxiv",
                f"[批次 {s2ag_arxiv_batch_idx}] 处理 {len(s2ag_arxiv_buf)} 条 "
                f"(scroll 遍历 {total_scrolled_s2ag_arxiv:,})",
            )

            abstract_map = fetch_abstracts_from_s2ag_arxiv_batch(s2ag_arxiv_buf, api_key)
            found = 0
            for doc in s2ag_arxiv_buf:
                aid = _normalize_arxiv_id(doc["arxiv_id"])
                abstract = abstract_map.get(aid)
                if abstract:
                    s2ag_arxiv_updates.append({
                        "_id": doc["_id"],
                        "abstract": abstract,
                        "abstract_source": "S2AG_ArXiv",
                    })
                    found += 1
                s2ag_arxiv_processed_ids.add(aid)

            _log("s2ag_arxiv", f"  本批获取 {found}/{len(s2ag_arxiv_buf)} 条摘要")
            s2ag_arxiv_buf.clear()

            # 每批立即写入 ES
            if s2ag_arxiv_updates:
                ok, err = update_es_abstracts(es, es_alias, s2ag_arxiv_updates)
                total_s2ag_arxiv_ok += ok
                if err > 0:
                    _log("s2ag_arxiv", f"⚠️  ES 写入失败 {err} 条")
                _log("s2ag_arxiv", f"  本批写入 ES {ok} 条")
                s2ag_arxiv_updates.clear()

            state.update({
                "s2ag_arxiv_processed_ids": sorted(s2ag_arxiv_processed_ids),
                "total_s2ag_arxiv_ok": total_s2ag_arxiv_ok,
            })
            save_state(output_dir, state)

        try:
            resp_s2a = es.search(
                index=es_alias,
                query=arxiv_pending_query,
                _source=["doi", "arxiv_id", "dblp_key"],
                size=ES_SCROLL_SIZE,
                scroll=ES_SCROLL_TIMEOUT,
            )
            scroll_id_s2a = resp_s2a.get("_scroll_id", "")
            s2a_total = resp_s2a["hits"]["total"]["value"]
            _log("s2ag_arxiv", f"有 arxiv_id 且无摘要的记录: {s2a_total:,} 条")

            while True:
                hits = resp_s2a["hits"]["hits"]
                if not hits:
                    break
                for hit in hits:
                    total_scrolled_s2ag_arxiv += 1
                    src = hit["_source"]
                    aid = src.get("arxiv_id") or ""
                    if not aid or not is_valid_arxiv_id(aid):
                        continue
                    norm = _normalize_arxiv_id(aid)
                    if norm in s2ag_arxiv_processed_ids:
                        continue
                    s2ag_arxiv_buf.append({
                        "_id": hit["_id"],
                        "arxiv_id": aid,
                        "doi": src.get("doi", ""),
                    })
                    if len(s2ag_arxiv_buf) >= S2AG_BATCH_SIZE:
                        _flush_s2ag_arxiv_buf()

                try:
                    resp_s2a = es.scroll(
                        scroll_id=scroll_id_s2a, scroll=ES_SCROLL_TIMEOUT
                    )
                    scroll_id_s2a = resp_s2a.get("_scroll_id", scroll_id_s2a)
                except Exception:
                    break

            try:
                es.clear_scroll(scroll_id=scroll_id_s2a)
            except Exception:
                pass

        except Exception as e:
            _log("s2ag_arxiv", f"❌ scroll 查询失败: {e}")

        _flush_s2ag_arxiv_buf()

        if s2ag_arxiv_updates:
            ok, err = update_es_abstracts(es, es_alias, s2ag_arxiv_updates)
            total_s2ag_arxiv_ok += ok
            s2ag_arxiv_updates.clear()

        state.update({
            "s2ag_arxiv_processed_ids": sorted(s2ag_arxiv_processed_ids),
            "total_s2ag_arxiv_ok": total_s2ag_arxiv_ok,
        })
        save_state(output_dir, state)
        _log("run", f"Phase 1.5 完成: S2AG ArXiv 累计更新 {total_s2ag_arxiv_ok:,} 条")

    # --- Phase 2: ArXiv API 兜底（S2AG 找不到的剩余部分）---
    if not no_arxiv:
        _log("run", "=" * 40)
        _log("run", "Phase 2: ArXiv API 兜底")
        _log("run", "=" * 40)

        must_clauses: list[dict] = [
            {"exists": {"field": "arxiv_id"}},
        ]
        if not overwrite:
            must_clauses.append(
                {"bool": {"must_not": {"exists": {"field": "abstract_source"}}}}
            )
        arxiv_query = {"bool": {"must": must_clauses}}

        # 流式处理：边 scroll 边积累，满 ARXIV_BATCH_SIZE 条立即批量查询
        arxiv_buf: list[dict] = []    # 当前积累的 doc 缓冲区
        arxiv_updates: list[dict] = [] # 待写入 ES 的更新列表
        arxiv_batch_idx = 0
        total_scrolled_arxiv = 0

        def _flush_arxiv_buf() -> None:
            """将 arxiv_buf 批量提交到 ArXiv API，结果追加到 arxiv_updates。"""
            nonlocal arxiv_batch_idx, total_arxiv_ok
            if not arxiv_buf:
                return

            # 限速：批次之间等待（第一批无需等待）
            if arxiv_batch_idx > 0:
                time.sleep(ARXIV_RATE_LIMIT_SECS)

            arxiv_batch_idx += 1
            ids = [doc["arxiv_id"] for doc in arxiv_buf]
            _log(
                "arxiv",
                f"[批次 {arxiv_batch_idx}] 查询 {len(ids)} 条 ArXiv ID "
                f"(scroll 遍历 {total_scrolled_arxiv:,})",
            )

            abstract_map = fetch_abstracts_from_arxiv_batch(ids)
            found = 0
            for doc in arxiv_buf:
                aid = doc["arxiv_id"]
                normalized = _normalize_arxiv_id(aid)
                abstract = abstract_map.get(normalized) or abstract_map.get(aid)
                if abstract:
                    arxiv_updates.append({
                        "_id": doc["_id"],
                        "abstract": abstract,
                        "abstract_source": "ArXiv",
                    })
                    found += 1
                arxiv_processed_ids.add(aid)

            _log("arxiv", f"  本批获取 {found}/{len(ids)} 条摘要")
            arxiv_buf.clear()

            # 每批立即写入 ES
            if arxiv_updates:
                ok, err = update_es_abstracts(es, es_alias, arxiv_updates)
                total_arxiv_ok += ok
                if err > 0:
                    _log("arxiv", f"⚠️  ES 写入失败 {err} 条")
                _log("arxiv", f"  本批写入 ES {ok} 条")
                arxiv_updates.clear()

            # 每批保存一次断点状态
            state.update({
                "arxiv_processed_ids": sorted(arxiv_processed_ids),
                "total_arxiv_ok": total_arxiv_ok,
            })
            save_state(output_dir, state)

        try:
            resp = es.search(
                index=es_alias,
                query=arxiv_query,
                _source=["doi", "arxiv_id", "dblp_key"],
                size=ES_SCROLL_SIZE,
                scroll=ES_SCROLL_TIMEOUT,
            )
            scroll_id = resp.get("_scroll_id", "")
            arxiv_total = resp["hits"]["total"]["value"]
            _log("arxiv", f"有 arxiv_id 且无摘要的记录: {arxiv_total:,} 条")

            while True:
                hits = resp["hits"]["hits"]
                if not hits:
                    break

                for hit in hits:
                    total_scrolled_arxiv += 1
                    src = hit["_source"]
                    aid = src.get("arxiv_id") or ""
                    # 过滤无效 arxiv_id（分类前缀如 'cs'、'math'、'cs.AI'）及已处理
                    if not aid or not is_valid_arxiv_id(aid) or aid in arxiv_processed_ids:
                        continue
                    arxiv_buf.append({
                        "_id": hit["_id"],
                        "doi": src.get("doi", ""),
                        "arxiv_id": aid,
                    })
                    if len(arxiv_buf) >= ARXIV_BATCH_SIZE:
                        _flush_arxiv_buf()

                try:
                    resp = es.scroll(scroll_id=scroll_id, scroll=ES_SCROLL_TIMEOUT)
                    scroll_id = resp.get("_scroll_id", scroll_id)
                except Exception:
                    break

            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass

        except Exception as e:
            _log("arxiv", f"❌ ArXiv scroll 查询失败: {e}")

        # 处理尾部不足一批的剩余文档
        _flush_arxiv_buf()

        # 写入剩余未达 ES_BULK_SIZE 的更新
        if arxiv_updates:
            ok, err = update_es_abstracts(es, es_alias, arxiv_updates)
            total_arxiv_ok += ok
            arxiv_updates.clear()

        state.update({
            "arxiv_processed_ids": sorted(arxiv_processed_ids),
            "total_arxiv_ok": total_arxiv_ok,
        })
        save_state(output_dir, state)

        _log("run", f"Phase 2 完成: ArXiv 累计更新 {total_arxiv_ok:,} 条")

    # --- Phase 3: OpenAlex API 兜底 ---
    if not no_openalex:
        _log("run", "=" * 40)
        _log("run", "Phase 3: OpenAlex API 兜底")
        _log("run", "=" * 40)

        # 重新 scroll：查找仍然无摘要但有 DOI 的记录（OpenAlex 也按 DOI 查询）
        must_clauses_ol: list[dict] = [
            {"exists": {"field": "doi"}},  # 必须有 DOI
        ]
        if not overwrite:
            must_clauses_ol.append(
                {"bool": {"must_not": {"exists": {"field": "abstract_source"}}}}
            )

        openalex_query = {"bool": {"must": must_clauses_ol}}

        # 流式处理：边 scroll 边逐条查 OpenAlex，与 Phase 1/2 一致
        openalex_updates: list[dict] = []
        total_scrolled_ol = 0
        total_openalex_hits = 0

        try:
            resp = es.search(
                index=es_alias,
                query=openalex_query,
                _source=["doi", "arxiv_id", "dblp_key"],
                size=ES_SCROLL_SIZE,
                scroll=ES_SCROLL_TIMEOUT,
            )
            scroll_id = resp.get("_scroll_id", "")
            openalex_total = resp["hits"]["total"]["value"]
            total_openalex_hits = openalex_total
            _log("openalex", f"有 DOI 且仍无摘要的记录: {openalex_total:,} 条")

            while True:
                hits = resp["hits"]["hits"]
                if not hits:
                    break

                for hit in hits:
                    total_scrolled_ol += 1
                    src = hit["_source"]
                    doi = src.get("doi") or ""
                    if not doi or doi in openalex_processed_dois:
                        continue

                    # 限速
                    if total_scrolled_ol > 1:
                        time.sleep(OPENALEX_RATE_LIMIT_SECS)

                    # 每 500 条或首条输出进度
                    if total_scrolled_ol % 500 == 1:
                        _log(
                            "openalex",
                            f"[{total_scrolled_ol:,}/{openalex_total:,}] "
                            f"查询 DOI:{doi}... 已获取 {len(openalex_updates)} 条摘要",
                        )

                    abstract = fetch_abstract_from_openalex(doi, openalex_email)
                    openalex_processed_dois.add(doi)

                    if abstract:
                        openalex_updates.append({
                            "_id": hit["_id"],
                            "abstract": abstract,
                            "abstract_source": "OpenAlex",
                        })

                    # 积累够 ES_BULK_SIZE 时写入 ES
                    if len(openalex_updates) >= ES_BULK_SIZE:
                        ok, err = update_es_abstracts(es, es_alias, openalex_updates)
                        total_openalex_ok += ok
                        if err > 0:
                            _log("openalex", f"⚠️  ES 写入失败 {err} 条")
                        openalex_updates.clear()

                    # 每 500 条保存一次状态
                    if total_scrolled_ol % 500 == 0:
                        state.update({
                            "openalex_processed_dois": sorted(openalex_processed_dois),
                            "total_openalex_ok": total_openalex_ok,
                        })
                        save_state(output_dir, state)

                try:
                    resp = es.scroll(scroll_id=scroll_id, scroll=ES_SCROLL_TIMEOUT)
                    scroll_id = resp.get("_scroll_id", scroll_id)
                except Exception:
                    break

            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass

        except Exception as e:
            _log("openalex", f"❌ OpenAlex scroll 查询失败: {e}")

        # 写入剩余
        if openalex_updates:
            ok, err = update_es_abstracts(es, es_alias, openalex_updates)
            total_openalex_ok += ok
            openalex_updates.clear()

        state.update({
            "openalex_processed_dois": sorted(openalex_processed_dois),
            "total_openalex_ok": total_openalex_ok,
        })
        save_state(output_dir, state)

        _log("run", f"Phase 3 完成: OpenAlex 累计更新 {total_openalex_ok:,} 条 "
                   f"(遍历 {total_scrolled_ol:,}/{total_openalex_hits:,})")

    es.close()

    # --- 最终报告 ---
    elapsed = time.time() - pipeline_start
    print("\n" + "=" * 65)
    print("  ✅ 增量摘要补全完成！")
    print(f"  S2AG 更新:     {total_s2ag_ok:,} 条")
    print(f"  ArXiv 更新:    {total_arxiv_ok:,} 条")
    print(f"  OpenAlex 更新: {total_openalex_ok:,} 条")
    print(f"  总更新:        {total_s2ag_ok + total_arxiv_ok + total_openalex_ok:,} 条")
    print(f"  总耗时:        {elapsed:.1f}s ({elapsed / 3600:.2f}h)")
    print("=" * 65)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="S2AG + ArXiv + OpenAlex 增量摘要补全脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--api-key",
        default=DEFAULT_S2AG_API_KEY,
        help="Semantic Scholar API Key（默认使用环境变量 S2AG_API_KEY）",
    )
    parser.add_argument(
        "--es-host",
        default=DEFAULT_ES_HOST,
        help=f"Elasticsearch 地址（默认 {DEFAULT_ES_HOST}）",
    )
    parser.add_argument(
        "--es-alias",
        default=DEFAULT_ES_ALIAS,
        help=f"ES 别名或索引名（默认 {DEFAULT_ES_ALIAS}）",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("S2AG_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        help=f"状态文件存储目录（默认 {DEFAULT_OUTPUT_DIR}）",
    )
    parser.add_argument(
        "--no-s2ag",
        action="store_true",
        help="跳过 S2AG Batch API 补全",
    )
    parser.add_argument(
        "--no-arxiv",
        action="store_true",
        help="跳过 ArXiv API 兜底",
    )
    parser.add_argument(
        "--no-openalex",
        action="store_true",
        help="跳过 OpenAlex API 兜底",
    )
    parser.add_argument(
        "--openalex-email",
        default=DEFAULT_OPENALEX_EMAIL,
        help="OpenAlex Polite Pool 邮箱（默认使用环境变量 OPENALEX_EMAIL）",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="强制覆盖 ES 中已有的摘要（默认仅填充空摘要）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多处理记录数（0=不限，用于测试）",
    )

    args = parser.parse_args()

    if args.no_s2ag and args.no_arxiv and args.no_openalex:
        parser.error("--no-s2ag、--no-arxiv 和 --no-openalex 不能同时使用")

    if not args.no_s2ag and not args.api_key:
        parser.error("S2AG 补全需要 API Key，请通过 --api-key 或环境变量 S2AG_API_KEY 提供")

    run(
        es_host=args.es_host,
        es_alias=args.es_alias,
        api_key=args.api_key,
        output_dir=Path(args.output_dir),
        no_s2ag=args.no_s2ag,
        no_arxiv=args.no_arxiv,
        no_openalex=args.no_openalex,
        openalex_email=args.openalex_email,
        overwrite=args.overwrite,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
