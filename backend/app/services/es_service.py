"""
Elasticsearch 服务层 - 连接管理、索引创建、文档写入与搜索
搜索算法：BM25（ES 8.x 默认），可调参 k1/b
"""

from elasticsearch import AsyncElasticsearch, NotFoundError, helpers

from app.core.config import settings
from app.models.schemas import PaperDoc, PaperSearchResponse, PaperSearchResult

# ---------------------------------------------------------------------------
# 索引 Mapping
# ---------------------------------------------------------------------------
# BM25 参数说明：
#   k1 (1.2): 控制词频饱和速度，越小饱和越快（默认 1.2）
#   b  (0.75): 文档长度归一化系数，0=关闭长度影响，1=完全归一化
#   学术标题一般较短，设 b=0.5 减少长文皇利

PAPERS_MAPPING: dict = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "similarity": {
            "paper_bm25": {
                "type": "BM25",
                "k1": 1.2,
                "b": 0.5,     # 学术标题较短，降低长度影响
            }
        },
        "analysis": {
            "analyzer": {
                "default": {"type": "english"},   # 英语词干、停用词过滤
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
            # title: BM25 + 定制分析器（boost 在查询时设置，ES 8.x 已弃用 Mapping 级 boost）
            "title": {
                "type": "text",
                "analyzer": "title_analyzer",
                "similarity": "paper_bm25",
                # 子字段：存储原始内容用于高亮返回
                "fields": {
                    "keyword": {"type": "keyword"},
                    "raw": {"type": "text", "analyzer": "standard"},
                },
            },
            # authors: BM25
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
        }
    },
}


# ---------------------------------------------------------------------------
# 客户端单例
# ---------------------------------------------------------------------------

_es_client: AsyncElasticsearch | None = None


def get_es_client() -> AsyncElasticsearch:
    """获取（或懒惰初始化）AsyncElasticsearch 单例"""
    global _es_client
    if _es_client is None:
        _es_client = AsyncElasticsearch(
            hosts=[settings.ES_HOST],
            request_timeout=30,
        )
    return _es_client


async def close_es_client() -> None:
    """关闭 ES 连接（在应用 shutdown 时调用）"""
    global _es_client
    if _es_client is not None:
        await _es_client.close()
        _es_client = None


# ---------------------------------------------------------------------------
# 索引管理
# ---------------------------------------------------------------------------

async def ensure_index() -> None:
    """若索引不存在则创建，幂等操作"""
    es = get_es_client()
    index = settings.ES_INDEX_PAPERS
    try:
        await es.indices.get(index=index)
    except NotFoundError:
        await es.indices.create(
            index=index,
            settings=PAPERS_MAPPING["settings"],
            mappings=PAPERS_MAPPING["mappings"],
        )


# ---------------------------------------------------------------------------
# 批量写入
# ---------------------------------------------------------------------------

async def bulk_index_papers(papers: list[PaperDoc]) -> tuple[int, list]:
    """批量索引论文文档。

    Args:
        papers: PaperDoc 列表。

    Returns:
        (成功数, 错误列表)
    """
    await ensure_index()
    es = get_es_client()
    index = settings.ES_INDEX_PAPERS

    actions = [
        {
            "_index": index,
            "_id": doc.dblp_key,
            "_source": doc.model_dump(),
        }
        for doc in papers
    ]

    success, errors = await helpers.async_bulk(
        es,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
    )
    return success, errors


# ---------------------------------------------------------------------------
# 搜索
# ---------------------------------------------------------------------------

async def search_papers(
    title: str | None = None,
    author: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    venue: str | None = None,
    search_mode: str = "bm25",
    page: int = 1,
    size: int = 10,
) -> PaperSearchResponse:
    """按标题/作者/年份/venue 搜索论文，支持多种搜索模式。

    Args:
        title:       标题关键词（可选）。
        author:      作者姓名关键词（可选）。
        year_from:   年份下限（含）。
        year_to:     年份上限（含）。
        venue:       会议/期刊名称精确过滤。
        search_mode: bm25(默认) | phrase(短语精确) | fuzzy(宽松模糊)。
        page:        页码（从 1 开始）。
        size:        每页结果数。

    Returns:
        PaperSearchResponse 封装的分页结果。
    """
    es = get_es_client()
    index = settings.ES_INDEX_PAPERS

    must_clauses: list[dict] = []
    filter_clauses: list[dict] = []

    # ---- 标题搜索 ----
    if title:
        if search_mode == "phrase":
            # 短语匹配：词序严格一致
            must_clauses.append({"match_phrase": {"title": {"query": title, "slop": 1}}})
        elif search_mode == "fuzzy":
            # 宽松模糊：容忍更多拼写错误
            must_clauses.append({
                "match": {"title": {"query": title, "fuzziness": 2, "operator": "or"}}
            })
        else:
            # BM25（默认）：multi_match 同时搜索 title + title.raw，并支持词干
            must_clauses.append({
                "multi_match": {
                    "query": title,
                    "fields": ["title^3", "title.raw"],   # title 权重 3x
                    "type": "best_fields",
                    "operator": "and",
                    "fuzziness": "AUTO",
                }
            })

    # ---- 作者搜索 ----
    if author:
        must_clauses.append({
            "match": {
                "authors": {
                    "query": author,
                    "operator": "and",
                    "fuzziness": "AUTO",
                }
            }
        })

    # ---- 年份范围过滤（filter 不影响相关度分） ----
    if year_from or year_to:
        year_range: dict = {}
        if year_from:
            year_range["gte"] = year_from
        if year_to:
            year_range["lte"] = year_to
        filter_clauses.append({"range": {"year": year_range}})

    # ---- venue 精确过滤 ----
    if venue:
        filter_clauses.append({"term": {"venue": venue.upper()}})

    # ---- 组装最终 query ----
    if not must_clauses and not filter_clauses:
        query: dict = {"match_all": {}}
        sort: list = [{"year": {"order": "desc"}}]
    else:
        query = {"bool": {"must": must_clauses, "filter": filter_clauses}}
        sort = ["_score", {"year": {"order": "desc"}}]

    from_ = (page - 1) * size

    # ---- 高亮配置 ----
    highlight: dict = {
        "fields": {
            "title": {"number_of_fragments": 1, "fragment_size": 200},
            "authors": {"number_of_fragments": 1},
        },
        "pre_tags": ["<em>"],
        "post_tags": ["</em>"],
    }

    resp = await es.search(
        index=index,
        query=query,
        sort=sort,
        from_=from_,
        size=size,
        highlight=highlight if must_clauses else None,
    )

    hits = resp["hits"]
    total = hits["total"]["value"]
    results: list[PaperSearchResult] = []

    for hit in hits["hits"]:
        src = hit["_source"]
        hl = hit.get("highlight", {})
        results.append(
            PaperSearchResult(
                dblp_key=src["dblp_key"],
                title=src["title"],
                authors=src["authors"],
                year=src.get("year"),
                venue=src.get("venue"),
                url=src.get("url"),
                ee=src.get("ee"),
                pub_type=src.get("pub_type", "inproceedings"),
                score=hit.get("_score"),
                highlight={
                    "title": hl.get("title", []),
                    "authors": hl.get("authors", []),
                } if hl else None,
            )
        )

    return PaperSearchResponse(total=total, page=page, size=size, results=results)
