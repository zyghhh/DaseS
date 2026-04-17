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
            "raw_xml":  {"type": "text", "index": False},  # 完整原始 XML，不索引不分词
            # --- 扩展结构化字段（高频过滤/展示）---
            "pages":       {"type": "keyword"},                           # 页码，如 "1877-1901"
            "volume":      {"type": "keyword"},                           # 卷号
            "mdate":       {"type": "date", "format": "yyyy-MM-dd"},      # 数据最后修改日期
            "author_pids": {"type": "keyword"},                           # DBLP 作者 PID，精确关联
            "ee_links":    {"type": "keyword"},                           # 所有电子版链接数组
            "school":      {"type": "keyword"},                           # 学校（学位论文）
            "publisher":   {"type": "keyword"},                           # 出版社
            # --- 跨源关联标识符（数据融合桥梁）---
            "doi":         {"type": "keyword"},                           # DOI，如 10.1016/j.artint.2023.103
            "arxiv_id":    {"type": "keyword"},                           # arXiv ID，如 2301.12345
            "ccf_rating":  {"type": "keyword"},                           # CCF 评级：A | B | C | N
            # --- 外部 API 补全字段 ---
            "abstract":    {"type": "text", "analyzer": "ik_max_word"},   # 摘要
            "abstract_source": {"type": "keyword"},                          # 摘要来源：S2AG | ArXiv | OpenAlex | None
            "keywords":    {"type": "keyword"},                           # 关键词
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

async def ensure_alias() -> None:
    """确保别名 dblp_search 存在。

    若别名不存在，自动创建一个默认索引并绑定别名，保证应用启动时查询不报错。
    """
    es = get_es_client()
    alias = settings.ES_ALIAS_PAPERS
    exists = await es.indices.exists_alias(name=alias)
    if not exists:
        # 别名不存在，创建一个初始索引并绑定
        init_index = f"{settings.ES_INDEX_PREFIX}_init"
        try:
            await es.indices.get(index=init_index)
        except NotFoundError:
            await es.indices.create(
                index=init_index,
                settings=PAPERS_MAPPING["settings"],
                mappings=PAPERS_MAPPING["mappings"],
            )
        await es.indices.put_alias(index=init_index, name=alias)


async def ensure_index() -> None:
    """向后兼容：确保别名存在（委托给 ensure_alias）"""
    await ensure_alias()


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
    index = settings.ES_ALIAS_PAPERS   # 统一通过别名写入

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
    ccf_rating: str | None = None,
    search_mode: str = "bm25",
    page: int = 1,
    size: int = 10,
) -> PaperSearchResponse:
    """按标题/作者/年份/venue/ccf_rating 搜索论文，支持多种搜索模式。

    Args:
        title:       标题关键词（可选）。
        author:      作者姓名关键词（可选）。
        year_from:   年份下限（含）。
        year_to:     年份上限（含）。
        venue:       会议/期刊名称精确过滤。
        ccf_rating:  CCF 评级（A|B|C|N）。
        search_mode: bm25(默认) | phrase(短语精确) | fuzzy(宽松模糊)。
        page:        页码（从 1 开始）。
        size:        每页结果数。

    Returns:
        PaperSearchResponse 封装的分页结果。
    """
    es = get_es_client()
    index = settings.ES_ALIAS_PAPERS   # 通过别名查询，支持蓝绿发布无感知切换

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
    if year_from is not None or year_to is not None:
        year_range: dict = {}
        if year_from:
            year_range["gte"] = year_from
        if year_to:
            year_range["lte"] = year_to
        filter_clauses.append({"range": {"year": year_range}})

    # ---- venue 精确过滤 ----
    if venue:
        filter_clauses.append({"term": {"venue": venue}})

    # ---- ccf_rating 精确过滤 ----
    if ccf_rating:
        filter_clauses.append({"term": {"ccf_rating": ccf_rating}})

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
        track_total_hits=True,  # 精确统计总命中数，突破 ES 默认 10000 上限
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
                pages=src.get("pages"),
                volume=src.get("volume"),
                mdate=src.get("mdate"),
                author_pids=src.get("author_pids", []),
                ee_links=src.get("ee_links", []),
                school=src.get("school"),
                publisher=src.get("publisher"),
                doi=src.get("doi"),
                arxiv_id=src.get("arxiv_id"),
                ccf_rating=src.get("ccf_rating"),
                abstract=src.get("abstract"),
                abstract_source=src.get("abstract_source"),
            )
        )

    return PaperSearchResponse(total=total, page=page, size=size, results=results)


# ---------------------------------------------------------------------------
# 消歧：基于 ES aggregation 分析查询分布，返回澄清问题
# ---------------------------------------------------------------------------

async def clarify_search(query: str) -> dict:
    """根据查询关键词分析 ES 中的分布情况，返回消歧问题供用户选择。

    Args:
        query: 用户输入的搜索关键词。

    Returns:
        dict: 包含 total_hits 和 questions 列表。
    """
    from datetime import datetime

    es = get_es_client()
    index = settings.ES_ALIAS_PAPERS

    # 先用 multi_match 搜索（title + abstract），同时做 aggregation
    resp = await es.search(
        index=index,
        query={
            "multi_match": {
                "query": query,
                "fields": ["title^3", "title.raw", "abstract^1"],
                "type": "best_fields",
            }
        },
        size=0,
        aggregations={
            "top_venues": {"terms": {"field": "venue", "size": 8, "min_doc_count": 5}},
            "year_histogram": {
                "histogram": {"field": "year", "interval": 2, "order": {"_key": "desc"}},
            },
            "ccf_distribution": {"terms": {"field": "ccf_rating", "size": 4}},
        },
        track_total_hits=True,
    )

    total = resp["hits"]["total"]["value"]
    aggs = resp.get("aggregations", {})

    questions: list[dict] = []

    # 问题 1：会议/期刊方向消歧
    venue_buckets = aggs.get("top_venues", {}).get("buckets", [])
    if len(venue_buckets) > 2:
        options = [{"label": b["key"], "count": b["doc_count"]} for b in venue_buckets[:6]]
        options.append({"label": "不限 (所有会议/期刊)", "count": total, "skip": True})
        questions.append({
            "id": "venue",
            "question": "您关注哪个会议/期刊方向？",
            "options": options,
        })

    # 问题 2：时间范围（按 2 年粒度动态统计，取有数据的时间段）
    year_buckets = aggs.get("year_histogram", {}).get("buckets", [])
    # 过滤掉无数据的桶，按年份降序排列
    year_buckets = [b for b in year_buckets if b["doc_count"] > 0]
    year_buckets.sort(key=lambda b: b["key"], reverse=True)
    if len(year_buckets) > 1:
        current_year = datetime.now().year
        time_options: list[dict] = []
        for b in year_buckets:
            start = int(b["key"])
            # interval=2，区间为 [start, start+1]
            end = start + 1
            # 最新桶的结束年份不超过当前年份
            end = min(end, current_year)
            if start == end:
                label = str(start)
            else:
                label = f"{start}–{end}"
            time_options.append({
                "label": label,
                "count": b["doc_count"],
                "value": {"year_from": start, "year_to": end},
            })
        time_options.append({"label": "不限", "count": total, "value": {}, "skip": True})
        questions.append({
            "id": "time_range",
            "question": "您关注哪个时间段的研究？",
            "options": time_options,
        })

    # 问题 3：CCF 评级
    ccf_buckets = aggs.get("ccf_distribution", {}).get("buckets", [])
    if ccf_buckets:
        questions.append({
            "id": "ccf_rating",
            "question": "对 CCF 评级有要求吗？",
            "options": [{"label": f"CCF-{b['key']}", "count": b["doc_count"], "value": b["key"]} for b in ccf_buckets]
                       + [{"label": "不限", "count": total, "value": None, "skip": True}],
        })

    return {"query": query, "total_hits": total, "questions": questions}
