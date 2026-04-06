"""
论文检索接口 - 支持标题 / 作者关键词搜索（基于 Elasticsearch）
"""

from fastapi import APIRouter, HTTPException, Query

from app.models.schemas import PaperSearchResponse
from app.services.es_service import search_papers

router = APIRouter()


@router.get("/papers", response_model=PaperSearchResponse, summary="搜索论文")
async def search_papers_endpoint(
    title: str | None = Query(None, description="标题关键词"),
    author: str | None = Query(None, description="作者姓名关键词"),
    year_from: int | None = Query(None, description="年份下限，如 2015"),
    year_to: int | None = Query(None, description="年份上限，如 2024"),
    venue: str | None = Query(None, description="会议/期刊名称，如 ICML、NeurIPS"),
    search_mode: str = Query("bm25", description="搜索模式: bm25(默认) | phrase(短语) | fuzzy(宽松)"),
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(10, ge=1, le=100, description="每页条数"),
) -> PaperSearchResponse:
    """搜索 DBLP 论文元数据。

    **搜索模式 (`search_mode`)**：
    - `bm25`（默认）：经典相关度排序，支持单词模糊 + 词干
    - `phrase`：短语匹配，关键词顺序严格一致
    - `fuzzy`：宽松匹配，容忍拼写错误

    参数均可选，可组合使用（AND 逻辑）。不传任何参数返回全部论文（按年份降序）。
    """
    if search_mode not in ("bm25", "phrase", "fuzzy"):
        raise HTTPException(status_code=400, detail="search_mode 必须为 bm25 | phrase | fuzzy")
    if title is None and author is None and venue is None and year_from is None and year_to is None:
        size = min(size, 50)
    try:
        return await search_papers(
            title=title, author=author,
            year_from=year_from, year_to=year_to,
            venue=venue, search_mode=search_mode,
            page=page, size=size,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Elasticsearch 查询失败: {exc}") from exc


@router.get("/papers/{dblp_key:path}", response_model=PaperSearchResponse, summary="按 DBLP Key 查询")
async def get_paper_by_key(dblp_key: str) -> PaperSearchResponse:
    """通过 DBLP key 精确查询单篇论文。

    例： `/api/v1/search/papers/conf/nips/BrownMRSKDNSSAA20`
    """
    try:
        from elasticsearch import AsyncElasticsearch, NotFoundError
        from app.services.es_service import get_es_client
        from app.core.config import settings
        from app.models.schemas import PaperSearchResult

        es: AsyncElasticsearch = get_es_client()
        try:
            resp = await es.get(index=settings.ES_INDEX_PAPERS, id=dblp_key)
        except NotFoundError:
            raise HTTPException(status_code=404, detail="论文未找到")
        src = resp["_source"]
        result = PaperSearchResult(
            dblp_key=src["dblp_key"],
            title=src["title"],
            authors=src["authors"],
            year=src.get("year"),
            venue=src.get("venue"),
            url=src.get("url"),
            ee=src.get("ee"),
            pub_type=src.get("pub_type", "inproceedings"),
            score=1.0,
        )
        return PaperSearchResponse(total=1, page=1, size=1, results=[result])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Elasticsearch 查询失败: {exc}") from exc
