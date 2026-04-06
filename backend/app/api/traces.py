"""
Agent Trace 可观测性接口 - 集成 LangFuse
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_traces(limit: int = 20, offset: int = 0):
    """获取最近的 agent trace 记录"""
    # TODO: 从 LangFuse 拉取 trace 数据
    pass


@router.get("/{trace_id}")
async def get_trace(trace_id: str):
    """获取单条 trace 详情 (含完整 span 树)"""
    pass
