"""
对话接口 - 用户与多智能体系统的对话入口 (SSE 流式)
"""

from fastapi import APIRouter

router = APIRouter()


@router.post("/completions")
async def chat_completions():
    """与智能体对话，返回 SSE 流式响应"""
    # TODO: 接入 LangGraph agent 编排
    pass
