"""
智能体管理接口 - Agent 状态、配置的 CRUD
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_agents():
    """列出所有可用智能体"""
    pass


@router.get("/{agent_id}/status")
async def get_agent_status(agent_id: str):
    """获取智能体运行状态"""
    pass
