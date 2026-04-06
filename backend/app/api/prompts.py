"""
Prompt 版本化管理接口
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_prompts():
    """列出所有 prompt 模板"""
    pass


@router.post("/")
async def create_prompt():
    """创建新 prompt 模板"""
    pass


@router.get("/{prompt_id}/versions")
async def list_prompt_versions(prompt_id: str):
    """获取某个 prompt 的所有历史版本"""
    pass


@router.post("/{prompt_id}/versions")
async def create_prompt_version(prompt_id: str):
    """为 prompt 创建新版本"""
    pass
