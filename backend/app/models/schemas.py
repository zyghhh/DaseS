"""
数据库模型定义 - 论文、Prompt、会话等
"""

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Paper 相关模型
# ---------------------------------------------------------------------------

class PaperDoc(BaseModel):
    """ES 索引文档结构"""
    dblp_key: str
    title: str
    authors: list[str]
    year: int | None = None
    venue: str | None = None      # 期刊名 / 会议名
    url: str | None = None
    ee: str | None = None         # 电子版链接
    pub_type: str = "inproceedings"  # article | inproceedings 等


class PaperSearchResult(BaseModel):
    """API 单条搜索结果"""
    dblp_key: str
    title: str
    authors: list[str]
    year: int | None = None
    venue: str | None = None
    url: str | None = None
    ee: str | None = None
    pub_type: str
    score: float | None = None    # ES 相关度得分
    highlight: dict[str, list[str]] | None = None  # 命中词高亮片段


class PaperSearchResponse(BaseModel):
    """API 搜索返回外层包装"""
    total: int
    page: int
    size: int
    results: list[PaperSearchResult]


# from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey, JSON
# from sqlalchemy.orm import DeclarativeBase, relationship
# import datetime


# class Base(DeclarativeBase):
#     pass


# class Paper(Base):
#     """论文元数据"""
#     __tablename__ = "papers"
#     id = Column(String, primary_key=True)
#     title = Column(Text, nullable=False)
#     abstract = Column(Text)
#     authors = Column(JSON)          # ["Author1", "Author2"]
#     venue = Column(String)          # 发表会议/期刊
#     year = Column(Integer)
#     arxiv_id = Column(String, index=True)
#     semantic_scholar_id = Column(String, index=True)
#     pdf_url = Column(Text)
#     created_at = Column(DateTime, default=datetime.datetime.utcnow)


# class PromptTemplate(Base):
#     """Prompt 模板"""
#     __tablename__ = "prompt_templates"
#     id = Column(String, primary_key=True)
#     name = Column(String, nullable=False, unique=True)
#     description = Column(Text)
#     agent_type = Column(String)     # 关联的智能体类型
#     created_at = Column(DateTime, default=datetime.datetime.utcnow)
#     versions = relationship("PromptVersion", back_populates="template")


# class PromptVersion(Base):
#     """Prompt 版本"""
#     __tablename__ = "prompt_versions"
#     id = Column(String, primary_key=True)
#     template_id = Column(String, ForeignKey("prompt_templates.id"))
#     version = Column(Integer, nullable=False)
#     content = Column(Text, nullable=False)
#     variables = Column(JSON)        # 模板变量定义
#     created_at = Column(DateTime, default=datetime.datetime.utcnow)
#     template = relationship("PromptTemplate", back_populates="versions")
