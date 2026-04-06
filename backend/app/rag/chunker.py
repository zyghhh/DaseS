"""
文本分块器
将解析后的论文按语义单元分块，准备向量化
"""

# TODO: 实现分块策略
# - 按章节/段落分块 (非固定 token 切分)
# - 保留 metadata: section_title, paper_id, chunk_index
# - 支持重叠分块 (overlap) 以保留上下文
