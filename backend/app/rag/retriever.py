"""
混合检索器
向量检索 + BM25 关键词检索，通过 RRF 融合排序
"""

# TODO: 实现混合检索
# - 向量相似度检索 (Elasticsearch 8.x kNN，dense_vector 字段 + approximate_knn)
# - BM25 关键词检索 (Elasticsearch paper_bm25)
# - Reciprocal Rank Fusion 融合排序
# - Metadata 过滤 (年份、会议、作者等)
