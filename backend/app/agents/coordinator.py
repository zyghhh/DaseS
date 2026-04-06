"""
Coordinator Agent - 多智能体协调器
负责理解用户意图，分发任务到各专职智能体，汇总结果
"""

# from langgraph.graph import StateGraph, END
# from app.agents.retriever import retriever_agent
# from app.agents.summarizer import summarizer_agent
# from app.agents.analyzer import analyzer_agent


# TODO: 定义 AgentState TypedDict
# TODO: 构建 StateGraph 编排流程
#
# 典型流程:
#   用户输入 → Coordinator(意图识别)
#       → PaperRetriever(论文检索)
#       → Summarizer(摘要生成) / Analyzer(深度分析)
#       → Coordinator(结果汇总) → 用户
