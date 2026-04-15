"""
DaseS MCP Server - 基于 CS 学术论文的检索与写作辅助工具。

支持两种运行模式：
1. HTTP（Streamable HTTP）：挂载到 FastAPI 应用，由 uvicorn 提供服务。
2. stdio：独立运行，供 CLI 工具直接启动。
   启动方式：uv run python -m app.mcp_server

会话状态通过 session_id 管理，存储在进程内存中（MVP 阶段，后续迁移 Redis）。
"""

import time
import uuid

from mcp.server.fastmcp import FastMCP

from app.services.es_service import clarify_search, search_papers

# streamable_http_path="/" 使得挂载到 /mcp 后，端点路径为 /mcp/
mcp = FastMCP(
    "DaseS",
    streamable_http_path="/",
    host="0.0.0.0",
    allowed_hosts=["localhost", "localhost:*", "127.0.0.1", "127.0.0.1:*", "49.52.27.139", "49.52.27.139:*"],
)

# ---------------------------------------------------------------------------
# 会话级工作区（进程内存，MVP 阶段）
# ---------------------------------------------------------------------------
# key: session_id, value: dict with query + questions + papers + created_at
_sessions: dict[str, dict] = {}
_SESSION_TTL = 3600  # 会话存活时间（秒），超时后自动清理，防止内存泄漏


def _create_session(session_id: str) -> dict:
    """创建并注册新的会话工作区。"""
    sess = {"query": "", "questions": [], "papers": [], "created_at": time.time()}
    _sessions[session_id] = sess
    return sess


def _get_session(session_id: str) -> dict | None:
    """获取现有会话，不存在或已过期则返回 None。

    Returns:
        会话工作区字典，找不到或过期时返回 None。
    """
    sess = _sessions.get(session_id)
    if sess is None:
        return None
    # TTL 检查：过期自动清理，防止内存泄漏
    if time.time() - sess["created_at"] > _SESSION_TTL:
        del _sessions[session_id]
        return None
    return sess


# ---------------------------------------------------------------------------
# 答案解析：根据 session 中存储的问题定义，将用户选择映射为搜索参数
# ---------------------------------------------------------------------------

def _resolve_answers(session: dict, answers: dict[str, str]) -> dict:
    """将用户的原始选项文本解析为 search_papers 可用的参数。

    Args:
        session: 包含 questions 定义的会话工作区
        answers: {question_id: 用户选择的 option label}

    Returns:
        传给 search_papers 的参数字典
    """
    questions_by_id = {q["id"]: q for q in session.get("questions", [])}
    params: dict = {"title": session["query"]}

    for qid, selected_label in answers.items():
        q = questions_by_id.get(qid)
        if not q:
            continue

        # 在问题的 options 中找到用户选择的那个
        matched = next((opt for opt in q["options"] if opt["label"] == selected_label), None)
        if not matched:
            continue

        # skip 标记的选项（如“不限”）不添加任何过滤条件，直接跳过
        if matched.get("skip"):
            continue

        if qid == "venue":
            # venue 的 label 就是会议名，直接用作精确过滤
            params["venue"] = matched["label"]

        elif qid == "time_range":
            # time_range 的 option 带 value 字典，如 {"year_from": 2020}
            value = matched.get("value", {})
            params.update(value)

        elif qid == "ccf_rating":
            # ccf_rating 的 value 是评级字母或 None
            val = matched.get("value")
            if val:
                params["ccf_rating"] = val

    return params


# ---------------------------------------------------------------------------
# 格式化
# ---------------------------------------------------------------------------

def _format_paper(p: dict, idx: int | None = None) -> str:
    """将单篇论文格式化为可读文本行。"""
    ccf = f" [CCF-{p.get('ccf_rating')}]" if p.get("ccf_rating") else ""
    prefix = f"{idx}." if idx is not None else "-"
    venue = p.get("venue") or "N/A"
    authors = ", ".join(p.get("authors", []))
    lines = [f"{prefix} **{p['title']}** ({p.get('year')}){ccf}"]
    lines.append(f"   {authors} | {venue}")
    if p.get("abstract"):
        lines.append(f"   > {p['abstract'][:250]}...")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP 工具
# ---------------------------------------------------------------------------


@mcp.tool()
async def start_search(query: str) -> str:
    """初始化搜索会话并返回消歧问题。

    根据查询关键词分析论文分布，如果存在歧义则返回消歧问题供用户选择；
    如果查询意图明确则直接告知可以搜索。

    必须最先调用此工具获取 session_id，后续所有工具调用都需携带。

    Args:
        query: 用户输入的搜索关键词，如 "transformer"、"graph neural network"
    """
    session_id = uuid.uuid4().hex[:12]
    try:
        result = await clarify_search(query)
    except Exception as e:
        return f"检索服务异常：{e}。请确认 Elasticsearch 服务正在运行。"

    sess = _create_session(session_id)
    sess["query"] = query
    sess["questions"] = result["questions"]

    header = f"Session: {session_id}\nFound {result['total_hits']} papers matching '{query}'.\n"
    header += f"You MUST pass session_id='{session_id}' to all subsequent tool calls.\n"

    if not result["questions"]:
        return header + "Query is clear — no disambiguation needed. Proceed with execute_search."

    lines = [header, "Please clarify:\n"]
    for q in result["questions"]:
        lines.append(f"[{q['id']}] {q['question']}")
        for opt in q["options"]:
            label = opt["label"]
            count = opt.get("count", "")
            count_str = f" ({count}篇)" if count else ""
            lines.append(f"  - {label}{count_str}")
        lines.append("")
    return "\n".join(lines)


@mcp.tool()
async def execute_search(
    session_id: str,
    answers: dict[str, str] | None = None,
    search_mode: str = "bm25",
    size: int = 10,
    page: int = 1,
) -> str:
    """执行论文检索，结果自动存入会话工作区。

    后端根据 session 中保存的问题定义自动解析 answers 为搜索参数。
    如果 start_search 未返回消歧问题，answers 可省略。

    Args:
        session_id: 由 start_search 返回的会话 ID，必填
        answers: 用户对消歧问题的选择，格式为 {questions ID: 选项文本}。
                 问题 ID 以 start_search 返回的 [id] 为准，不要硬编码。
                 选项文本必须与 start_search 返回的选项 label 完全一致（含空格、括号）。
                 若用户选择“不限”，请直接省略该 key，勿传入。
        search_mode: 搜索模式 bm25(默认) | phrase(短语精确) | fuzzy(宽松模糊)
        size: 返回结果数，默认 10
        page: 页码，默认 1；翻页时递增传入
    """
    sess = _get_session(session_id)
    if sess is None:
        return f"Session '{session_id}' not found or expired. Call start_search first."

    # 解析答案 → 搜索参数
    params = _resolve_answers(sess, answers or {})
    params["search_mode"] = search_mode
    params["page"] = page
    params["size"] = size

    try:
        resp = await search_papers(**params)
    except Exception as e:
        return f"检索失败：{e}。请检查参数后重试。"

    # 追加到会话工作区，去重
    existing_keys = {p["dblp_key"] for p in sess["papers"]}
    for r in resp.results:
        if r.dblp_key not in existing_keys:
            sess["papers"].append(r.model_dump())

    total_pages = max(1, (resp.total + size - 1) // size)
    lines = [f"Found {resp.total} papers (第 {page}/{total_pages} 页，session 共 {len(sess['papers'])} 篇):\n"]
    for i, r in enumerate(resp.results, start=(page - 1) * size + 1):
        lines.append(_format_paper(r.model_dump(), idx=i))
        lines.append("")
    if page < total_pages:
        lines.append(f"提示：调用 execute_search(page={page + 1}) 可获取下一页结果。")
    return "\n".join(lines)


@mcp.tool()
async def list_session_papers(session_id: str) -> str:
    """查看当前会话工作区中的所有论文。

    Args:
        session_id: 由 start_search 返回的会话 ID，必填
    """
    sess = _get_session(session_id)
    if sess is None:
        return f"Session '{session_id}' not found or expired. Call start_search first."
    papers = sess["papers"]
    if not papers:
        return "No papers in session. Use execute_search to add papers first."

    lines = [f"Session has {len(papers)} papers:\n"]
    for i, p in enumerate(papers, 1):
        lines.append(_format_paper(p, idx=i))
        lines.append("")
    return "\n".join(lines)


@mcp.tool()
async def write_related_work(
    session_id: str,
    topic: str,
    dblp_keys: list[str] | None = None,
    style: str = "academic",
) -> str:
    """将会话工作区中的论文按 venue 分组，输出结构化的论文列表。

    注意：本工具仅格式化输出论文元数据，不调用 LLM。
    如需生成流畅的学术综述段落，请将本工具的输出交由 LLM 进一步撰写。

    Args:
        session_id: 由 start_search 返回的会话 ID，必填
        topic: 相关工作的研究主题，如 "retrieval augmented generation"
        dblp_keys: 指定包含的论文 key 列表，为空则使用工作区全部论文
        style: 输出样式 academic(学术) | survey(综述) | brief(简述)
    """
    sess = _get_session(session_id)
    if sess is None:
        return f"Session '{session_id}' not found or expired. Call start_search first."
    papers = sess["papers"]
    if not papers:
        return "No papers in session. Use start_search and execute_search first."

    if dblp_keys:
        papers = [p for p in papers if p["dblp_key"] in dblp_keys]

    if not papers:
        return f"No matching papers found for keys: {dblp_keys}"

    # 按 venue 分组
    groups: dict[str, list[dict]] = {}
    for p in papers:
        venue = p.get("venue") or "Others"
        groups.setdefault(venue, []).append(p)

    # 按风格选择标题
    if style == "survey":
        section_title = f"Survey on {topic}"
    elif style == "brief":
        section_title = "Related Work"
    else:
        section_title = f"Related Work on {topic}"

    lines = [f"### {section_title}\n"]
    lines.append(f"Based on {len(papers)} papers in session:\n")

    for venue, group in groups.items():
        ccf_tags = {f"CCF-{p['ccf_rating']}" for p in group if p.get("ccf_rating")}
        tag_str = f" ({', '.join(ccf_tags)})" if ccf_tags else ""

        lines.append(f"**{venue}**{tag_str}")
        for p in group:
            year = p.get("year", "n/a")
            authors = ", ".join(p.get("authors", [])[:3])
            et_al = " et al." if len(p.get("authors", [])) > 3 else ""
            lines.append(f"- {authors}{et_al} ({year}) \"{p['title']}\"")
            if p.get("abstract"):
                lines.append(f"  > {p['abstract'][:200]}...")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


def get_streamable_http_app():
    """返回 Streamable HTTP ASGI 子应用，供 FastAPI mount 使用。
    适用于：Claude Code、Gemini CLI 及所有支持 MCP Streamable HTTP 协议的客户端。
    端点：/mcp/
    """
    return mcp.streamable_http_app()


if __name__ == "__main__":
    mcp.run(transport="stdio")
