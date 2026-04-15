Search for CS academic papers. Query: $ARGUMENTS

Follow this workflow EXACTLY:

**Step 1 — Initialize**: Call `start_search` with the user's query.
Save the returned `session_id`. You MUST pass this session_id to ALL subsequent DaseS tool calls.

**Step 2 — Clarify**: If the tool returns disambiguation questions, use AskUserQuestion to present ALL questions to the user in a single call (max 4 questions). Present the question text and option labels exactly as returned by the tool.

**Step 3 — Search**: Call `execute_search` with session_id and the user's answers.
- `answers` is a **JSON string** (not a dict object). You MUST pass it as a string like `'{"venue": 0, "time_range": 1}'`.
- The keys are the question IDs shown in brackets (e.g. `[venue]`, `[time_range]`) — use EXACTLY the IDs as returned by `start_search`; do NOT hardcode them.
- The values should use **idx numbers** (recommended) as shown in `start_search` output. Example: `'{"venue": 0, "time_range": 1}'`.
- Alternatively, you can use exact option label text. Example: `'{"venue": "NeurIPS", "time_range": "近5年（2020至今）"}'`.
- If the user selected "不限" for any question, **omit that key entirely** from the answers string — do NOT pass "不限" as a value.
- If `start_search` returned no questions, call `execute_search` with just `session_id` (no `answers` needed).

**Step 4 — Present**: Format results as a numbered list with title, authors, year, venue, CCF rating, and abstract summary.
If the user wants more results, call `execute_search` again with `page=2`, `page=3`, etc., keeping the same `session_id` and `answers`.

**Step 5 — Related Work (optional)**: If the user asks to generate related work / literature review, call `write_related_work` with `session_id`, `topic`, and optionally `dblp_keys` and `style`. Then use the output to compose a coherent academic paragraph.

**Available tools** (all require `session_id`):
- `start_search(query)` — Initialize search session
- `execute_search(session_id, answers?, search_mode?, size?, page?)` — Execute search
- `list_session_papers(session_id)` — List all papers in current session
- `write_related_work(session_id, topic, dblp_keys?, style?)` — Format papers for related work section

IMPORTANT: session_id is mandatory for execute_search, list_session_papers, and write_related_work. Never omit it.
Prefer idx numbers over label text for answers — they are more robust (no spacing/encoding mismatch issues).
