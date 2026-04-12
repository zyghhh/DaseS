Search for CS academic papers. Query: $ARGUMENTS

Follow this workflow EXACTLY:

**Step 1 — Initialize**: Call `start_search` with the user's query.
Save the returned `session_id`. You MUST pass this session_id to ALL subsequent DaseS tool calls.

**Step 2 — Clarify**: If the tool returns disambiguation questions, use AskUserQuestion to present ALL questions to the user in a single call (max 4 questions). Present the question text and option labels exactly as returned by the tool.

**Step 3 — Search**: Call `execute_search` with session_id and the user's answers as a dict.
- The keys are the question IDs shown in brackets (e.g. "[venue]", "[time_range]") — use EXACTLY the IDs as returned by `start_search`; do NOT hardcode them.
- The values are the EXACT option labels the user selected — copy them character-for-character from `start_search` output (including parentheses and spaces).
- If the user selected "不限" for any question, **omit that key entirely** from the answers dict — do NOT pass "不限" as a value.
- If `start_search` returned no questions, call `execute_search` with just `session_id` (no `answers` needed).

**Step 4 — Present**: Format results as a numbered list with title, authors, year, venue, CCF rating, and abstract summary.
If the user wants more results, call `execute_search` again with `page=2`, `page=3`, etc., keeping the same `session_id` and `answers`.

IMPORTANT: session_id is mandatory for execute_search, list_session_papers, and write_related_work. Never omit it.
Answer labels MUST be copied exactly from start_search output — do not paraphrase, translate, or add spaces.