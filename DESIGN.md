# Design Document — Telco Churn Analytics MCP Server

**Gil Nussbaum · August 2026 · [github.com/Gilnuss/gil-cheq-assignment](https://github.com/Gilnuss/gil-cheq-assignment)**

## Design

A local **text-to-SQL MCP server** (Python + DuckDB, ~250 lines) over the Telco churn dataset (7,043 customers, 52 columns, committed to the repo). The MCP client (Claude Code / Codex) **is the LLM**: it reads the schema, writes SQL, and interprets results. Four tools:

- **`get_schema`** — real column types, actual categorical values, semantic notes (e.g. churn rate = `AVG(Churn)`). The anti-hallucination layer.
- **`run_query`** — read-only SQL; every response echoes the SQL it ran, so answers are auditable.
- **`churn_summary`** — curated headline stats; broad questions never depend on SQL generation.
- **`sample_rows`** — capped orientation.

No API key: the server never calls a model — it is called *by* one.

## Why

The natural questions here — rates, medians, group-bys — are **aggregations**. RAG can't count or average; SQL gives exact, checkable numbers. Also rejected: canned queries (no long tail) and the server-side `ask()` design that calls its own LLM — it hides reasoning in a black box. We follow MCP's premise: **the client brings the brain, the server brings the tools.** The user's own AI writes the SQL in the open; they see it, and can challenge it.

**Why DuckDB:** it queries the committed CSVs directly (zero ETL, nothing to build), it's an engine designed for exactly this analytical workload, and its SQL dialect keeps LLM-written queries correct.

## Guardrails — the LLM is the untrusted component

All controls sit below the model, never in a prompt:

1. **Engine** — external access off and configuration locked; data served from an in-memory copy.
2. **Query** — DuckDB's own parser gates every call: one statement, SELECT only.
3. **Output** — 200-row cap.
4. **Audit** — every call logged to a DuckDB table the LLM-facing connection cannot reach.

A committed `smoke_test.py` proves it: 15 attacks (injection, exfiltration, config re-enable…) — 15/15 blocked — plus ground-truth checks (26.54% churn).

## Production

- CSVs → the **warehouse** (Snowflake/BigQuery); local stdio → remote authenticated service (SSO).
- **RLS + column masking** per authenticated user, enforced in the engine. (Absent today by design: one local analyst, no user identity.)
- **PII** (synthetic here): hash identifiers at ingestion, mask by role, suppress small result groups (k-anonymity).
- CI **eval set** of question→answer pairs to catch accuracy regressions; rate limits, timeouts, centralized audit log.

## Risks

- **Confidently wrong SQL** (worst case) → schema grounding, SQL echoed for audit, curated summary tool, CI evals.
- **Malicious / injected queries** → engine-level read-only, adversarially tested.
- **Misread semantics** (e.g. 0/1 flags) → semantic notes in `get_schema`.
- **Context flooding** → row caps.

## Business impact at CHEQ

CHEQ's product generates this exact data shape at scale: traffic verdicts, invalid-click events, account health. Questions like *"which customers spiked in invalid traffic this week?"* currently queue behind analysts. This pattern gives every CS manager and security analyst **safe, auditable, natural-language access** to that data — insight loops drop from days to minutes. Pointed at per-tenant data with RLS, the same server becomes a customer-facing "ask your traffic data" feature competitors' static dashboards can't match.
