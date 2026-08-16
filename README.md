# Telco Churn Analytics — MCP Server

Ask natural-language questions about the [Telco Customer Churn dataset](https://huggingface.co/datasets/aai510-group1/telco-customer-churn) from Claude Code or Codex, and get accurate, auditable answers.

```
"What's our churn rate by contract type?"
"Why are high-CLTV customers leaving?"
"Median tenure of churned fiber-optic customers?"
```

## How it works

This is a **text-to-SQL MCP server**. The connected client (Claude Code / Codex) *is* the LLM:
it reads the real schema through `get_schema`, writes DuckDB SQL, and executes it through
`run_query`. The server's job is to make that loop **accurate** (schema grounding: real column
names, real categorical values, semantic notes about the dataset's quirks) and **safe**
(parser-enforced read-only SQL — see [Guardrails](#guardrails)).

> **No API key required.** This server never calls an LLM — it is called *by* one. The model
> answering your questions is whatever runs your MCP client (e.g. Claude in Claude Code).

The dataset (7,043 customers, 52 columns) is committed in [`data/`](data/), so the repo is fully
self-contained — no network access needed at runtime. The train/validation/test split is an ML
artifact; the server unions all three files into a single `customers` table.

## Tools

| Tool | Purpose |
|---|---|
| `get_schema` | All 52 columns with types, real categorical values, numeric ranges, and semantic notes (the anti-hallucination layer — call before writing SQL) |
| `run_query` | Execute read-only SQL; every response echoes the SQL it ran, so answers are auditable |
| `churn_summary` | Pre-computed headline stats: churn rate overall / by contract / internet type / tenure band, top churn reasons, revenue at stake |
| `export_result` | Write a query's **complete** result to a CSV in `exports/` — any size, zero context cost. The bulk-data channel: chat delivers insight, files deliver bulk |
| `sample_rows` | A few example rows for orientation (capped at 20) |

The server also ships **MCP instructions** — a strategy guide delivered to the client at
handshake (schema first, aggregate for insight, export for bulk) so the model uses the tools
well from the first question.

## Setup

Requires Python 3.10+.

```bash
git clone https://github.com/Gilnuss/gil-cheq-assignment.git
cd telco-churn-mcp
python3 -m venv .venv
.venv/bin/pip install -e .
```

Verify everything works (guardrail suite + ground-truth checks):

```bash
.venv/bin/python smoke_test.py
```

## Connect to Claude Code

**Option A — zero config:** the repo ships a project-scoped [`.mcp.json`](.mcp.json). Just start
`claude` from the repo root and approve the server when prompted.

**Option B — register explicitly** (works from any directory):

```bash
claude mcp add churn-analytics -- $(pwd)/.venv/bin/python $(pwd)/server.py
```

Then ask away:

```
> what's the churn rate for month-to-month customers?
> which cities have the highest churn?
> compare satisfaction scores of churned vs retained customers
```

## Connect to Codex

Add to `~/.codex/config.toml` (replace `/path/to/repo` with the absolute clone path):

```toml
[mcp_servers.churn_analytics]
command = "/path/to/repo/.venv/bin/python"
args = ["/path/to/repo/server.py"]
```

## Guardrails

The design principle: **the LLM is the untrusted component — every control is enforced below
it**, in the parser and the engine, never in a prompt.

1. **Engine layer** — `enable_external_access = false` (no filesystem, network, or extension
   access from SQL) then `lock_configuration = true` (cannot be re-enabled by any query). Data
   is served from an in-memory copy; the CSVs on disk are never opened for writing.
2. **Query layer** — DuckDB's own parser (not regex) classifies every statement: exactly one
   statement per call, type must be SELECT; PRAGMA and CALL are rejected explicitly.
3. **Compute layer** — even a small table lets SQL manufacture unbounded work (a 3-way cross
   join of 7K rows is 3.5×10¹¹ combinations; a recursive CTE can run forever). A watchdog
   cancels any query past the timeout (default 10s, `CHURN_MCP_TIMEOUT_S`, 0 = off), and the
   engine's memory is capped at 1 GB — set before the configuration lock, so no query can
   raise it.
4. **Output layer (cost guardrail, owner-configurable)** — everything returned lands in the
   client LLM's context window, which you pay for per token. Responses are budgeted by
   **size** (default ~50 KB ≈ 12k tokens), and an oversized result is **refused up front,
   never shipped partially**: the client gets the total row count and size for a few hundred
   bytes and decides itself — aggregate, select fewer columns, or paginate with
   `LIMIT/OFFSET` (full data always reachable, no tokens wasted on a partial dump). The
   budget is **policy, not a limit of the tool** — set `CHURN_MCP_MAX_KB` to raise it, or
   `0` to disable it entirely. And bulk raw data has its own channel: `export_result`
   writes complete results to CSV at any size, so no question and no dataset size is ever
   out of reach.

`smoke_test.py` runs a 15-attack suite against these layers (multi-statement injection,
`COPY TO` exfiltration, reading `/etc/passwd`, re-enabling external access, extension installs,
PRAGMA smuggling, …) and verifies known ground truth (7,043 customers, 26.54% churn rate).

## Request log

Every tool call is appended to a DuckDB table at `logs/query_log.duckdb` (created on first
run, gitignored): timestamp, tool, input SQL, status (`ok` / `blocked` / `error`), block
reason or error message, rows returned, and duration. So the usage log is itself queryable
with SQL:

```bash
.venv/bin/python -c "import duckdb; print(duckdb.connect('logs/query_log.duckdb', read_only=True).execute('SELECT ts, tool, input, status, rows_returned FROM query_log ORDER BY ts DESC LIMIT 20').fetchall())"
```

The log lives in a **separate database via a separate connection** — the guarded, LLM-facing
connection has external access disabled and cannot read, modify, or attach it. The model can
query the data but can never see or tamper with its own audit trail.

## Model configuration

No model is configured here — the LLM is supplied by the MCP client. Tested with Claude Code
(Claude). Any MCP-capable client/model works; accuracy of generated SQL will vary with the
client's model quality.

## Design document

See [DESIGN.md](DESIGN.md) for the one-page design: what was built, why (and what was
deliberately avoided), production changes, risks, and business impact.
