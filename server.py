"""MCP server for natural-language analytics over the Telco Customer Churn dataset.

Architecture: the connected MCP client (Claude Code / Codex) is the LLM. It reads
the schema via get_schema, writes SQL, and executes it through run_query. This
server's job is to make that loop accurate and safe:

  - get_schema returns real column types, categorical values, and semantic notes,
    so the model cannot hallucinate column names or filter values.
  - run_query enforces read-only analytics: single statement, SELECT/WITH only,
    validated by DuckDB's own parser (not regex), on a connection with external
    access disabled and the configuration locked.
  - Results are row-capped so a runaway query cannot flood the client's context.
"""

from __future__ import annotations

import os
import time
from typing import Any

import duckdb
from mcp.server import MCPServer

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_GLOB = os.path.join(_BASE_DIR, "data", "*.csv")
LOG_DB = os.path.join(_BASE_DIR, "logs", "query_log.duckdb")

# Cost guardrail: everything returned lands in the client LLM's context window,
# which the user pays for per token (and re-pays on every following turn). Cap
# the response by PAYLOAD SIZE — the actual unit of cost — not by an arbitrary
# small row count. ~50 KB ≈ 12k tokens: cents, not dollars, per question.
MAX_PAYLOAD_CHARS = 50_000
HARD_MAX_ROWS = 2_000   # absolute backstop regardless of row width
MAX_SAMPLE = 20         # cap for sample_rows
MAX_DISTINCT = 30       # columns with more distinct values than this are not enumerated

mcp = MCPServer("telco-churn-analytics")


def _connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB over the committed CSVs, locked down after ingestion.

    The train/validation/test split is an ML artifact; analytics questions are
    about all 7,043 customers, so the view unions all three files.
    """
    con = duckdb.connect(database=":memory:")
    con.execute(
        f"CREATE VIEW customers AS SELECT * FROM read_csv('{DATA_GLOB}', union_by_name=true)"
    )
    # Materialize so the view works after external access is disabled.
    con.execute("CREATE TABLE _customers AS SELECT * FROM customers")
    con.execute("DROP VIEW customers")
    con.execute("ALTER TABLE _customers RENAME TO customers")
    # Guardrail layer 1 (engine): no filesystem/network access, no extension
    # loading, and lock the configuration so a query cannot re-enable any of it.
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


CON = _connect()

# --- Request log: an append-only DuckDB table in its own database file. -------
# Deliberately a SEPARATE connection: the guarded, LLM-facing connection above
# has external access disabled and its configuration locked, so no SQL arriving
# through run_query can ever read, modify, or attach the audit log. Only this
# code path writes to it. Never log to stdout — that carries the MCP protocol.
os.makedirs(os.path.dirname(LOG_DB), exist_ok=True)
_LOG_CON = duckdb.connect(LOG_DB)
_LOG_CON.execute(
    """
    CREATE TABLE IF NOT EXISTS query_log (
        ts            TIMESTAMP DEFAULT now(),
        tool          VARCHAR NOT NULL,
        input         VARCHAR,
        status        VARCHAR NOT NULL,   -- 'ok' | 'blocked' | 'error'
        detail        VARCHAR,            -- block reason / error message
        rows_returned INTEGER,
        duration_ms   DOUBLE
    )
    """
)


def _log(
    tool: str,
    input_: str | None,
    status: str,
    detail: str | None = None,
    rows: int | None = None,
    duration_ms: float | None = None,
) -> None:
    try:
        _LOG_CON.execute(
            "INSERT INTO query_log (tool, input, status, detail, rows_returned, duration_ms) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [tool, input_, status, detail, rows, duration_ms],
        )
    except duckdb.Error:
        pass  # logging must never break the actual request

# Guardrail layer 2 (query validation) uses DuckDB's parser via extract_statements;
# only these statement types are allowed through.
_ALLOWED_STATEMENT_TYPES = {duckdb.StatementType.SELECT}

_SEMANTIC_NOTES = [
    "One row per customer; 7,043 customers total (train/validation/test CSVs are unioned — the split is an ML artifact, ignore it).",
    "Churn: 1 = customer churned, 0 = stayed. Overall churn rate = AVG(Churn).",
    "Customer Status is the 3-way version: 'Churned', 'Stayed', or 'Joined' (new this quarter).",
    "Churn Category / Churn Reason / Churn Score: only meaningful for churned customers; Category and Reason are NULL for everyone else.",
    "Internet Service is a 0/1 flag; the actual service kind is in Internet Type ('Cable', 'DSL', 'Fiber Optic', NULL = no internet).",
    "Most yes/no columns (Married, Dependents, Phone Service, Online Security, Streaming TV, ...) are 0/1 integers, not 'Yes'/'No' strings.",
    "Column names contain spaces — always double-quote them, e.g. \"Monthly Charge\", \"Tenure in Months\".",
    "Quarter is constant ('Q3') — the data is a single-quarter snapshot; there is no time series.",
    "Monetary columns (Monthly Charge, Total Revenue, CLTV, ...) are in USD.",
]


@mcp.tool()
def get_schema() -> dict[str, Any]:
    """Get the schema of the `customers` table: every column with its type, the
    actual distinct values for categorical columns, min/max for numeric columns,
    and semantic notes about the dataset's quirks.

    ALWAYS call this before writing SQL — filter values must match the real
    values listed here (e.g. Contract = 'Month-to-Month', not 'monthly').
    """
    _log("get_schema", None, "ok")
    columns = []
    for name, dtype in CON.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'customers' ORDER BY ordinal_position"
    ).fetchall():
        info: dict[str, Any] = {"name": name, "type": dtype}
        if dtype == "VARCHAR":
            distinct = CON.execute(
                f'SELECT DISTINCT "{name}" FROM customers WHERE "{name}" IS NOT NULL '
                f"ORDER BY 1 LIMIT {MAX_DISTINCT + 1}"
            ).fetchall()
            if len(distinct) <= MAX_DISTINCT:
                info["values"] = [v[0] for v in distinct]
            else:
                info["values"] = f"high-cardinality ({MAX_DISTINCT}+ distinct values)"
        else:
            lo, hi = CON.execute(
                f'SELECT MIN("{name}"), MAX("{name}") FROM customers'
            ).fetchone()
            info["range"] = [lo, hi]
            if (lo, hi) == (0, 1):
                info["note"] = "binary flag: 1 = yes, 0 = no"
        columns.append(info)

    return {
        "table": "customers",
        "row_count": CON.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
        "dialect": "DuckDB SQL (supports MEDIAN, QUANTILE_CONT, PIVOT; division is float by default)",
        "notes": _SEMANTIC_NOTES,
        "columns": columns,
    }


@mcp.tool()
def run_query(sql: str) -> dict[str, Any]:
    """Run a read-only SQL query against the `customers` table and return the
    results along with the SQL that was executed (so the answer is auditable).

    Rules: exactly one statement; SELECT (or WITH ... SELECT) only. Results are
    budgeted by size (~50 KB) to protect the user's context-window cost — large
    results are trimmed with a warning, so prefer aggregation over raw dumps.
    Call get_schema first to see real column names and categorical values.
    """
    start = time.perf_counter()

    def blocked(reason: str) -> dict[str, Any]:
        _log("run_query", sql, "blocked", reason)
        return {"error": reason}

    try:
        statements = CON.extract_statements(sql)
    except duckdb.Error as e:
        return blocked(f"SQL parse error: {e}")

    if len(statements) != 1:
        return blocked("Exactly one SQL statement is allowed per call.")
    # Read-only PRAGMAs are internally rewritten to SELECTs by DuckDB; reject
    # them anyway so the SELECT-only guarantee is literal.
    if sql.lstrip().lstrip("(").lower().startswith("pragma"):
        return blocked("PRAGMA statements are not allowed. This server is read-only SELECT.")
    if statements[0].type not in _ALLOWED_STATEMENT_TYPES:
        return blocked(
            f"Only SELECT queries are allowed; got a "
            f"{statements[0].type.name} statement. This server is read-only."
        )

    try:
        cursor = CON.execute(sql)
        column_names = [d[0] for d in cursor.description]
        fetched = cursor.fetchmany(HARD_MAX_ROWS + 1)
    except duckdb.Error as e:
        # Return the engine's message verbatim — it usually names the bad
        # column/value, which lets the client self-correct and retry.
        _log("run_query", sql, "error", str(e),
             duration_ms=(time.perf_counter() - start) * 1000)
        return {"error": f"Query failed: {e}", "sql": sql}

    # Keep rows until the payload budget is spent.
    rows: list[tuple] = []
    budget = MAX_PAYLOAD_CHARS
    for row in fetched[:HARD_MAX_ROWS]:
        budget -= len(str(row))
        if budget < 0:
            break
        rows.append(row)
    truncated = len(rows) < len(fetched)

    _log("run_query", sql, "ok", rows=len(rows),
         duration_ms=(time.perf_counter() - start) * 1000)
    return {
        "sql": sql,
        "columns": column_names,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        **(
            {
                "warning": f"Result trimmed to {len(rows)} rows to respect the "
                f"~{MAX_PAYLOAD_CHARS // 1000} KB response budget (context-window cost "
                "guardrail). Use aggregation, fewer columns, or LIMIT/OFFSET to paginate."
            }
            if truncated
            else {}
        ),
    }


@mcp.tool()
def churn_summary() -> dict[str, Any]:
    """Get pre-computed headline churn statistics: overall churn rate, churn rate
    by contract / internet type / tenure band, top churn reasons, and the revenue
    at stake. A reliable starting point for broad questions like "give me an
    overview of churn" — use run_query for anything more specific.
    """

    _log("churn_summary", None, "ok")

    def q(sql: str) -> list[tuple]:
        return CON.execute(sql).fetchall()

    overall = q(
        "SELECT COUNT(*) AS customers, SUM(Churn) AS churned, ROUND(AVG(Churn) * 100, 2) AS churn_rate_pct FROM customers"
    )[0]
    return {
        "customers": overall[0],
        "churned": overall[1],
        "churn_rate_pct": overall[2],
        "churn_rate_by_contract": q(
            "SELECT Contract, COUNT(*) AS customers, ROUND(AVG(Churn) * 100, 1) AS churn_rate_pct "
            "FROM customers GROUP BY Contract ORDER BY churn_rate_pct DESC"
        ),
        "churn_rate_by_internet_type": q(
            'SELECT COALESCE("Internet Type", \'No Internet\') AS internet_type, COUNT(*) AS customers, '
            "ROUND(AVG(Churn) * 100, 1) AS churn_rate_pct "
            "FROM customers GROUP BY 1 ORDER BY churn_rate_pct DESC"
        ),
        "churn_rate_by_tenure_band": q(
            "SELECT CASE WHEN \"Tenure in Months\" < 12 THEN '0-11 months' "
            "WHEN \"Tenure in Months\" < 36 THEN '12-35 months' "
            "ELSE '36+ months' END AS tenure_band, COUNT(*) AS customers, "
            "ROUND(AVG(Churn) * 100, 1) AS churn_rate_pct "
            "FROM customers GROUP BY 1 ORDER BY 1"
        ),
        "top_churn_reasons": q(
            'SELECT "Churn Reason", COUNT(*) AS customers FROM customers '
            'WHERE "Churn Reason" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10'
        ),
        "monthly_revenue_lost_to_churn_usd": q(
            'SELECT ROUND(SUM("Monthly Charge"), 0) FROM customers WHERE Churn = 1'
        )[0][0],
        "avg_satisfaction": q(
            'SELECT Churn, ROUND(AVG("Satisfaction Score"), 2) FROM customers GROUP BY Churn ORDER BY Churn'
        ),
    }


@mcp.tool()
def sample_rows(n: int = 5) -> dict[str, Any]:
    """Get n random example rows (max 20) from the `customers` table, to see what
    real records look like. For analysis use run_query with aggregation instead.
    """
    n = max(1, min(int(n), MAX_SAMPLE))
    _log("sample_rows", f"n={n}", "ok", rows=n)
    cursor = CON.execute(f"SELECT * FROM customers USING SAMPLE {n} ROWS")
    return {
        "columns": [d[0] for d in cursor.description],
        "rows": cursor.fetchall(),
    }


if __name__ == "__main__":
    mcp.run()
