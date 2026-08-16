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

import csv
import os
import re
import threading
import time
from typing import Any

import duckdb
from mcp.server import MCPServer

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_GLOB = os.path.join(_BASE_DIR, "data", "*.csv")
LOG_DB = os.path.join(_BASE_DIR, "logs", "query_log.duckdb")
EXPORT_DIR = os.path.join(_BASE_DIR, "exports")

# Cost guardrail: everything returned lands in the client LLM's context window,
# which the user pays for per token (and re-pays on every following turn). The
# response budget is by PAYLOAD SIZE — the actual unit of cost — and it is the
# OWNER'S policy, not the tool's: configurable via CHURN_MCP_MAX_KB (default 50,
# ~12k tokens; set 0 to disable and return everything). The tool defaults safe
# but never dictates — full data is always reachable via pagination or a higher
# budget.
DEFAULT_MAX_KB = 50


def _payload_budget_chars() -> float:
    """Read each call so the owner can tune without restarting anything."""
    try:
        kb = float(os.environ.get("CHURN_MCP_MAX_KB", DEFAULT_MAX_KB))
    except ValueError:
        kb = DEFAULT_MAX_KB
    return kb * 1000 if kb > 0 else float("inf")


MAX_SAMPLE = 20         # cap for sample_rows
MAX_DISTINCT = 30       # columns with more distinct values than this are not enumerated

mcp = MCPServer(
    "telco-churn-analytics",
    instructions=(
        "Analytics over the Telco churn dataset (7,043 customers). Strategy: "
        "(1) Call get_schema FIRST — filter values must match the real categorical "
        "values it lists. (2) Answer questions with AGGREGATION (GROUP BY / AVG / "
        "COUNT) via run_query — exact numbers over all rows, tiny responses. For "
        "deep or compound analyses ('analyze churned customers'), run as MANY "
        "aggregate queries as the analysis needs — different dimensions, segments, "
        "comparisons; there is no limit on query count and each result is small. "
        "(3) When the user wants raw data in bulk (for Excel/BI/inspection), use "
        "export_result — it writes the COMPLETE result to a CSV file at any size, "
        "bypassing the chat entirely. Never try to page a whole table through "
        "run_query. (4) churn_summary answers broad overview questions instantly. "
        "All queries are read-only SELECT."
    ),
)


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
    # loading, a memory ceiling so a query bomb (e.g. a giant cross join) can't
    # eat the machine's RAM — then lock the configuration so a query cannot
    # re-enable or raise any of it.
    con.execute("SET enable_external_access = false")
    con.execute("SET memory_limit = '1GB'")
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


# Compute guardrail: SQL can manufacture unbounded WORK even on a small table
# (a 3-way cross join of 7K rows = 3.5e11 combinations; a recursive CTE can run
# forever). A watchdog interrupts any query that exceeds the timeout. Owner's
# policy, like the size budget: CHURN_MCP_TIMEOUT_S (default 30, 0 = off).
DEFAULT_TIMEOUT_S = 30


def _query_timeout_s() -> float:
    try:
        s = float(os.environ.get("CHURN_MCP_TIMEOUT_S", DEFAULT_TIMEOUT_S))
    except ValueError:
        s = DEFAULT_TIMEOUT_S
    return s if s > 0 else float("inf")


def _execute_guarded(sql: str) -> duckdb.DuckDBPyConnection:
    """Execute on the guarded connection under the compute watchdog.

    Safe because the stdio server handles one request at a time, so the
    interrupt can only hit the query that armed it.
    """
    timeout = _query_timeout_s()
    if timeout == float("inf"):
        return CON.execute(sql)
    watchdog = threading.Timer(timeout, CON.interrupt)
    watchdog.start()
    try:
        return CON.execute(sql)
    finally:
        watchdog.cancel()


def _new_export_path(filename: str) -> str:
    """A fresh .csv path inside exports/ — name sanitized so it can't escape."""
    safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", filename).strip("_") or "export"
    os.makedirs(EXPORT_DIR, exist_ok=True)
    path = os.path.join(EXPORT_DIR, f"{safe_name}.csv")
    counter = 1
    while os.path.exists(path):
        counter += 1
        path = os.path.join(EXPORT_DIR, f"{safe_name}_{counter}.csv")
    return path


def _write_csv(column_names: list[str], rows: list[tuple], filename: str) -> str:
    path = _new_export_path(filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(rows)
    return path


def _validate_select(sql: str) -> str | None:
    """Return a rejection reason, or None if sql is a single SELECT statement."""
    try:
        statements = CON.extract_statements(sql)
    except duckdb.Error as e:
        return f"SQL parse error: {e}"
    if len(statements) != 1:
        return "Exactly one SQL statement is allowed per call."
    # Read-only PRAGMAs are internally rewritten to SELECTs by DuckDB; reject
    # them anyway so the SELECT-only guarantee is literal.
    if sql.lstrip().lstrip("(").lower().startswith("pragma"):
        return "PRAGMA statements are not allowed. This server is read-only SELECT."
    if statements[0].type not in _ALLOWED_STATEMENT_TYPES:
        return (
            f"Only SELECT queries are allowed; got a "
            f"{statements[0].type.name} statement. This server is read-only."
        )
    return None

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

    Rules: exactly one statement; SELECT (or WITH ... SELECT) only. Small
    results are returned inline; results too large for a chat response
    (default ~50 KB, owner-configurable via CHURN_MCP_MAX_KB) are delivered
    complete as a CSV file instead — you get the path, total rows, and a
    preview. Call get_schema first to see real column names and values.
    """
    start = time.perf_counter()

    rejection = _validate_select(sql)
    if rejection:
        _log("run_query", sql, "blocked", rejection)
        return {"error": rejection}

    try:
        cursor = _execute_guarded(sql)
        column_names = [d[0] for d in cursor.description]
        fetched = cursor.fetchall()
    except duckdb.InterruptException:
        reason = (
            f"Query exceeded the {_query_timeout_s():.0f}s compute timeout and was "
            "cancelled (guardrail against runaway compute, e.g. huge cross joins). "
            "Simplify the query — check JOIN conditions and recursive CTEs. The owner "
            "can adjust via CHURN_MCP_TIMEOUT_S (0 = off)."
        )
        _log("run_query", sql, "timeout", reason,
             duration_ms=(time.perf_counter() - start) * 1000)
        return {"error": reason, "sql": sql}
    except duckdb.Error as e:
        # Return the engine's message verbatim — it usually names the bad
        # column/value, which lets the client self-correct and retry.
        _log("run_query", sql, "error", str(e),
             duration_ms=(time.perf_counter() - start) * 1000)
        return {"error": f"Query failed: {e}", "sql": sql}

    duration_ms = (time.perf_counter() - start) * 1000
    payload_chars = sum(len(str(row)) for row in fetched)

    # Response routing: small results travel inline in the chat; results too
    # big for a chat response are AUTOMATICALLY delivered as a complete CSV
    # file instead — like an email attachment. Nothing is refused or trimmed;
    # the client gets the file path plus a small preview.
    if payload_chars > _payload_budget_chars():
        path = _write_csv(column_names, fetched, "query_result")
        _log("run_query", sql, "routed_to_file",
             f"{len(fetched)} rows, ~{payload_chars // 1000} KB -> {os.path.basename(path)}",
             rows=len(fetched), duration_ms=duration_ms)
        return {
            "sql": sql,
            "columns": column_names,
            "delivered_as_file": True,
            "path": path,
            "total_rows": len(fetched),
            "size_kb": os.path.getsize(path) // 1000,
            "preview_rows": fetched[:5],
            "note": "Result too large for an inline chat response, so the COMPLETE "
            "result was saved to the CSV at 'path' — give the user that path; the "
            "file is for THEM (Excel/BI/scripts), do not read it back into the "
            "conversation unless they explicitly ask. Use preview_rows to describe "
            "its shape. If they wanted an insight rather than raw rows, run an "
            "aggregated query instead. (Inline threshold is owner-configurable: "
            "CHURN_MCP_MAX_KB, 0 = always inline.)",
        }

    _log("run_query", sql, "ok", rows=len(fetched), duration_ms=duration_ms)
    return {
        "sql": sql,
        "columns": column_names,
        "rows": fetched,
        "row_count": len(fetched),
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
def export_result(sql: str, filename: str = "export") -> dict[str, Any]:
    """Run a read-only SQL query and write the COMPLETE result to a local CSV
    file — any size, no response budget, nothing shipped through the chat.
    Returns the file path, row count, and size.

    Use this whenever the user wants raw data in bulk (for Excel, BI tools, or
    inspection) instead of paging rows through run_query. Same rules as
    run_query: exactly one SELECT statement.
    """
    start = time.perf_counter()

    rejection = _validate_select(sql)
    if rejection:
        _log("export_result", sql, "blocked", rejection)
        return {"error": rejection}

    path = _new_export_path(filename)

    try:
        cursor = _execute_guarded(sql)
        column_names = [d[0] for d in cursor.description]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            row_count = 0
            while batch := cursor.fetchmany(1000):
                writer.writerows(batch)
                row_count += len(batch)
    except duckdb.InterruptException:
        reason = (
            f"Query exceeded the {_query_timeout_s():.0f}s compute timeout and was "
            "cancelled. Simplify the query, or the owner can adjust "
            "CHURN_MCP_TIMEOUT_S (0 = off)."
        )
        _log("export_result", sql, "timeout", reason,
             duration_ms=(time.perf_counter() - start) * 1000)
        return {"error": reason, "sql": sql}
    except duckdb.Error as e:
        _log("export_result", sql, "error", str(e),
             duration_ms=(time.perf_counter() - start) * 1000)
        return {"error": f"Query failed: {e}", "sql": sql}

    size_kb = os.path.getsize(path) // 1000
    _log("export_result", sql, "ok", rows=row_count,
         duration_ms=(time.perf_counter() - start) * 1000)
    return {
        "sql": sql,
        "path": path,
        "rows_written": row_count,
        "size_kb": size_kb,
        "note": "Complete result written to disk — tell the user the file path. "
        "Do not read the file back into the conversation unless asked.",
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
