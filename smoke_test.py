"""Smoke test: guardrail attack suite + ground-truth correctness checks.

Run with:  .venv/bin/python smoke_test.py
Exits non-zero on any failure.
"""

import sys

import server

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    if not ok:
        FAILURES.append(name)


print("== Guardrails: every attack must be blocked ==")
ATTACKS = [
    ("DROP", "DROP TABLE customers"),
    ("DELETE", "DELETE FROM customers"),
    ("UPDATE", "UPDATE customers SET Churn = 1"),
    ("INSERT", "INSERT INTO customers SELECT * FROM customers"),
    ("CREATE", "CREATE TABLE evil AS SELECT 1"),
    ("multi-statement injection", "SELECT 1; DROP TABLE customers"),
    ("COPY TO file exfiltration", "COPY customers TO '/tmp/exfil.csv'"),
    ("filesystem read", "SELECT * FROM read_csv('/etc/passwd')"),
    ("re-enable external access", "SET enable_external_access = true"),
    ("extension install", "INSTALL httpfs"),
    ("attach external database", "ATTACH '/tmp/other.db'"),
    ("PRAGMA (read-only)", "PRAGMA database_list"),
    ("PRAGMA (config)", "PRAGMA memory_limit='10GB'"),
    ("PRAGMA via whitespace", "  pragma database_list"),
    ("CALL of pragma function", "CALL pragma_database_list()"),
]
for name, sql in ATTACKS:
    check(f"blocks {name}", "error" in server.run_query(sql))

print("== Legitimate queries: must all succeed ==")
LEGIT = [
    ("simple aggregate", "SELECT AVG(Churn) FROM customers"),
    ("CTE", "WITH x AS (SELECT Churn FROM customers) SELECT AVG(Churn) FROM x"),
    ("DESCRIBE", "DESCRIBE customers"),
    ("MEDIAN", 'SELECT MEDIAN("Tenure in Months") FROM customers'),
    ("quoted spaced columns", 'SELECT AVG("Monthly Charge") FROM customers'),
]
for name, sql in LEGIT:
    check(f"allows {name}", "error" not in server.run_query(sql))

print("== Ground truth ==")
r = server.run_query(
    "SELECT COUNT(*), SUM(Churn), ROUND(AVG(Churn) * 100, 2) FROM customers"
)["rows"][0]
check("7,043 customers", r[0] == 7043, str(r[0]))
check("1,869 churned", r[1] == 1869, str(r[1]))
check("26.54% churn rate", float(r[2]) == 26.54, str(r[2]))

r = server.run_query("SELECT 385/7043")["rows"][0][0]
check("float division (no silent integer truncation)", 0 < r < 1, str(r))

r = server.run_query("SELECT * FROM customers")
check(
    "oversized result refused up front (no partial dump)",
    r.get("result_too_large") and r["rows"] == [] and r["total_rows"] == 7043,
    f"reported {r.get('total_rows')} rows / ~{r.get('estimated_kb')} KB without shipping them",
)
r = server.run_query('SELECT "Customer ID" FROM customers LIMIT 1500')
check(
    "narrow queries keep many rows within budget",
    r["row_count"] == 1500 and not r.get("result_too_large"),
    f"{r['row_count']} rows",
)
import os

os.environ["CHURN_MCP_MAX_KB"] = "0"  # owner disables the budget -> unlimited
r = server.run_query("SELECT * FROM customers")
check(
    "budget disabled returns every row",
    r["row_count"] == 7043 and not r.get("result_too_large"),
    f"{r['row_count']} rows",
)
del os.environ["CHURN_MCP_MAX_KB"]

print("== Tool sanity ==")
schema = server.get_schema()
check("schema: 52 columns", len(schema["columns"]) == 52)
contract = next(c for c in schema["columns"] if c["name"] == "Contract")
check(
    "schema: real categorical values",
    contract["values"] == ["Month-to-Month", "One Year", "Two Year"],
)
check("churn_summary rate", server.churn_summary()["churn_rate_pct"] == 26.54)
check("sample_rows cap", len(server.sample_rows(999)["rows"]) == 20)

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    sys.exit(1)
print("All checks passed.")
