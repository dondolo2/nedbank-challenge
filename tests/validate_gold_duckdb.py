"""Run DuckDB checks similar to stage1/infrastructure/run_tests.sh (needs duckdb Python package)."""

import os
import sys

import duckdb


def main() -> int:
    gold = os.environ.get("GOLD_ROOT", "/data/output/gold")
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(
        f"CREATE OR REPLACE VIEW fact_transactions AS SELECT * FROM delta_scan('{gold}/fact_transactions');"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW dim_accounts AS SELECT * FROM delta_scan('{gold}/dim_accounts');"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW dim_customers AS SELECT * FROM delta_scan('{gold}/dim_customers');"
    )

    q1 = con.sql(
        "SELECT COUNT(*) FROM (SELECT transaction_type FROM fact_transactions GROUP BY transaction_type)"
    ).fetchone()[0]
    q2 = con.sql(
        "SELECT COUNT(*) FROM dim_accounts a LEFT JOIN dim_customers c ON a.customer_id = c.customer_id "
        "WHERE c.customer_id IS NULL"
    ).fetchone()[0]
    q3 = con.sql(
        "SELECT COUNT(*) FROM (SELECT c.province FROM dim_accounts a JOIN dim_customers c "
        "ON a.customer_id = c.customer_id GROUP BY c.province)"
    ).fetchone()[0]

    print(f"Q1 transaction_type groups: {q1} (expect 4)")
    print(f"Q2 unlinked accounts: {q2} (expect 0)")
    print(f"Q3 province groups: {q3} (expect 9)")
    ok = q1 == 4 and q2 == 0 and 1 <= q3 <= 9
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
