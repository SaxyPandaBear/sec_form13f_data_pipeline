"""Read-only JSON API over the gold `holder_positions` Iceberg table, for the React UI.

Queries go through Trino (the `gold` catalog already wired up in docker-compose /
trino/catalog/gold.properties), the same path a human runs ad hoc SQL through in the
README. This process never talks to S3, Postgres/JdbcCatalog, or Glue directly.

All query values are passed as bound parameters (`?` placeholders, `cursor.execute(sql,
params)`), not interpolated into the SQL string — verified against the installed
`trino` client (paramstyle `qmark`) directly, including that a `date` column needs an
actual `datetime.date` object, not an ISO string, to compare correctly. `limit` is the
one exception: it's already a FastAPI/pydantic-validated `int` by the time it reaches
SQL, so an f-string is no different in risk from a bound parameter there.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime

import trino
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "gold")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "sec13f_gold")
TRINO_USER = os.environ.get("TRINO_USER", "gold_ui")
TABLE = "holder_positions"

CIK_RE = re.compile(r"^\d{1,10}$")
CUSIP_RE = re.compile(r"^[A-Za-z0-9]{1,9}$")

app = FastAPI(title="SEC 13F Gold Explorer API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _connect():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )


def _query(sql: str, params: tuple = ()) -> list[list]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def _parse_period(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "period must be an ISO date (YYYY-MM-DD)") from None


def _require_cik(value: str) -> str:
    if not CIK_RE.match(value):
        raise HTTPException(400, "cik must be numeric")
    return value


def _require_cusip(value: str) -> str:
    if not CUSIP_RE.match(value):
        raise HTTPException(400, "cusip must be an alphanumeric CUSIP")
    return value


@app.get("/api/health")
def health() -> dict:
    try:
        _query(f"SELECT 1 FROM {TABLE} LIMIT 1")
    except Exception as exc:  # noqa: BLE001 - surface any Trino/table-not-found error as-is
        return {"status": "degraded", "detail": str(exc)}
    return {"status": "ok"}


@app.get("/api/periods")
def periods() -> list[str]:
    try:
        rows = _query(f"SELECT DISTINCT periodofreport FROM {TABLE} ORDER BY periodofreport DESC")
    except Exception:  # noqa: BLE001 - table doesn't exist until the gold task has run once
        return []
    return [row[0].isoformat() if isinstance(row[0], date) else row[0] for row in rows]


@app.get("/api/top-positions")
def top_positions(period: str = Query(...), limit: int = Query(25, ge=1, le=200)) -> list[dict]:
    """The default landing view: every holder+security position reported for one quarter,
    across every security (not scoped to one CUSIP), ordered by total_value descending.
    `holder_positions.rank` isn't used here — it's ranked *within* a (cusip, periodofreport)
    partition, so it would read as ~1 on nearly every row of a value-sorted global list; a
    leaderboard position is just this response's own row order instead.
    """
    period_date = _parse_period(period)
    sql = f"""
        SELECT cik, filingmanager_name, cusip, nameofissuer, total_value, total_shares,
               total_value_pct_change, total_shares_pct_change
        FROM {TABLE}
        WHERE periodofreport = ?
        ORDER BY total_value DESC
        LIMIT {limit}
    """
    rows = _query(sql, (period_date,))
    return [
        {
            "cik": r[0],
            "filingmanager_name": r[1],
            "cusip": r[2],
            "nameofissuer": r[3],
            "total_value": r[4],
            "total_shares": r[5],
            "total_value_pct_change": r[6],
            "total_shares_pct_change": r[7],
        }
        for r in rows
    ]


@app.get("/api/holder-history")
def holder_history(cik: str = Query(...), cusip: str = Query(...)) -> list[dict]:
    cik = _require_cik(cik)
    cusip = _require_cusip(cusip)
    sql = f"""
        SELECT periodofreport, total_value, total_shares,
               total_value_pct_change, total_shares_pct_change
        FROM {TABLE}
        WHERE cik = ? AND cusip = ?
        ORDER BY periodofreport
    """
    rows = _query(sql, (cik, cusip))
    return [
        {
            "periodofreport": r[0].isoformat() if isinstance(r[0], date) else r[0],
            "total_value": r[1],
            "total_shares": r[2],
            "total_value_pct_change": r[3],
            "total_shares_pct_change": r[4],
        }
        for r in rows
    ]
