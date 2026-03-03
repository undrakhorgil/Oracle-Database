#!/usr/bin/env python3
"""
Oracle Morning Health Check (DBA-style)
- Connects to Oracle (CDB/PDB ok)
- Runs common health queries
- Writes a timestamped report file
- Optionally emails the report (SMTP)

Install:
  pip install oracledb

Run:
  python oracle_morning_healthcheck.py
"""

from __future__ import annotations
import os
import sys
import socket
import smtplib
from datetime import datetime
from email.message import EmailMessage

import oracledb


# ---------------------------
# Config (edit these)
# ---------------------------
DSN = os.getenv("ORACLE_DSN", "localhost:1521/ORCLPDB1")
DB_USER = os.getenv("ORACLE_USER", "system")
DB_PASS = os.getenv("ORACLE_PASS", "Welcome1")

REPORT_DIR = os.getenv("REPORT_DIR", "./reports")

# Email (optional). Leave EMAIL_TO empty to disable emailing.
EMAIL_TO = os.getenv("EMAIL_TO", "undrakhorgil.o@gmail.com")              # e.g. "dba-team@company.com"
EMAIL_FROM = os.getenv("EMAIL_FROM", "usamuruudul@gmail.com")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"

SMTP_USER = os.getenv("SMTP_USER", "usamuruudul@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "heKzig-cajgyh-dubgu6")  # NOT your normal password


# Thresholds
TBSP_PCT_WARN = float(os.getenv("TBSP_PCT_WARN", "85"))
FRA_PCT_WARN = float(os.getenv("FRA_PCT_WARN", "85"))
BLOCKED_SESS_WARN = int(os.getenv("BLOCKED_SESS_WARN", "1"))
INVALID_OBJ_WARN = int(os.getenv("INVALID_OBJ_WARN", "1"))
FAILED_RMAN_WARN_DAYS = int(os.getenv("FAILED_RMAN_WARN_DAYS", "1"))  # last N days


# ---------------------------
# Helpers
# ---------------------------
def section(title: str) -> str:
    return f"\n{'=' * 80}\n{title}\n{'=' * 80}\n"

def fmt_kv(k: str, v) -> str:
    return f"{k:<28}: {v}\n"

def run_query(cur: oracledb.Cursor, sql: str, params: dict | None = None, max_rows: int = 50):
    cur.execute(sql, params or {})
    cols = [d[0] for d in cur.description]
    rows = cur.fetchmany(max_rows)
    return cols, rows

def format_table(cols, rows) -> str:
    if not rows:
        return "OK (no rows)\n"
    # compute column widths
    widths = [len(c) for c in cols]
    for r in rows:
        for i, val in enumerate(r):
            widths[i] = max(widths[i], len("" if val is None else str(val)))
    line = " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    sep  = "-+-".join("-" * w for w in widths)
    out = [line, sep]
    for r in rows:
        out.append(" | ".join(("" if v is None else str(v)).ljust(widths[i]) for i, v in enumerate(r)))
    return "\n".join(out) + "\n"

def send_email(subject: str, body: str, attachment_path: str):
    if not EMAIL_TO.strip():
        return  # disabled

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        data = f.read()
    msg.add_attachment(data, maintype="text", subtype="plain", filename=os.path.basename(attachment_path))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASS:
            s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


# ---------------------------
# Health checks (queries)
# ---------------------------
SQL_DB_INFO = """
SELECT d.name AS db_name,
       d.open_mode,
       d.database_role,
       d.db_unique_name
FROM v$database d
"""

SQL_INST_INFO = """
SELECT i.instance_name,
       i.host_name,
       i.status,
       i.version,
       TO_CHAR(i.startup_time, 'YYYY-MM-DD HH24:MI:SS') AS startup_time
FROM v$instance i
"""

SQL_PDBS = """
SELECT name AS pdb_name,
       open_mode,
       restricted
FROM v$pdbs
ORDER BY name
"""

SQL_DG_STATS = """
SELECT name, value, unit, time_computed
FROM v$dataguard_stats
WHERE name IN ('transport lag','apply lag','apply finish time')
ORDER BY name
"""

SQL_TBSP_USAGE = """
WITH df AS (
  SELECT tablespace_name, SUM(bytes) bytes
  FROM dba_data_files
  GROUP BY tablespace_name
),
fs AS (
  SELECT tablespace_name, SUM(bytes) bytes
  FROM dba_free_space
  GROUP BY tablespace_name
)
SELECT df.tablespace_name,
       ROUND((df.bytes - NVL(fs.bytes,0))/1024/1024) AS used_mb,
       ROUND(NVL(fs.bytes,0)/1024/1024) AS free_mb,
       ROUND(((df.bytes - NVL(fs.bytes,0)) / NULLIF(df.bytes,0)) * 100, 2) AS pct_used
FROM df
LEFT JOIN fs ON fs.tablespace_name = df.tablespace_name
ORDER BY pct_used DESC NULLS LAST
FETCH FIRST 15 ROWS ONLY
"""

SQL_FRA = """
SELECT name,
       ROUND(space_limit/1024/1024) AS limit_mb,
       ROUND(space_used/1024/1024) AS used_mb,
       ROUND(space_reclaimable/1024/1024) AS reclaimable_mb,
       ROUND((space_used / NULLIF(space_limit,0)) * 100, 2) AS pct_used
FROM v$recovery_file_dest
"""

SQL_INVALID_OBJECTS = """
SELECT owner, object_type, COUNT(*) AS invalid_count
FROM dba_objects
WHERE status <> 'VALID'
GROUP BY owner, object_type
ORDER BY invalid_count DESC
FETCH FIRST 20 ROWS ONLY
"""

SQL_BLOCKING = """
SELECT
  bs.sid AS blocking_sid,
  bs.serial# AS blocking_serial,
  ws.sid AS waiting_sid,
  ws.serial# AS waiting_serial,
  ws.username AS waiting_user,
  ws.event AS waiting_event,
  ws.seconds_in_wait
FROM v$session ws
JOIN v$session bs ON ws.blocking_session = bs.sid
WHERE ws.blocking_session IS NOT NULL
ORDER BY ws.seconds_in_wait DESC
FETCH FIRST 20 ROWS ONLY
"""

SQL_LONG_RUNNING = """
SELECT sid, serial#, username, status, sql_id,
       ROUND(last_call_et/60,1) AS minutes
FROM v$session
WHERE type='USER'
  AND username IS NOT NULL
  AND status='ACTIVE'
  AND last_call_et > 30*60
ORDER BY last_call_et DESC
FETCH FIRST 20 ROWS ONLY
"""

SQL_RMAN_JOBS = """
SELECT TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI') AS start_time,
       status,
       input_type,
       ROUND(input_bytes/1024/1024/1024,2) AS input_gb,
       ROUND(output_bytes/1024/1024/1024,2) AS output_gb,
       ROUND(elapsed_seconds/60,1) AS elapsed_min
FROM v$rman_backup_job_details
WHERE start_time >= SYSDATE - :days
ORDER BY start_time DESC
FETCH FIRST 20 ROWS ONLY
"""

SQL_ARCHIVE_GAP = """
SELECT * FROM v$archive_gap
"""

SQL_ALERT_LOG_ERRORS = """
-- Lightweight "recent errors" signal if ADR isn't accessible: check diag alert views (12c+)
SELECT TO_CHAR(originating_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS ts,
       message_text
FROM v$diag_alert_ext
WHERE originating_timestamp >= SYSTIMESTAMP - INTERVAL '1' DAY
  AND (message_text LIKE '%ORA-%' OR message_text LIKE '%Error%')
ORDER BY originating_timestamp DESC
FETCH FIRST 30 ROWS ONLY
"""


def main() -> int:
    os.makedirs(REPORT_DIR, exist_ok=True)
    now = datetime.now()
    hostname = socket.gethostname()
    report_name = f"oracle_healthcheck_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(REPORT_DIR, report_name)

    warnings = []

    try:
        with oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DSN) as conn:
            with conn.cursor() as cur:
                report = []
                report.append(section("RUN CONTEXT"))
                report.append(fmt_kv("Timestamp", now.strftime("%Y-%m-%d %H:%M:%S")))
                report.append(fmt_kv("Host (runner)", hostname))
                report.append(fmt_kv("DSN", DSN))
                report.append(fmt_kv("DB User", DB_USER))

                # DB / Instance
                report.append(section("DATABASE / INSTANCE"))
                cols, rows = run_query(cur, SQL_DB_INFO)
                report.append(format_table(cols, rows))
                cols, rows = run_query(cur, SQL_INST_INFO)
                report.append(format_table(cols, rows))

                # PDBs (if CDB)
                report.append(section("PDB STATUS (if CDB)"))
                try:
                    cols, rows = run_query(cur, SQL_PDBS)
                    report.append(format_table(cols, rows))
                    # flag closed PDBs
                    for r in rows:
                        if str(r[1]).upper() not in ("READ WRITE", "READ ONLY"):
                            warnings.append(f"PDB {r[0]} open_mode={r[1]}")
                except oracledb.Error:
                    report.append("Not a CDB or insufficient privileges for v$pdbs.\n")

                # Data Guard
                report.append(section("DATA GUARD STATS (if configured)"))
                try:
                    cols, rows = run_query(cur, SQL_DG_STATS)
                    report.append(format_table(cols, rows))
                except oracledb.Error:
                    report.append("Data Guard stats not accessible or not configured.\n")

                # Archive gap
                report.append(section("ARCHIVE GAP (if standby)"))
                try:
                    cols, rows = run_query(cur, SQL_ARCHIVE_GAP)
                    report.append(format_table(cols, rows))
                    if rows:
                        warnings.append("Archive gap detected (v$archive_gap has rows).")
                except oracledb.Error:
                    report.append("v$archive_gap not accessible.\n")

                # Tablespaces
                report.append(section(f"TABLESPACE USAGE (top) - WARN >= {TBSP_PCT_WARN}%"))
                cols, rows = run_query(cur, SQL_TBSP_USAGE)
                report.append(format_table(cols, rows))
                for ts, used_mb, free_mb, pct in rows:
                    try:
                        if pct is not None and float(pct) >= TBSP_PCT_WARN and ts not in ("SYSTEM", "SYSAUX"):
                            warnings.append(f"Tablespace {ts} is {pct}% used")
                    except Exception:
                        pass

                # FRA
                report.append(section(f"FRA USAGE - WARN >= {FRA_PCT_WARN}%"))
                try:
                    cols, rows = run_query(cur, SQL_FRA)
                    report.append(format_table(cols, rows))
                    for _name, _limit_mb, _used_mb, _reclaim_mb, pct in rows:
                        if pct is not None and float(pct) >= FRA_PCT_WARN:
                            warnings.append(f"FRA usage {pct}%")
                except oracledb.Error:
                    report.append("FRA view not accessible (v$recovery_file_dest).\n")

                # Invalid objects
                report.append(section(f"INVALID OBJECTS - WARN if total invalid >= {INVALID_OBJ_WARN}"))
                cols, rows = run_query(cur, SQL_INVALID_OBJECTS)
                report.append(format_table(cols, rows))
                total_invalid = sum(int(r[2]) for r in rows) if rows else 0
                if total_invalid >= INVALID_OBJ_WARN:
                    warnings.append(f"Invalid objects total={total_invalid}")

                # Blocking sessions
                report.append(section(f"BLOCKING / WAITING SESSIONS - WARN if >= {BLOCKED_SESS_WARN}"))
                cols, rows = run_query(cur, SQL_BLOCKING)
                report.append(format_table(cols, rows))
                if len(rows) >= BLOCKED_SESS_WARN:
                    warnings.append(f"Blocking sessions detected rows={len(rows)}")

                # Long running
                report.append(section("LONG-RUNNING ACTIVE SESSIONS (>30 min)"))
                cols, rows = run_query(cur, SQL_LONG_RUNNING)
                report.append(format_table(cols, rows))

                # RMAN jobs
                report.append(section(f"RMAN BACKUP JOBS (last {FAILED_RMAN_WARN_DAYS} day(s))"))
                try:
                    cols, rows = run_query(cur, SQL_RMAN_JOBS, {"days": FAILED_RMAN_WARN_DAYS})
                    report.append(format_table(cols, rows))
                    # flag failures
                    for r in rows:
                        status = str(r[1]).upper()
                        if status not in ("COMPLETED", "COMPLETED WITH WARNINGS"):
                            warnings.append(f"RMAN job status={r[1]} at {r[0]}")
                except oracledb.Error:
                    report.append("RMAN views not accessible (v$rman_backup_job_details).\n")

                # Alert errors (last 24h)
                report.append(section("ALERT LOG SIGNAL (v$diag_alert_ext) - last 24h ORA-/Error"))
                try:
                    cols, rows = run_query(cur, SQL_ALERT_LOG_ERRORS)
                    report.append(format_table(cols, rows))
                    if rows:
                        warnings.append(f"Recent alert log error lines={len(rows)}")
                except oracledb.Error:
                    report.append("v$diag_alert_ext not accessible.\n")

                # Summary
                report.append(section("SUMMARY"))
                if warnings:
                    report.append("WARNING/ATTENTION ITEMS:\n")
                    for w in warnings:
                        report.append(f" - {w}\n")
                else:
                    report.append("All checks look OK (no warning thresholds triggered).\n")

        # write report
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("".join(report))

        # email (optional)
        subject = f"Oracle Health Check: {'WARN' if warnings else 'OK'} - {DSN} - {now.strftime('%Y-%m-%d')}"
        body = "See attached report.\n\n" + ("Warnings:\n" + "\n".join(warnings) if warnings else "No warnings.")
        send_email(subject, body, report_path)

        print(f"Report written: {report_path}")
        if EMAIL_TO.strip():
            print(f"Emailed to: {EMAIL_TO}")
        return 0

    except oracledb.Error as e:
        err = f"Oracle healthcheck failed: {e}"
        print(err, file=sys.stderr)
        # also write a failure report for audit
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(section("FAILED") + err + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())