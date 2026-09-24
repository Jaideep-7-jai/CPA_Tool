#!/usr/bin/env python3
"""
Consolidated criteria processor for Suppression and Mailing requests.

This module owns the complete non-DoorDash processing path for Age, State,
ZIP, Gender, and any supported combination of those criteria. It replaces
the old AGE_STATE, ZIPS, and MULTI_CRITERIA runtime routes. DoorDash imports
the shared low-level helpers it needs from here, but retains its own workflow.

Key differences from age_state:
  - ZIP codes are provided via UI upload (file attachment)
  - A SINGLE shared Snowflake ZIP staging table is created ONCE before
    any channel runs and is DROPPED only after ALL channels complete.
  - Each channel's perm table is still created/dropped per-channel as in age_state.
  - Matching is done by ZIP field (b.ZIP / ZIP column) via IN / NOT IN
    against the shared zip staging table.

Run directory layout
--------------------

    <output_dir>/run_zips_<YYYYMMDD_HHMMSS>/
        logs/
            zips_<YYYYMMDD_HHMMSS>.log          <- MAIN log  (orchestrator only)
            GREEN_zips_<YYYYMMDD_HHMMSS>.log
            BLUE_zips_<YYYYMMDD_HHMMSS>.log
            ARCAMAX_zips_<YYYYMMDD_HHMMSS>.log
            ORANGE_zips_<YYYYMMDD_HHMMSS>.log
        FINAL_FILES/
            <client>_<request_type>_GREEN_<date>.csv
            ...
        GREEN_tmp/
        BLUE_tmp/
        ARCAMAX_tmp/
        ORANGE_tmp/

Processing flow
---------------
  PRE-CHANNEL (orchestrator):
    1. Upload ZIP codes file from UI attachment -> S3
    2. CREATE shared ZIP staging table (APT_CPA_ZIPS_STAGING_<YYYYMMDD_HHMMSS>) ONCE
    3. COPY ZIP codes from S3 into staging table

  PER CHANNEL (same 7-step flow as age_state):
    1. INSERT raw query results into permanent Snowflake table
         APT_CPA_<CHANNEL>_<YYYYMMDD>  (matching on ZIP via staging table)
    2. Export FINAL FILE  -> path_FINAL  (S3)
    3. Export COMPLETE DATA FILE -> path_COMPLETE (S3)
    4. DROP the per-channel permanent table
    5. Download FINAL FILE parts + combine
    6. Move combined file to FINAL_FILES/
    7. FTP upload from FINAL_FILES/
    8. Record FTP path in requests.<CHANNEL>_FTP column

  POST-CHANNEL (orchestrator):
    - DROP the shared ZIP staging table (only after ALL channels finish)
"""

import csv
import io
import json
import os
import re
import time
import gzip
import shutil
import logging
import pymysql
import subprocess
import shlex
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import (
    run_command,
    send_success_email,
    send_error_email,
    ensure_output_dir,
)
from MERGE_OUTPUT.merge_output import merge_current_file, update_orange_merge_source_storage
from REQUEST_PROCESSOR.zip_radius import expand_zip_radius

DB_CONFIG = {
    # Service-provided values; do not put operational credentials in Git.
    "host":      os.getenv("CPA_DB_HOST", ""),
    "user":      os.getenv("CPA_DB_USER", ""),
    "password":  os.getenv("CPA_DB_PASSWORD", ""),
    "database":  os.getenv("CPA_DB_NAME", "CUST_TECH_DB"),
    "charset":   "utf8mb4",
    "autocommit": True,
}

FTP_USERNAME = os.getenv("CPA_FTP_USERNAME", "")
FTP_PASSWORD = os.getenv("CPA_FTP_PASSWORD", "")
FTP_HOST = os.getenv("CPA_FTP_HOST", "")

CHANNELS = ["GREEN", "BLUE", "ARCAMAX", "ORANGE"]

_FMT = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

# ---------------------------------------------------------------------------
# DB retry constants (same as age_state)
# ---------------------------------------------------------------------------

_DB_RETRY_ATTEMPTS   = 3
_DB_RETRY_BASE_DELAY = 2


# ---------------------------------------------------------------------------
# Step banner helper
# ---------------------------------------------------------------------------

def _step(log, step_num, total_steps, description, channel_name=""):
    prefix = f"[{channel_name}] " if channel_name else ""
    log.info(
        f"{prefix}{'─' * 4} STEP {step_num}/{total_steps} {'─' * 4}  {description}"
    )


def _trace(log, event, **values):
    """Write a safe, one-line diagnostic record for validation/debugging."""
    details = " | ".join(
        f"{key}={value}" for key, value in values.items()
    )
    log.info("  TRACE | %s%s", event, f" | {details}" if details else "")


def _safe_sql_for_log(sql):
    """Keep SQL debugging useful without putting AWS credentials in logs."""
    sql = re.sub(r"AWS_KEY_ID='[^']*'", "AWS_KEY_ID='***'", sql)
    return re.sub(r"AWS_SECRET_KEY='[^']*'", "AWS_SECRET_KEY='***'", sql)


def _verify_local_file(log, label, file_path, require_content=True):
    """Validate an expected local artifact and log its exact state."""
    path = Path(file_path)
    exists = path.is_file()
    size = path.stat().st_size if exists else -1
    _trace(log, "file validation", label=label, path=path,
           exists=exists, size_bytes=size)
    if not exists:
        raise RuntimeError(f"Required {label} was not created: {path}")
    if require_content and size <= 0:
        raise RuntimeError(f"Required {label} is empty: {path}")
    return size


# ---------------------------------------------------------------------------
# Logging helpers  (mirrors age_state exactly)
# ---------------------------------------------------------------------------

def _file_handler(log_path: Path) -> logging.FileHandler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.FileHandler(str(log_path))
    h.setFormatter(_FMT)
    return h


def setup_main_logging(run_dir: Path) -> logging.Logger:
    """
    Create the MAIN logger 'zips_main'.
    Writes to:  <run_dir>/logs/zips_<YYYYMMDD_HHMMSS>.log
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"zips_{ts}.log"

    lg = logging.getLogger("zips_main")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


def setup_channel_logging(run_dir: Path, channel_name: str) -> logging.Logger:
    """
    Create a per-channel logger 'zips_<CHANNEL>'.
    Writes to:  <run_dir>/logs/<CHANNEL>_zips_<YYYYMMDD_HHMMSS>.log
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / f"{channel_name.upper()}_zips_{ts}.log"

    lg = logging.getLogger(f"zips_{channel_name.upper()}")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    lg.handlers.clear()
    lg.addHandler(_file_handler(log_file))
    return lg


def setup_processor_main_logging(run_dir: Path) -> logging.Logger:
    """Create the main log for the consolidated non-DoorDash workflow."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = run_dir / "logs" / "request_processor_{0}.log".format(ts)
    log = logging.getLogger("request_processor_main")
    log.setLevel(logging.INFO)
    log.propagate = False
    log.handlers.clear()
    log.addHandler(_file_handler(log_file))
    return log


def setup_processor_channel_logging(run_dir: Path, channel_name: str) -> logging.Logger:
    """Create a per-channel log for the consolidated non-DoorDash workflow."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = str(channel_name).upper()
    log_file = run_dir / "logs" / "{0}_request_processor_{1}.log".format(name, ts)
    log = logging.getLogger("request_processor_{0}".format(name))
    log.setLevel(logging.INFO)
    log.propagate = False
    log.handlers.clear()
    log.addHandler(_file_handler(log_file))
    return log


# ---------------------------------------------------------------------------
# DB helpers  (mirrors age_state exactly)
# ---------------------------------------------------------------------------

def get_db_with_retry(log=None):
    missing = [key for key in ("host", "user", "password") if not DB_CONFIG[key]]
    if missing:
        raise RuntimeError(
            "Missing database configuration: set "
            + ", ".join("CPA_DB_" + key.upper() for key in missing)
            + "."
        )
    last_exc = None
    for attempt in range(_DB_RETRY_ATTEMPTS):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            if attempt < _DB_RETRY_ATTEMPTS - 1:
                delay = _DB_RETRY_BASE_DELAY * (2 ** attempt)
                msg = (
                    f"DB connect failed (attempt {attempt + 1}/{_DB_RETRY_ATTEMPTS}): "
                    f"{exc} — retrying in {delay}s"
                )
                if log:
                    log.warning(msg)
                else:
                    print(msg)
                time.sleep(delay)
    raise last_exc


def fetch_request_details(request_id):
    """Fetch complete notification/processing metadata for all workflows."""
    conn = get_db_with_retry()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT r.id, r.client_name, r.request_type, r.request_name,
                       r.criteria_type, r.criteria_value, r.comp_type,
                       r.criteria_json, r.channel, r.output_dir,
                       r.responder_match, r.responder_days, r.zip_radius,
                       r.merge_source_request_id,
                       u.username,
                       source.request_name AS merge_source_request_name
                  FROM requests r
                  JOIN users u ON u.id=r.created_by
             LEFT JOIN requests source ON source.id=r.merge_source_request_id
                 WHERE r.id=%s
                """,
                (request_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def update_request_status(request_id, status, status_column, log):
    last_exc = None
    for attempt in range(_DB_RETRY_ATTEMPTS):
        try:
            conn = get_db_with_retry(log)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE requests SET {status_column}=%s WHERE id=%s",
                        (status, request_id),
                    )
                conn.commit()
            finally:
                conn.close()
            log.info(f"DB STATUS UPDATE: {status_column} -> '{status}'  (request_id={request_id})")
            return
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            if attempt < _DB_RETRY_ATTEMPTS - 1:
                delay = _DB_RETRY_BASE_DELAY * (2 ** attempt)
                log.warning(
                    f"update_request_status failed (attempt {attempt + 1}/"
                    f"{_DB_RETRY_ATTEMPTS}): {exc} — retrying in {delay}s"
                )
                time.sleep(delay)
        except Exception:
            log.exception(f"Failed updating {status_column}")
            raise
    log.error(
        f"update_request_status gave up after {_DB_RETRY_ATTEMPTS} attempts: "
        f"{last_exc}"
    )
    raise last_exc


def update_ftp_path(request_id, channel_name, ftp_path, log, record_count=None):
    """
    Save the FTP file path and, when supplied, final record count into the
    corresponding requests table columns.
    Uses the same retry logic as update_request_status.

    Columns expected in requests table:
        GREEN_FTP   VARCHAR(500)
        BLUE_FTP    VARCHAR(500)
        ARCAMAX_FTP VARCHAR(500)
        ORANGE_FTP  VARCHAR(500)
    """
    channel = channel_name.upper()
    ftp_column = f"{channel}_FTP"
    count_column = f"{channel}_FILECOUNT"
    last_exc = None
    for attempt in range(_DB_RETRY_ATTEMPTS):
        try:
            conn = get_db_with_retry(log)
            try:
                with conn.cursor() as cur:
                    if record_count is None:
                        cur.execute(
                            f"UPDATE requests SET {ftp_column}=%s WHERE id=%s",
                            (ftp_path, request_id),
                        )
                    else:
                        cur.execute(
                            f"UPDATE requests SET {ftp_column}=%s, {count_column}=%s WHERE id=%s",
                            (ftp_path, str(record_count), request_id),
                        )
                conn.commit()
            finally:
                conn.close()
            log.info(
                f"DB FTP PATH UPDATE: {ftp_column} -> '{ftp_path}'"
                f" | {count_column} -> {record_count if record_count is not None else 'unchanged'}  "
                f"(request_id={request_id})"
            )
            return  # success
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            if attempt < _DB_RETRY_ATTEMPTS - 1:
                delay = _DB_RETRY_BASE_DELAY * (2 ** attempt)
                log.warning(
                    f"update_ftp_path failed (attempt {attempt + 1}/"
                    f"{_DB_RETRY_ATTEMPTS}): {exc} — retrying in {delay}s"
                )
                time.sleep(delay)
        except Exception:
            log.exception(f"Failed updating {ftp_column}")
            raise
    log.error(
        f"update_ftp_path gave up after {_DB_RETRY_ATTEMPTS} attempts: "
        f"{last_exc}"
    )
    raise last_exc


def update_channel_storage(request_id, channel_name, s3_path, row_count, log):
    """Persist the S3 location and exported-row count for one channel."""
    channel = channel_name.upper()
    if channel not in {"GREEN", "BLUE", "ARCAMAX", "ORANGE", "APPTNESS"}:
        raise ValueError(f"Unsupported channel storage update: {channel}")
    conn = get_db_with_retry(log)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE requests SET {channel}_FILEPATH=%s, {channel}_FILESIZE=%s WHERE id=%s",
                (s3_path.rstrip("/") + "/", row_count if row_count >= 0 else None, request_id),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Common context builder  (same pattern as age_state._build_common_context)
# ---------------------------------------------------------------------------

def _build_common_context(request_id, channel_name, run_dir: Path):
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found")

    client_name   = request_data["client_name"]
    request_type  = request_data["request_type"]
    request_name  = request_data["request_name"]
    criteria_type = request_data["criteria_type"]
    comp_type     = request_data["comp_type"]

    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)

    channel_tmp = run_dir / f"{channel_name.upper()}_tmp"
    channel_tmp.mkdir(parents=True, exist_ok=True)

    path_date = datetime.now().strftime("%Y%m%d")
    if request_type == "Doordash":
        # Doordash has its own S3 namespace.  Keeping this here also makes
        # process_orange_zip (which is shared with the ZIP workflow) write
        # its FINAL and COMPLETE extracts beneath Doordash rather than the
        # path of an unrelated Suppression or Mailing request.
        perm_table = (
            f"APT_CPA_DOORDASH_{channel_name.upper()}_{request_name}_{path_date}"
        )
        path_FINAL = (
            f"{S3_BASE}/Doordash/{path_date}/{request_name}/{channel_name}_FINAL"
        )
        path_COMPLETE = (
            f"{S3_BASE}/Doordash/{path_date}/{request_name}/{channel_name}_COMPLETE"
        )
        output_file = f"Doordash_{channel_name}_{path_date}.csv"
    else:
        perm_table = (
            f"APT_CPA_{channel_name.upper()}_{client_name}_{request_name}_{request_type}_{path_date}"
        )
        path_FINAL = (
            f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}_FINAL"
        )
        path_COMPLETE = (
            f"{S3_BASE}/{request_type}/{path_date}/{request_name}/{channel_name}_COMPLETE"
        )
        output_file = f"{client_name}_{request_type}_{channel_name}_{path_date}.csv"

    return {
        "request_data"   : request_data,
        "client_name"    : client_name,
        "request_type"   : request_type,
        "request_name"   : request_name,
        "criteria_type"  : criteria_type,
        "comp_type"      : comp_type,
        "final_files_dir": final_files_dir,
        "channel_tmp"    : channel_tmp,
        "path_date"      : path_date,
        "path_FINAL"     : path_FINAL,
        "path_COMPLETE"  : path_COMPLETE,
        "output_file"    : output_file,
        "perm_table"     : perm_table,
    }


# ---------------------------------------------------------------------------
# Snowflake helpers  (mirrors age_state helpers; log param on every helper)
# ---------------------------------------------------------------------------

def _query_snowflake(query_sql, log):
    """Run a snowsql query and return the first integer found in output, else -1."""
    try:
        _trace(log, "Snowflake validation query starting",
               query=_safe_sql_for_log(query_sql))
        os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
        output = subprocess.check_output(
            ["snowsql", "-c", "datateam1", "-q", query_sql,
             "-o", "output_format=csv",
             "-o", "header=false",
             "-o", "timing=false",
             "-o", "friendly=false"],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        match = re.search(r"(\d+)", output)
        count = int(match.group(1)) if match else -1
        _trace(log, "Snowflake validation query finished", count=count)
        return count
    except Exception as exc:
        log.warning(f"_query_snowflake failed (non-fatal): {exc}")
        return -1


def _query_copy_unload_rows(copy_sql, log):
    """Run a Snowflake S3 unload and sum the ROW_COUNT column, or fail."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    output = run_command([
        "snowsql", "-c", "datateam1", "-q", copy_sql,
        "-o", "output_format=csv", "-o", "header=true",
        "-o", "timing=false", "-o", "friendly=false",
        "-o", "exit_on_error=true",
    ])
    rows = list(csv.reader(io.StringIO(output)))
    for position, row in enumerate(rows):
        headings = [field.strip().upper() for field in row]
        if "ROW_COUNT" not in headings:
            continue
        count_position = headings.index("ROW_COUNT")
        counts = []
        for result in rows[position + 1:]:
            if len(result) != len(headings):
                continue
            value = result[count_position].strip()
            if not value.isdigit():
                raise RuntimeError("Snowflake unload returned a nonnumeric ROW_COUNT.")
            counts.append(int(value))
        if not counts:
            raise RuntimeError("Snowflake unload returned no file row counts.")
        total = sum(counts)
        _trace(log, "Snowflake S3 unload verified", files=len(counts), rows=total)
        return total
    raise RuntimeError("Snowflake unload did not return a ROW_COUNT column.")


def _count_file_lines(file_path):
    cmd    = f"wc -l < {shlex.quote(str(file_path))}"
    result = subprocess.check_output(cmd, shell=True, universal_newlines=True).strip()
    return int(result) if result else 0


# ── ZIP staging table ────────────────────────────────────────────────────────

def _create_zip_staging_table(zip_staging_table: str, log) -> None:
    """
    CREATE the shared ZIP staging table ONCE in Snowflake.
    Called by the orchestrator before any channel runs.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = (
        f"CREATE OR REPLACE TABLE {zip_staging_table} "
        f"(zip_code VARCHAR(10));"
    )
    log.info(f"  Creating ZIP staging table: {zip_staging_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  ZIP staging table created: {zip_staging_table}")


def _load_zips_from_s3(zip_staging_table: str, s3_zip_path: str, log) -> int:
    """
    COPY the ZIP codes CSV file from S3 into the shared staging table.
    Called by the orchestrator ONCE after creating the staging table.
    Returns the number of ZIP codes loaded.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    copy_sql = (
        f"COPY INTO {zip_staging_table} "
        f"FROM '{s3_zip_path}' "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1 "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
        f"ON_ERROR='CONTINUE' PURGE=FALSE;"
    )
    log.info(f"  Loading ZIP codes from S3: {s3_zip_path}")
    log.info(f"  COPY SQL: {_safe_sql_for_log(copy_sql)}")
    _trace(log, "ZIP staging load parameters", staging_table=zip_staging_table,
           s3_file=s3_zip_path, file_format="CSV comma-delimited, skip header=1")
    run_command(["snowsql", "-c", "datateam1", "-q", copy_sql])

    count_sql    = f"SELECT COUNT(*) FROM {zip_staging_table};"
    loaded_count = _query_snowflake(count_sql, log)
    if loaded_count >= 0:
        log.info(f"  ZIP codes loaded into {zip_staging_table}: {loaded_count:,}")
    else:
        log.warning(f"  Could not verify ZIP code count (non-fatal)")
    return loaded_count


def _drop_zip_staging_table(zip_staging_table: str, log) -> None:
    """
    DROP the shared ZIP staging table.
    Called by the orchestrator ONLY after ALL channels have completed.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {zip_staging_table};"
    log.info(f"  Dropping shared ZIP staging table: {zip_staging_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  Shared ZIP staging table {zip_staging_table} dropped successfully")


# ── Per-channel perm table helpers ───────────────────────────────────────────

def _responder_join(channel_name, responder_match, responder_days):
    """Return the optional, deduplicated responder join for one channel."""
    channel_name = str(channel_name).upper()
    if not responder_match or channel_name not in ("GREEN", "BLUE", "ORANGE"):
        return ""
    days = int(responder_days or 0)
    if days < 1:
        raise ValueError("Responder Match requires responder_days to be at least 1.")
    if channel_name in ("GREEN", "BLUE"):
        responder_channel = "GREEN" if channel_name == "GREEN" else "ORANGE"
        return (
            "JOIN (SELECT DISTINCT LOWER(TRIM(emailid)) AS email "
            "FROM GREEN.GREEN_LPT.RAW_OPENS_FOLLOWUP "
            "WHERE CHANNELNAME='{0}' "
            "AND opendate >= DATEADD(day, -{1}, CURRENT_DATE())) responders "
            "ON LOWER(TRIM(a.email)) = responders.email ".format(
                responder_channel, days
            )
        )
    return (
        "JOIN (SELECT DISTINCT LOWER(TRIM(email)) AS email "
        "FROM GREEN.DT_DATA.APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND "
        "WHERE OPEN_DATE >= DATEADD(day, -{0}, CURRENT_DATE())) responders "
        "ON LOWER(TRIM(a.email_address)) = responders.email ".format(days)
    )


def _insert_into_perm_table(
    perm_table, channel_name, zip_staging_table, comp_type, responder_match,
    responder_days, log
):
    """
    CREATE OR REPLACE the per-channel permanent table by joining each
    channel's source table against the shared ZIP staging table.

    Matching logic:
      include  → zip_col IN  (SELECT zip_code FROM <zip_staging_table>)
      exclude  → zip_col NOT IN (SELECT zip_code FROM <zip_staging_table>)
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    responder_join = _responder_join(channel_name, responder_match, responder_days)

    kw = "IN" if comp_type == "include" else "NOT IN"
    _trace(
        log, "Snowflake ZIP load parameters", channel=channel_name,
        comparison=comp_type, sql_operator=kw,
        staging_table=zip_staging_table,
        responder_match=bool(responder_match),
        responder_days=responder_days if responder_match else "not enabled",
        responder_join_enabled=bool(responder_join),
        target_table=perm_table,
    )

    if channel_name in ("GREEN", "BLUE"):
        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE"
            if channel_name == "GREEN"
            else "INFS_LPT.INFS_PROFILE"
        )
        condition = (
            f"b.ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT a.email, b.ZIP "
            f"FROM {profile_table} a "
            f"JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash = b.EMAIL_MD5 "
            f"{responder_join}"
            f"WHERE {condition};"
        )

    elif channel_name == "ARCAMAX":
        condition = (
            f"ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT email, ZIP "
            f"FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            f"WHERE {condition};"
        )

    else:  # ORANGE
        condition = (
            f"ZIP {kw} (SELECT zip_code FROM {zip_staging_table})"
        )
        insert_sql = (
            f"CREATE OR REPLACE TABLE {perm_table} AS "
            f"SELECT a.email_address, a.ZIP, b.ACCOUNT_NAME "
            f"FROM ("
            f"  SELECT a.FEED_ID, a.email_address, a.ZIP "
            f"  FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            f"  JOIN ("
            f"    SELECT email_address, MAX(created_at) AS maxdate "
            f"    FROM APT_CUSTOM_ORANGE_TRANSACTION_DND "
            f"    WHERE {condition} GROUP BY 1"
            f"  ) b ON a.email_address = b.email_address AND a.created_at = b.maxdate"
            f") a "
            f"JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 b "
            f"  ON a.FEED_ID = b.FEEDID "
            f"JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND c "
            f"  ON a.email_address = c.email_address "
            f"{responder_join}"
            f"WHERE 1=1;"
        )

    log.info(f"  Target table     : {perm_table}")
    log.info(f"  ZIP staging table: {zip_staging_table}")
    log.info(f"  comp_type        : {comp_type}  ({kw})")
    _trace(log, "Snowflake ZIP condition resolved", channel=channel_name,
           condition=condition)
    log.info(f"  INSERT SQL       : {insert_sql}")
    log.info("  Executing CREATE + INSERT via snowsql ...")

    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    log.info("  CREATE + INSERT executed successfully")

    count_sql     = f"SELECT COUNT(*) FROM {perm_table};"
    inserted_rows = _query_snowflake(count_sql, log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")

    return inserted_rows


def _export_complete_final_file(export_type, perm_table, export_path, channel_name,
                                log, criteria_columns=None, request_type=None,
                                orange_merge_source=False):
    """
    Export FINAL / COMPLETE file from permanent table -> S3 path.
    FINAL   : DISTINCT email (GREEN/BLUE/ARCAMAX)  |  DISTINCT email_address, account_name (ORANGE)
    COMPLETE: email, ZIP     (GREEN/BLUE/ARCAMAX)  |  email_address, ZIP, account_name      (ORANGE)

    ``criteria_columns`` selects the consolidated request's audit fields in
    their original selection order.  If omitted, preserve the DoorDash ZIP
    helper's existing output format.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    channel_name = str(channel_name).upper()
    if criteria_columns is not None:
        if channel_name == "ORANGE":
            email = 'email_address AS "email"'
            account = 'account_name AS "accountname"'
            if export_type == "COMPLETE":
                selected = [email, account] + [
                    '{0} AS "{1}"'.format(col.upper(), col) for col in criteria_columns
                ]
                select_clause = ", ".join(selected)
            elif orange_merge_source or str(request_type).lower() == "mailing":
                select_clause = "DISTINCT " + email + ", " + account
            else:
                select_clause = "DISTINCT " + email
        elif export_type == "COMPLETE":
            select_clause = ", ".join(['email AS "email"'] + [
                '{0} AS "{1}"'.format(col.upper(), col) for col in criteria_columns
            ])
        else:
            select_clause = 'DISTINCT email AS "email"'
    elif export_type == "FINAL":
        if channel_name == "ORANGE":
            select_clause = "DISTINCT email_address, account_name"
        else:
            select_clause = "DISTINCT email"
    else:  # COMPLETE
        if channel_name == "ORANGE":
            select_clause = "email_address, ZIP, account_name"
        else:
            select_clause = "email, ZIP"

    sql = (
        f"COPY INTO '{export_path}/' "
        f"FROM (SELECT {select_clause} FROM {perm_table}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
        f"NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE) "
        f"HEADER=TRUE "
        f"MAX_FILE_SIZE=490000000 DETAILED_OUTPUT=TRUE;"
    )

    _trace(log, "S3 export parameters", export_type=export_type,
           channel=channel_name, source_table=perm_table,
           s3_prefix=export_path, select_clause=select_clause)
    log.info(f"  Source table : {perm_table}")
    log.info(f"  S3 target    : {export_path}/")
    log.info(f"  SELECT clause: {select_clause}")
    log.info(f"  Executing COPY INTO ({export_type} FILE) via snowsql ...")

    unloaded_rows = _query_copy_unload_rows(sql, log)
    log.info(f"  COPY INTO ({export_type} FILE) completed successfully")

    if unloaded_rows >= 0:
        log.info(f"  Rows unloaded to {export_type} S3 path: {unloaded_rows:,}")
    else:
        log.warning(f"  Could not verify {export_type} FILE S3 row count (non-fatal)")
    return unloaded_rows


def _drop_perm_table(perm_table, log):
    """DROP the per-channel permanent table after both S3 exports are done."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    sql = f"DROP TABLE IF EXISTS {perm_table};"
    log.info(f"  Dropping permanent table: {perm_table}")
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    log.info(f"  Table {perm_table} dropped successfully")


# ── Download + combine helper ─────────────────────────────────────────────────

def _download_and_combine(s3_path, download_dir, work_dir,
                           output_file, channel_name, log, final_header=None):
    """
    Download Snowflake part files and stream them into one pipe-delimited
    output.  Snowflake writes a header in *every* part.  The old shell pipeline
    removed only the first global header, so multi-part files contained extra
    header rows and the stored final count was too high.  This stream-based
    implementation skips each part header without ever loading the data set
    into memory.
    """
    download_dir = Path(download_dir)
    work_dir = Path(work_dir)
    channel_name = str(channel_name).upper()
    if final_header is not None:
        output_header = list(final_header)
    elif channel_name == "ORANGE":
        output_header = ["email_address", "account_name"]
    else:
        output_header = ["email"]

    shutil.rmtree(str(download_dir), ignore_errors=True)
    download_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    # Always ensure trailing slash so aws s3 cp treats it as a prefix (folder)
    s3_prefix = s3_path.rstrip("/") + "/"

    log.info(f"  S3 source   : {s3_prefix}")
    log.info(f"  Download dir: {download_dir}")
    log.info(f"  Output file : {work_dir / output_file}")
    log.info("  Starting aws s3 cp (recursive) ...")

    run_command(
        ["aws", "s3", "cp", s3_prefix, str(download_dir), "--recursive", "--quiet"]
    )

    # Snowflake COPY INTO COMPRESSION=GZIP produces *.csv.gz part files
    downloaded = sorted(download_dir.glob("*.gz"))
    if not downloaded:
        # Fallback: check for any file (uncompressed edge case)
        downloaded = sorted(f for f in download_dir.iterdir() if f.is_file())

    if not downloaded:
        raise RuntimeError(
            f"aws s3 cp from {s3_prefix} downloaded 0 files into {download_dir}."
        )
    log.info(
        f"  Downloaded {len(downloaded)} part file(s): "
        f"{[p.name for p in downloaded]}"
    )
    for part in downloaded:
        _trace(log, "downloaded S3 part", part=part.name,
               size_bytes=part.stat().st_size)

    out_path = work_dir / output_file
    data_count = 0
    try:
        with open(str(out_path), "w", newline="") as destination:
            writer = csv.writer(destination, delimiter="|", lineterminator="\n")
            writer.writerow(output_header)
            for part in downloaded:
                part_count = 0
                opener = gzip.open if part.suffix.lower() == ".gz" else open
                with opener(str(part), "rt", newline="") as source:
                    reader = csv.reader(source, delimiter="|")
                    first_row = next(reader, None)
                    expected = [column.lower() for column in output_header]
                    if not first_row:
                        raise RuntimeError("Snowflake FINAL part has no header: {0}".format(part))
                    actual = [str(value).strip().lower() for value in first_row]
                    if actual != expected:
                        raise RuntimeError(
                            "Unexpected FINAL header in {0}: {1}; expected {2}".format(
                                part, actual, expected
                            )
                        )
                    for row in reader:
                        if not row:
                            continue
                        if len(row) != len(output_header):
                            raise RuntimeError(
                                "FINAL row in {0} has {1} columns; expected {2}".format(
                                    part, len(row), len(output_header)
                                )
                            )
                        values = [str(row[index]).strip() if len(row) > index else ""
                                  for index in range(len(output_header))]
                        if not values[0]:
                            continue
                        writer.writerow(values)
                        data_count += 1
                        part_count += 1
                _trace(log, "S3 part combined", part=part.name,
                       data_rows=part_count, header_skipped=True)
    finally:
        for path in downloaded:
            try:
                path.unlink()
            except OSError:
                pass

    _verify_local_file(log, "combined download", out_path)
    combined_count = data_count + 1  # delivery file header + data rows
    _trace(log, "combined download validation", output=out_path,
           line_count=combined_count, data_rows=data_count,
           channel=channel_name, header="|".join(output_header))
    log.info(f"  Combined file written: {out_path}  |  data rows: {data_count:,}")
    return combined_count


# ── FTP / cleanup helpers ─────────────────────────────────────────────────────

def _post_to_ftp(final_files_dir, path_date, output_file, log):
    """FTP upload from FINAL_FILES/ and return the remote FTP path."""
    if not all((FTP_USERNAME, FTP_PASSWORD, FTP_HOST)):
        raise RuntimeError(
            "Missing FTP configuration: set CPA_FTP_USERNAME, "
            "CPA_FTP_PASSWORD, and CPA_FTP_HOST."
        )
    ftp_dest = f"/CPA/{path_date}/{output_file}"
    ftp_cmd = (
        f'lftp -u "{FTP_USERNAME},{FTP_PASSWORD}" ftp://{FTP_HOST} '
        f'-e "mkdir -p /CPA/{path_date};cd /CPA/{path_date};put {output_file};bye"'
    )
    local_path = Path(final_files_dir) / output_file
    local_size = _verify_local_file(log, "FTP upload file", local_path)

    log.info(f"  FTP destination : {ftp_dest}")
    log.info(f"  Local file size : {local_size:,} bytes  ({local_path})")
    log.info("  Starting lftp upload ...")

    run_command(ftp_cmd, cwd=str(final_files_dir))
    log.info(f"  FTP upload completed successfully -> {ftp_dest}")
    _trace(log, "FTP command completed", destination=ftp_dest,
           local_file=local_path.name, size_bytes=local_size)

    # Return the full FTP path so callers can persist it to the DB
    return ftp_dest


def _cleanup_channel_tmp(channel_tmp, log):
    try:
        shutil.rmtree(str(channel_tmp))
        log.info(f"  Removed temp dir: {channel_tmp}")
    except Exception as exc:
        log.warning(f"  Could not remove temp dir {channel_tmp}: {exc}")


def _success_result(channel_name, output_file, final_file_path, elapsed, record_count=0):
    return {
        "channel"         : channel_name,
        "file"            : output_file,
        "final_file_path" : final_file_path,
        "status"          : "SUCCESS",
        "elapsed"         : elapsed,
        "count"           : record_count,
    }


# ---------------------------------------------------------------------------
# Channel processors  (7-step flow — mirrors age_state channel processors)
# ---------------------------------------------------------------------------

def process_green_blue_zip(request_id, channel_name, zip_staging_table, run_dir: Path):
    """
    GREEN and BLUE channel processor for ZIPS.
    Same 7-step flow as age_state.process_green_blue.
    Receives the shared zip_staging_table name; does NOT create or drop it.
    After FTP upload the FTP path is saved to requests.<CHANNEL>_FTP.
    """
    TOTAL_STEPS    = 7
    channel_name   = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  {channel_name} CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    _trace(
        log, "channel input validation", request_id=request_id,
        channel=channel_name, request_type=ctx["request_type"],
        comparison=ctx["comp_type"], target_table=ctx["perm_table"],
        staging_table=zip_staging_table,
        responder_match=bool(ctx["request_data"].get("responder_match")),
        responder_days=(ctx["request_data"].get("responder_days")
                        if ctx["request_data"].get("responder_match") else "not enabled"),
        merge_source_request_id=(ctx["request_data"].get("merge_source_request_id") or "NULL"),
    )
    log.info(
        f"  comp_type      = {ctx['comp_type']}\n"
        f"  perm_table     = {ctx['perm_table']}\n"
        f"  path_FINAL     = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE  = {ctx['path_COMPLETE']}\n"
        f"  output_file    = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"],
            ctx["request_data"].get("responder_match"),
            ctx["request_data"].get("responder_days"), log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(
                f"  NO DATA RETRIEVED: 0 rows inserted into {perm_table}. "
                f"Skipping remaining steps for {channel_name}."
            )
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / f"{channel_name}_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        src_file  = channel_tmp / ctx["output_file"]
        dest_file = final_files_dir / ctx["output_file"]
        _verify_local_file(log, "combined channel file before move", src_file)
        shutil.move(str(src_file), str(dest_file))
        _verify_local_file(log, "final channel file after move", dest_file)
        record_count = _count_file_lines(str(dest_file))
        _trace(log, "channel storage update", channel=channel_name,
               s3_path=path_FINAL, row_count=record_count)
        update_channel_storage(request_id, channel_name, path_FINAL, record_count, log)
        merge_mode = ""
        if ctx["request_data"].get("merge_source_request_id"):
            _trace(log, "merge enabled", current_request_id=request_id,
                   source_request_id=ctx["request_data"]["merge_source_request_id"],
                   channel=channel_name)
            merge_result = merge_current_file(
                request_id, ctx["request_data"]["merge_source_request_id"], channel_name,
                dest_file, path_FINAL, channel_tmp, log
            )
            record_count, merge_mode = merge_result["count"], merge_result["merge_mode"]
            _verify_local_file(log, "merged channel file", dest_file)
            _trace(log, "merge completed", channel=channel_name,
                   merge_mode=merge_mode, merged_row_count=record_count,
                   merged_s3_path=merge_result.get("s3_path", ""))
        else:
            _trace(log, "merge skipped", channel=channel_name,
                   reason="merge_source_request_id is NULL")
        log.info(f"  STEP 6 DONE: Moved {src_file.name} -> FINAL_FILES/  |  rows: {record_count:,}")

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, f"FTP upload -> /CPA/{ctx['path_date']}/{ctx['output_file']}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        ftp_path = _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        update_ftp_path(request_id, channel_name, ftp_path, log, record_count)
        log.info(f"  STEP 7 DONE: FTP upload successful | FTP path saved to DB -> {ftp_path}")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  {channel_name} CHANNEL (ZIPS) PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{ctx['output_file']}")
        log.info(f"  Final row count : {record_count:,}")
        log.info(f"  FTP path        : {ftp_path}")
        log.info("=" * 70)

        _cleanup_channel_tmp(channel_tmp, log)
        result = _success_result(
            channel_name, ctx["output_file"], str(dest_file), elapsed, record_count
        )
        result["merge_mode"] = merge_mode
        return result

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception(f"  {channel_name} CHANNEL (ZIPS) FAILED")
        raise


def process_arcamax_zip(request_id, zip_staging_table, run_dir: Path):
    """
    ARCAMAX channel processor for ZIPS.
    Same 7-step flow as age_state.process_arcamax.
    Uses the shared zip_staging_table for ZIP matching in Snowflake.
    After FTP upload the FTP path is saved to requests.ARCAMAX_FTP.
    """
    TOTAL_STEPS    = 7
    channel_name   = "ARCAMAX"
    channel_status = "ARCAMAX_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  ARCAMAX CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    _trace(
        log, "channel input validation", request_id=request_id,
        channel=channel_name, request_type=ctx["request_type"],
        comparison=ctx["comp_type"], target_table=ctx["perm_table"],
        staging_table=zip_staging_table,
        responder_match=bool(ctx["request_data"].get("responder_match")),
        responder_days=(ctx["request_data"].get("responder_days")
                        if ctx["request_data"].get("responder_match") else "not enabled"),
        merge_source_request_id=(ctx["request_data"].get("merge_source_request_id") or "NULL"),
    )
    log.info(
        f"  comp_type     = {ctx['comp_type']}\n"
        f"  perm_table    = {ctx['perm_table']}\n"
        f"  path_FINAL    = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE = {ctx['path_COMPLETE']}\n"
        f"  output_file   = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"],
            ctx["request_data"].get("responder_match"),
            ctx["request_data"].get("responder_days"), log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(f"  NO DATA: 0 rows inserted. Skipping remaining steps.")
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT emails) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / "ARCAMAX_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ──────────────────────────────────────────────────────
        _step(log, 6, TOTAL_STEPS, "Moving combined file to FINAL_FILES/", channel_name)
        src_file  = channel_tmp / ctx["output_file"]
        dest_file = final_files_dir / ctx["output_file"]
        _verify_local_file(log, "combined channel file before move", src_file)
        shutil.move(str(src_file), str(dest_file))
        _verify_local_file(log, "final channel file after move", dest_file)
        record_count = _count_file_lines(str(dest_file))
        _trace(log, "channel storage update", channel=channel_name,
               s3_path=path_FINAL, row_count=record_count)
        update_channel_storage(request_id, channel_name, path_FINAL, record_count, log)
        merge_mode = ""
        if ctx["request_data"].get("merge_source_request_id"):
            _trace(log, "merge enabled", current_request_id=request_id,
                   source_request_id=ctx["request_data"]["merge_source_request_id"],
                   channel=channel_name)
            merge_result = merge_current_file(
                request_id, ctx["request_data"]["merge_source_request_id"], channel_name,
                dest_file, path_FINAL, channel_tmp, log
            )
            record_count, merge_mode = merge_result["count"], merge_result["merge_mode"]
            _verify_local_file(log, "merged channel file", dest_file)
            _trace(log, "merge completed", channel=channel_name,
                   merge_mode=merge_mode, merged_row_count=record_count,
                   merged_s3_path=merge_result.get("s3_path", ""))
        else:
            _trace(log, "merge skipped", channel=channel_name,
                   reason="merge_source_request_id is NULL")
        log.info(f"  STEP 6 DONE: Moved {src_file.name} -> FINAL_FILES/  |  rows: {record_count:,}")

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, f"FTP upload -> /CPA/{ctx['path_date']}/{ctx['output_file']}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        ftp_path = _post_to_ftp(final_files_dir, ctx["path_date"], ctx["output_file"], log)
        update_ftp_path(request_id, channel_name, ftp_path, log, record_count)
        log.info(f"  STEP 7 DONE: FTP upload successful | FTP path saved to DB -> {ftp_path}")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  ARCAMAX CHANNEL (ZIPS) PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{ctx['output_file']}")
        log.info(f"  Final row count : {record_count:,}")
        log.info(f"  FTP path        : {ftp_path}")
        log.info("=" * 70)

        _cleanup_channel_tmp(channel_tmp, log)
        result = _success_result(
            channel_name, ctx["output_file"], str(dest_file), elapsed, record_count
        )
        result["merge_mode"] = merge_mode
        return result

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("  ARCAMAX CHANNEL (ZIPS) FAILED")
        raise


def process_orange_zip(request_id, zip_staging_table, run_dir: Path):
    """
    ORANGE channel processor for ZIPS.
    Same 7-step flow as age_state.process_orange.
    Uses the shared zip_staging_table for ZIP matching in Snowflake.
    ORANGE final file is split per-ESP and zipped.
    After FTP upload the FTP path is saved to requests.ORANGE_FTP.
    """
    TOTAL_STEPS    = 7
    channel_name   = "ORANGE"
    channel_status = "ORANGE_STATUS"
    log            = setup_channel_logging(run_dir, channel_name)

    log.info("=" * 70)
    log.info(f"  ORANGE CHANNEL (ZIPS) PROCESSING STARTED")
    log.info(f"  request_id       : {request_id}")
    log.info(f"  zip_staging_table: {zip_staging_table}")
    log.info(f"  run_dir          : {run_dir}")
    log.info("=" * 70)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    _trace(
        log, "channel input validation", request_id=request_id,
        channel=channel_name, request_type=ctx["request_type"],
        comparison=ctx["comp_type"], target_table=ctx["perm_table"],
        staging_table=zip_staging_table,
        responder_match=bool(ctx["request_data"].get("responder_match")),
        responder_days=(ctx["request_data"].get("responder_days")
                        if ctx["request_data"].get("responder_match") else "not enabled"),
        merge_source_request_id=(ctx["request_data"].get("merge_source_request_id") or "NULL"),
    )
    log.info(
        f"  comp_type     = {ctx['comp_type']}\n"
        f"  perm_table    = {ctx['perm_table']}\n"
        f"  path_FINAL    = {ctx['path_FINAL']}\n"
        f"  path_COMPLETE = {ctx['path_COMPLETE']}\n"
        f"  output_file   = {ctx['output_file']}"
    )

    try:
        channel_tmp     = ctx["channel_tmp"]
        final_files_dir = ctx["final_files_dir"]
        perm_table      = ctx["perm_table"]
        path_FINAL      = ctx["path_FINAL"]
        path_COMPLETE   = ctx["path_COMPLETE"]
        start_time      = time.time()

        # ── STEP 1/7 ──────────────────────────────────────────────────────
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        inserted_count = _insert_into_perm_table(
            perm_table, channel_name, zip_staging_table, ctx["comp_type"],
            ctx["request_data"].get("responder_match"),
            ctx["request_data"].get("responder_days"), log
        )

        if inserted_count == 0:
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            log.warning(f"  NO DATA: 0 rows inserted. Skipping remaining steps.")
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "file": None, "final_file_path": None,
                "status": "NO_DATA", "elapsed": time.time() - start_time, "count": 0,
            }

        log.info(f"  STEP 1 DONE: Data loaded into {perm_table}")

        # ── STEP 2/7 ──────────────────────────────────────────────────────
        _step(log, 2, TOTAL_STEPS, "Exporting FINAL FILE (DISTINCT email_address + account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Final File", channel_status, log)
        _export_complete_final_file("FINAL", perm_table, path_FINAL, channel_name, log)
        log.info(f"  STEP 2 DONE: FINAL FILE exported -> {path_FINAL}")

        # ── STEP 3/7 ──────────────────────────────────────────────────────
        _step(log, 3, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email_address + ZIP + account_name) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, path_COMPLETE, channel_name, log)
        log.info(f"  STEP 3 DONE: COMPLETE FILE exported -> {path_COMPLETE}")

        # ── STEP 4/7 ──────────────────────────────────────────────────────
        _step(log, 4, TOTAL_STEPS, f"Dropping permanent Snowflake table {perm_table}", channel_name)
        _drop_perm_table(perm_table, log)
        log.info(f"  STEP 4 DONE: Table {perm_table} dropped")

        # ── STEP 5/7 ──────────────────────────────────────────────────────
        _step(log, 5, TOTAL_STEPS, "Downloading FINAL FILE parts from S3 + combining", channel_name)
        update_request_status(request_id, "Combining Data", channel_status, log)
        download_dir   = channel_tmp / "ORANGE_FINAL_DL"
        combined_count = _download_and_combine(
            path_FINAL, download_dir, channel_tmp,
            ctx["output_file"], channel_name, log
        )
        combined_path = channel_tmp / ctx["output_file"]
        _verify_local_file(log, "ORANGE combined raw file", combined_path)
        _trace(log, "channel storage update", channel=channel_name,
               s3_path=path_FINAL, row_count=combined_count)
        update_channel_storage(request_id, channel_name, path_FINAL, combined_count, log)
        merge_mode = ""
        if ctx["request_data"].get("merge_source_request_id"):
            _trace(log, "merge enabled", current_request_id=request_id,
                   source_request_id=ctx["request_data"]["merge_source_request_id"],
                   channel=channel_name)
            merge_result = merge_current_file(
                request_id, ctx["request_data"]["merge_source_request_id"], channel_name,
                combined_path, path_FINAL, channel_tmp, log
            )
            combined_count, merge_mode = merge_result["count"], merge_result["merge_mode"]
            _verify_local_file(log, "merged ORANGE raw file", combined_path)
            _trace(log, "merge completed", channel=channel_name,
                   merge_mode=merge_mode, merged_row_count=combined_count,
                   merged_s3_path=merge_result.get("s3_path", ""))
        else:
            _trace(log, "merge skipped", channel=channel_name,
                   reason="merge_source_request_id is NULL")
        log.info(f"  STEP 5 DONE: Combined file rows: {combined_count:,}")

        # ── STEP 6/7 ── Build the format requested by the CURRENT request ─
        request_type = str(ctx["request_type"]).lower()
        df_final = pd.read_csv(
            str(combined_path), sep="|", header=0,
            names=["email", "account_name"], dtype=str,
        ).fillna("")
        _trace(log, "ORANGE data validation", raw_file=combined_path,
               dataframe_rows=len(df_final),
               expected_columns="email,account_name",
               actual_columns="|".join(str(column) for column in df_final.columns),
               empty_email_rows=int((df_final["email"].str.strip() == "").sum()))

        legacy_doordash_output = request_type == "doordash"
        if request_type == "suppression":
            _step(log, 6, TOTAL_STEPS, "Creating ORANGE suppression email list", channel_name)
            output_file = ctx["output_file"]
            final_file_path = final_files_dir / output_file
            email_rows = df_final[["email"]].drop_duplicates()
            email_rows.to_csv(str(final_file_path), index=False, header=False)
            _verify_local_file(log, "ORANGE suppression CSV", final_file_path)
            record_count = len(email_rows)
            log.info(
                f"  STEP 6 DONE: Suppression CSV created -> {final_file_path} "
                f"| unique emails: {record_count:,}"
            )

        else:
            _step(log, 6, TOTAL_STEPS, "Splitting ORANGE file per-ESP + creating ZIP archive", channel_name)
            output_path = channel_tmp / "ORANGE_OP_PATH"
            output_path.mkdir(exist_ok=True)
            esp_names = df_final["account_name"].drop_duplicates().sort_values().tolist()
            record_count = 0
            log.info(f"  Total ORANGE records: {len(df_final):,} across {len(esp_names)} ESPs")

            for esp in esp_names:
                df_esp = df_final[df_final["account_name"] == esp][["email"]].drop_duplicates()
                esp_file = output_path / f"{esp}_ORANGE_DATA.csv"
                df_esp.to_csv(esp_file, index=False, header=False)
                _verify_local_file(log, f"ORANGE ESP file ({esp})", esp_file)
                record_count += len(df_esp)
                log.info(f"  ESP {esp}: {len(df_esp):,} records")

            output_file = ctx["output_file"].replace(".csv", ".zip")
            final_file_path = final_files_dir / output_file
            if not esp_names:
                raise RuntimeError(
                    "ORANGE mailing output has no non-empty ESP groups; "
                    "a ZIP archive cannot be created."
                )
            _trace(log, "ORANGE mailing archive validation",
                   esp_file_count=len(esp_names), zip_path=final_file_path)
            run_command(
                ["zip", "-r", str(final_file_path), output_path.name],
                cwd=str(channel_tmp)
            )
            _verify_local_file(log, "ORANGE mailing ZIP", final_file_path)
            log.info(f"  STEP 6 DONE: Mailing ESP ZIP created -> {final_file_path}")
            if legacy_doordash_output:
                # The raw CSV is already retained in S3 for audit/merge.
                # FINAL_FILES contains delivery artifacts only.
                _trace(log, "DoorDash Orange raw data retained in S3",
                       s3_prefix=path_FINAL, delivery_zip=output_file)

        _cleanup_channel_tmp(channel_tmp, log)

        # ── STEP 7/7 ──────────────────────────────────────────────────────
        _step(log, 7, TOTAL_STEPS, f"FTP upload -> {output_file}", channel_name)
        update_request_status(request_id, "Posting To FTP", channel_status, log)
        if legacy_doordash_output:
            _trace(log, "DoorDash Orange ZIP-only FTP delivery",
                   zip_filename=output_file)
        ftp_path = _post_to_ftp(final_files_dir, ctx["path_date"], output_file, log)
        update_ftp_path(request_id, channel_name, ftp_path, log, record_count)
        log.info(f"  STEP 7 DONE: FTP upload successful | FTP path saved to DB -> {ftp_path}")

        elapsed = time.time() - start_time
        update_request_status(request_id, "Completed", channel_status, log)

        log.info("=" * 70)
        log.info(f"  ORANGE CHANNEL (ZIPS) PROCESSING COMPLETED SUCCESSFULLY")
        log.info(f"  Total elapsed   : {elapsed:.2f}s")
        log.info(f"  Final file      : FINAL_FILES/{output_file}")
        log.info(f"  Total records   : {record_count:,}")
        log.info(f"  FTP path        : {ftp_path}")
        log.info("=" * 70)

        result = _success_result(
            channel_name, output_file, str(final_file_path), elapsed, record_count
        )
        result["merge_mode"] = merge_mode
        return result

    except Exception:
        update_request_status(request_id, "Failed", channel_status, log)
        log.exception("  ORANGE CHANNEL (ZIPS) FAILED")
        raise


# ---------------------------------------------------------------------------
# Orchestrator  (mirrors process_age_state_request)
# ---------------------------------------------------------------------------

def process_zip_request(
    request_id: int,
    zip_file: str,
    channel,
    output_dir: str,
):
    """
    Main entry point called by main.py.

    request_id : DB request ID (used to fetch client_name, request_type, comp_type ...)
    zip_file   : absolute path to the ZIP codes file uploaded via UI
    channel    : list like ['ALL'] or ['GREEN', 'BLUE'] or single string 'ALL'
    output_dir : base output directory

    Flow
    ----
      PRE:   Upload ZIP file -> S3
             CREATE shared ZIP staging table ONCE
             COPY ZIP codes from S3 into staging table

      PARALLEL: Run all requested channels concurrently (same as age_state)

      POST:  DROP shared ZIP staging table (only after all channels finish)
    """
    # ── Run directory ─────────────────────────────────────────────────────
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_zips_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    log = setup_main_logging(run_dir)

    log.info("=" * 70)
    log.info("  ZIP REQUEST STARTED")
    log.info(f"  request_id : {request_id}")
    log.info(f"  zip_file   : {zip_file}")
    log.info(f"  channel    : {channel}")
    log.info(f"  output_dir : {output_dir}")
    log.info(f"  run_dir    : {run_dir}")
    log.info("=" * 70)

    # ── Resolve channels ──────────────────────────────────────────────────
    if isinstance(channel, str):
        channel = [channel]

    if "ALL" in channel:
        channels_to_run = list(CHANNELS)
    else:
        channels_to_run = [ch.upper() for ch in channel if ch.upper() in CHANNELS]

    requested_channels = [str(ch).upper() for ch in channel]
    invalid_channels = [ch for ch in requested_channels if ch != "ALL" and ch not in CHANNELS]
    if invalid_channels:
        raise ValueError(
            "Unsupported ZIP channel(s): " + ", ".join(invalid_channels)
        )
    if not channels_to_run:
        raise ValueError("At least one supported channel must be selected for a ZIP request.")

    log.info(f"  Channels to run: {channels_to_run}")

    # ── Fetch request details (for comp_type) ─────────────────────────────
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise Exception(f"Request ID {request_id} not found in DB")

    comp_type = request_data["comp_type"]  # 'include' | 'exclude'
    log.info(f"  comp_type: {comp_type}")
    _verify_local_file(log, "uploaded ZIP input", zip_file)
    _trace(
        log, "request validation passed", request_type=request_data["request_type"],
        comparison=comp_type, selected_channels=",".join(channels_to_run),
        responder_match=bool(request_data.get("responder_match")),
        responder_days=(request_data.get("responder_days")
                        if request_data.get("responder_match") else "not enabled"),
        merge_source_request_id=(request_data.get("merge_source_request_id") or "NULL"),
        execution_mode=("single-channel" if len(channels_to_run) == 1 else "parallel"),
    )

    # ── PRE-CHANNEL STEP A: Upload ZIP file -> S3 ─────────────────────────
    path_date   = datetime.now().strftime("%Y%m%d")
    s3_zip_dir  = f"{S3_BASE}/ZIPS/{path_date}/staging"
    s3_zip_path = f"{s3_zip_dir}/{os.path.basename(zip_file)}"

    log.info(f"  [PRE] Uploading ZIP codes file to S3: {s3_zip_path}")
    run_command(["aws", "s3", "cp", zip_file, s3_zip_path, "--quiet"])
    log.info(f"  [PRE] ZIP file uploaded -> {s3_zip_path}")
    _trace(log, "ZIP input upload completed", local_file=zip_file,
           local_size_bytes=Path(zip_file).stat().st_size, s3_path=s3_zip_path)

    # ── PRE-CHANNEL STEP B: Create shared ZIP staging table ONCE ─────────
    zip_staging_table = f"APT_CPA_ZIPS_STAGING_{ts}"
    log.info(f"  [PRE] Creating shared ZIP staging table: {zip_staging_table}")
    _create_zip_staging_table(zip_staging_table, log)

    # ── PRE-CHANNEL STEP C: Load ZIP codes into staging table ─────────────
    log.info(f"  [PRE] Loading ZIP codes into staging table from S3 ...")
    zip_count = _load_zips_from_s3(zip_staging_table, s3_zip_path, log)
    log.info(f"  [PRE] {zip_count:,} ZIP codes loaded into {zip_staging_table}")

    if zip_count == 0:
        log.error("  [PRE] No ZIP codes loaded into staging table — aborting all channels")
        _drop_zip_staging_table(zip_staging_table, log)
        raise RuntimeError("ZIP staging table is empty — no ZIP codes were loaded from the file.")

    # ── Channel processor map ──────────────────────────────────────────────
    def _run_channel(ch):
        _trace(log, "dispatching channel", channel=ch,
               processor=("GREEN_BLUE" if ch in ("GREEN", "BLUE") else ch))
        if ch in ("GREEN", "BLUE"):
            return process_green_blue_zip(request_id, ch, zip_staging_table, run_dir)
        elif ch == "ARCAMAX":
            return process_arcamax_zip(request_id, zip_staging_table, run_dir)
        elif ch == "ORANGE":
            return process_orange_zip(request_id, zip_staging_table, run_dir)
        else:
            raise ValueError(f"Unknown channel: {ch}")

    # ── Parallel channel execution (same pattern as age_state) ────────────
    results = {}
    errors  = []

    if len(channels_to_run) == 1:
        ch = channels_to_run[0]
        log.info(f"  Single channel '{ch}' — running directly (no thread pool)")
        try:
            result = _run_channel(ch)
            results[ch] = result
            log.info(f"  Channel '{ch}' completed: {result.get('count', 0):,} records")
        except Exception as exc:
            log.error(f"  Channel '{ch}' FAILED: {exc}")
            errors.append((ch, str(exc)))
    else:
        log.info(f"  Multiple channels — running in parallel with ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
            future_to_ch = {
                executor.submit(_run_channel, ch): ch
                for ch in channels_to_run
            }
            for future in as_completed(future_to_ch):
                ch = future_to_ch[future]
                try:
                    result = future.result()
                    results[ch] = result
                    log.info(
                        f"  Channel '{ch}' completed: "
                        f"{result.get('count', 0):,} records"
                    )
                except Exception as exc:
                    log.error(f"  Channel '{ch}' FAILED: {exc}")
                    errors.append((ch, str(exc)))

    # ── POST-CHANNEL: DROP shared ZIP staging table ───────────────────────
    log.info(f"  [POST] All channels finished. Dropping shared ZIP staging table: {zip_staging_table}")
    try:
        _drop_zip_staging_table(zip_staging_table, log)
    except Exception as exc:
        log.warning(f"  [POST] Failed to drop ZIP staging table (non-fatal): {exc}")

    # ── Summary ───────────────────────────────────────────────────────────
    total_records = sum(
        v.get("count", 0) for v in results.values() if isinstance(v, dict)
    )
    summary_lines = [
        f"  {ch}: {v.get('file')} ({v.get('count', 0):,} records)"
        for ch, v in results.items()
        if isinstance(v, dict)
    ]
    summary = (
        f"\n{'=' * 60}\n"
        f"ZIP REQUEST COMPLETE\n"
        f"request_id  : {request_id}\n"
        f"comp_type   : {comp_type}\n"
        f"Channels    : {', '.join(channels_to_run)}\n"
        f"Total recs  : {total_records:,}\n"
        f"ZIP staging : {zip_staging_table} (DROPPED)\n"
        f"Output      : {run_dir}/FINAL_FILES\n"
        + "\n".join(summary_lines)
        + (
            f"\nERRORS ({len(errors)}): " + "; ".join(f"{c}: {e}" for c, e in errors)
            if errors
            else ""
        )
        + f"\n{'=' * 60}"
    )
    log.info(summary)
    _trace(log, "orchestrator result validation",
           requested_channels=",".join(channels_to_run),
           completed_channels=",".join(sorted(results.keys())) or "none",
           failed_channels=",".join(ch for ch, _ in errors) or "none",
           total_records=total_records)

    if errors:
        send_error_email(
            request_data,
            "\n".join(f"{c}: {e}" for c, e in errors),
            run_dir,
        )
    else:
        send_success_email(request_data, results, run_dir)

    if errors:
        raise RuntimeError(
            "ZIP request failed for channel(s): "
            + "; ".join(f"{channel}: {error}" for channel, error in errors)
        )


# ---------------------------------------------------------------------------
# Output naming and FTP routing
# ---------------------------------------------------------------------------

def _safe_filename_part(value):
    return str(value or "").strip().replace("/", "_").replace("\\", "_").replace(" ", "_")


_ORIGINAL_BUILD_COMMON_CONTEXT = _build_common_context
_ORIGINAL_POST_TO_FTP = _post_to_ftp
_REQUEST_TYPE_BY_FINAL_DIR = {}


def _build_common_context(request_id, channel_name, run_dir):
    ctx = _ORIGINAL_BUILD_COMMON_CONTEXT(request_id, channel_name, run_dir)
    request_data = ctx["request_data"]
    client_name = _safe_filename_part(request_data["client_name"])
    criteria_type = _safe_filename_part(request_data["criteria_type"].title())
    request_type = _safe_filename_part(request_data["request_type"])
    channel = str(channel_name).upper()
    path_date = ctx["path_date"]
    extension = Path(ctx["output_file"]).suffix or ".csv"
    ctx["output_file"] = f"{client_name}_{criteria_type}_{request_type}_{channel}_{path_date}{extension}"
    _REQUEST_TYPE_BY_FINAL_DIR[str(ctx["final_files_dir"])] = request_type
    return ctx


def _post_to_ftp(final_files_dir, path_date, output_file, log, request_type=None):
    request_type = request_type or _REQUEST_TYPE_BY_FINAL_DIR.get(str(Path(final_files_dir)), "Suppression")
    return _ORIGINAL_POST_TO_FTP(final_files_dir, f"{path_date}/{request_type}", output_file, log)


# =============================================================================
# Consolidated non-DoorDash criteria processor
# =============================================================================
#
# The original ZIP-only functions above remain private compatibility helpers for
# the DoorDash workflow.  All Suppression and Mailing requests now enter through
# process_request() below, regardless of whether they contain one criterion or
# a mixture of Age, State, and ZIP criteria.

CONSOLIDATED_CRITERIA = ("age", "state", "zips", "gender")


def _safe_identifier(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", str(value or ""))


def _safe_sql_identifier(value, label):
    value = str(value or "").strip()
    if not value or not re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", value):
        raise ValueError("Invalid {0} SQL identifier.".format(label))
    return value


def _criterion_values(item):
    """Return cleaned State or Gender values."""
    values = item.get("values")
    if values is None:
        values = item.get("value", "")
    if isinstance(values, str):
        values = values.split(",")
    if not isinstance(values, (list, tuple)):
        values = [values]
    return [str(value).strip().upper() for value in values if str(value).strip()]


def _fetch_consolidated_request(request_id):
    """Read every persisted input needed by the unified processor."""
    conn = get_db_with_retry()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT r.id, r.request_name, r.client_name, r.request_type,
                       r.criteria_type, r.comp_type, r.criteria_value,
                       r.criteria_json, r.zip_file_path, r.channel,
                       r.output_dir, r.merge_source_request_id,
                       r.responder_match, r.responder_days, r.zip_radius, u.username,
                       source.request_name AS merge_source_request_name
                  FROM requests r
                  JOIN users u ON u.id = r.created_by
             LEFT JOIN requests source ON source.id = r.merge_source_request_id
                 WHERE r.id=%s
                """,
                (request_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _criteria_from_request(request_data):
    """Normalise new JSON criteria and legacy one-criterion request rows."""
    raw_json = request_data.get("criteria_json")
    if raw_json:
        try:
            criteria = json.loads(raw_json)
        except (TypeError, ValueError):
            raise ValueError("Saved criteria_json is invalid for this request.")
    else:
        criteria_type = str(request_data.get("criteria_type") or "").lower()
        comparison = str(request_data.get("comp_type") or "").lower()
        value = request_data.get("criteria_value") or ""
        if criteria_type == "age":
            if comparison == "between":
                parts = [part.strip() for part in str(value).split(",", 1)]
                if len(parts) != 2:
                    raise ValueError("Legacy Age Between request requires two values.")
                criteria = [{"type": "age", "comparison": comparison,
                             "from": parts[0], "to": parts[1]}]
            else:
                criteria = [{"type": "age", "comparison": comparison,
                             "value": str(value)}]
        elif criteria_type in ("state", "gender"):
            criteria = [{"type": criteria_type, "comparison": comparison,
                         "values": [part.strip() for part in str(value).split(",")
                                    if part.strip()]}]
        elif criteria_type in ("zip", "zips"):
            criteria = [{"type": "zips", "comparison": comparison,
                         "file_path": request_data.get("zip_file_path")}]
        else:
            raise ValueError("Request has no supported criteria definition.")

    if not isinstance(criteria, list) or not criteria:
        raise ValueError("At least one criterion is required.")
    if len(criteria) > len(CONSOLIDATED_CRITERIA):
        raise ValueError("A request can contain at most Age, State, ZIP, and Gender.")

    normalised = []
    seen = set()
    for raw_item in criteria:
        if not isinstance(raw_item, dict):
            raise ValueError("Each criterion must be an object.")
        item = dict(raw_item)
        item_type = str(item.get("type") or "").lower()
        if item_type == "zip":
            item_type = "zips"
        comparison = str(item.get("comparison") or "").lower()
        if item_type not in CONSOLIDATED_CRITERIA:
            raise ValueError("Unsupported criterion: {0}".format(item_type))
        if item_type in seen:
            raise ValueError("Each criterion can be selected only once.")
        seen.add(item_type)
        item["type"] = item_type
        item["comparison"] = comparison

        if item_type == "age":
            if comparison == "between":
                try:
                    item["from"] = str(int(item.get("from")))
                    item["to"] = str(int(item.get("to")))
                except (TypeError, ValueError):
                    raise ValueError("Age Between requires numeric From and To values.")
            elif comparison in ("greater", "less"):
                try:
                    item["value"] = str(int(item.get("value")))
                except (TypeError, ValueError):
                    raise ValueError("Age requires a numeric value.")
            else:
                raise ValueError("Age supports Greater Than, Lesser Than, or Between.")
        elif item_type == "state":
            item["values"] = _criterion_values(item)
            if comparison not in ("include", "exclude") or not item["values"]:
                raise ValueError(
                    "{0} requires Include/Exclude and at least one value."
                    .format(item_type.title())
                )
        elif item_type == "gender":
            item["values"] = _criterion_values(item)
            if comparison != "include" or len(item["values"]) != 1 or item["values"][0] not in ("MALE", "FEMALE"):
                raise ValueError("Gender requires Include and exactly one choice: MALE or FEMALE.")
        elif item_type == "zips":
            if comparison not in ("include", "exclude"):
                raise ValueError("ZIP supports Include or Exclude.")
        normalised.append(item)
    return normalised


def _column_map(channel):
    channel = str(channel).upper()
    if channel in ("GREEN", "BLUE"):
        # BLUE joins the same DND table as GREEN; its profile selects the email.
        return {"age": "b.AGE", "state": "b.STATE", "zips": "b.ZIP",
                "gender": "b.GENDER"}
    if channel == "ARCAMAX":
        return {"age": "a.birthday", "state": "a.STATE", "zips": "a.ZIP",
                "gender": "g.gender"}
    if channel == "ORANGE":
        return {"age": "a.dob", "state": "a.STATE", "zips": "a.ZIP",
                "gender": "a.GENDER"}
    raise ValueError("Unsupported channel: {0}".format(channel))


def _gender_expression(channel):
    """Normalize M/F and Male/Female source values to one audit value."""
    column = _column_map(channel)["gender"]
    return (
        "CASE UPPER(TRIM(TO_VARCHAR({0}))) "
        "WHEN 'M' THEN 'MALE' WHEN 'MALE' THEN 'MALE' "
        "WHEN 'F' THEN 'FEMALE' WHEN 'FEMALE' THEN 'FEMALE' "
        "ELSE NULL END"
    ).format(column)


def _arcamax_gender_join(log):
    """Join the verified Arcamax SEX source using a discovered email key.

    The Arcamax birthday/state/ZIP source is not the table in the supplied SEX
    screenshot. Discover the key in Snowflake metadata so an unexpected schema
    fails clearly instead of silently matching on a guessed column.
    """
    table = "APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE_DND_SF"
    metadata_sql = (
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_CATALOG=CURRENT_DATABASE() "
        "AND TABLE_SCHEMA=CURRENT_SCHEMA() "
        "AND TABLE_NAME='{0}' "
        "AND COLUMN_NAME IN ('EMAIL', 'EMAILID', 'EMAIL_ADDRESS') "
        "ORDER BY CASE COLUMN_NAME WHEN 'EMAIL' THEN 1 "
        "WHEN 'EMAILID' THEN 2 ELSE 3 END"
    ).format(table)
    output = run_command([
        "snowsql", "-c", "datateam1", "-q", metadata_sql,
        "-o", "output_format=csv", "-o", "header=false",
        "-o", "timing=false", "-o", "friendly=false", "-o", "exit_on_error=true",
    ])
    candidates = [line.strip().strip('"').upper() for line in output.splitlines()]
    email_key = next((name for name in candidates
                      if name in ("EMAIL", "EMAILID", "EMAIL_ADDRESS")), None)
    if not email_key:
        raise RuntimeError(
            "Arcamax Gender source {0} has no verified EMAIL, EMAILID or "
            "EMAIL_ADDRESS column in the current Snowflake schema."
            .format(table)
        )
    _trace(log, "Arcamax Gender join key verified", table=table, email_key=email_key,
           conflict_rule="different SEX values for the same email become NULL")
    return (
        "LEFT JOIN (SELECT email_key, "
        "IFF(MIN(gender_value)=MAX(gender_value), MIN(gender_value), NULL) AS gender "
        "FROM (SELECT LOWER(TRIM(TO_VARCHAR({key}))) AS email_key, "
        "CASE UPPER(TRIM(TO_VARCHAR(SEX))) "
        "WHEN 'M' THEN 'MALE' WHEN 'MALE' THEN 'MALE' "
        "WHEN 'F' THEN 'FEMALE' WHEN 'FEMALE' THEN 'FEMALE' "
        "ELSE NULL END AS gender_value "
        "FROM {table} WHERE UPPER(TRIM(TO_VARCHAR(SEX))) "
        "IN ('M','MALE','F','FEMALE')) gender_records "
        "WHERE email_key <> '' GROUP BY email_key) g "
        "ON LOWER(TRIM(TO_VARCHAR(a.email)))=g.email_key "
    ).format(key=email_key, table=table)


def _age_expression(channel):
    """Use the same integer age in filtering and COMPLETE output."""
    source = _column_map(channel)["age"]
    if channel in ("GREEN", "BLUE"):
        return "TRY_TO_NUMBER(TO_VARCHAR({0}))".format(source)
    birthday = "TRY_TO_DATE(TO_VARCHAR({0}))".format(source)
    years = "DATEDIFF(year, {0}, CURRENT_DATE())".format(birthday)
    return "({0} - IFF(DATEADD(year, {0}, {1}) > CURRENT_DATE(), 1, 0))".format(
        years, birthday
    )


def _criteria_predicate(channel, criteria, zip_staging_table, log):
    """Build the OR condition used for one channel's source query."""
    channel = str(channel).upper()
    columns = _column_map(channel)
    conditions = []
    for item in criteria:
        item_type = item["type"]
        comparison = item["comparison"]
        _trace(log, "criterion resolved", channel=channel, criterion=item_type,
               comparison=comparison,
               values=(item.get("values") or item.get("value") or
                       "{0},{1}".format(item.get("from", ""), item.get("to", ""))),
               zip_staging_table=(zip_staging_table or "not used"))
        if item_type == "age":
            column = _age_expression(channel)
            if comparison == "between":
                low, high = sorted((int(item["from"]), int(item["to"])))
                conditions.append("{0} BETWEEN {1} AND {2}".format(column, low, high))
            else:
                age_value = int(item["value"])
                operator = ">" if comparison == "greater" else "<"
                conditions.append("{0} {1} {2}".format(column, operator, age_value))
        elif item_type == "state":
            values = [value.replace("'", "''") for value in item["values"]]
            operator = "IN" if comparison == "include" else "NOT IN"
            conditions.append(
                "{0} {1} ({2})".format(
                    columns[item_type], operator,
                    ",".join("'{0}'".format(value) for value in values)
                )
            )
        elif item_type == "gender":
            # Validation above restricts this SQL literal to MALE/FEMALE.
            conditions.append("{0} = '{1}'".format(
                _gender_expression(channel), item["values"][0]
            ))
        elif item_type == "zips":
            if not zip_staging_table:
                raise ValueError("ZIP criterion requires a populated ZIP staging table.")
            operator = "IN" if comparison == "include" else "NOT IN"
            conditions.append(
                "{0} {1} (SELECT zip_code FROM {2})".format(
                    columns["zips"], operator, _safe_sql_identifier(zip_staging_table, "ZIP staging table")
                )
            )
    if not conditions:
        raise ValueError("At least one valid criterion is required.")
    predicate = "(" + " OR ".join("(" + condition + ")" for condition in conditions) + ")"
    _trace(log, "OR predicate validated", channel=channel, predicate=predicate,
           predicate_count=len(conditions))
    return predicate


def _create_criteria_channel_table(perm_table, channel, criteria,
                                   zip_staging_table, responder_match,
                                   responder_days, log):
    """Create one channel's standardised table for any supported criteria set."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    channel = str(channel).upper()
    predicate = _criteria_predicate(channel, criteria, zip_staging_table, log)
    responder_join = _responder_join(channel, responder_match, responder_days)
    selected = {item["type"] for item in criteria}
    columns = _column_map(channel)
    output_columns = []
    for kind, alias in (("age", "AGE"), ("state", "STATE"),
                        ("zips", "ZIP"), ("gender", "GENDER")):
        if kind in selected:
            if kind == "age":
                value = _age_expression(channel)
            elif kind == "gender":
                value = _gender_expression(channel)
            else:
                value = columns[kind]
            output_columns.append("{0} AS {1}".format(value, alias))
    selected_columns = ", " + ", ".join(output_columns)
    if channel in ("GREEN", "BLUE"):
        profile_table = (
            "GREEN_LPT.UNIVERSAL_PROFILE" if channel == "GREEN"
            else "INFS_LPT.INFS_PROFILE"
        )
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT a.email{selected_columns} FROM {profile} a "
            "JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 "
            "{responder_join}WHERE {predicate};"
        ).format(perm=perm_table, profile=profile_table,
                 responder_join=responder_join, predicate=predicate,
                 selected_columns=selected_columns)
    elif channel == "ARCAMAX":
        gender_join = _arcamax_gender_join(log) if "gender" in selected else ""
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT a.email{selected_columns} FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE a "
            "{gender_join}"
            "WHERE {predicate};"
        ).format(perm=perm_table, gender_join=gender_join, predicate=predicate,
                 selected_columns=selected_columns)
    else:
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT a.email_address, esp.ACCOUNT_NAME AS account_name{selected_columns} "
            "FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            "JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 esp "
            "ON a.FEED_ID=esp.FEEDID "
            "JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND p "
            "ON a.email_address=p.email_address "
            "{responder_join}WHERE {predicate} "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY a.email_address "
            "ORDER BY a.created_at DESC)=1;"
        ).format(perm=perm_table, responder_join=responder_join,
                 predicate=predicate, selected_columns=selected_columns)
    _trace(log, "Snowflake criteria table creation", channel=channel,
           target_table=perm_table, responder_match=bool(responder_match),
           responder_days=(responder_days if responder_match else "not enabled"),
           selected_columns=",".join(item["type"] for item in criteria),
           sql=_safe_sql_for_log(sql))
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    count = _query_snowflake("SELECT COUNT(*) FROM {0}".format(perm_table), log)
    if count < 0:
        raise RuntimeError("Could not verify Snowflake row count for {0}".format(perm_table))
    _trace(log, "Snowflake criteria table validated", channel=channel,
           target_table=perm_table, row_count=count)
    return count


def _consolidated_context(request_data, criteria, channel, run_dir):
    """Build paths/names without relying on the retired processor modules."""
    channel = str(channel).upper()
    path_date = datetime.now().strftime("%Y%m%d")
    request_type = str(request_data["request_type"])
    request_name = _safe_identifier(request_data["request_name"])
    client_name = _safe_filename_part(request_data["client_name"])
    criteria_label = _safe_filename_part(
        "Multi" if len(criteria) > 1 else str(criteria[0]["type"]).title()
    )
    final_files_dir = Path(run_dir) / "FINAL_FILES"
    channel_tmp = Path(run_dir) / (channel + "_criteria_tmp")
    final_files_dir.mkdir(parents=True, exist_ok=True)
    channel_tmp.mkdir(parents=True, exist_ok=True)
    return {
        "path_date": path_date,
        "final_files_dir": final_files_dir,
        "channel_tmp": channel_tmp,
        "perm_table": "APT_CPA_REQUEST_{0}_{1}_{2}".format(
            channel, request_data["id"], path_date
        ),
        "path_FINAL": "{0}/{1}/{2}/{3}/{4}_FINAL".format(
            S3_BASE, request_type, path_date, request_name, channel
        ),
        "path_COMPLETE": "{0}/{1}/{2}/{3}/{4}_COMPLETE".format(
            S3_BASE, request_type, path_date, request_name, channel
        ),
        "output_file": "{0}_{1}_{2}_{3}_{4}.csv".format(
            client_name, criteria_label, _safe_filename_part(request_type), channel, path_date
        ),
    }


def _write_orange_delivery(raw_file, context, request_type, log):
    """Create Orange delivery output without loading a large file into memory.

    The Snowflake FINAL export is already email-deduplicated.  This function
    therefore streams it once: Suppression writes an email-only file, while
    Mailing writes one file per ESP/account and then creates an archive.
    """
    raw_file = Path(raw_file)
    final_dir = context["final_files_dir"]
    _verify_local_file(log, "Orange raw output", raw_file)

    with open(str(raw_file), "r", newline="") as source:
        reader = csv.DictReader(source, delimiter="|")
        fieldnames = reader.fieldnames or []
        email_column = "email_address" if "email_address" in fieldnames else "email"
        account_column = "accountname" if "accountname" in fieldnames else "account_name"
        if email_column not in fieldnames:
            raise RuntimeError("Orange raw output must contain an email column.")
        if str(request_type).lower() == "mailing" and account_column not in fieldnames:
            raise RuntimeError("Orange Mailing output must contain an accountname column.")
        _trace(log, "Orange raw delivery header validated", raw_file=raw_file,
               email_column=email_column, account_column=account_column,
               request_type=request_type)

        if str(request_type).lower() == "suppression":
            final_file = final_dir / context["output_file"]
            record_count = 0
            with open(str(final_file), "w", newline="") as destination:
                writer = csv.writer(destination, lineterminator="\n")
                # Account/ESP is routing metadata only.  The delivered
                # suppression file always has the requested email header.
                writer.writerow(["email"])
                for row in reader:
                    email = str(row.get(email_column) or "").strip()
                    if not email:
                        continue
                    writer.writerow([email])
                    record_count += 1
            _verify_local_file(log, "Orange suppression delivery", final_file)
            _trace(log, "Orange suppression delivery created",
                   filename=final_file.name, row_count=record_count)
            return final_file.name, final_file, record_count

        esp_dir = context["channel_tmp"] / "ORANGE_ESP"
        esp_dir.mkdir(exist_ok=True)
        open_outputs = {}
        counts = {}
        try:
            for row in reader:
                email = str(row.get(email_column) or "").strip()
                account = str(row.get(account_column) or "").strip()
                if not email:
                    continue
                if not account:
                    raise RuntimeError(
                        "Orange Mailing output contains an email without an ESP/account_name. "
                        "The source data must include an ESP mapping before merge/delivery."
                    )
                if account not in open_outputs:
                    filename = "{0}_ORANGE_DATA.csv".format(
                        _safe_filename_part(account) or "UNKNOWN_ESP"
                    )
                    output = esp_dir / filename
                    handle = open(str(output), "w", newline="")
                    writer = csv.writer(handle, lineterminator="\n")
                    # Every inner ESP file is a delivery list, never the raw
                    # email/account S3 audit export.
                    writer.writerow(["email"])
                    open_outputs[account] = (output, handle, writer)
                    counts[account] = 0
                output, handle, writer = open_outputs[account]
                del output, handle
                writer.writerow([email])
                counts[account] += 1
        finally:
            for output, handle, writer in open_outputs.values():
                del output, writer
                handle.close()

    if not counts:
        raise RuntimeError("Orange Mailing output has no ESP/account_name values.")
    record_count = 0
    for account, count in sorted(counts.items()):
        output = open_outputs[account][0]
        _verify_local_file(log, "Orange ESP delivery ({0})".format(account), output)
        record_count += count
        _trace(log, "Orange ESP delivery created", account_name=account,
               filename=output.name, unique_email_count=count)
    archive_name = Path(context["output_file"]).with_suffix(".zip").name
    archive_file = final_dir / archive_name
    run_command(["zip", "-r", str(archive_file), esp_dir.name], cwd=str(context["channel_tmp"]))
    _verify_local_file(log, "Orange Mailing ESP archive", archive_file)
    _trace(log, "Orange Mailing ESP archive created", archive_file=archive_file,
           account_count=len(counts), row_count=record_count)
    return archive_name, archive_file, record_count


def _process_criteria_channel(request_data, criteria, zip_staging_table, run_dir, channel):
    """Run one complete non-DoorDash channel using the common criteria engine."""
    channel = str(channel).upper()
    request_id = request_data["id"]
    context = _consolidated_context(request_data, criteria, channel, run_dir)
    log = setup_processor_channel_logging(run_dir, channel)
    started = time.time()
    table_created = False
    selected_columns = ["zip" if item["type"] == "zips" else item["type"]
                        for item in criteria]
    is_orange_mailing = (channel == "ORANGE" and
                         str(request_data["request_type"]).lower() == "mailing")
    final_data_header = "email|accountname" if is_orange_mailing else "email"
    complete_data_header = "|".join(
        (["email", "accountname"] if channel == "ORANGE" else ["email"])
        + selected_columns
    )
    orange_merge_source_s3 = (
        "{0}/{1}/{2}/{3}/ORANGE_MERGE_SOURCE".format(
            S3_BASE, request_data["request_type"], context["path_date"],
            _safe_identifier(request_data["request_name"])
        ) if channel == "ORANGE" and not is_orange_mailing else None
    )
    _trace(log, "consolidated channel start", request_id=request_id, channel=channel,
           criteria_json=json.dumps(criteria, sort_keys=True),
           zip_staging_table=(zip_staging_table or "not used"),
           merge_source_request_id=(request_data.get("merge_source_request_id") or "NULL"),
           final_s3=context["path_FINAL"], complete_s3=context["path_COMPLETE"],
           final_data_header=final_data_header,
           complete_data_header=complete_data_header)
    try:
        update_request_status(request_id, "Started", channel + "_STATUS", log)
        _step(log, 1, 9, "Creating Snowflake criteria table", channel)
        update_request_status(request_id, "Loading to Snowflake", channel + "_STATUS", log)
        count = _create_criteria_channel_table(
            context["perm_table"], channel, criteria, zip_staging_table,
            request_data.get("responder_match"), request_data.get("responder_days"), log,
        )
        table_created = True
        if count == 0:
            update_request_status(request_id, "No Data Retrieved", channel + "_STATUS", log)
            _drop_perm_table(context["perm_table"], log)
            table_created = False
            return {"channel": channel, "status": "NO_DATA", "count": 0,
                    "file": None, "elapsed": time.time() - started}

        _step(log, 2, 9, "Exporting final data to S3", channel)
        update_request_status(request_id, "Exporting Final File", channel + "_STATUS", log)
        final_export_count = _export_complete_final_file(
            "FINAL", context["perm_table"], context["path_FINAL"], channel, log,
            criteria_columns=selected_columns, request_type=request_data["request_type"],
        )
        _step(log, 3, 9, "Exporting complete audit data to S3", channel)
        update_request_status(request_id, "Exporting Complete File", channel + "_STATUS", log)
        complete_export_count = _export_complete_final_file(
            "COMPLETE", context["perm_table"], context["path_COMPLETE"], channel, log,
            criteria_columns=selected_columns, request_type=request_data["request_type"],
        )
        if orange_merge_source_s3:
            _trace(log, "exporting private Orange merge source",
                   s3_path=orange_merge_source_s3)
            _export_complete_final_file(
                "FINAL", context["perm_table"], orange_merge_source_s3,
                channel, log, criteria_columns=selected_columns,
                request_type=request_data["request_type"],
                orange_merge_source=True,
            )
        elif channel == "ORANGE":
            orange_merge_source_s3 = context["path_FINAL"]

        _step(log, 4, 9, "Dropping validated Snowflake table", channel)
        _drop_perm_table(context["perm_table"], log)
        table_created = False

        _step(log, 5, 9, "Downloading and combining final S3 data", channel)
        update_request_status(request_id, "Combining Data", channel + "_STATUS", log)
        raw_file = context["channel_tmp"] / context["output_file"]
        _download_and_combine(
            context["path_FINAL"], context["channel_tmp"] / "download",
            context["channel_tmp"], context["output_file"], channel, log,
            final_header=final_data_header.split("|"),
        )
        _verify_local_file(log, "combined current output", raw_file)

        _step(log, 6, 9, "Applying optional previous-output merge", channel)
        if request_data.get("merge_source_request_id"):
            merge_result = merge_current_file(
                request_id, request_data["merge_source_request_id"], channel,
                raw_file, context["path_FINAL"], context["channel_tmp"], log,
                orange_merge_source_s3=orange_merge_source_s3,
                request_type=request_data["request_type"],
            )
            count = merge_result["count"]
            merge_mode = merge_result["merge_mode"]
            output_s3 = merge_result["s3_path"]
            _verify_local_file(log, "merged current output", raw_file)
        else:
            count = max(_count_file_lines(str(raw_file)) - 1, 0)
            merge_mode = "CURRENT_ONLY"
            output_s3 = context["path_FINAL"]
            if channel == "ORANGE":
                update_orange_merge_source_storage(
                    request_id, orange_merge_source_s3, log
                )
            _trace(log, "merge skipped", channel=channel,
                   reason="merge_source_request_id is NULL", row_count=count)

        _step(log, 7, 9, "Building current request delivery artifact", channel)
        if channel == "ORANGE":
            output_name, final_file, count = _write_orange_delivery(
                raw_file, context, request_data["request_type"], log
            )
            artifacts = [{
                "channel": channel, "file": output_name,
                "final_file_path": str(final_file), "count": count,
                "delivery_header": (
                    "email" if str(request_data["request_type"]).lower() == "suppression"
                    else "ZIP archive (each ESP CSV header: email)"
                ),
            }]
            delivery_count = count
        else:
            output_name = context["output_file"]
            final_file = context["final_files_dir"] / output_name
            shutil.move(str(raw_file), str(final_file))
            _verify_local_file(log, "final delivery file", final_file)
            artifacts = [{
                "channel": channel, "file": output_name,
                "final_file_path": str(final_file), "count": count,
                "delivery_header": "email",
            }]
            delivery_count = count

        _step(log, 8, 9, "Persisting output details", channel)
        if merge_mode == "CURRENT_ONLY":
            update_channel_storage(request_id, channel, output_s3, delivery_count, log)
        update_request_status(request_id, "Posting To FTP", channel + "_STATUS", log)

        _step(log, 9, 9, "Posting delivery artifact to FTP", channel)
        ftp_paths = []
        for artifact in artifacts:
            artifact_ftp_path = _post_to_ftp(
                context["final_files_dir"], context["path_date"], artifact["file"], log,
                request_type=request_data["request_type"],
            )
            artifact["ftp_path"] = artifact_ftp_path
            ftp_paths.append(artifact_ftp_path)
            _trace(log, "delivery artifact posted", channel=channel,
                   filename=artifact["file"], ftp_path=artifact_ftp_path,
                   row_count=artifact["count"])
        ftp_path = " | ".join(ftp_paths)
        update_ftp_path(request_id, channel, ftp_path, log, delivery_count)
        update_request_status(request_id, "Completed", channel + "_STATUS", log)
        elapsed = time.time() - started
        common_artifact_metadata = {
            "merge_mode": merge_mode,
            "merge_source_request_id": request_data.get("merge_source_request_id") or "",
            "merge_source_request_name": request_data.get("merge_source_request_name") or "",
            "s3_path": output_s3,
            "final_s3_path": output_s3,
            "complete_s3_path": context["path_COMPLETE"],
            "final_data_header": final_data_header,
            "complete_data_header": complete_data_header,
            "final_count": delivery_count,
            "complete_count": complete_export_count if complete_export_count >= 0 else "",
            "local_output_dir": str(context["final_files_dir"]),
        }
        for artifact in artifacts:
            artifact.update(common_artifact_metadata)
        _trace(log, "consolidated channel completed", channel=channel,
               row_count=delivery_count, final_export_count=final_export_count,
               complete_export_count=complete_export_count, merge_mode=merge_mode,
               s3_path=output_s3, ftp_path=ftp_path,
               final_files=",".join(item["file"] for item in artifacts),
               elapsed_seconds="{0:.2f}".format(elapsed))
        first_artifact = artifacts[0]
        return {"channel": channel, "status": "SUCCESS", "count": delivery_count,
                "file": first_artifact["file"],
                "final_file_path": first_artifact["final_file_path"],
                "ftp_path": ftp_path, "s3_path": output_s3,
                "final_s3_path": output_s3,
                "complete_s3_path": context["path_COMPLETE"],
                "final_data_header": final_data_header,
                "complete_data_header": complete_data_header,
                "final_count": delivery_count,
                "complete_count": complete_export_count if complete_export_count >= 0 else "",
                "merge_source_request_id": request_data.get("merge_source_request_id") or "",
                "merge_source_request_name": request_data.get("merge_source_request_name") or "",
                "merge_mode": merge_mode, "artifacts": artifacts,
                "elapsed": elapsed}
    except Exception as exc:
        if table_created:
            try:
                _drop_perm_table(context["perm_table"], log)
            except Exception:
                log.exception("Unable to drop failed criteria table %s", context["perm_table"])
        update_request_status(request_id, "Failed", channel + "_STATUS", log)
        _trace(log, "consolidated channel failed", channel=channel, error=exc)
        log.exception("Consolidated channel failed: %s", channel)
        raise
    finally:
        shutil.rmtree(str(context["channel_tmp"]), ignore_errors=True)


def _set_request_overall_status(request_id, status, log):
    conn = get_db_with_retry(log)
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE requests SET overall_status=%s WHERE id=%s", (status, request_id))
        conn.commit()
    finally:
        conn.close()
    _trace(log, "overall status updated", request_id=request_id, overall_status=status)


def process_request(request_id, channel, output_dir=None, zip_file=None):
    """Run any non-DoorDash request through one consolidated processor."""
    request_data = _fetch_consolidated_request(request_id)
    if not request_data:
        raise RuntimeError("Request ID {0} was not found.".format(request_id))
    if request_data["request_type"] == "Doordash":
        raise RuntimeError("DoorDash requests must use Doordash/doordash_zips.py.")
    criteria = _criteria_from_request(request_data)

    requested_channels = [channel.upper()] if isinstance(channel, str) else [str(item).upper() for item in channel]
    invalid_channels = [item for item in requested_channels if item != "ALL" and item not in CHANNELS]
    if invalid_channels:
        raise ValueError("Unsupported channel(s): " + ", ".join(invalid_channels))
    channels_to_run = list(CHANNELS) if "ALL" in requested_channels else list(dict.fromkeys(requested_channels))
    if not channels_to_run:
        raise ValueError("At least one channel must be selected.")

    run_dir = Path(ensure_output_dir(output_dir or request_data["output_dir"], "criteria"))
    (run_dir / "FINAL_FILES").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    log = setup_processor_main_logging(run_dir)
    _trace(log, "consolidated request started", request_id=request_id,
           request_name=request_data["request_name"], request_type=request_data["request_type"],
           criteria_json=json.dumps(criteria, sort_keys=True),
           channels=",".join(channels_to_run), output_dir=run_dir)

    zip_staging_table = None
    try:
        zip_criteria = [item for item in criteria if item["type"] == "zips"]
        if zip_criteria:
            selected_zip_file = zip_file or zip_criteria[0].get("file_path") or request_data.get("zip_file_path")
            if not selected_zip_file:
                raise RuntimeError("ZIP criterion has no uploaded ZIP file.")
            _verify_local_file(log, "consolidated ZIP input", selected_zip_file)
            path_date = datetime.now().strftime("%Y%m%d")
            s3_zip = "{0}/{1}/ZIPS/{2}/{3}/staging/{4}".format(
                S3_BASE, request_data["request_type"], path_date,
                _safe_identifier(request_data["request_name"]), Path(selected_zip_file).name
            )
            _step(log, 1, 4, "Uploading and staging ZIP input", "REQUEST")
            run_command(["aws", "s3", "cp", selected_zip_file, s3_zip, "--quiet"])
            zip_staging_table = "APT_CPA_REQUEST_ZIPS_{0}_{1}".format(request_id, datetime.now().strftime("%H%M%S"))
            _create_zip_staging_table(zip_staging_table, log)
            radius = request_data.get("zip_radius")
            if radius is not None:
                expansion = expand_zip_radius(
                    s3_zip, zip_staging_table, int(radius), request_id, log
                )
                zip_count = expansion["expanded_count"]
                _trace(log, "ZIP radius expansion validated",
                       original_count=expansion["source_count"],
                       expanded_count=zip_count, radius_miles=radius,
                       expanded_s3_path=expansion["s3_path"])
            else:
                zip_count = _load_zips_from_s3(zip_staging_table, s3_zip, log)
            if zip_count <= 0:
                raise RuntimeError("ZIP criterion file contains no ZIP values.")
            _trace(log, "ZIP staging validated", staging_table=zip_staging_table,
                   zip_count=zip_count, s3_path=s3_zip)
        else:
            if request_data.get("zip_radius") is not None:
                raise ValueError("ZIP radius requires a ZIP criterion.")
            _trace(log, "ZIP staging skipped", reason="no ZIP criterion")

        for selected_channel in channels_to_run:
            update_request_status(request_id, "Queued", selected_channel + "_STATUS", log)

        _step(log, 2, 4, "Processing selected channels", "REQUEST")
        results = {}
        errors = []
        if len(channels_to_run) == 1:
            selected_channel = channels_to_run[0]
            try:
                results[selected_channel] = _process_criteria_channel(
                    request_data, criteria, zip_staging_table, run_dir, selected_channel
                )
            except Exception as exc:
                errors.append("{0}: {1}".format(selected_channel, exc))
        else:
            with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
                futures = {
                    executor.submit(_process_criteria_channel, request_data, criteria,
                                    zip_staging_table, run_dir, selected_channel): selected_channel
                    for selected_channel in channels_to_run
                }
                for future in as_completed(futures):
                    selected_channel = futures[future]
                    try:
                        results[selected_channel] = future.result()
                    except Exception as exc:
                        errors.append("{0}: {1}".format(selected_channel, exc))

        _step(log, 3, 4, "Finalising request status and notification", "REQUEST")
        if errors:
            _set_request_overall_status(request_id, "failed", log)
            message = "; ".join(errors)
            send_error_email(request_data, message, run_dir, results=results)
            raise RuntimeError("Request failed for channel(s): " + message)
        _set_request_overall_status(request_id, "completed", log)
        send_success_email(request_data, results, run_dir)
        _trace(log, "consolidated request completed", request_id=request_id,
               completed_channels=",".join(sorted(results.keys())),
               total_rows=sum(result.get("count", 0) for result in results.values()))
        return results
    except Exception as exc:
        _set_request_overall_status(request_id, "failed", log)
        _trace(log, "consolidated request failed", request_id=request_id, error=exc)
        raise
    finally:
        if zip_staging_table:
            try:
                _step(log, 4, 4, "Dropping ZIP staging table", "REQUEST")
                _drop_zip_staging_table(zip_staging_table, log)
            except Exception:
                log.exception("Unable to drop consolidated ZIP staging table")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run a consolidated Suppression/Mailing criteria request."
    )
    parser.add_argument("--request-id", required=True, type=int)
    parser.add_argument("--channel", required=True, action="append",
                        choices=["ALL", "GREEN", "BLUE", "ARCAMAX", "ORANGE"])
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--zip-file")
    cli_args = parser.parse_args()
    process_request(
        request_id=cli_args.request_id,
        channel=cli_args.channel,
        output_dir=cli_args.output_dir,
        zip_file=cli_args.zip_file,
    )
