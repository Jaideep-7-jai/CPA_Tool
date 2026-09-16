#!/usr/bin/env python3
"""Run Age, State, and ZIP criteria with OR matching for one CPA request."""

import json
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import ensure_output_dir, run_command, send_error_email, send_success_email
from MERGE_OUTPUT.merge_output import merge_current_file
from AGE_STATE.age_state import (
    get_dob_cutoff,
    setup_channel_logging,
    setup_main_logging,
    update_request_status,
)
from ZIPS.zips import (
    _create_zip_staging_table,
    _download_and_combine,
    _drop_perm_table,
    _drop_zip_staging_table,
    _export_complete_final_file,
    _load_zips_from_s3,
    _post_to_ftp,
    _query_snowflake,
    update_channel_storage,
    update_ftp_path,
    get_db_with_retry,
)


CHANNELS = ("GREEN", "BLUE", "ARCAMAX", "ORANGE")


def _safe_name(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", str(value or ""))


def _fetch_request(request_id):
    conn = get_db_with_retry()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.request_name, r.client_name, r.request_type,
                       r.channel, r.output_dir, r.criteria_json,
                       r.merge_source_request_id, r.responder_match,
                       r.responder_days, u.username
                FROM requests r
                JOIN users u ON u.id = r.created_by
                WHERE r.id=%s
                """,
                (request_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0], "request_name": row[1], "client_name": row[2],
                "request_type": row[3], "channel": row[4], "output_dir": row[5],
                "criteria_json": row[6], "merge_source_request_id": row[7],
                "responder_match": bool(row[8]), "responder_days": row[9],
                "created_by_username": row[10], "criteria_type": "multi",
                "criteria_value": "Multiple criteria (OR)", "comp_type": "include",
            }
    finally:
        conn.close()


def _fetch_previous_path(previous_request_id, channel):
    conn = get_db_with_retry()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT request_type, overall_status, {0}_FILEPATH FROM requests WHERE id=%s".format(channel),
                (previous_request_id,),
            )
            row = cur.fetchone()
            if not row or row[1] != "completed":
                raise RuntimeError("Previous request must exist and be completed.")
            if not row[2]:
                raise RuntimeError("Previous request has no {0} S3 final path.".format(channel))
            return row[2]
    finally:
        conn.close()


def _criteria_conditions(channel, criteria, zip_staging_table):
    """Build OR predicates using each channel's existing column conventions."""
    if channel in ("GREEN", "BLUE"):
        age_col, state_col, zip_col = "b.AGE", "b.STATE", "b.ZIP"
    elif channel == "ARCAMAX":
        age_col, state_col, zip_col = "birthday", "STATE", "ZIP"
    else:
        age_col, state_col, zip_col = "a.dob", "a.STATE", "a.ZIP"

    conditions = []
    for item in criteria:
        kind = (item.get("type") or "").lower()
        comparison = (item.get("comparison") or "include").lower()

        if kind == "age":
            if comparison == "between":
                low, high = int(item["from"]), int(item["to"])
                if low > high:
                    low, high = high, low
                if channel in ("GREEN", "BLUE"):
                    conditions.append("{0} BETWEEN {1} AND {2}".format(age_col, low, high))
                else:
                    older_date = get_dob_cutoff(high, "greater")
                    younger_date = get_dob_cutoff(low, "greater")
                    conditions.append(
                        "TRY_TO_DATE({0}) BETWEEN '{1}' AND '{2}'".format(age_col, older_date, younger_date)
                    )
            else:
                value = int(item["value"])
                if channel in ("GREEN", "BLUE"):
                    op = ">" if comparison == "greater" else "<"
                    conditions.append("{0} {1} {2}".format(age_col, op, value))
                else:
                    cutoff = get_dob_cutoff(value, "greater" if comparison == "greater" else "less")
                    op = "<=" if comparison == "greater" else ">="
                    conditions.append("TRY_TO_DATE({0}) {1} '{2}'".format(age_col, op, cutoff))

        elif kind == "state":
            states = [str(v).replace("'", "''").strip().upper() for v in item.get("values", []) if str(v).strip()]
            if states:
                keyword = "IN" if comparison == "include" else "NOT IN"
                conditions.append("{0} {1} ({2})".format(age_col.replace(age_col, state_col), keyword, ",".join("'{0}'".format(v) for v in states)))

        elif kind == "zips" and zip_staging_table:
            keyword = "IN" if comparison == "include" else "NOT IN"
            conditions.append("{0} {1} (SELECT zip_code FROM {2})".format(zip_col, keyword, zip_staging_table))

    if not conditions:
        raise ValueError("At least one valid Age, State, or ZIP criterion is required.")
    return "(" + " OR ".join(conditions) + ")"


def _responder_join(channel, responder_match, responder_days):
    """Return the optional, deduplicated responder join for one channel."""
    if not responder_match or channel not in ("GREEN", "BLUE", "ORANGE"):
        return ""
    days = int(responder_days or 0)
    if days < 1:
        raise ValueError("Responder Match requires responder_days to be at least 1.")
    if channel in ("GREEN", "BLUE"):
        return (
            "JOIN (SELECT DISTINCT LOWER(TRIM(emailid)) AS email "
            "FROM GREEN.GREEN_LPT.RAW_OPENS_FOLLOWUP "
            "WHERE opendate >= DATEADD(day, -{0}, CURRENT_DATE())) responders "
            "ON LOWER(TRIM(a.email)) = responders.email ".format(days)
        )
    return (
        "JOIN (SELECT DISTINCT LOWER(TRIM(email)) AS email "
        "FROM GREEN.DT_DATA.APT_CUSTOM_L90_ORANGE_UNIQ_RESPONDERS_UNIQ_DND "
        "WHERE OPEN_DATE >= DATEADD(day, -{0}, CURRENT_DATE())) responders "
        "ON LOWER(TRIM(a.email_address)) = responders.email ".format(days)
    )


def _create_channel_table(perm_table, channel, criteria, zip_staging_table,
                          responder_match, responder_days, log):
    condition = _criteria_conditions(channel, criteria, zip_staging_table)
    responder_join = _responder_join(channel, responder_match, responder_days)
    if channel in ("GREEN", "BLUE"):
        profile_table = "GREEN_LPT.UNIVERSAL_PROFILE" if channel == "GREEN" else "INFS_LPT.INFS_PROFILE"
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT a.email, b.ZIP FROM {profile} a "
            "JOIN APT_CUSTOM_GREEN_REA_DATA_DND b ON a.md5hash=b.EMAIL_MD5 "
            "{responder_join}"
            "WHERE {condition};"
        ).format(perm=perm_table, profile=profile_table, responder_join=responder_join, condition=condition)
    elif channel == "ARCAMAX":
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT email, ZIP FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE "
            "WHERE {condition};"
        ).format(perm=perm_table, condition=condition)
    else:
        sql = (
            "CREATE OR REPLACE TABLE {perm} AS "
            "SELECT a.email_address, a.ZIP, esp.ACCOUNT_NAME "
            "FROM APT_CUSTOM_ORANGE_TRANSACTION_DND a "
            "JOIN APT_ADHOC_JAIDEEP_ZIP_ESP_DETAILS_INCLUDE_ORANGE_20260604 esp "
            "ON a.FEED_ID=esp.FEEDID "
            "JOIN APT_CUSTOM_ORANGE_PROFILE_EMAIL_DND p ON a.email_address=p.email_address "
            "{responder_join}"
            "WHERE {condition} "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY a.email_address ORDER BY a.created_at DESC)=1;"
        ).format(perm=perm_table, responder_join=responder_join, condition=condition)

    log.info("Multi-criteria OR condition: %s", condition)
    run_command(["snowsql", "-c", "datateam1", "-q", sql])
    return _query_snowflake("SELECT COUNT(*) FROM {0}".format(perm_table), log)


def _merge_csv(new_file, previous_file, channel):
    """Merge same-channel final files, keeping one case-insensitive email record."""
    if channel == "ORANGE":
        new_df = pd.read_csv(str(new_file), sep="|", dtype=str).fillna("")
        old_df = pd.read_csv(str(previous_file), sep="|", dtype=str).fillna("")
        email_col = "email_address" if "email_address" in new_df.columns else new_df.columns[0]
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged["_dedupe"] = merged[email_col].str.strip().str.lower()
        merged = merged.drop_duplicates("_dedupe").drop(columns=["_dedupe"])
    else:
        new_df = pd.read_csv(str(new_file), sep="|", dtype=str).fillna("")
        old_df = pd.read_csv(str(previous_file), sep="|", dtype=str).fillna("")
        email_col = "email" if "email" in new_df.columns else new_df.columns[0]
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged["_dedupe"] = merged[email_col].str.strip().str.lower()
        merged = merged.drop_duplicates("_dedupe")[[email_col]]
    merged.to_csv(str(new_file), sep="|", index=False)
    return len(merged)


def _orange_mailing_zip(csv_file, final_files_dir):
    data = pd.read_csv(str(csv_file), sep="|", dtype=str).fillna("")
    email_col = "email_address" if "email_address" in data.columns else data.columns[0]
    account_col = "account_name" if "account_name" in data.columns else data.columns[1]
    esp_dir = final_files_dir / "ORANGE_ESP"
    esp_dir.mkdir(exist_ok=True)
    for account, group in data.groupby(account_col):
        safe_account = _safe_name(account) or "UNKNOWN_ESP"
        group[[email_col]].drop_duplicates().to_csv(
            str(esp_dir / (safe_account + "_ORANGE_DATA.csv")), index=False, header=False
        )
    zip_name = csv_file.stem + ".zip"
    run_command(["zip", "-r", str(final_files_dir / zip_name), esp_dir.name], cwd=str(final_files_dir))
    return zip_name


def _process_channel(request_data, criteria, zip_staging_table, run_dir, channel):
    log = setup_channel_logging(run_dir, channel)
    request_id = request_data["id"]
    update_request_status(request_id, "Started", channel + "_STATUS", log)
    date_value = datetime.now().strftime("%Y%m%d")
    final_dir = run_dir / "FINAL_FILES"
    temp_dir = run_dir / (channel + "_MULTI_TMP")
    temp_dir.mkdir(parents=True, exist_ok=True)
    basename = "{0}_Multi_{1}_{2}_{3}.csv".format(
        _safe_name(request_data["client_name"]), request_data["request_type"], channel, date_value
    )
    perm_table = "APT_CPA_MULTI_{0}_{1}_{2}".format(channel, request_id, date_value)
    s3_final = "{0}/{1}/{2}/{3}/{4}_FINAL".format(
        S3_BASE, request_data["request_type"], date_value, request_data["request_name"], channel
    )
    s3_complete = s3_final.replace("_FINAL", "_COMPLETE")
    started = time.time()
    try:
        count = _create_channel_table(
            perm_table, channel, criteria, zip_staging_table,
            request_data.get("responder_match"), request_data.get("responder_days"), log
        )
        if count == 0:
            update_request_status(request_id, "No Data Retrieved", channel + "_STATUS", log)
            _drop_perm_table(perm_table, log)
            return {"channel": channel, "status": "NO_DATA", "count": 0, "file": None}
        _export_complete_final_file("FINAL", perm_table, s3_final, channel, log)
        _export_complete_final_file("COMPLETE", perm_table, s3_complete, channel, log)
        _drop_perm_table(perm_table, log)
        new_file = temp_dir / basename
        _download_and_combine(s3_final, temp_dir / "download", temp_dir, basename, channel, log)
        if request_data.get("merge_source_request_id"):
            merge_result = merge_current_file(
                request_id, request_data["merge_source_request_id"], channel, new_file,
                s3_final, temp_dir, log, orange=(channel == "ORANGE")
            )
            count = merge_result["count"]
            merge_mode = merge_result["merge_mode"]
            output_s3 = merge_result["s3_path"]
        else:
            count = len(pd.read_csv(str(new_file), sep="|", dtype=str))
            merge_mode = "CURRENT_ONLY"
            output_s3 = s3_final
        final_file = final_dir / basename
        shutil.move(str(new_file), str(final_file))
        upload_file = basename
        if channel == "ORANGE" and request_data["request_type"] == "Mailing":
            upload_file = _orange_mailing_zip(final_file, final_dir)
        if merge_mode == "CURRENT_ONLY":
            update_channel_storage(request_id, channel, output_s3, count, log)
        ftp_path = _post_to_ftp(final_dir, date_value, upload_file, log)
        update_ftp_path(request_id, channel, ftp_path, log, count)
        update_request_status(request_id, "Completed", channel + "_STATUS", log)
        return {
            "channel": channel, "status": "SUCCESS", "file": upload_file,
            "final_file_path": str(final_dir / upload_file), "ftp_path": ftp_path,
            "s3_path": output_s3, "count": count, "merge_mode": merge_mode,
            "elapsed": time.time() - started,
        }
    except Exception:
        update_request_status(request_id, "Failed", channel + "_STATUS", log)
        log.exception("Multi-criteria channel failed: %s", channel)
        raise
    finally:
        if temp_dir.exists():
            shutil.rmtree(str(temp_dir), ignore_errors=True)


def process_multi_criteria_request(request_id, channel, output_dir=None):
    request_data = _fetch_request(request_id)
    if not request_data:
        raise RuntimeError("Request ID {0} was not found.".format(request_id))
    criteria = json.loads(request_data.get("criteria_json") or "[]")
    if not criteria:
        raise RuntimeError("No multi-criteria definition was saved for this request.")
    channels = list(CHANNELS) if "ALL" in channel else [c.upper() for c in channel if c.upper() in CHANNELS]
    run_dir = Path(ensure_output_dir(output_dir or request_data["output_dir"], "multi"))
    (run_dir / "FINAL_FILES").mkdir(parents=True, exist_ok=True)
    log = setup_main_logging(run_dir, "multi")
    zip_items = [item for item in criteria if item.get("type") == "zips"]
    zip_staging_table = None
    try:
        if zip_items:
            zip_file = zip_items[0].get("file_path")
            if not zip_file:
                raise RuntimeError("ZIP criterion has no uploaded file.")
            date_value = datetime.now().strftime("%Y%m%d")
            s3_zip = "{0}/MULTI/ZIPS/{1}/staging/{2}".format(S3_BASE, date_value, Path(zip_file).name)
            run_command(["aws", "s3", "cp", zip_file, s3_zip, "--quiet"])
            zip_staging_table = "APT_CPA_MULTI_ZIPS_{0}_{1}".format(request_id, date_value)
            _create_zip_staging_table(zip_staging_table, log)
            if _load_zips_from_s3(zip_staging_table, s3_zip, log) == 0:
                raise RuntimeError("ZIP criterion file contains no ZIP codes.")
        results, errors = {}, []
        with ThreadPoolExecutor(max_workers=len(channels)) as executor:
            futures = {executor.submit(_process_channel, request_data, criteria, zip_staging_table, run_dir, ch): ch for ch in channels}
            for future in as_completed(futures):
                ch = futures[future]
                try:
                    results[ch] = future.result()
                except Exception as exc:
                    errors.append("{0}: {1}".format(ch, exc))
        if errors:
            raise RuntimeError("Multi-criteria request failed for channel(s): " + "; ".join(errors))
        send_success_email(request_data, results, run_dir)
        return results
    except Exception as exc:
        send_error_email(request_data, str(exc), run_dir)
        raise
    finally:
        if zip_staging_table:
            try:
                _drop_zip_staging_table(zip_staging_table, log)
            except Exception:
                log.exception("Failed to drop multi-criteria ZIP staging table")
