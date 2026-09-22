#!/usr/bin/env python3
"""DoorDash ZIP workflow using the consolidated processor's shared helpers."""

import os
import time
import shutil
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import SNOWSQL_PASSPHRASE, AWS_KEY_ID, AWS_SECRET_KEY, S3_BASE
from utils import run_command, send_success_email, send_error_email
from MERGE_OUTPUT.merge_output import merge_doordash_file
from REQUEST_PROCESSOR.request_processor import (
    _build_common_context,
    _create_zip_staging_table,
    _download_and_combine,
    _drop_perm_table,
    _drop_zip_staging_table,
    _export_complete_final_file,
    _insert_into_perm_table,
    _load_zips_from_s3,
    _post_to_ftp,
    _query_snowflake,
    _step,
    _trace,
    _verify_local_file,
    fetch_request_details,
    process_orange_zip,
    setup_channel_logging,
    setup_main_logging,
    update_request_status,
    update_channel_storage,
)

CHANNELS = ["GREEN", "BLUE", "APPTNESS", "ARCAMAX", "ORANGE"]
COMBINED_CHANNELS = ["GREEN", "BLUE", "APPTNESS", "ARCAMAX"]


def _insert_apptness_into_perm_table(perm_table, zip_staging_table, comp_type, log):
    """Create APPTNESS perm table from APPT_RT_MAIL_REQUESTS_SF."""
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    kw = "IN" if comp_type == "include" else "NOT IN"
    _trace(log, "APPTNESS Snowflake load parameters", target_table=perm_table,
           staging_table=zip_staging_table, comparison=comp_type,
           sql_operator=kw, source_table="APPT_RT_MAIL_REQUESTS_SF")
    insert_sql = (
        f"CREATE OR REPLACE TABLE {perm_table} AS "
        f"SELECT EMAILID AS email, "
        f"PARSE_JSON(PROFILEDATAJSON):zipcode::VARCHAR AS ZIP "
        f"FROM APPT_RT_MAIL_REQUESTS_SF "
        f"WHERE PARSE_JSON(PROFILEDATAJSON):zipcode::VARCHAR {kw} "
        f"(SELECT zip_code FROM {zip_staging_table}) "
        f"AND EMAILID IS NOT NULL;"
    )
    log.info(f"  Target table     : {perm_table}")
    log.info(f"  ZIP staging table: {zip_staging_table}")
    log.info(f"  comp_type        : {comp_type} ({kw})")
    log.info("  APPTNESS source  : APPT_RT_MAIL_REQUESTS_SF")
    _trace(log, "APPTNESS ZIP condition resolved",
           condition=("PROFILEDATAJSON:zipcode " + kw + " staging ZIP values"))
    log.info(f"  INSERT SQL       : {insert_sql}")
    run_command(["snowsql", "-c", "datateam1", "-q", insert_sql])
    inserted_rows = _query_snowflake(f"SELECT COUNT(*) FROM {perm_table};", log)
    if inserted_rows >= 0:
        log.info(f"  Rows inserted into {perm_table}: {inserted_rows:,}")
    else:
        log.warning(f"  Could not verify row count for {perm_table} (non-fatal)")
    return inserted_rows


def insert_complete_extract(request_id, channel_name, zip_staging_table, run_dir: Path):
    """Create a non-Orange perm table and export its COMPLETE data to S3."""
    TOTAL_STEPS = 2
    channel_name = channel_name.upper()
    channel_status = f"{channel_name}_STATUS"
    log = setup_channel_logging(run_dir, channel_name)
    update_request_status(request_id, "Started", channel_status, log)

    ctx = _build_common_context(request_id, channel_name, run_dir)
    perm_table = ctx["perm_table"]
    log.info("=" * 70)
    log.info("  DOORDASH CHANNEL COMPLETE-EXTRACT STARTED")
    _trace(
        log, "channel input validation", request_id=request_id,
        channel=channel_name, request_type=ctx["request_type"],
        comparison=ctx["comp_type"], staging_table=zip_staging_table,
        target_table=perm_table, complete_s3_path=ctx["path_COMPLETE"],
        responder_match=bool(ctx["request_data"].get("responder_match")),
        responder_days=(ctx["request_data"].get("responder_days")
                        if ctx["request_data"].get("responder_match") else "not enabled"),
    )

    try:
        start_time = time.time()
        _step(log, 1, TOTAL_STEPS, "Creating Snowflake table + inserting ZIP-matched data", channel_name)
        update_request_status(request_id, "Loading to Snowflake", channel_status, log)
        if channel_name in ("GREEN", "BLUE", "ARCAMAX"):
            inserted_count = _insert_into_perm_table(
                perm_table, channel_name, zip_staging_table, ctx["comp_type"],
                ctx["request_data"].get("responder_match"),
                ctx["request_data"].get("responder_days"), log
            )
        elif channel_name == "APPTNESS":
            inserted_count = _insert_apptness_into_perm_table(
                perm_table, zip_staging_table, ctx["comp_type"], log
            )
        else:
            raise ValueError(f"Unsupported Doordash extract channel: {channel_name}")

        if inserted_count == 0:
            _trace(log, "no-data validation", channel=channel_name,
                   inserted_count=inserted_count, table=perm_table,
                   cleanup_action="drop permanent table")
            update_request_status(request_id, "No Data Retrieved", channel_status, log)
            _drop_perm_table(perm_table, log)
            return {
                "channel": channel_name, "status": "NO_DATA", "count": 0,
                "elapsed": time.time() - start_time, "perm_table": None,
            }

        _step(log, 2, TOTAL_STEPS, "Exporting COMPLETE DATA FILE (email + ZIP) to S3", channel_name)
        update_request_status(request_id, "Exporting Complete File", channel_status, log)
        _export_complete_final_file("COMPLETE", perm_table, ctx["path_COMPLETE"], channel_name, log)
        _trace(log, "complete export validated", channel=channel_name,
               source_table=perm_table, s3_path=ctx["path_COMPLETE"],
               inserted_count=inserted_count)
        update_channel_storage(request_id, channel_name, ctx["path_COMPLETE"], inserted_count, log)
        elapsed = time.time() - start_time
        update_request_status(request_id, "Complete Data Exported", channel_status, log)
        _trace(log, "channel complete extract finished", channel=channel_name,
               elapsed_seconds="{0:.2f}".format(elapsed),
               retained_table=perm_table,
               next_action="aggregate export or cleanup")
        return {
            "channel": channel_name, "status": "COMPLETE_EXPORTED",
            "count": inserted_count if inserted_count >= 0 else 0,
            "elapsed": elapsed, "perm_table": perm_table,
        }
    except Exception as exc:
        try:
            _drop_perm_table(perm_table, log)
        except Exception:
            log.exception(f"  Failed to clean up {perm_table} after extract failure")
        update_request_status(request_id, "Failed", channel_status, log)
        _trace(log, "channel complete extract failed", channel=channel_name,
               error=exc, cleanup_attempted=True)
        log.exception(f"  {channel_name} CHANNEL (DOORDASH ZIPS) FAILED")
        raise


def _create_combined_outputs(request_id, run_dir: Path, path_date, results, log):
    completed = [
        ch for ch in COMBINED_CHANNELS
        if results.get(ch, {}).get("status") == "COMPLETE_EXPORTED"
    ]
    _trace(log, "combined-output eligibility", request_id=request_id,
           completed_channels=",".join(completed) or "none",
           result_statuses=";".join(
               "{0}={1}".format(ch, results.get(ch, {}).get("status", "NOT_RUN"))
               for ch in COMBINED_CHANNELS
           ))
    if not completed:
        _trace(log, "combined-output skipped",
               reason="no completed non-ORANGE channel extracts")
        return []

    final_files_dir = run_dir / "FINAL_FILES"
    final_files_dir.mkdir(parents=True, exist_ok=True)
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise RuntimeError(
            "Cannot create DoorDash combined outputs: request {0} was not found."
            .format(request_id)
        )
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE
    client_name = request_data["client_name"]
    criteria_type = request_data["criteria_type"].title()
    request_type = request_data["request_type"]
    email_name = f"{client_name}_{criteria_type}_{request_type}_EMAIL_{path_date}.csv"
    md5_name = f"{client_name}_{criteria_type}_{request_type}_MD5HASH_{path_date}.csv"
    email_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_data['request_name']}/FINAL_EMAIL"
    md5_s3 = f"{S3_BASE}/Doordash/{path_date}/{request_data['request_name']}/FINAL_ARCAMAX_MD5"

    _trace(
        log, "combined-output plan", request_name=request_data["request_name"],
        email_filename=email_name, md5_filename=md5_name,
        email_s3_prefix=email_s3, md5_s3_prefix=md5_s3,
        merge_source_request_id=(request_data.get("merge_source_request_id") or "NULL"),
        source_tables=",".join(results[ch]["perm_table"] for ch in completed),
    )

    union_sql = " UNION ".join(
        f"SELECT TRIM(email) AS email FROM {results[ch]['perm_table']} WHERE email IS NOT NULL"
        for ch in completed
    )
    copy_email = (
        f"COPY INTO '{email_s3}/' FROM ({union_sql}) "
        f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
        f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
        f"HEADER=TRUE MAX_FILE_SIZE=490000000;"
    )
    arcamax_table = results.get("ARCAMAX", {}).get("perm_table")
    email_count = 0
    md5_count = 0
    try:
        _step(log, 3, 7, "Exporting combined DoorDash email file to S3", "DOORDASH")
        _trace(log, "combined email export starting", source_channel_count=len(completed),
               selected_channels=",".join(completed), destination=email_s3)
        run_command(["snowsql", "-c", "datateam1", "-q", copy_email])
        _trace(log, "combined email export completed", destination=email_s3)
        email_count = _download_and_combine(
            email_s3, run_dir / "EMAIL_FINAL_DL", run_dir / "EMAIL_FINAL_TMP",
            email_name, "GREEN", log,
        )
        _verify_local_file(log, "combined DoorDash email file",
                           run_dir / "EMAIL_FINAL_TMP" / email_name)
        _trace(log, "combined email download validated", email_count=email_count,
               local_file=run_dir / "EMAIL_FINAL_TMP" / email_name)
        if arcamax_table:
            _step(log, 4, 7, "Exporting DoorDash MD5 file from ARCAMAX", "DOORDASH")
            copy_md5 = (
                f"COPY INTO '{md5_s3}/' FROM (SELECT DISTINCT MD5(LOWER(TRIM(email))) AS md5hash "
                f"FROM {arcamax_table} WHERE email IS NOT NULL) "
                f"CREDENTIALS=(AWS_KEY_ID='{AWS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_KEY}') "
                f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
                f"HEADER=TRUE MAX_FILE_SIZE=490000000;"
            )
            _trace(log, "combined MD5 export starting", source_table=arcamax_table,
                   destination=md5_s3)
            run_command(["snowsql", "-c", "datateam1", "-q", copy_md5])
            _trace(log, "combined MD5 export completed", destination=md5_s3)
            md5_count = _download_and_combine(
                md5_s3, run_dir / "MD5_FINAL_DL", run_dir / "MD5_FINAL_TMP",
                md5_name, "GREEN", log,
            )
            _verify_local_file(log, "combined DoorDash MD5 file",
                               run_dir / "MD5_FINAL_TMP" / md5_name)
            _trace(log, "combined MD5 download validated", md5_count=md5_count,
                   local_file=run_dir / "MD5_FINAL_TMP" / md5_name)
        else:
            _trace(log, "MD5 output skipped",
                   reason="ARCAMAX was not completed/selected")
    except Exception:
        log.exception("  Doordash aggregate export validation failed; source tables retained")
        raise

    _step(log, 5, 7, "Dropping verified source tables", "DOORDASH")
    for ch in completed:
        _trace(log, "dropping verified source table", channel=ch,
               table=results[ch]["perm_table"])
        _drop_perm_table(results[ch]["perm_table"], log)

    email_dest = final_files_dir / email_name
    shutil.move(str(run_dir / "EMAIL_FINAL_TMP" / email_name), str(email_dest))
    _verify_local_file(log, "DoorDash final email file", email_dest)
    email_merge_mode = "CURRENT_ONLY"
    if request_data.get("merge_source_request_id"):
        _trace(log, "DoorDash email merge enabled", current_request_id=request_id,
               source_request_id=request_data["merge_source_request_id"])
        email_merge = merge_doordash_file(
            request_id, request_data["merge_source_request_id"], "EMAIL",
            email_dest, email_s3, run_dir, log,
        )
        email_count = email_merge["count"]
        email_s3 = email_merge["s3_path"]
        email_merge_mode = email_merge["merge_mode"]
        _verify_local_file(log, "merged DoorDash email file", email_dest)
        _trace(log, "DoorDash email merge completed", merge_mode=email_merge_mode,
               email_count=email_count, s3_path=email_s3)
    else:
        _trace(log, "DoorDash email merge skipped",
               reason="merge_source_request_id is NULL")
    email_ftp_path = _post_to_ftp(final_files_dir, path_date, email_name, log, request_type=request_type)
    _trace(log, "DoorDash email FTP validated", ftp_path=email_ftp_path,
           email_count=email_count, merge_mode=email_merge_mode)
    shutil.rmtree(str(run_dir / "EMAIL_FINAL_TMP"), ignore_errors=True)
    outputs = [{"channel": "DOORDASH_EMAIL", "file": email_name, "final_file_path": str(email_dest), "status": "SUCCESS", "count": email_count, "s3_path": email_s3, "ftp_path": email_ftp_path, "merge_mode": email_merge_mode}]

    if arcamax_table:
        md5_dest = final_files_dir / md5_name
        shutil.move(str(run_dir / "MD5_FINAL_TMP" / md5_name), str(md5_dest))
        _verify_local_file(log, "DoorDash final MD5 file", md5_dest)
        md5_merge_mode = "CURRENT_ONLY"
        if request_data.get("merge_source_request_id"):
            _trace(log, "DoorDash MD5 merge enabled", current_request_id=request_id,
                   source_request_id=request_data["merge_source_request_id"])
            md5_merge = merge_doordash_file(
                request_id, request_data["merge_source_request_id"], "MD5HASH",
                md5_dest, md5_s3, run_dir, log,
            )
            md5_count = md5_merge["count"]
            md5_s3 = md5_merge["s3_path"]
            md5_merge_mode = md5_merge["merge_mode"]
            _verify_local_file(log, "merged DoorDash MD5 file", md5_dest)
            _trace(log, "DoorDash MD5 merge completed", merge_mode=md5_merge_mode,
                   md5_count=md5_count, s3_path=md5_s3)
        else:
            _trace(log, "DoorDash MD5 merge skipped",
                   reason="merge_source_request_id is NULL")
        md5_ftp_path = _post_to_ftp(final_files_dir, path_date, md5_name, log, request_type=request_type)
        _trace(log, "DoorDash MD5 FTP validated", ftp_path=md5_ftp_path,
               md5_count=md5_count, merge_mode=md5_merge_mode)
        shutil.rmtree(str(run_dir / "MD5_FINAL_TMP"), ignore_errors=True)
        outputs.append({"channel": "DOORDASH_ARCAMAX_MD5", "file": md5_name, "final_file_path": str(md5_dest), "status": "SUCCESS", "count": md5_count, "s3_path": md5_s3, "ftp_path": md5_ftp_path, "merge_mode": md5_merge_mode})
    _trace(log, "combined-output generation completed", output_count=len(outputs),
           outputs=";".join(
               "{0}:{1}:{2}".format(item["channel"], item["count"], item["merge_mode"])
               for item in outputs
           ))
    return outputs


def process_doordash_zip_request(request_id: int, zip_file: str, channel, output_dir: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_doordash_zips_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log = setup_main_logging(run_dir)

    log.info("=" * 70)
    log.info("  DOORDASH ZIP REQUEST — ORCHESTRATOR STARTED")
    _trace(log, "entry parameters", request_id=request_id, zip_file=zip_file,
           requested_channel=channel, output_dir=output_dir, run_dir=run_dir)
    _verify_local_file(log, "uploaded DoorDash ZIP input", zip_file)

    if isinstance(channel, str):
        channel = [channel]
    requested_channels = [str(ch).upper() for ch in channel]
    invalid_channels = [ch for ch in requested_channels if ch != "ALL" and ch not in CHANNELS]
    if invalid_channels:
        raise ValueError(
            "Unsupported DoorDash channel(s): " + ", ".join(invalid_channels)
        )
    channels_to_run = (
        list(CHANNELS) if "ALL" in requested_channels
        else list(dict.fromkeys(requested_channels))
    )
    if not channels_to_run:
        raise ValueError("At least one supported channel must be selected for DoorDash.")
    request_data = fetch_request_details(request_id)
    if not request_data:
        raise RuntimeError(f"Request ID {request_id} was not found in requests")
    if request_data["request_type"] != "Doordash":
        raise RuntimeError(
            f"Request ID {request_id} is a {request_data['request_type']} request. "
            "Doordash jobs must use the ID from the requests table for a Doordash row."
        )

    _trace(
        log, "request validation passed", request_name=request_data["request_name"],
        request_type=request_data["request_type"], comparison=request_data["comp_type"],
        selected_channels=",".join(channels_to_run),
        responder_match=bool(request_data.get("responder_match")),
        responder_days=(request_data.get("responder_days")
                        if request_data.get("responder_match") else "not enabled"),
        merge_source_request_id=(request_data.get("merge_source_request_id") or "NULL"),
        execution_mode=("single-channel" if len(channels_to_run) == 1 else "parallel"),
    )

    path_date = datetime.now().strftime("%Y%m%d")
    s3_zip_dir = f"{S3_BASE}/Doordash/ZIPS/{path_date}/staging"
    s3_zip_path = f"{s3_zip_dir}/{os.path.basename(zip_file)}"
    _step(log, 1, 7, "Uploading DoorDash ZIP input to S3", "DOORDASH")
    run_command(["aws", "s3", "cp", zip_file, s3_zip_path, "--quiet"])
    _trace(log, "ZIP input upload completed", local_file=zip_file,
           local_size_bytes=Path(zip_file).stat().st_size, s3_path=s3_zip_path)
    zip_staging_table = f"APT_CPA_DOORDASH_ZIPS_STAGING_{ts}"
    _step(log, 2, 7, "Creating and loading shared DoorDash ZIP staging table", "DOORDASH")
    _create_zip_staging_table(zip_staging_table, log)
    zip_count = _load_zips_from_s3(zip_staging_table, s3_zip_path, log)
    _trace(log, "ZIP staging validation", staging_table=zip_staging_table,
           loaded_zip_count=zip_count, s3_path=s3_zip_path)
    if zip_count == 0:
        _trace(log, "ZIP staging rejected", staging_table=zip_staging_table,
               reason="zero ZIP values loaded")
        _drop_zip_staging_table(zip_staging_table, log)
        raise RuntimeError("ZIP staging table is empty — no ZIP codes were loaded from the file.")

    def _run_channel(ch):
        _trace(log, "dispatching channel", channel=ch,
               processor=("ORANGE_ZIP" if ch == "ORANGE" else "COMPLETE_EXTRACT"))
        if ch == "ORANGE":
            return process_orange_zip(request_id, zip_staging_table, run_dir)
        return insert_complete_extract(request_id, ch, zip_staging_table, run_dir)

    results = {}
    errors = []
    if len(channels_to_run) == 1:
        ch = channels_to_run[0]
        _trace(log, "channel execution mode", mode="single-channel", channel=ch)
        try:
            results[ch] = _run_channel(ch)
            _trace(log, "channel finished", channel=ch,
                   status=results[ch].get("status"),
                   count=results[ch].get("count"),
                   elapsed_seconds="{0:.2f}".format(results[ch].get("elapsed", 0)))
        except Exception as exc:
            errors.append((ch, str(exc)))
            log.exception("  Channel '%s' FAILED", ch)
    else:
        _trace(log, "channel execution mode", mode="parallel",
               worker_count=len(channels_to_run),
               channels=",".join(channels_to_run))
        with ThreadPoolExecutor(max_workers=len(channels_to_run)) as executor:
            future_to_ch = {executor.submit(_run_channel, ch): ch for ch in channels_to_run}
            for future in as_completed(future_to_ch):
                ch = future_to_ch[future]
                try:
                    results[ch] = future.result()
                    _trace(log, "channel finished", channel=ch,
                           status=results[ch].get("status"),
                           count=results[ch].get("count"),
                           elapsed_seconds="{0:.2f}".format(results[ch].get("elapsed", 0)))
                except Exception as exc:
                    errors.append((ch, str(exc)))
                    log.exception("  Channel '%s' FAILED", ch)

    combined_outputs = []
    try:
        _trace(log, "combined-output phase starting",
               completed_extract_channels=",".join(
                   ch for ch, result in results.items()
                   if result.get("status") == "COMPLETE_EXPORTED"
               ) or "none")
        combined_outputs = _create_combined_outputs(request_id, run_dir, path_date, results, log)
        email_output = next((item for item in combined_outputs if item["channel"] == "DOORDASH_EMAIL"), None)
        md5_output = next((item for item in combined_outputs if item["channel"] == "DOORDASH_ARCAMAX_MD5"), None)
        if email_output or md5_output:
            from REQUEST_PROCESSOR.request_processor import get_db_with_retry
            conn = get_db_with_retry(log)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE requests SET DOORDASH_EMAIL_FTP=%s, DOORDASH_EMAIL_FILECOUNT=%s, DOORDASH_EMAIL_FILEPATH=%s, DOORDASH_EMAIL_MERGE_STATUS=%s, DOORDASH_MD5HASH_FTP=%s, DOORDASH_MD5HASH_FILECOUNT=%s, DOORDASH_MD5HASH_FILEPATH=%s, DOORDASH_MD5HASH_MERGE_STATUS=%s WHERE id=%s",
                        (
                            email_output.get("ftp_path") if email_output else None,
                            email_output.get("count") if email_output else None,
                            email_output.get("s3_path") if email_output else None,
                            email_output.get("merge_mode") if email_output else None,
                            md5_output.get("ftp_path") if md5_output else None,
                            md5_output.get("count") if md5_output else None,
                            md5_output.get("s3_path") if md5_output else None,
                            md5_output.get("merge_mode") if md5_output else None,
                            request_id,
                        ),
                    )
                conn.commit()
                _trace(log, "DoorDash output storage updated", request_id=request_id,
                       email_file=(email_output.get("file") if email_output else "not created"),
                       email_count=(email_output.get("count") if email_output else 0),
                       md5_file=(md5_output.get("file") if md5_output else "not created"),
                       md5_count=(md5_output.get("count") if md5_output else 0))
            finally:
                conn.close()
        for ch in COMBINED_CHANNELS:
            if results.get(ch, {}).get("status") == "COMPLETE_EXPORTED":
                update_request_status(request_id, "Completed", f"{ch}_STATUS", log)
                _trace(log, "channel status finalized", channel=ch,
                       status="Completed")
    except Exception as exc:
        errors.append(("COMBINED", str(exc)))
        log.exception("  Combined Doordash output generation failed")

    try:
        _step(log, 6, 7, "Dropping shared DoorDash ZIP staging table", "DOORDASH")
        _drop_zip_staging_table(zip_staging_table, log)
        _trace(log, "ZIP staging cleanup completed", staging_table=zip_staging_table)
    except Exception as exc:
        log.warning(f"  Failed to drop ZIP staging table (non-fatal): {exc}")

    if errors:
        error_summary = "\n".join(f"{channel_name}: {error}" for channel_name, error in errors)
        _trace(log, "DoorDash request failed", error_count=len(errors),
               failed_components=",".join(channel_name for channel_name, _ in errors))
        send_error_email(request_data, error_summary, run_dir)
        raise RuntimeError(f"Doordash request failed:\n{error_summary}")

    notification_results = {
        **results,
        **{output["channel"]: output for output in combined_outputs},
    }
    _step(log, 7, 7, "Sending success notification", "DOORDASH")
    _trace(log, "DoorDash request success validation",
           channel_results=",".join(sorted(results.keys())) or "none",
           final_outputs=",".join(output["channel"] for output in combined_outputs) or "none")
    send_success_email(request_data, notification_results, run_dir)
    log.info("  DOORDASH ZIP REQUEST — ORCHESTRATOR COMPLETED SUCCESSFULLY")
    log.info("=" * 70)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: doordash_zips.py <request_id> <zip_file> <channel> [output_dir]")
        sys.exit(1)
    process_doordash_zip_request(
        request_id=int(sys.argv[1]),
        zip_file=sys.argv[2],
        channel=sys.argv[3],
        output_dir=sys.argv[4] if len(sys.argv) > 4 else ".",
    )
