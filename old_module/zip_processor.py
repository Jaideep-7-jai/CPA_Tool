"""Archived ZIP-only Suppression/Mailing processor.

The application uses REQUEST_PROCESSOR.process_request instead. These functions
are retained for reference and explicitly invoked legacy jobs; DoorDash still
uses the shared process_orange_zip helper in the active processor.
"""

import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from REQUEST_PROCESSOR.request_processor import (
    CHANNELS, S3_BASE, _build_common_context, _cleanup_channel_tmp,
    _count_file_lines, _create_zip_staging_table, _download_and_combine,
    _drop_perm_table, _drop_zip_staging_table, _export_complete_final_file,
    _insert_into_perm_table, _load_zips_from_s3, _post_to_ftp, _step,
    _success_result, _trace, _verify_local_file, fetch_request_details,
    merge_current_file, process_orange_zip, run_command, send_error_email,
    send_success_email, setup_channel_logging, setup_main_logging,
    update_channel_storage, update_ftp_path, update_request_status,
)


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
    Archived entry point; main.py calls process_request instead.

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


