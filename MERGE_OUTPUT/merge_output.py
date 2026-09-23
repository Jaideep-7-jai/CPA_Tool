"""Shared, Snowflake-based post-processing for previous-output merges.

Large CPA files must never be loaded into pandas. This module stages the
previous and current S3 exports in one SnowSQL session, deduplicates there,
and streams the resulting export back to the local delivery file.

The source request is always read-only. A selected current channel/artifact
without a compatible completed source file remains a valid current-only
output.
"""

import csv
import gzip
import os
import re
import shutil
import uuid
from pathlib import Path

from config import AWS_KEY_ID, AWS_SECRET_KEY, SNOWSQL_PASSPHRASE
from utils import run_command


_CHANNELS = {"GREEN", "BLUE", "ARCAMAX", "ORANGE", "APPTNESS"}
_DOORDASH_ARTIFACTS = {
    "EMAIL": ("DOORDASH_EMAIL_FILEPATH", "DOORDASH_EMAIL_MERGE_STATUS"),
    "MD5HASH": ("DOORDASH_MD5HASH_FILEPATH", "DOORDASH_MD5HASH_MERGE_STATUS"),
}


def _trace(log, event, **values):
    """Write a compact, credential-safe merge diagnostic entry."""
    if not log:
        return
    details = " | ".join(
        "{0}={1}".format(key, value) for key, value in values.items()
    )
    log.info("  MERGE TRACE | %s%s", event, " | " + details if details else "")


def _verify_local_file(log, label, file_path, require_content=True):
    """Validate a local merge artifact before it is used or returned."""
    path = Path(file_path)
    exists = path.is_file()
    size = path.stat().st_size if exists else -1
    _trace(log, "file validation", label=label, path=path,
           exists=exists, size_bytes=size)
    if not exists:
        raise RuntimeError("Required {0} was not created: {1}".format(label, path))
    if require_content and size <= 0:
        raise RuntimeError("Required {0} is empty: {1}".format(label, path))
    return size


def _redact_sql(sql):
    """Make verbose Snowflake debug SQL safe to persist in the job log."""
    sql = re.sub(r"AWS_KEY_ID='[^']*'", "AWS_KEY_ID='***'", sql)
    return re.sub(r"AWS_SECRET_KEY='[^']*'", "AWS_SECRET_KEY='***'", sql)


def _db_connection(log=None):
    """Import lazily so the consolidated processor can use merge safely."""
    from REQUEST_PROCESSOR.request_processor import get_db_with_retry
    _trace(log, "opening database connection for merge metadata")
    return get_db_with_retry(log)


def _previous_path(previous_request_id, channel, log=None):
    """Return a completed previous per-channel S3 path, if one exists."""
    if channel not in _CHANNELS:
        _trace(log, "previous channel lookup skipped", channel=channel,
               reason="unsupported channel")
        return None
    _trace(log, "previous channel lookup starting", previous_request_id=previous_request_id,
           channel=channel)
    conn = _db_connection(log)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT overall_status, {0}_FILEPATH FROM requests WHERE id=%s".format(channel),
                (previous_request_id,),
            )
            row = cur.fetchone()
            previous_s3 = row[1] if row and row[0] == "completed" and row[1] else None
            _trace(log, "previous channel lookup completed",
                   previous_request_id=previous_request_id, channel=channel,
                   request_found=bool(row),
                   overall_status=(row[0] if row else "not found"),
                   source_s3_path=(previous_s3 or "not available"))
            return previous_s3
    finally:
        conn.close()


def _previous_doordash_path(previous_request_id, artifact, log=None):
    """Return a completed previous DoorDash Email/MD5 S3 path, if one exists."""
    path_column, _ = _DOORDASH_ARTIFACTS[artifact]
    _trace(log, "previous DoorDash artifact lookup starting",
           previous_request_id=previous_request_id, artifact=artifact,
           database_column=path_column)
    conn = _db_connection(log)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT request_type, overall_status, {0} FROM requests WHERE id=%s".format(path_column),
                (previous_request_id,),
            )
            row = cur.fetchone()
            if row and str(row[0]).lower() != "doordash":
                _trace(log, "previous DoorDash artifact rejected",
                       previous_request_id=previous_request_id,
                       artifact=artifact, request_type=row[0],
                       reason="previous request is not DoorDash")
                raise RuntimeError(
                    "DoorDash output can be merged only with a completed DoorDash request."
                )
            if row and row[1] != "completed":
                _trace(log, "previous DoorDash artifact rejected",
                       previous_request_id=previous_request_id,
                       artifact=artifact, request_type=row[0],
                       overall_status=row[1],
                       reason="previous DoorDash request is not completed")
                raise RuntimeError(
                    "DoorDash output can be merged only with a completed DoorDash request."
                )
            previous_s3 = (
                row[2]
                if row and row[2]
                else None
            )
            _trace(log, "previous DoorDash artifact lookup completed",
                   previous_request_id=previous_request_id, artifact=artifact,
                   request_found=bool(row),
                   request_type=(row[0] if row else "not found"),
                   overall_status=(row[1] if row else "not found"),
                   source_s3_path=(previous_s3 or "not available"))
            return previous_s3
    finally:
        conn.close()


def _set_merge_status(request_id, channel, status, log=None):
    _trace(log, "merge status update starting", request_id=request_id,
           channel=channel, merge_status=status)
    conn = _db_connection(log)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET {0}_MERGE_STATUS=%s WHERE id=%s".format(channel),
                (status, request_id),
            )
        conn.commit()
        _trace(log, "merge status update completed", request_id=request_id,
               channel=channel, merge_status=status)
    finally:
        conn.close()


def _set_doordash_storage(request_id, artifact, s3_path, merge_status, log=None):
    path_column, status_column = _DOORDASH_ARTIFACTS[artifact]
    _trace(log, "DoorDash merge storage update starting", request_id=request_id,
           artifact=artifact, s3_path=s3_path, merge_status=merge_status,
           path_column=path_column, status_column=status_column)
    conn = _db_connection(log)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET {0}=%s, {1}=%s WHERE id=%s".format(
                    path_column, status_column
                ),
                (s3_path, merge_status, request_id),
            )
        conn.commit()
        _trace(log, "DoorDash merge storage update completed", request_id=request_id,
               artifact=artifact, s3_path=s3_path, merge_status=merge_status)
    finally:
        conn.close()


def _safe_identifier(value):
    """Return a valid, bounded Snowflake identifier component."""
    safe_value = re.sub(r"[^A-Za-z0-9_]", "_", str(value or "").upper())
    return safe_value[:80] or "MERGE"


def _sql_literal(value):
    """Quote a value used only as a Snowflake SQL string literal."""
    return str(value).replace("'", "''")


def _s3_prefix(value):
    return str(value or "").rstrip("/")


def _row_count(file_path, log=None):
    """Count data rows in a headered pipe-delimited file without pandas."""
    _verify_local_file(log, "merge row-count input", file_path)
    count = 0
    with open(str(file_path), "r", newline="") as source:
        reader = csv.reader(source, delimiter="|")
        next(reader, None)
        for row in reader:
            if row:
                count += 1
    _trace(log, "streamed row count completed", file_path=file_path, row_count=count)
    return count


def _snowflake_merge(previous_s3, current_s3, merged_s3, channel, request_id,
                     log, preserve_gender=False):
    """Merge two S3 exports in Snowflake and write a deduplicated export.

    The whole script must be one SnowSQL call: temporary stages and the
    temporary table exist only for that SnowSQL session.
    """
    os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = SNOWSQL_PASSPHRASE

    merge_token = "{0}_{1}_{2}".format(
        _safe_identifier(channel), request_id, uuid.uuid4().hex[:12].upper()
    )
    previous_stage = "CPA_MERGE_PREVIOUS_{0}".format(merge_token)
    current_stage = "CPA_MERGE_CURRENT_{0}".format(merge_token)
    merge_table = "CPA_MERGE_DATA_{0}".format(merge_token)
    if channel == "ORANGE":
        output_columns = "email, account_name" + (", gender" if preserve_gender else "")
        staged_previous = "SELECT $1::VARCHAR, $2::VARCHAR, {0}, 1".format(
            "$3::VARCHAR" if preserve_gender else "NULL::VARCHAR"
        )
        staged_current = "SELECT $1::VARCHAR, $2::VARCHAR, {0}, 2".format(
            "$3::VARCHAR" if preserve_gender else "NULL::VARCHAR"
        )
    else:
        output_columns = "email" + (", gender" if preserve_gender else "")
        staged_previous = "SELECT $1::VARCHAR, NULL::VARCHAR, {0}, 1".format(
            "$2::VARCHAR" if preserve_gender else "NULL::VARCHAR"
        )
        staged_current = "SELECT $1::VARCHAR, NULL::VARCHAR, {0}, 2".format(
            "$2::VARCHAR" if preserve_gender else "NULL::VARCHAR"
        )
    _trace(
        log, "Snowflake merge plan", request_id=request_id, channel=channel,
        previous_s3=_s3_prefix(previous_s3), current_s3=_s3_prefix(current_s3),
        merged_s3=_s3_prefix(merged_s3), output_columns=output_columns,
        dedupe_rule="LOWER(TRIM(email)); previous source wins",
        preserve_gender=preserve_gender,
        previous_stage=previous_stage, current_stage=current_stage,
        temporary_table=merge_table,
    )

    load_format = (
        "FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=1 "
        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' COMPRESSION=AUTO "
        "NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE)"
    )
    unload_format = (
        "FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_DELIMITER='|' "
        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=() "
        "EMPTY_FIELD_AS_NULL=FALSE)"
    )
    credentials = "CREDENTIALS=(AWS_KEY_ID='{0}' AWS_SECRET_KEY='{1}')".format(
        _sql_literal(AWS_KEY_ID), _sql_literal(AWS_SECRET_KEY)
    )

    sql_script = """
CREATE OR REPLACE TEMPORARY STAGE {previous_stage}
  URL='{previous_url}' {credentials};
CREATE OR REPLACE TEMPORARY STAGE {current_stage}
  URL='{current_url}' {credentials};
CREATE OR REPLACE TEMPORARY TABLE {merge_table} (
  email VARCHAR,
  account_name VARCHAR,
  gender VARCHAR,
  source_order NUMBER
);
COPY INTO {merge_table} (email, account_name, gender, source_order)
FROM (
  {staged_previous}
  FROM @{previous_stage}
)
{load_format}
ON_ERROR='ABORT_STATEMENT';
COPY INTO {merge_table} (email, account_name, gender, source_order)
FROM (
  {staged_current}
  FROM @{current_stage}
)
{load_format}
ON_ERROR='ABORT_STATEMENT';
COPY INTO '{merged_url}'
FROM (
  SELECT {output_columns}
  FROM (
    SELECT email,
           account_name,
           gender,
           ROW_NUMBER() OVER (
             PARTITION BY LOWER(TRIM(COALESCE(email, '')))
             ORDER BY source_order
           ) AS row_number
    FROM {merge_table}
  )
  WHERE row_number = 1
)
{credentials}
{unload_format}
HEADER=TRUE
OVERWRITE=TRUE
MAX_FILE_SIZE=490000000;
""".format(
        previous_stage=previous_stage,
        current_stage=current_stage,
        merge_table=merge_table,
        previous_url=_sql_literal(_s3_prefix(previous_s3) + "/"),
        current_url=_sql_literal(_s3_prefix(current_s3) + "/"),
        merged_url=_sql_literal(_s3_prefix(merged_s3) + "/"),
        credentials=credentials,
        load_format=load_format,
        unload_format=unload_format,
        output_columns=output_columns,
        staged_previous=staged_previous,
        staged_current=staged_current,
    )

    log.info(
        "Snowflake merge started for %s: previous=%s, current=%s, output=%s",
        channel, previous_s3, current_s3, merged_s3,
    )
    log.info("Snowflake merge SQL (credentials redacted):\n%s", _redact_sql(sql_script))
    run_command(["snowsql", "-c", "datateam1", "-q", sql_script])
    log.info("Snowflake merge completed for %s", channel)
    _trace(log, "Snowflake merge command completed", channel=channel,
           merged_s3=_s3_prefix(merged_s3),
           next_action="stream merged export to local delivery file")


def _open_export_file(path):
    """Open either a gzip Snowflake part or a plain CSV output file."""
    if path.suffix.lower() == ".gz":
        return gzip.open(str(path), "rt", newline="")
    return open(str(path), "r", newline="")


def _download_merged_export(merged_s3, current_file, channel, work_dir, log,
                            preserve_gender=False):
    """Stream merged S3 parts into the final local file without pandas."""
    current_file = Path(current_file)
    work_dir = Path(work_dir)
    download_dir = work_dir / ("merged_" + current_file.stem + "_download")
    temporary_file = current_file.with_name(current_file.name + ".merge_tmp")

    shutil.rmtree(str(download_dir), ignore_errors=True)
    download_dir.mkdir(parents=True, exist_ok=True)
    current_file.parent.mkdir(parents=True, exist_ok=True)
    _trace(log, "merged export download starting", channel=channel,
           merged_s3=_s3_prefix(merged_s3), download_dir=download_dir,
           temporary_file=temporary_file, destination_file=current_file)
    try:
        run_command([
            "aws", "s3", "cp", _s3_prefix(merged_s3) + "/", str(download_dir),
            "--recursive", "--quiet",
        ])
        source_files = sorted(path for path in download_dir.rglob("*") if path.is_file())
        if not source_files:
            raise RuntimeError("No merged files were downloaded from {0}".format(merged_s3))
        _trace(log, "merged export parts downloaded", channel=channel,
               file_count=len(source_files),
               part_names=",".join(path.name for path in source_files))
        for source_path in source_files:
            _trace(log, "merged export part validation", part=source_path.name,
                   size_bytes=source_path.stat().st_size)

        record_count = 0
        with open(str(temporary_file), "w", newline="") as destination:
            writer = csv.writer(destination, delimiter="|", lineterminator="\n")
            if channel == "ORANGE":
                header = ["email_address", "account_name"]
            else:
                header = ["email"]
            if preserve_gender:
                header.append("gender")
            writer.writerow(header)

            for source_path in source_files:
                part_count = 0
                with _open_export_file(source_path) as source:
                    reader = csv.reader(source, delimiter="|")
                    next(reader, None)  # Snowflake writes a header in every part.
                    for row in reader:
                        if not row:
                            continue
                        if channel == "ORANGE":
                            output_row = [
                                row[0] if len(row) > 0 else "",
                                row[1] if len(row) > 1 else "",
                            ]
                            if preserve_gender:
                                output_row.append(row[2] if len(row) > 2 else "")
                        else:
                            output_row = [row[0] if len(row) > 0 else ""]
                            if preserve_gender:
                                output_row.append(row[1] if len(row) > 1 else "")
                        writer.writerow(output_row)
                        record_count += 1
                        part_count += 1
                _trace(log, "merged export part streamed", part=source_path.name,
                       data_rows=part_count)

        _verify_local_file(log, "temporary streamed merge output", temporary_file)
        os.replace(str(temporary_file), str(current_file))
        _verify_local_file(log, "final streamed merge output", current_file)
        log.info(
            "Merged local file written: %s | unique rows: %s",
            current_file, "{0:,}".format(record_count),
        )
        _trace(log, "merged export download completed", channel=channel,
               destination_file=current_file, unique_row_count=record_count,
               size_bytes=current_file.stat().st_size)
        return record_count
    except Exception as exc:
        _trace(log, "merged export download failed", channel=channel,
               error=exc, temporary_file=temporary_file)
        try:
            temporary_file.unlink()
        except OSError:
            pass
        raise
    finally:
        shutil.rmtree(str(download_dir), ignore_errors=True)
        _trace(log, "merged export download cleanup", download_dir=download_dir,
               exists_after_cleanup=download_dir.exists())


def _merged_s3_path(current_s3_prefix, current_file):
    """Use a request-specific folder so retry data cannot be mixed in."""
    return "{0}/MERGED/{1}".format(
        _s3_prefix(current_s3_prefix), _safe_identifier(Path(current_file).stem)
    )


def _merge_to_current_file(previous_s3, current_s3_prefix, current_file,
                           channel, request_id, work_dir, log,
                           preserve_gender=False):
    merged_s3 = _merged_s3_path(current_s3_prefix, current_file)
    _trace(log, "merge execution starting", request_id=request_id, channel=channel,
           previous_s3=_s3_prefix(previous_s3), current_s3=_s3_prefix(current_s3_prefix),
           merged_s3=merged_s3, local_current_file=current_file)
    _snowflake_merge(
        previous_s3, current_s3_prefix, merged_s3, channel, request_id, log,
        preserve_gender=preserve_gender,
    )
    count = _download_merged_export(
        merged_s3, current_file, channel, work_dir, log,
        preserve_gender=preserve_gender,
    )
    _trace(log, "merge execution completed", request_id=request_id, channel=channel,
           merged_s3=merged_s3, unique_row_count=count,
           local_current_file=current_file)
    return count, merged_s3


def merge_current_file(request_id, previous_request_id, channel, current_file,
                       current_s3_prefix, work_dir, log, orange=False,
                       preserve_gender=False):
    """Merge one normal channel through Snowflake, or retain current output.

    Orange retains ``account_name`` in the merged file. The caller therefore
    creates the final Orange format from the *current* request type: ESP-wise
    files for Mailing, and an email-only file for Suppression.
    """
    del orange  # File format is determined by the channel name.
    channel = str(channel).upper()
    if channel not in _CHANNELS:
        raise ValueError("Unsupported merge channel: {0}".format(channel))
    if not _s3_prefix(current_s3_prefix):
        raise ValueError("Current S3 path is required for a merge.")
    current_file = Path(current_file)
    _trace(log, "normal-channel merge requested", request_id=request_id,
           previous_request_id=previous_request_id, channel=channel,
           current_file=current_file, current_s3=_s3_prefix(current_s3_prefix),
           preserve_gender=preserve_gender)
    _verify_local_file(log, "current channel output before merge", current_file)
    previous_s3 = _previous_path(previous_request_id, channel, log)
    if not previous_s3:
        count = _row_count(current_file, log)
        _set_merge_status(request_id, channel, "CURRENT_ONLY", log)
        _trace(log, "normal-channel merge resolved as current-only", request_id=request_id,
               channel=channel, previous_request_id=previous_request_id,
               reason="previous request/channel has no completed source output",
               row_count=count, s3_path=current_s3_prefix)
        return {
            "merge_mode": "CURRENT_ONLY",
            "count": count,
            "s3_path": current_s3_prefix,
        }

    count, merged_s3 = _merge_to_current_file(
        previous_s3, current_s3_prefix, current_file, channel,
        request_id, work_dir, log, preserve_gender=preserve_gender,
    )
    from REQUEST_PROCESSOR.request_processor import update_channel_storage
    update_channel_storage(request_id, channel, merged_s3, count, log)
    _set_merge_status(request_id, channel, "MERGED", log)
    _trace(log, "normal-channel merge finalized", request_id=request_id,
           channel=channel, previous_request_id=previous_request_id,
           merge_mode="MERGED", merged_s3=merged_s3, row_count=count)
    return {"merge_mode": "MERGED", "count": count, "s3_path": merged_s3}


def merge_doordash_file(request_id, previous_request_id, artifact, current_file,
                        current_s3_prefix, work_dir, log):
    """Merge the corresponding DoorDash Email or MD5 artifact in Snowflake."""
    artifact = str(artifact).upper()
    if artifact not in _DOORDASH_ARTIFACTS:
        raise ValueError("Unsupported DoorDash artifact: {0}".format(artifact))
    if not _s3_prefix(current_s3_prefix):
        raise ValueError("Current DoorDash S3 path is required for a merge.")
    current_file = Path(current_file)
    _trace(log, "DoorDash artifact merge requested", request_id=request_id,
           previous_request_id=previous_request_id, artifact=artifact,
           current_file=current_file, current_s3=_s3_prefix(current_s3_prefix))
    _verify_local_file(log, "current DoorDash artifact before merge", current_file)
    previous_s3 = _previous_doordash_path(previous_request_id, artifact, log)
    if not previous_s3:
        count = _row_count(current_file, log)
        _set_doordash_storage(
            request_id, artifact, current_s3_prefix, "CURRENT_ONLY", log
        )
        _trace(log, "DoorDash artifact merge resolved as current-only",
               request_id=request_id, artifact=artifact,
               previous_request_id=previous_request_id,
               reason="previous DoorDash request/artifact has no completed source output",
               row_count=count, s3_path=current_s3_prefix)
        return {
            "merge_mode": "CURRENT_ONLY",
            "count": count,
            "s3_path": current_s3_prefix,
        }

    count, merged_s3 = _merge_to_current_file(
        previous_s3, current_s3_prefix, current_file,
        "DOORDASH_{0}".format(artifact), request_id, work_dir, log,
    )
    _set_doordash_storage(request_id, artifact, merged_s3, "MERGED", log)
    _trace(log, "DoorDash artifact merge finalized", request_id=request_id,
           artifact=artifact, previous_request_id=previous_request_id,
           merge_mode="MERGED", merged_s3=merged_s3, row_count=count)
    return {"merge_mode": "MERGED", "count": count, "s3_path": merged_s3}
