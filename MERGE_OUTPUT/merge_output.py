"""Shared post-processing for optional previous-output merges.

The source request is always read-only. A selected current channel/artifact
without a matching completed source file remains a valid current-only output.
"""

import shutil
from pathlib import Path

import pandas as pd

from utils import run_command


_CHANNELS = {"GREEN", "BLUE", "ARCAMAX", "ORANGE", "APPTNESS"}
_DOORDASH_ARTIFACTS = {
    "EMAIL": ("DOORDASH_EMAIL_FILEPATH", "DOORDASH_EMAIL_MERGE_STATUS"),
    "MD5HASH": ("DOORDASH_MD5HASH_FILEPATH", "DOORDASH_MD5HASH_MERGE_STATUS"),
}


def _db_connection(log=None):
    """Import lazily so ZIPS can use this shared module without a cycle."""
    from ZIPS.zips import get_db_with_retry
    return get_db_with_retry(log)


def _previous_path(previous_request_id, channel):
    """Return a completed previous per-channel S3 path, if one exists."""
    if channel not in _CHANNELS:
        return None
    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT overall_status, {0}_FILEPATH FROM requests WHERE id=%s".format(channel),
                (previous_request_id,),
            )
            row = cur.fetchone()
            return row[1] if row and row[0] == "completed" and row[1] else None
    finally:
        conn.close()


def _previous_doordash_path(previous_request_id, artifact):
    """Return a completed DoorDash Email/MD5 S3 path, if one exists."""
    path_column, _ = _DOORDASH_ARTIFACTS[artifact]
    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT overall_status, {0} FROM requests WHERE id=%s".format(path_column),
                (previous_request_id,),
            )
            row = cur.fetchone()
            return row[1] if row and row[0] == "completed" and row[1] else None
    finally:
        conn.close()


def _set_merge_status(request_id, channel, status):
    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET {0}_MERGE_STATUS=%s WHERE id=%s".format(channel),
                (status, request_id),
            )
        conn.commit()
    finally:
        conn.close()


def _set_doordash_storage(request_id, artifact, s3_path, merge_status):
    path_column, status_column = _DOORDASH_ARTIFACTS[artifact]
    conn = _db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests SET {0}=%s, {1}=%s WHERE id=%s".format(
                    path_column, status_column
                ),
                (s3_path, merge_status, request_id),
            )
        conn.commit()
    finally:
        conn.close()


def _download_previous_csv(s3_path, destination, channel, log):
    """Download a Snowflake export or a prior /MERGED CSV as one CSV file."""
    destination = Path(destination)
    download_dir = destination.parent / (destination.stem + "_download")
    shutil.rmtree(str(download_dir), ignore_errors=True)
    download_dir.mkdir(parents=True, exist_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)

    run_command([
        "aws", "s3", "cp", s3_path.rstrip("/") + "/", str(download_dir),
        "--recursive", "--quiet",
    ])
    files = sorted(path for path in download_dir.rglob("*") if path.is_file())
    if not files:
        raise RuntimeError("No files were downloaded from previous output: {0}".format(s3_path))

    frames = []
    for source in files:
        compression = "gzip" if source.suffix.lower() == ".gz" else None
        frames.append(pd.read_csv(str(source), sep="|", dtype=str, compression=compression).fillna(""))
    data = pd.concat(frames, ignore_index=True)
    if channel != "ORANGE":
        email_column = "email" if "email" in data.columns else data.columns[0]
        data = data[[email_column]].rename(columns={email_column: "email"})
    data.to_csv(str(destination), sep="|", index=False)
    shutil.rmtree(str(download_dir), ignore_errors=True)


def _row_count(file_path):
    return len(pd.read_csv(str(file_path), sep="|", dtype=str))


def _merge_files(current_file, previous_file, channel):
    """Merge two headered pipe-delimited files by a case-insensitive email."""
    current = pd.read_csv(str(current_file), sep="|", dtype=str).fillna("")
    previous = pd.read_csv(str(previous_file), sep="|", dtype=str).fillna("")
    email_column = "email" if "email" in current.columns else current.columns[0]
    merged = pd.concat([previous, current], ignore_index=True)
    merged["_merge_email"] = merged[email_column].astype(str).str.strip().str.lower()
    merged = merged.drop_duplicates("_merge_email", keep="first").drop(columns=["_merge_email"])
    if channel != "ORANGE":
        merged = merged[[email_column]].rename(columns={email_column: "email"})
    merged.to_csv(str(current_file), sep="|", index=False)
    try:
        Path(previous_file).unlink()
    except OSError:
        pass
    return len(merged)


def merge_current_file(request_id, previous_request_id, channel, current_file,
                       current_s3_prefix, work_dir, log, orange=False):
    """Merge a selected normal channel, or retain the new output if absent."""
    del orange  # File format is determined by the channel name.
    current_file = Path(current_file)
    previous_s3 = _previous_path(previous_request_id, channel)
    if not previous_s3:
        _set_merge_status(request_id, channel, "CURRENT_ONLY")
        return {
            "merge_mode": "CURRENT_ONLY", "count": _row_count(current_file),
            "s3_path": current_s3_prefix,
        }

    work_dir = Path(work_dir)
    previous_file = work_dir / ("previous_" + current_file.name)
    _download_previous_csv(previous_s3, previous_file, channel, log)
    count = _merge_files(current_file, previous_file, channel)
    merged_s3 = current_s3_prefix.rstrip("/") + "/MERGED"
    run_command(["aws", "s3", "cp", str(current_file), merged_s3 + "/" + current_file.name, "--quiet"])

    from ZIPS.zips import update_channel_storage
    update_channel_storage(request_id, channel, merged_s3, count, log)
    _set_merge_status(request_id, channel, "MERGED")
    return {"merge_mode": "MERGED", "count": count, "s3_path": merged_s3}


def merge_doordash_file(request_id, previous_request_id, artifact, current_file,
                        current_s3_prefix, work_dir, log):
    """Merge the corresponding DoorDash Email or MD5 output artifact.

    Email can only merge with the prior DoorDash Email file; MD5 can only
    merge with the prior DoorDash MD5 file. A missing source artifact does
    not block the current request.
    """
    if artifact not in _DOORDASH_ARTIFACTS:
        raise ValueError("Unsupported DoorDash artifact: {0}".format(artifact))
    current_file = Path(current_file)
    previous_s3 = _previous_doordash_path(previous_request_id, artifact)
    if not previous_s3:
        _set_doordash_storage(request_id, artifact, current_s3_prefix, "CURRENT_ONLY")
        return {
            "merge_mode": "CURRENT_ONLY", "count": _row_count(current_file),
            "s3_path": current_s3_prefix,
        }

    work_dir = Path(work_dir)
    previous_file = work_dir / ("previous_{0}_".format(artifact.lower()) + current_file.name)
    _download_previous_csv(previous_s3, previous_file, "GREEN", log)
    count = _merge_files(current_file, previous_file, "GREEN")
    merged_s3 = current_s3_prefix.rstrip("/") + "/MERGED"
    run_command(["aws", "s3", "cp", str(current_file), merged_s3 + "/" + current_file.name, "--quiet"])
    _set_doordash_storage(request_id, artifact, merged_s3, "MERGED")
    return {"merge_mode": "MERGED", "count": count, "s3_path": merged_s3}
