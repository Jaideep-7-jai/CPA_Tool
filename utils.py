#!/usr/bin/env python3
"""
Enhanced Utilities with Directory Safety + Run Isolation
"""

import os
import json
import time
import gzip
import shutil
import logging
import smtplib
from typing import Tuple
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Union
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import DB_CONFIG, SENDER, RECIPIENT, CC_RECIPIENTS, CPAUSER_EMAIL, TECH_NOTIFICATION_RECIPIENTS

def ensure_output_dir(output_dir, criteria_type):
    """
    Create output directory if not exists + add timestamped run subdir.

    Layout:
        <output_dir>/run_<criteria_type>_<YYYYMMDD_HHMMSS>/
            logs/         <- combined + per-channel log files + filedetails.json
            FINAL_DIR/    <- final output CSVs/ZIPs that remain after run

    Each execution gets its own isolated folder with full date+time stamp.
    The prefix reflects the actual criteria type (age / state / zips).
    """
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_criteria = (criteria_type or "run").lower().strip()
    safe_dir = path / f"run_{safe_criteria}_{timestamp}"
    safe_dir.mkdir(exist_ok=True)

    # Pre-create logs/ and FINAL_DIR/ so they always exist
    (safe_dir / "logs").mkdir(exist_ok=True)
    (safe_dir / "FINAL_DIR").mkdir(exist_ok=True)

    logging.info(f"Using run directory: {safe_dir}")
    return safe_dir


def run_command(cmd, cwd=None, timeout=3600, stdout=None):
    """Execute command with full error handling"""
    cmd_str = ' '.join(cmd) if isinstance(cmd, list) else cmd
    logging.info(f"Running: {cmd_str}")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            shell=isinstance(cmd, str),
            cwd=cwd,
            stdout=subprocess.PIPE if stdout is None else stdout,
            stderr=subprocess.PIPE if stdout is None else None,
            universal_newlines=True,
            timeout=timeout,
        )

        elapsed = time.time() - start_time
        logging.info(f"Completed: {elapsed:.1f}s (code: {result.returncode})")

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed (code {result.returncode}):\n{result.stderr}"
            )

        return result.stdout.strip() if result.stdout else ""

    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Timeout: {timeout}s")


def download_combine(s3_path, output_file, cwd):
    """Download S3 -> Combine -> Count -> Zip -> Return (zip_filename, line_count)"""
    logging.info(f"{s3_path} -> {output_file}")
    start_total = time.time()

    run_command(["aws", "s3", "cp", s3_path, ".", "--recursive", "--quiet"], cwd=cwd)

    data_files = sorted(Path(cwd).glob("data*"))
    if not data_files:
        raise RuntimeError("No data files downloaded")

    start = time.time()
    output_path = Path(cwd) / output_file
    line_count = 0

    with open(output_path, "w") as out_f:
        for gz_file in data_files:
            with gzip.open(gz_file, "rt") as in_f:
                for line in in_f:
                    clean_line = line.replace('"', '').strip()
                    if clean_line:
                        out_f.write(clean_line + '\n')
                        line_count += 1

    elapsed = time.time() - start
    logging.info(f"Combined {line_count:,} lines in {elapsed:.1f}s")

    start = time.time()
    zip_name = f"{output_file}.zip"
    zip_path = Path(cwd) / zip_name
    shutil.make_archive(
        base_name=zip_path.with_suffix('').as_posix(),
        format='zip',
        root_dir=cwd,
        base_dir=output_file,
    )

    if not zip_path.exists():
        raise RuntimeError("Zip file not created")

    elapsed = time.time() - start
    logging.info(f"Zipped file in {elapsed:.1f}s")

    size_mb = zip_path.stat().st_size / 1e6
    logging.info(f"{zip_name}: {size_mb:.1f}MB ({line_count:,} records)")

    start = time.time()
    if output_path.exists():
        output_path.unlink()
    for f in data_files:
        f.unlink()
    elapsed = time.time() - start
    logging.info(f"Cleanup in {elapsed:.1f}s")

    total_elapsed = time.time() - start_total
    logging.info(f"Total: {total_elapsed:.1f}s")

    return zip_name, line_count


def get_db_connection(channel):
    """Get DB connection params from config"""
    config = DB_CONFIG.get(channel)
    if not config:
        raise ValueError(f"Unknown channel: {channel}")
    return config


def send_email(subject, body_text, recipients, is_error=False):
    """Send an email to the supplied notification recipients."""
    try:
        recipients = [
            recipient.strip()
            for recipient in recipients
            if recipient and recipient.strip()
        ]

        if not recipients:
            logging.warning("Email was not sent because no recipients were configured.")
            return

        msg = MIMEMultipart()
        msg["Subject"] = f"[{'ERROR' if is_error else 'SUCCESS'}] {subject}"
        msg["From"] = SENDER
        msg["To"] = ", ".join(recipients)

        msg_body = MIMEMultipart("alternative")
        msg_body.attach(MIMEText(body_text, "plain"))
        msg.attach(msg_body)

        server = smtplib.SMTP("localhost")
        server.sendmail(SENDER, recipients, msg.as_string())
        server.quit()

        logging.info(
            f"{'ERROR' if is_error else 'SUCCESS'} email sent: {subject}"
        )

    except Exception as exc:
        logging.error(f"Email failed: {exc}")


def _success_recipients(request_details):
    created_by = (
        request_details.get("created_by_username", "")
        .strip()
        .lower()
    )

    if created_by == "cpauser":
        return [CPAUSER_EMAIL]

    return TECH_NOTIFICATION_RECIPIENTS


def _count_rows_in_file(filepath):
    """
    Count data rows (non-empty, non-header lines) in a CSV/TXT file.
    Returns integer count, or 0 on any error.
    """
    try:
        count = 0
        with open(str(filepath), 'r', errors='ignore') as fh:
            for i, line in enumerate(fh):
                if i == 0:
                    # Skip header line
                    continue
                if line.strip():
                    count += 1
        return count
    except Exception as e:
        logging.warning(f"_count_rows_in_file failed for {filepath}: {e}")
        return 0


def _get_file_size_bytes(filepath):
    """
    Return the file size in bytes, or 0 on any error.
    """
    try:
        return Path(str(filepath)).stat().st_size
    except Exception as e:
        logging.warning(f"_get_file_size_bytes failed for {filepath}: {e}")
        return 0


def build_file_details_json(final_files_dir, results=None):
    """
    Scan FINAL_FILES/ directory and build a list of file-detail dicts.
    Each entry contains:
        filename   - basename of the file
        file_count - number of data rows in the file (header excluded)
        row_count  - same as file_count (kept for backward compat)
        file_size_bytes - raw file size in bytes
        channel    - channel name derived from results dict or filename
        path       - full absolute path

    Also merges in row_count and channel from the `results` dict returned
    by process_age_state_request() so the JSON is as rich as possible.

    Returns a list of dicts sorted by channel name.
    """
    final_dir = Path(final_files_dir)
    file_details = []

    # Build a quick lookup: filename -> result entry
    result_by_file = {}
    if results and isinstance(results, dict):
        for ch, r in results.items():
            if r and r.get('file'):
                result_by_file[r['file']] = r

    if final_dir.exists():
        for fp in sorted(final_dir.iterdir()):
            if not fp.is_file():
                continue
            fname = fp.name
            result_entry = result_by_file.get(fname, {})

            # Prefer count from results dict; fallback to counting file rows
            row_count = result_entry.get('count', None)
            if row_count is None or row_count == 0:
                row_count = _count_rows_in_file(fp)

            file_size_bytes = _get_file_size_bytes(fp)

            file_details.append({
                "filename":        fname,
                "file_count":      row_count,        # number of data rows
                "row_count":       row_count,         # kept for backward compat
                "file_size_bytes": file_size_bytes,   # raw file size in bytes
                "channel":         result_entry.get('channel', ''),
                "path":            str(fp),
                "s3_path":         result_entry.get("s3_path", ""),
            })
    else:
        logging.warning(f"build_file_details_json: directory not found: {final_dir}")

    return file_details


def _read_filedetails_json(json_path):
    """
    Safely read and parse a filedetails.json file.
    Returns parsed list on success, or empty list on any error.
    """
    try:
        with open(str(json_path), "r") as jf:
            return json.load(jf)
    except Exception as e:
        logging.error(f"Failed to read filedetails.json at {json_path}: {e}")
        return []


def _format_size_bytes(size_bytes):
    """
    Format a byte count into a human-readable string.
    Examples:
        512        -> "512 B"
        1536       -> "1.5 KB"
        2097152    -> "2.0 MB"
        1073741824 -> "1.0 GB"
    """
    if size_bytes is None or size_bytes == 0:
        return "0 B"
    try:
        size_bytes = int(size_bytes)
    except (TypeError, ValueError):
        return "0 B"
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.1f} MB"
    else:
        return f"{size_bytes / 1024 ** 3:.1f} GB"


def _request_summary(request_details):
    """Return a readable request summary for notification emails."""
    request_details = request_details or {}
    fields = (
        ("Request ID", request_details.get("id", "")),
        ("Request Name", request_details.get("request_name", "")),
        ("Client Name", request_details.get("client_name", "")),
        ("Request Type", request_details.get("request_type", "")),
        ("Criteria Type", request_details.get("criteria_type", "")),
        ("Criteria Value", request_details.get("criteria_value", "")),
        ("Comparison", request_details.get("comp_type", "")),
        ("Channels", request_details.get("channel", "")),
        ("Output Directory", request_details.get("output_dir", "")),
    )
    return "\n".join(
        f"{label}: {value or '-'}"
        for label, value in fields
    )


def _request_subject(status, request_details):
    request_details = request_details or {}
    request_name = request_details.get("request_name") or "Unknown Request"
    request_id = request_details.get("id") or "Unknown ID"

    return f"CPA Tool {status} | Request: {request_name} | ID: {request_id}"


def _latest_log_details(run_dir, max_chars=20000):
    """Return the latest request log contents for an error notification."""
    if not run_dir:
        return "No request log directory was supplied."

    logs_dir = Path(run_dir) / "logs"
    if not logs_dir.exists():
        return f"No log directory found at: {logs_dir}"

    log_files = sorted(
        logs_dir.glob("*.log"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not log_files:
        return f"No log files found in: {logs_dir}"

    latest_log = log_files[0]

    try:
        with open(str(latest_log), "r", errors="replace") as log_file:
            content = log_file.read()

        if len(content) > max_chars:
            content = "[Earlier log content omitted]\n" + content[-max_chars:]

        return f"Log File: {latest_log}\n\n{content}"

    except Exception as exc:
        return f"Could not read log file {latest_log}: {exc}"


def send_success_email(request_details, results, run_dir):
    """Send a success email with request and generated-file details."""
    run_dir_path = Path(run_dir)
    final_files_dir = run_dir_path / "FINAL_FILES"
    logs_dir = run_dir_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    file_details = build_file_details_json(final_files_dir, results)

    json_path = logs_dir / "filedetails.json"
    try:
        with open(str(json_path), "w") as json_file:
            json.dump(file_details, json_file, indent=2)
        logging.info(f"filedetails.json written -> {json_path}")
    except Exception as exc:
        logging.error(f"Failed to write filedetails.json: {exc}")

    email_file_details = (
        _read_filedetails_json(json_path)
        if json_path.exists()
        else file_details
    )

    if email_file_details:
        lines = []

        for file_detail in email_file_details:
            channel_tag = (
                f"[{file_detail['channel']}] "
                if file_detail.get("channel")
                else ""
            )
            row_count = (
                file_detail.get("file_count",
                                file_detail.get("row_count", 0))
                or 0
            )
            size_bytes = file_detail.get("file_size_bytes", 0) or 0
            size_text = _format_size_bytes(size_bytes)

            lines.append(
                f"  . {channel_tag}{file_detail['filename']}"
                f"  |  Rows: {row_count:,}"
                f"  |  Size: {size_text}"
                f"  |  Raw bytes: {size_bytes:,}"
            )

        file_section = "\n".join(lines)
    else:
        file_section = "(no files found in FINAL_FILES directory)"

    subject = _request_subject("COMPLETED", request_details)

    body = f"""SUCCESS: Request completed successfully

Request Details:
{_request_summary(request_details)}

Output Directory: {final_files_dir}

Generated Files:
{file_section}

Processing completed successfully!"""

    send_email(
    subject,
    body,
    recipients=_success_recipients(request_details),
    is_error=False,
    )

    return file_details, json_path


def send_error_email(request_details, error_msg, run_dir=None):
    """Send an error email with request metadata, error details, and log output."""
    subject = _request_subject("FAILED", request_details)

    body = f"""ERROR: Request failed

Request Details:
{_request_summary(request_details)}

Error Details:
{error_msg}

Error Log Details:
{_latest_log_details(run_dir)}"""

    send_email(
    subject,
    body,
    recipients=TECH_NOTIFICATION_RECIPIENTS,
    is_error=True,
    )