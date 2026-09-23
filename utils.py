#!/usr/bin/env python3
"""
Enhanced Utilities with Directory Safety + Run Isolation
"""

import os
import re
import tempfile
import json
import time
import gzip
import shutil
import logging
import smtplib
import html
from typing import Tuple
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Union
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import config as _app_config


# Keep the notification implementation compatible with both the newer
# RECIPIENT/CC_RECIPIENTS settings and the repository's original distribution
# list settings.  The latter are used when a deployed config.py has not yet
# been updated with the newer names.
DB_CONFIG = _app_config.DB_CONFIG
SENDER = _app_config.SENDER
RECIPIENT = getattr(_app_config, "RECIPIENT", "")
CC_RECIPIENTS = getattr(_app_config, "CC_RECIPIENTS", "")
LEGACY_TECH_RECIPIENTS = getattr(_app_config, "TECH_NOTIFICATION_RECIPIENTS", ())
LEGACY_CPA_RECIPIENTS = getattr(_app_config, "CPAUSER_EMAIL", ())

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
    def redact(value):
        return re.sub(
            r"AWS_(?:KEY_ID|SECRET_KEY)\s*=\s*'[^']*'",
            "AWS_CREDENTIAL='***'", str(value), flags=re.IGNORECASE,
        )
    logging.info("Running: %s", redact(cmd_str))
    start_time = time.time()

    # Keep S3 COPY credentials out of the operating system process arguments.
    # SnowSQL reads a private temporary SQL file with the same query instead.
    query_path = None
    if isinstance(cmd, list) and cmd and Path(cmd[0]).name == "snowsql" and "-q" in cmd:
        query_index = cmd.index("-q")
        if query_index + 1 < len(cmd) and re.search(
            r"\bCREDENTIALS\s*=\s*\(", cmd[query_index + 1], re.IGNORECASE
        ):
            descriptor, query_path = tempfile.mkstemp(prefix="cpa_sql_", suffix=".sql")
            with os.fdopen(descriptor, "w") as query_file:
                query_file.write(cmd[query_index + 1].rstrip() + "\n")
            os.chmod(query_path, 0o600)
            cmd = (cmd[:query_index] + ["-f", query_path]
                   + cmd[query_index + 2:] + ["-o", "echo=false"])

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
                f"Failed (code {result.returncode}):\n{redact(result.stderr)}"
            )

        return result.stdout.strip() if result.stdout else ""

    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Timeout: {timeout}s")
    finally:
        if query_path:
            os.unlink(query_path)


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


def _split_recipients(*values):
    """Return an ordered, de-duplicated recipient list."""
    recipients = []
    seen = set()
    for value in values:
        items = value if isinstance(value, (list, tuple, set)) else (value,)
        for item in items:
            for address in str(item or "").replace(";", ",").split(","):
                address = address.strip()
                key = address.lower()
                if address and key not in seen:
                    recipients.append(address)
                    seen.add(key)
    return recipients


def _is_truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_cpa_user(request_details):
    """Identify CPA users without hard-coding an e-mail address in source."""
    usernames = {
        value.strip().lower()
        for value in os.getenv("CPA_EMAIL_CPA_USERNAMES", "cpauser").split(",")
        if value.strip()
    }
    return str((request_details or {}).get("username") or "").strip().lower() in usernames


def _notification_recipients(request_details, is_error=False):
    """Resolve notification recipients from service environment configuration.

    The legacy RECIPIENT/CC_RECIPIENTS values remain safe fallbacks.  Configure
    the environment variables below in production so recipient routing is not
    embedded in Git:

    * CPA_EMAIL_TECH_RECIPIENTS
    * CPA_EMAIL_DATATEAM_RECIPIENTS
    * CPA_EMAIL_CPA_RECIPIENTS
    * CPA_EMAIL_CPA_USERNAMES
    """
    tech_recipients = _split_recipients(
        os.getenv("CPA_EMAIL_TECH_RECIPIENTS", ""),
        LEGACY_TECH_RECIPIENTS,
        RECIPIENT,
    )
    datateam_recipients = _split_recipients(
        os.getenv("CPA_EMAIL_DATATEAM_RECIPIENTS", ""),
        CC_RECIPIENTS,
    )
    cpa_recipients = _split_recipients(
        os.getenv("CPA_EMAIL_CPA_RECIPIENTS", ""),
        LEGACY_CPA_RECIPIENTS,
    )

    if _is_cpa_user(request_details):
        # CPA users receive the FTP-only view plus the Data Team.  If an
        # operator has not configured a dedicated CPA distribution list, retain
        # the old primary recipient rather than silently dropping a mail.
        recipients = _split_recipients(
            cpa_recipients or tech_recipients,
            datateam_recipients,
        )
    else:
        recipients = _split_recipients(tech_recipients, datateam_recipients)

    if not recipients:
        raise RuntimeError(
            "No notification recipients are configured. Set CPA_EMAIL_*_RECIPIENTS."
        )
    return recipients


def send_email(subject, body_text, is_error=False, html_body=None, recipients=None):
    """Send a text and HTML notification and return whether SMTP accepted it."""
    try:
        recipients = list(recipients or _split_recipients(
            LEGACY_TECH_RECIPIENTS, RECIPIENT, CC_RECIPIENTS,
        ))
        if not recipients:
            raise RuntimeError("No notification recipients are configured.")

        msg = MIMEMultipart("alternative")
        msg['Subject'] = "[{0}] {1}".format("ERROR" if is_error else "SUCCESS", subject)
        msg['From'] = SENDER
        msg['To'] = recipients[0]
        if len(recipients) > 1:
            msg['Cc'] = ", ".join(recipients[1:])
        msg.attach(MIMEText(body_text, 'plain', 'utf-8'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        server = smtplib.SMTP('localhost')
        try:
            server.sendmail(SENDER, recipients, msg.as_string())
        finally:
            server.quit()
        logging.info(
            "%s email sent: %s | recipients=%s",
            "ERROR" if is_error else "SUCCESS", subject, ", ".join(recipients),
        )
        return True
    except Exception as exc:
        logging.error("Email failed: %s", exc)
        return False


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


def _iter_result_artifacts(results):
    """Yield one metadata dict for every generated delivery artifact."""
    if not isinstance(results, dict):
        return
    for _channel, result in results.items():
        if not isinstance(result, dict):
            continue
        artifacts = result.get("artifacts") or []
        if artifacts:
            for artifact in artifacts:
                if not isinstance(artifact, dict) or not artifact.get("file"):
                    continue
                merged = dict(result)
                merged.update(artifact)
                yield merged
        elif result.get("file"):
            yield result


def _derive_channel_from_filename(filename):
    filename = str(filename or "").upper()
    for channel in ("GREEN", "BLUE", "ARCAMAX", "ORANGE", "APPTNESS"):
        if channel in filename:
            return channel
    return ""


def _read_delivery_header(filepath, fallback=""):
    """Read one local delivery-file header without reading the whole artifact."""
    path = Path(filepath)
    if path.suffix.lower() == ".zip":
        return fallback or "ZIP archive (each inner CSV header: email)"
    try:
        with open(str(path), "r", errors="replace") as handle:
            return handle.readline().strip() or fallback
    except Exception as exc:
        logging.warning("Unable to read delivery header from %s: %s", path, exc)
        return fallback


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
    for result in _iter_result_artifacts(results):
        result_by_file[result['file']] = result

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

            channel = result_entry.get('channel') or _derive_channel_from_filename(fname)
            delivery_header = _read_delivery_header(
                fp, result_entry.get("delivery_header", "")
            )
            file_details.append({
                "filename":        fname,
                "file_count":      row_count,        # number of data rows
                "row_count":       row_count,         # kept for backward compat
                "file_size_bytes": file_size_bytes,   # raw file size in bytes
                "channel":         channel,
                "merge_mode":      result_entry.get('merge_mode', ''),
                "path":            str(fp),
                "local_output_dir": result_entry.get("local_output_dir", ""),
                "ftp_path":        result_entry.get("ftp_path", ""),
                "s3_path":         result_entry.get("s3_path", ""),
                "final_s3_path":   result_entry.get("final_s3_path", result_entry.get("s3_path", "")),
                "complete_s3_path": result_entry.get("complete_s3_path", ""),
                "delivery_header": delivery_header,
                "final_data_header": result_entry.get("final_data_header", ""),
                "complete_data_header": result_entry.get("complete_data_header", ""),
                "final_count":     result_entry.get("final_count", row_count),
                "complete_count":  result_entry.get("complete_count", ""),
                "merge_source_request_id": result_entry.get("merge_source_request_id", ""),
                "merge_source_request_name": result_entry.get("merge_source_request_name", ""),
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


def _criteria_email_display(request_details):
    """Render saved single or multi-criteria data consistently with the UI."""
    request_details = request_details or {}
    raw_items = request_details.get("criteria_json")
    try:
        items = json.loads(raw_items) if isinstance(raw_items, str) else raw_items
    except (TypeError, ValueError):
        items = None
    if not isinstance(items, list) or not items:
        return (
            str(request_details.get("criteria_type") or "-").upper(),
            request_details.get("criteria_value") or "-",
            request_details.get("comp_type") or "-",
        )

    labels = {"age": "Age", "state": "State", "zip": "ZIP", "zips": "ZIP", "gender": "Gender"}
    comparison_labels = {
        "greater": "Greater Than", "less": "Lesser Than", "between": "Between",
        "include": "Include", "exclude": "Exclude",
    }
    value_parts, comparison_parts, criteria_labels = [], [], []
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").lower()
        label = labels.get(kind, kind.title() or "Criterion")
        comparison = str(item.get("comparison") or "").lower()
        if kind == "age" and comparison == "between":
            value = "{0}-{1}".format(item.get("from", ""), item.get("to", ""))
        elif kind in {"state", "gender"}:
            values = item.get("values") or []
            if isinstance(values, str):
                values = [part.strip() for part in values.split(",")]
            value = ", ".join(str(part) for part in values if str(part).strip())
        elif kind in {"zip", "zips"}:
            value = "Uploaded ZIP file"
        else:
            value = str(item.get("value") or "-")
        criteria_labels.append(label)
        value_parts.append("{0}: {1}".format(label, value or "-"))
        comparison_parts.append("{0}: {1}".format(
            label, comparison_labels.get(comparison, comparison or "-")
        ))
    criteria_label = "MULTI" if len(criteria_labels) > 1 else (criteria_labels[0].upper() if criteria_labels else "-")
    return criteria_label, " | ".join(value_parts) or "-", " | ".join(comparison_parts) or "-"


def _request_detail_rows(request_details):
    """Return the standard notification request-details table rows."""
    request_details = request_details or {}
    criteria, criteria_value, comparison = _criteria_email_display(request_details)
    responder = "Yes — last {0} day(s)".format(request_details.get("responder_days") or "-") \
        if _is_truthy(request_details.get("responder_match")) else "No"
    source_id = request_details.get("merge_source_request_id")
    if source_id:
        source_name = request_details.get("merge_source_request_name")
        merge = "Yes — #{0}{1}".format(
            source_id, " ({0})".format(source_name) if source_name else ""
        )
    else:
        merge = "No"
    return [
        ("Request Name", request_details.get("request_name") or "-"),
        ("Client Name", request_details.get("client_name") or "-"),
        ("Request Type", request_details.get("request_type") or "-"),
        ("Criteria", criteria),
        ("Criteria Value", criteria_value),
        ("Comparison", comparison),
        ("Channels", request_details.get("channel") or "-"),
        ("Responder Match", responder),
        ("Merge Previous Output", merge),
    ]


def _request_summary(request_details):
    return "\n".join("{0}: {1}".format(label, value) for label, value in _request_detail_rows(request_details))


def _html_table(headers, rows):
    """Build a compact, Outlook-safe HTML table from untrusted text values."""
    cell_style = "border:1px solid #cbd5e1;padding:7px 9px;text-align:left;vertical-align:top;"
    header_style = cell_style + "background:#eff6ff;color:#0f172a;font-weight:600;"
    header_html = "".join("<th style=\"{0}\">{1}</th>".format(header_style, html.escape(str(value))) for value in headers)
    body_html = []
    for row in rows:
        body_html.append("<tr>{0}</tr>".format("".join(
            "<td style=\"{0}\">{1}</td>".format(cell_style, html.escape(str(value if value not in (None, "") else "-")))
            for value in row
        )))
    return "<table style=\"border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;margin:8px 0 18px;\"><thead><tr>{0}</tr></thead><tbody>{1}</tbody></table>".format(header_html, "".join(body_html))


def _request_details_html(request_details):
    return _html_table(["Field", "Value"], _request_detail_rows(request_details))


def _request_subject(status, request_details):
    request_details = request_details or {}
    request_name = request_details.get("request_name") or "Unknown Request"
    request_id = request_details.get("id") or "Unknown ID"
    return f"CPA Tool {status} | Request: {request_name} | ID: {request_id}"


def _latest_log_path(run_dir):
    """Return the latest log location; never attach its full contents to mail."""
    if not run_dir:
        return ""

    logs_dir = Path(run_dir) / "logs"
    if not logs_dir.exists():
        return ""

    log_files = sorted(logs_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not log_files:
        return ""
    return str(log_files[0])


def _short_error_reason(error_msg, max_length=360):
    """Reduce a traceback/subprocess dump to the actionable reason only."""
    text = str(error_msg or "Request processing failed.")
    ignored_prefixes = ("traceback", "file ", "raise ", "during handling")
    candidates = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.lower().startswith(ignored_prefixes):
            continue
        if cleaned.startswith("^"):
            continue
        candidates.append(cleaned)
    reason = " ".join(candidates) if candidates else text.strip()
    reason = " ".join(reason.split())
    if len(reason) > max_length:
        reason = reason[:max_length - 1].rstrip() + "…"
    return reason or "Request processing failed."


def _error_marker_path(run_dir):
    if not run_dir:
        return None
    return Path(run_dir) / "logs" / "error_email_sent.json"


def error_notification_sent(run_dir):
    """Return True only when this request run already sent an error notice."""
    marker = _error_marker_path(run_dir)
    return bool(marker and marker.exists())


def _write_error_marker(run_dir, subject, reason):
    marker = _error_marker_path(run_dir)
    if not marker:
        return
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        with open(str(marker), "w") as handle:
            json.dump({"subject": subject, "reason": reason, "sent_at": datetime.utcnow().isoformat()}, handle)
    except Exception as exc:
        logging.warning("Unable to write error-email marker: %s", exc)


def _output_detail_rows(file_details, include_private_paths):
    """Build role-appropriate output rows for email bodies."""
    rows = []
    for detail in file_details:
        merge_note = detail.get("merge_mode") or "CURRENT_ONLY"
        if detail.get("merge_source_request_id"):
            source_name = detail.get("merge_source_request_name") or ""
            merge_note += " — source #{0}{1}".format(
                detail["merge_source_request_id"],
                " ({0})".format(source_name) if source_name else "",
            )
        if not include_private_paths:
            rows.append([
                detail.get("channel") or "-",
                detail.get("filename") or "-",
                detail.get("delivery_header") or "-",
                detail.get("file_count") or 0,
                detail.get("ftp_path") or "-",
                merge_note,
            ])
            continue
        rows.append([
            detail.get("channel") or "-",
            detail.get("filename") or "-",
            detail.get("delivery_header") or "-",
            detail.get("file_count") or 0,
            detail.get("ftp_path") or "-",
            detail.get("path") or detail.get("local_output_dir") or "-",
            "{0} | header: {1} | count: {2}".format(
                detail.get("final_s3_path") or "-",
                detail.get("final_data_header") or "-",
                detail.get("final_count") if detail.get("final_count") not in (None, "") else "-",
            ),
            "{0} | header: {1} | count: {2}".format(
                detail.get("complete_s3_path") or "-",
                detail.get("complete_data_header") or "-",
                detail.get("complete_count") if detail.get("complete_count") not in (None, "") else "-",
            ),
            merge_note,
        ])
    return rows


def _output_details_html(file_details, include_private_paths):
    rows = _output_detail_rows(file_details, include_private_paths)
    if not rows:
        return "<p>No delivery file was generated because no matching data was returned.</p>"
    if include_private_paths:
        headers = [
            "Channel", "File", "Delivery Header", "Final Count", "FTP Path",
            "Local Output", "Final Data (S3)", "Complete Data (S3)", "Merge",
        ]
    else:
        headers = ["Channel", "File", "Header", "Final Count", "FTP Path", "Merge"]
    return _html_table(headers, rows)


def _output_details_text(file_details, include_private_paths):
    rows = _output_detail_rows(file_details, include_private_paths)
    if not rows:
        return "No delivery file was generated because no matching data was returned."
    lines = []
    for row in rows:
        lines.append(" | ".join(str(value) for value in row))
    return "\n".join(lines)


def send_success_email(request_details, results, run_dir):
    """
    Success email with rich per-file details.

    Steps performed:
    1. Scan <run_dir>/FINAL_FILES/ to collect real file names, row counts
       and file sizes in bytes.
    2. Merge row_count / channel from the `results` dict.
    3. Write <run_dir>/logs/filedetails.json  (persists for audit and DB
       upsert which is handled by app.py:_persist_filedetails_to_db after
       the subprocess returns).
    4. Build the email body by reading from filedetails.json (so mail
       content is always consistent with what was persisted to disk/DB).
    5. Send the email.

    Parameters
    ----------
    request_details : dict – request metadata used in the subject and body
    results   : dict  – channel -> result dict from process_age_state_request()
    run_dir   : str or Path – the run directory (contains FINAL_FILES/ and logs/)

    Returns
    -------
    file_details : list  – list of file-detail dicts (also written to JSON + DB)
    json_path    : Path  – path to the written filedetails.json
    """
    run_dir_path    = Path(run_dir)
    final_files_dir = run_dir_path / "FINAL_FILES"
    logs_dir        = run_dir_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Build file details list ─────────────────────────────
    file_details = build_file_details_json(final_files_dir, results)

    # ── 2. Write filedetails.json to logs/ ────────────────────
    json_path = logs_dir / "filedetails.json"
    try:
        with open(str(json_path), "w") as jf:
            json.dump(file_details, jf, indent=2)
        logging.info(f"filedetails.json written -> {json_path}")
    except Exception as e:
        logging.error(f"Failed to write filedetails.json: {e}")

    # ── 3. Read back from JSON file for email body ─────────────
    email_file_details = _read_filedetails_json(json_path) if json_path.exists() else file_details

    # ── 4. Build role-aware text and HTML messages ──────────────
    cpa_view = _is_cpa_user(request_details)
    include_private_paths = not cpa_view
    file_section = _output_details_text(email_file_details, include_private_paths)
    output_html = _output_details_html(email_file_details, include_private_paths)
    subject = _request_subject("COMPLETED", request_details)
    private_note = (
        "The Final Data S3 export is delivery data. The Complete Data S3 export is audit data and includes ZIP/account fields by design."
        if include_private_paths else
        "This recipient view intentionally excludes local and S3 paths; use the FTP path to retrieve the delivery file."
    )
    body = """SUCCESS: Request completed successfully

Request Details:
{summary}

Generated Output Details:
{files}

{note}
""".format(summary=_request_summary(request_details), files=file_section, note=private_note)
    html_body = """<html><body style=\"font-family:Arial,sans-serif;color:#111827;\">
<p><strong>SUCCESS:</strong> Request completed successfully.</p>
<h3>Request Details</h3>{request_table}
<h3>Generated Output Details</h3>{output_table}
<p>{note}</p>
</body></html>""".format(
        request_table=_request_details_html(request_details),
        output_table=output_html,
        note=html.escape(private_note),
    )
    send_email(
        subject, body, is_error=False, html_body=html_body,
        recipients=_notification_recipients(request_details, is_error=False),
    )
    # Return both so callers that need the data or path can use them
    return file_details, json_path


def send_error_email(request_details, error_msg, run_dir=None, results=None):
    """Send a concise, deduplicated failure email with request context."""
    subject = _request_subject("FAILED", request_details)
    reason = _short_error_reason(error_msg)
    log_path = _latest_log_path(run_dir)
    final_files_dir = Path(run_dir) / "FINAL_FILES" if run_dir else None
    file_details = (
        build_file_details_json(final_files_dir, results)
        if final_files_dir and final_files_dir.exists() else []
    )
    cpa_view = _is_cpa_user(request_details)
    include_private_paths = not cpa_view
    output_text = (
        _output_details_text(file_details, include_private_paths)
        if file_details else "No completed delivery artifact was available."
    )
    body = """ERROR: Request failed

Request Details:
{summary}

Exit reason:
{reason}

Available Output Details:
{output_details}
{log_line}
""".format(
        summary=_request_summary(request_details), reason=reason,
        output_details=output_text,
        log_line=("\nSupport log: {0}".format(log_path)
                  if log_path and include_private_paths else ""),
    )
    html_body = """<html><body style=\"font-family:Arial,sans-serif;color:#111827;\">
<p><strong style=\"color:#b91c1c;\">ERROR:</strong> Request failed.</p>
<h3>Request Details</h3>{request_table}
<h3>Exit Reason</h3><p>{reason}</p>
<h3>Available Output Details</h3>{output_table}
{log_line}
</body></html>""".format(
        request_table=_request_details_html(request_details),
        reason=html.escape(reason),
        output_table=_output_details_html(file_details, include_private_paths),
        log_line=("<p><strong>Support log:</strong> {0}</p>".format(html.escape(log_path))
                  if log_path and include_private_paths else ""),
    )
    sent = send_email(
        subject, body, is_error=True, html_body=html_body,
        recipients=_notification_recipients(request_details, is_error=True),
    )
    if sent:
        _write_error_marker(run_dir, subject, reason)
    return sent
