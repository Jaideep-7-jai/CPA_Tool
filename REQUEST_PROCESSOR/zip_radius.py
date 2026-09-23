"""Expand an uploaded ZIP list by distance in a second Snowflake account.

``expand_zip_radius`` is called after the normal request has uploaded its ZIP
file to S3 and created its (empty) ZIP staging table in the datateam account.
The returned table is that same datateam table, populated with the original
ZIPs plus ZIPs within the requested radius. The caller owns that table and
drops it after all selected channels finish.

The ``snowflake`` connection must have SELECT on
ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE and CREATE TABLE in its configured
staging schema. Both Snowflake connections must be able to read/write the
configured S3 prefix. Credentials are supplied through the deployment config
or environment, never embedded in this module or written to the logs.
"""

import os
import re
import subprocess
import tempfile
import uuid

import config


_MILES_TO_KM = "1.609347218694"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(value, description, allow_schema=False):
    parts = str(value or "").split(".")
    if not parts or (not allow_schema and len(parts) != 1) or len(parts) > 3:
        raise ValueError("Invalid {0}: {1!r}".format(description, value))
    if not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError("Invalid {0}: {1!r}".format(description, value))
    return ".".join(parts)


def _s3_path(value, description):
    value = str(value or "")
    if not re.match(r"^s3://[A-Za-z0-9._-]+/.+", value):
        raise ValueError("{0} must be a nonempty S3 path".format(description))
    if any(char in value for char in ("'", '"', "\n", "\r", "\\")):
        raise ValueError("{0} contains an unsupported character".format(description))
    return value


def _aws_credentials():
    key = os.getenv("CPA_ZIP_RADIUS_AWS_KEY_ID") or getattr(config, "AWS_KEY_ID", "")
    secret = os.getenv("CPA_ZIP_RADIUS_AWS_SECRET_KEY") or getattr(config, "AWS_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError(
            "ZIP radius needs CPA_ZIP_RADIUS_AWS_KEY_ID and "
            "CPA_ZIP_RADIUS_AWS_SECRET_KEY (or configured AWS credentials)."
        )
    if any(char in value for value in (key, secret) for char in ("'", "\n", "\r")):
        raise ValueError("ZIP radius AWS credentials contain an unsupported character")
    return key, secret


def _redact(message, key, secret):
    message = str(message or "")
    for value in (secret, key):
        if value:
            message = message.replace(value, "***")
    return re.sub(
        r"AWS_(?:KEY_ID|SECRET_KEY)\s*=\s*'[^']*'",
        "AWS_CREDENTIAL='***'", message, flags=re.IGNORECASE,
    )


def _run_snow_sql(connection, query, log, key, secret, *, passphrase=""):
    """Send SQL from a private temporary file, keeping keys out of argv/logs."""
    descriptor, query_path = tempfile.mkstemp(prefix="cpa_zip_radius_", suffix=".sql")
    try:
        with os.fdopen(descriptor, "w") as query_file:
            query_file.write(query.rstrip() + "\n")
        os.chmod(query_path, 0o600)
        command = [
            "snowsql", "-c", connection, "-f", query_path,
            "-o", "output_format=tsv", "-o", "header=false",
            "-o", "friendly=false", "-o", "timing=false",
            "-o", "exit_on_error=true", "-o", "echo=false",
        ]
        environment = os.environ.copy()
        if passphrase:
            environment["SNOWSQL_PRIVATE_KEY_PASSPHRASE"] = passphrase
        log.debug("ZIP radius SnowSQL query on %s: %s", connection,
                  _redact(query, key, secret))
        try:
            result = subprocess.run(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=3600, env=environment,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("ZIP radius SnowSQL query timed out on {0}".format(connection))
        if result.returncode:
            detail = _redact(result.stderr or result.stdout, key, secret).strip()
            raise RuntimeError(
                "ZIP radius SnowSQL query failed on {0} (exit {1}): {2}".format(
                    connection, result.returncode, detail[:1000]
                )
            )
        return result.stdout or ""
    finally:
        os.unlink(query_path)


def _count_rows(connection, table, log, key, secret, passphrase):
    output = _run_snow_sql(
        connection, "SELECT COUNT(*) FROM {0};".format(table), log,
        key, secret, passphrase=passphrase,
    )
    rows = [line.strip().strip('"') for line in output.splitlines()]
    numbers = [int(row) for row in rows if row.isdigit()]
    if len(numbers) != 1:
        raise RuntimeError("Could not verify the ZIP radius row count in {0}".format(table))
    return numbers[0]


def expand_zip_radius(source_s3_path, destination_table, radius_miles,
                      request_id, log, *, s3_base=None):
    """Load radius-expanded ZIPs into an existing datateam ZIP staging table.

    Returns ``{'table', 'source_count', 'expanded_count', 's3_path'}``.
    ``radius_miles`` must be an integer from 1 to 100; zero means the caller
    should follow its normal ZIP path instead of calling this function.
    """
    if isinstance(radius_miles, bool) or not str(radius_miles).isdigit():
        raise ValueError("ZIP radius must be a whole number from 1 to 100 miles.")
    radius_miles = int(radius_miles)
    if not 1 <= radius_miles <= 100:
        raise ValueError("ZIP radius must be between 1 and 100 miles.")
    if isinstance(request_id, bool) or not str(request_id).isdigit():
        raise ValueError("ZIP radius requires a numeric request ID.")
    source_s3_path = _s3_path(source_s3_path, "Original ZIP file")
    destination_table = _identifier(destination_table, "datateam ZIP table", allow_schema=True)
    base = _s3_path(s3_base or os.getenv("CPA_ZIP_RADIUS_S3_BASE") or config.S3_BASE,
                    "ZIP radius S3 base")
    source_connection = _identifier(
        os.getenv("CPA_ZIP_RADIUS_SOURCE_CONNECTION", "snowflake"),
        "ZIP radius source connection",
    )
    destination_connection = _identifier(
        os.getenv("CPA_ZIP_RADIUS_TARGET_CONNECTION", "datateam1"),
        "ZIP radius destination connection",
    )
    staging_schema = os.getenv("CPA_ZIP_RADIUS_STAGING_SCHEMA", "")
    if staging_schema:
        staging_schema = _identifier(staging_schema, "ZIP radius source schema", allow_schema=True)
    token = uuid.uuid4().hex[:12].upper()
    source_table = "APT_CPA_RADIUS_SOURCE_{0}_{1}".format(request_id, token)
    if staging_schema:
        source_table = staging_schema + "." + source_table
    output_s3_path = "{0}/ZIP_RADIUS/{1}/{2}/expanded/".format(
        base.rstrip("/"), request_id, token.lower(),
    )
    key, secret = _aws_credentials()
    credentials = "CREDENTIALS=(AWS_KEY_ID='{0}' AWS_SECRET_KEY='{1}')".format(
        key, secret,
    )
    source_passphrase = (os.getenv("CPA_ZIP_RADIUS_SNOWSQL_PASSPHRASE")
                         or getattr(config, "SNOWSQL_PASSPHRASE", ""))
    target_passphrase = getattr(config, "SNOWSQL_PASSPHRASE", "")

    def execute(connection, query):
        passphrase = (source_passphrase if connection == source_connection
                      else target_passphrase)
        return _run_snow_sql(connection, query, log, key, secret,
                             passphrase=passphrase)

    log.info("ZIP radius: request=%s radius=%s miles (%s km per mile)",
             request_id, radius_miles, _MILES_TO_KM)
    log.info("ZIP radius: source=%s table=%s; destination=%s table=%s",
             source_connection, source_table, destination_connection, destination_table)
    existing_count = _count_rows(destination_connection, destination_table, log,
                                 key, secret, target_passphrase)
    if existing_count != 0:
        raise RuntimeError(
            "ZIP radius destination table must be empty before expansion: {0}".format(
                destination_table
            )
        )
    source_created = False
    try:
        log.info("ZIP radius step 1: create source ZIP table")
        execute(source_connection,
                "CREATE TRANSIENT TABLE {0} (ZIP_CODE VARCHAR(10));".format(source_table))
        source_created = True

        log.info("ZIP radius step 2: load provided ZIPs from %s", source_s3_path)
        execute(source_connection, (
            "COPY INTO {0} FROM '{1}' {2} "
            "FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 "
            "FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
            "ON_ERROR='ABORT_STATEMENT';"
        ).format(source_table, source_s3_path, credentials))
        source_count = _count_rows(source_connection, source_table, log, key,
                                   secret, source_passphrase)
        if source_count < 1:
            raise RuntimeError("ZIP radius source file did not load any ZIP values.")
        log.info("ZIP radius: loaded %s provided ZIP rows", source_count)

        log.info("ZIP radius step 3: find target ZIPs and unload to %s", output_s3_path)
        # Preserve the original ZIPs even if the distance table has no row
        # recording the source ZIP's distance from itself.
        execute(source_connection, (
            "COPY INTO '{output}' FROM ("
            " SELECT DISTINCT LPAD(TRIM(TO_VARCHAR(d.TARGET_ZIP)), 5, '0') AS ZIP_CODE"
            " FROM ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE d"
            " JOIN (SELECT DISTINCT LPAD(TRIM(ZIP_CODE), 5, '0') AS ZIP_CODE"
            "       FROM {source}"
            "       WHERE REGEXP_LIKE(TRIM(ZIP_CODE), '^[0-9]{{1,5}}$')) s"
            "   ON LPAD(TRIM(TO_VARCHAR(d.SOURCE_ZIP)), 5, '0') = s.ZIP_CODE"
            " WHERE d.DISTANCE <= ({radius} * {km_per_mile})"
            "   AND REGEXP_LIKE(TRIM(TO_VARCHAR(d.TARGET_ZIP)), '^[0-9]{{1,5}}$')"
            " UNION"
            " SELECT DISTINCT LPAD(TRIM(ZIP_CODE), 5, '0') AS ZIP_CODE"
            " FROM {source} WHERE REGEXP_LIKE(TRIM(ZIP_CODE), '^[0-9]{{1,5}}$')"
            " ) {credentials}"
            " FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|')"
            " HEADER=TRUE;"
        ).format(output=output_s3_path, source=source_table,
                 radius=radius_miles, km_per_mile=_MILES_TO_KM,
                 credentials=credentials))

        log.info("ZIP radius step 4: load expanded ZIPs into %s", destination_table)
        execute(destination_connection, (
            "COPY INTO {0} FROM '{1}' {2} "
            "FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|' SKIP_HEADER=1) "
            "ON_ERROR='ABORT_STATEMENT';"
        ).format(destination_table, output_s3_path, credentials))
        expanded_count = _count_rows(destination_connection, destination_table,
                                     log, key, secret, target_passphrase)
        if expanded_count < 1:
            raise RuntimeError("ZIP radius expansion returned no ZIP values.")
        log.info("ZIP radius: %s expanded ZIP rows ready in %s", expanded_count,
                 destination_table)
        return {
            "table": destination_table,
            "source_count": source_count,
            "expanded_count": expanded_count,
            "s3_path": output_s3_path,
        }
    finally:
        if source_created:
            try:
                log.info("ZIP radius step 5: drop source ZIP table %s", source_table)
                execute(source_connection,
                        "DROP TABLE IF EXISTS {0};".format(source_table))
            except Exception as error:
                log.warning("ZIP radius source table cleanup failed: %s", error)
