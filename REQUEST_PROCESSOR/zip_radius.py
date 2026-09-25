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
import time
import uuid

import config


_MILES_TO_KM = "1.609347218694"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The main processor sets this global SnowSQL variable to the datateam key
# before creating its ZIP staging table.  Capture the HUBUSERS key supplied
# to the worker at startup while it still has its original value.
_SOURCE_PASSPHRASE_AT_STARTUP = os.getenv("SNOWSQL_PRIVATE_KEY_PASSPHRASE", "")


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


def _sql_timeout_seconds():
    value = os.getenv("CPA_ZIP_RADIUS_SQL_TIMEOUT_SECONDS", "300")
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        raise ValueError("CPA_ZIP_RADIUS_SQL_TIMEOUT_SECONDS must be an integer.")
    if not 10 <= seconds <= 3600:
        raise ValueError("CPA_ZIP_RADIUS_SQL_TIMEOUT_SECONDS must be 10-3600.")
    return seconds


def _run_snow_sql(connection, query, log, key, secret, *, passphrase=None):
    """Send SQL from a private file with credentials scoped to this connection.

    Always give each connection its own passphrase in the child environment.
    Standard input is closed so a missing key cannot leave a request waiting
    for an interactive SnowSQL prompt.
    """
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
        else:
            environment.pop("SNOWSQL_PRIVATE_KEY_PASSPHRASE", None)
        log.debug("ZIP radius SnowSQL query on %s: %s", connection,
                  _redact(query, key, secret))
        timeout_seconds = _sql_timeout_seconds()
        log.info("ZIP radius SnowSQL started: connection=%s timeout=%ss",
                 connection, timeout_seconds)
        started = time.monotonic()
        try:
            result = subprocess.run(
                command, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=timeout_seconds,
                env=environment,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                "ZIP radius SnowSQL timed out after {0}s on {1}; check the "
                "connection, private-key passphrase, and warehouse.".format(
                    timeout_seconds, connection
                )
            )
        if result.returncode:
            detail = _redact(result.stderr or result.stdout, key, secret).strip()
            raise RuntimeError(
                "ZIP radius SnowSQL query failed on {0} (exit {1}): {2}".format(
                    connection, result.returncode, detail[:1000]
                )
            )
        log.info("ZIP radius SnowSQL finished: connection=%s elapsed=%.1fs",
                 connection, time.monotonic() - started)
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
    # At worker startup SNOWSQL_PRIVATE_KEY_PASSPHRASE belongs to the source
    # ZX_DATAOPS_SERVICE connection. The datateam processor later replaces the
    # process variable with its own passphrase, so use the captured original
    # value for HUBUSERS and the deployment's config for DATATEAM_DP_SERVICE.
    source_passphrase = (os.getenv("CPA_ZIP_RADIUS_SNOWSQL_PASSPHRASE")
                         or _SOURCE_PASSPHRASE_AT_STARTUP)
    target_passphrase = getattr(config, "SNOWSQL_PASSPHRASE", "")
    source_warehouse = os.getenv("CPA_ZIP_RADIUS_SOURCE_WAREHOUSE", "ADHOC_L_WH")
    if source_warehouse:
        source_warehouse = _identifier(source_warehouse, "ZIP radius source warehouse")

    def execute(connection, query):
        passphrase = (source_passphrase if connection == source_connection
                      else target_passphrase)
        return _run_snow_sql(connection, query, log, key, secret,
                             passphrase=passphrase)

    log.info("ZIP radius: request=%s radius=%s miles (%s km per mile)",
             request_id, radius_miles, _MILES_TO_KM)
    log.info("ZIP radius: source=%s table=%s; destination=%s table=%s",
             source_connection, source_table, destination_connection, destination_table)
    log.info("ZIP radius: source passphrase available=%s; destination passphrase available=%s",
             bool(source_passphrase), bool(target_passphrase))
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
                "CREATE TABLE {0} (ZIP_CODE VARCHAR(10));".format(source_table))
        source_created = True

        log.info("ZIP radius step 2: load provided ZIPs from %s", source_s3_path)
        execute(source_connection, (
            "COPY INTO {0} FROM '{1}' {2} "
            "FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER=',' SKIP_HEADER=0 "
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
        radius_query = (
            "COPY INTO '{output}' FROM ("
            " SELECT DISTINCT LPAD(TRIM(TO_VARCHAR(d.TARGET_ZIP)), 5, '0') AS ZIP_CODE"
            " FROM ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE d"
            " WHERE d.SOURCE_ZIP IN ("
            "   SELECT DISTINCT LPAD(TRIM(ZIP_CODE), 5, '0')"
            "   FROM {source} WHERE REGEXP_LIKE(TRIM(ZIP_CODE), '^[0-9]{{1,5}}$')"
            " )"
            "   AND d.DISTANCE <= ({radius} * {km_per_mile})"
            "   AND REGEXP_LIKE(TRIM(TO_VARCHAR(d.TARGET_ZIP)), '^[0-9]{{1,5}}$')"
            " UNION"
            " SELECT DISTINCT LPAD(TRIM(ZIP_CODE), 5, '0') AS ZIP_CODE"
            " FROM {source} WHERE REGEXP_LIKE(TRIM(ZIP_CODE), '^[0-9]{{1,5}}$')"
            " ) {credentials}"
            " FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER='|')"
            " HEADER=TRUE;"
        ).format(output=output_s3_path, source=source_table,
                 radius=radius_miles, km_per_mile=_MILES_TO_KM,
                 credentials=credentials)
        if source_warehouse:
            # Each -f invocation opens a new SnowSQL session, so the warehouse
            # change must be in the same file as the distance query.
            radius_query = "USE WAREHOUSE {0};\n{1}".format(
                source_warehouse, radius_query
            )
        execute(source_connection, radius_query)

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
