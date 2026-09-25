# CPA Request Portal

CPA Tool is a Flask/MySQL request portal with Snowflake, S3, and FTP-backed
output processing. It has one unified processor for every non-DoorDash
request and separate modules for DoorDash and previous-output merging.

## Runtime modules

| Module | Responsibility |
| --- | --- |
| `app.py` | Validates the form, saves the request/criteria in MySQL, and starts `main.py` in the background. |
| `main.py` | Routes DoorDash requests to the DoorDash processor; routes every Suppression/Mailing request to the consolidated processor. |
| `REQUEST_PROCESSOR/request_processor.py` | Main non-DoorDash engine. Handles one or many Age, State, ZIP, and Gender criteria using OR logic, responder match, S3 exports, optional merge, Orange ESP output, FTP delivery, and detailed logs. |
| `REQUEST_PROCESSOR/zip_radius.py` | Expands a selected ZIP file by up to 100 miles using the separate `snowflake` SnowSQL connection, then loads the expanded ZIP list into the normal `datateam1` staging table. |
| `Doordash/doordash_zips.py` | DoorDash ZIP workflow. It imports only common low-level helpers from the consolidated processor and does not depend on a `ZIPS` module. |
| `MERGE_OUTPUT/merge_output.py` | Merges compatible current/previous S3 files in Snowflake so large files are never loaded into pandas. |
| `utils.py` | Shared command execution, output-directory, and notification helpers. |

The old `AGE_STATE`, `ZIPS`, and `MULTI_CRITERIA` runtime modules are retired.

## Request routing

1. The UI builds `criteria_json` from selected Age, State, ZIP, and/or Gender
   rows. A criterion can be selected once; matching uses OR logic. Gender
   accepts exactly one choice, Male or Female.
2. `app.py` validates request/client-name uniqueness, responder days, ZIP radius,
   optional merge eligibility, criteria values, channels, and ZIP uploads.
3. `main.py` receives the saved request ID.
4. For `Suppression` and `Mailing`, `main.py` calls
   `REQUEST_PROCESSOR.request_processor.process_request`.
5. For `Doordash`, `main.py` calls
   `Doordash.doordash_zips.process_doordash_zip_request`.

## Consolidated processor flow

For a non-DoorDash request the processor:

1. Reads the persisted request and normalized criteria JSON from MySQL.
2. Uploads and loads a shared Snowflake ZIP staging table only when a ZIP
   criterion is present. If ZIP radius is enabled, expands the source ZIPs in
   the second Snowflake connection and copies the expanded ZIPs into that
   same staging table before channel processing.
3. Builds one per-channel Snowflake query with an OR predicate across all
   selected criteria.
4. Applies an optional responder join. Green uses `CHANNELNAME='GREEN'` and
   Blue uses `CHANNELNAME='ORANGE'` in `RAW_OPENS_FOLLOWUP`.
5. Exports FINAL and COMPLETE datasets to S3, then drops temporary Snowflake
   tables. COMPLETE carries `email` plus each selected criterion field in
   selection order: `age`, `state`, `zip`, and/or `gender`. Orange COMPLETE inserts
   `accountname` immediately after `email`.
6. Downloads the final data, optionally merges a compatible previous request
   in Snowflake, then writes delivery artifacts.
7. Creates Orange delivery output from account/ESP data: FINAL is email-only
   for Suppression, and `email|accountname` for Mailing. Mailing delivery
   contains one ESP file per account inside a ZIP. A separate private Orange
   source retains account names for future cross-type merges even when the
   public Suppression FINAL contains only email.
8. Posts deliverables to FTP, stores S3/FTP/count metadata, updates status,
   sends notification, and removes temporary files/tables.

DoorDash posts ZIP archives only for the combined email and MD5 outputs and
Orange. The generated CSV data remains available in S3 for later merges.

Every major action is logged in the request's `logs/` directory.

## ZIP radius configuration

ZIP radius is available to requests with a ZIP criterion and to DoorDash.
Select 1–100 miles on the form; the processor uses kilometers in the distance
comparison (`miles * 1.609347218694`). Both SnowSQL connections must be able
to use the configured S3 bucket. Configure the alternate `snowflake`
connection with SELECT access to
`ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE` and CREATE TABLE access in its
working schema. ZIP lists are loaded to that connection, expanded and
unloaded to S3, then loaded into the normal `datateam1` ZIP staging table.
The source ZIPs are retained even when no zero-mile distance row exists.
The `snowflake` connection is `zx_dataops_service` in `HUBUSERS.ZX_DATAOPS`.
Export its key's `SNOWSQL_PRIVATE_KEY_PASSPHRASE` **before starting Flask**;
the background worker captures that value before the main processor changes
the process environment for `datateam1`. A dedicated
`CPA_ZIP_RADIUS_SNOWSQL_PASSPHRASE` can override the source key. Keep
`config.SNOWSQL_PASSPHRASE` configured separately for `datateam1`
(`DATATEAM_DP_SERVICE`), as it is used by all channel queries. The radius
query switches to `ADHOC_L_WH` (override with
`CPA_ZIP_RADIUS_SOURCE_WAREHOUSE`), then exports results to the shared S3
prefix. Source ZIP files can be headerless; the loader does not skip their
first ZIP. Each SnowSQL operation has a 300-second default timeout
(`CPA_ZIP_RADIUS_SQL_TIMEOUT_SECONDS`) and cannot wait for interactive input.

## Runtime configuration

Database and FTP credentials are intentionally not stored in tracked
application modules. Configure the service environment before starting Flask:

```bash
export FLASK_SECRET_KEY='replace-with-a-random-value'
export CPA_DB_HOST='…'
export CPA_DB_USER='…'
export CPA_DB_PASSWORD='…'
export CPA_DB_NAME='CUST_TECH_DB'
export CPA_FTP_USERNAME='…'
export CPA_FTP_PASSWORD='…'
export CPA_FTP_HOST='…'
# Private-key passphrase for the snowflake (zx_dataops_service) connection:
export SNOWSQL_PRIVATE_KEY_PASSPHRASE='…'
export CPA_EMAIL_TECH_RECIPIENTS='…'
export CPA_EMAIL_DATATEAM_RECIPIENTS='…'
export CPA_EMAIL_CPA_RECIPIENTS='…'
export CPA_EMAIL_CPA_USERNAMES='cpauser'
# Only if the snowflake connection requires its own private-key passphrase:
# export CPA_ZIP_RADIUS_SNOWSQL_PASSPHRASE='…'
export CPA_ZIP_RADIUS_AWS_KEY_ID='…'
export CPA_ZIP_RADIUS_AWS_SECRET_KEY='…'
```

## Notification views

Every completion/failure message includes a request-details table with request
name, client name, request type, criteria/value/comparison, channels, responder
match days, requested ZIP radius, and merge source. Error e-mails contain a short exit reason and a
support-log location rather than embedding the full log.

`CPA_EMAIL_CPA_USERNAMES` controls the FTP-only recipient view.  Those users
receive file name, header, final count, FTP path, and merge status.  Technical
and Data Team recipients additionally receive the local output location and
the `FINAL`/`COMPLETE` S3 paths, headers, and counts.  `COMPLETE` is audit data
and may include ZIP/account fields even when a delivery file contains only
`email`.

## Previous-output merge

When a prior completed request has output for a selected channel, Snowflake
loads that channel's prior FINAL S3 export and the current FINAL export into
temporary staging, normalizes emails with `LOWER(TRIM(email))`, and keeps one
row per email. The previous row wins if an email appears in both. The merged
FINAL is written to a new path for the **current** request, replacing its
local delivery input and updating its path/count; the previous request stays
unchanged. Missing previous channel output leaves that channel current-only.
COMPLETE remains an audit of the current query and is not merged. Orange
retains a separate two-column email/account source across Suppression/Mailing
merges so later Mailing delivery can still group inherited emails by ESP.

Use `.env.example` as the variable reference. Existing Snowflake/S3 settings
remain in the deployment's protected runtime configuration.

## Local validation

```bash
python3.9 -m py_compile \
  app.py main.py utils.py \
  REQUEST_PROCESSOR/request_processor.py \
  REQUEST_PROCESSOR/zip_radius.py \
  Doordash/doordash_zips.py \
  MERGE_OUTPUT/merge_output.py

node --check static/request-form.js
python3 -m unittest discover -s tests -v
git diff --check
```

Run the portal with the site's supported Python version, for example:

```bash
python3.6 app.py
```
