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
| `Doordash/doordash_zips.py` | DoorDash ZIP workflow. It imports only common low-level helpers from the consolidated processor and does not depend on a `ZIPS` module. |
| `MERGE_OUTPUT/merge_output.py` | Merges compatible current/previous S3 files in Snowflake so large files are never loaded into pandas. |
| `utils.py` | Shared command execution, output-directory, and notification helpers. |

The old `AGE_STATE`, `ZIPS`, and `MULTI_CRITERIA` runtime modules are retired.

## Request routing

1. The UI builds `criteria_json` from selected Age, State, ZIP, and/or Gender
   rows. A criterion can be selected once; matching uses OR logic.
2. `app.py` validates request/client-name uniqueness, responder days,
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
   criterion is present.
3. Builds one per-channel Snowflake query with an OR predicate across all
   selected criteria.
4. Applies an optional responder join. Green uses `CHANNELNAME='GREEN'` and
   Blue uses `CHANNELNAME='ORANGE'` in `RAW_OPENS_FOLLOWUP`.
5. Exports FINAL and COMPLETE datasets to S3, then drops temporary Snowflake
   tables.
6. Downloads the final data, optionally merges a compatible previous request
   in Snowflake, then writes delivery artifacts.
7. Creates Orange delivery output from account/ESP data: email-only for
   Suppression and one ESP file per account inside a ZIP for Mailing.
8. Posts deliverables to FTP, stores S3/FTP/count metadata, updates status,
   sends notification, and removes temporary files/tables.

Every major action is logged in the request's `logs/` directory.

## Gender column configuration

Gender is supported by the request contract and the consolidated query
builder. The default source columns are `b.GENDER` for Green/Blue, `GENDER`
for Arcamax, and `a.GENDER` for Orange. Override any source-specific value
without a code change using:

```bash
export CPA_GENDER_GREEN_COLUMN='b.GENDER'
export CPA_GENDER_BLUE_COLUMN='b.GENDER'
export CPA_GENDER_ARCAMAX_COLUMN='GENDER'
export CPA_GENDER_ORANGE_COLUMN='a.GENDER'
```

Confirm these columns against the Snowflake source schemas before enabling
Gender in production.

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
export CPA_EMAIL_TECH_RECIPIENTS='…'
export CPA_EMAIL_DATATEAM_RECIPIENTS='…'
export CPA_EMAIL_CPA_RECIPIENTS='…'
export CPA_EMAIL_CPA_USERNAMES='cpauser'
```

## Notification views

Every completion/failure message includes a request-details table with request
name, client name, request type, criteria/value/comparison, channels, responder
match days, and merge source.  Error e-mails contain a short exit reason and a
support-log location rather than embedding the full log.

`CPA_EMAIL_CPA_USERNAMES` controls the FTP-only recipient view.  Those users
receive file name, header, final count, FTP path, and merge status.  Technical
and Data Team recipients additionally receive the local output location and
the `FINAL`/`COMPLETE` S3 paths, headers, and counts.  `COMPLETE` is audit data
and may include ZIP/account fields even when a delivery file contains only
`email`.

Use `.env.example` as the variable reference. Existing Snowflake/S3 settings
remain in the deployment's protected runtime configuration.

## Local validation

```bash
python3.9 -m py_compile \
  app.py main.py utils.py \
  REQUEST_PROCESSOR/request_processor.py \
  Doordash/doordash_zips.py \
  MERGE_OUTPUT/merge_output.py

node --check static/request-form.js
git diff --check
```

Run the portal with the site's supported Python version, for example:

```bash
python3.6 app.py
```
