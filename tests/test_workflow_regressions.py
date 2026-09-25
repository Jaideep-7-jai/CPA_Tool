"""Small, offline checks for the request paths that failed in production."""

import gzip
import json
import logging
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

# The production service has PyMySQL; offline test runners may not. These
# tests exercise SQL construction only and never create a database connection.
try:
    import pymysql  # noqa: F401
except ImportError:
    sys.modules["pymysql"] = types.ModuleType("pymysql")

from Doordash import doordash_zips as doordash
from MERGE_OUTPUT import merge_output as merge
from REQUEST_PROCESSOR import request_processor as processor
from REQUEST_PROCESSOR import zip_radius
import utils


LOG = logging.getLogger(__name__)


class WorkflowRegressions(unittest.TestCase):
    def test_gender_is_one_choice_and_works_with_other_criteria(self):
        request = {
            "criteria_json": '[{"type":"gender","comparison":"include",'
                             '"values":["female"]},{"type":"age",'
                             '"comparison":"greater","value":"40"}]'
        }
        criteria = processor._criteria_from_request(request)
        self.assertEqual(criteria[0]["values"], ["FEMALE"])
        for channel in ("GREEN", "BLUE", "ORANGE", "ARCAMAX"):
            predicate = processor._criteria_predicate(channel, criteria, None, LOG)
            self.assertIn("'FEMALE'", predicate)
            self.assertIn(" OR ", predicate)
        for values in (["MALE", "FEMALE"], ["UNKNOWN"], []):
            request["criteria_json"] = (
                '[{"type":"gender","comparison":"include","values":' +
                json.dumps(values) + '}]'
            )
            with self.assertRaises(ValueError):
                processor._criteria_from_request(request)

    def test_gender_complete_only_and_arcamax_join_key(self):
        captured = []
        with patch.object(processor, "_query_copy_unload_rows",
                          side_effect=lambda sql, log: captured.append(sql) or 1):
            processor._export_complete_final_file(
                "COMPLETE", "TEST_TABLE", "s3://bucket/complete",
                "GREEN", LOG, criteria_columns=["gender"], request_type="Mailing",
            )
            processor._export_complete_final_file(
                "FINAL", "TEST_TABLE", "s3://bucket/final",
                "GREEN", LOG, criteria_columns=["gender"], request_type="Mailing",
            )
        self.assertIn('GENDER AS "gender"', captured[0])
        self.assertNotIn('GENDER AS "gender"', captured[1])
        with patch.object(processor, "run_command", return_value="EMAIL_ADDRESS\n"):
            join = processor._arcamax_gender_join(LOG)
        self.assertIn("TO_VARCHAR(EMAIL_ADDRESS)", join)
        self.assertIn("GROUP BY email_key", join)

    def test_zip_radius_does_not_reuse_other_connection_passphrase(self):
        environments = []

        def fake_run(command, **kwargs):
            environments.append((command, kwargs["env"], kwargs["stdin"]))
            return types.SimpleNamespace(returncode=0, stdout="1\n", stderr="")

        with patch.dict(os.environ, {"SNOWSQL_PRIVATE_KEY_PASSPHRASE": "datateam-key"}):
            with patch.object(zip_radius.subprocess, "run", side_effect=fake_run):
                zip_radius._run_snow_sql("snowflake", "SELECT 1;", LOG, "a", "b")
                zip_radius._run_snow_sql(
                    "datateam1", "SELECT 1;", LOG, "a", "b",
                    passphrase="datateam-key",
                )
        self.assertNotIn("SNOWSQL_PRIVATE_KEY_PASSPHRASE", environments[0][1])
        self.assertEqual(environments[1][1]["SNOWSQL_PRIVATE_KEY_PASSPHRASE"],
                         "datateam-key")
        self.assertEqual(environments[0][2], subprocess.DEVNULL)

    def test_zip_radius_loads_hubusers_then_datateam_with_separate_keys(self):
        executed = []
        destination_counts = iter((0, 38601))

        def fake_snow_sql(connection, query, log, key, secret, *, passphrase=None):
            executed.append((connection, query, passphrase))
            if query.startswith("SELECT COUNT(*)"):
                if connection == "datateam1":
                    return "{0}\n".format(next(destination_counts))
                return "23040\n"
            return ""

        environment = {
            "SNOWSQL_PRIVATE_KEY_PASSPHRASE": "datateam-key",
            "CPA_ZIP_RADIUS_AWS_KEY_ID": "test-key",
            "CPA_ZIP_RADIUS_AWS_SECRET_KEY": "test-secret",
            "CPA_ZIP_RADIUS_SOURCE_WAREHOUSE": "ADHOC_L_WH",
        }
        with patch.dict(os.environ, environment, clear=True):
            with patch.object(zip_radius, "_SOURCE_PASSPHRASE_AT_STARTUP", "source-key"):
                with patch.object(zip_radius.config, "SNOWSQL_PASSPHRASE", "datateam-key"):
                    with patch.object(zip_radius, "_run_snow_sql", side_effect=fake_snow_sql):
                        result = zip_radius.expand_zip_radius(
                            "s3://bucket/uploads/zip.csv", "DEST_ZIPS", 30, 179, LOG,
                            s3_base="s3://bucket/shared",
                        )
        self.assertEqual(result["source_count"], 23040)
        self.assertEqual(result["expanded_count"], 38601)
        source_queries = [query for connection, query, _ in executed
                          if connection == "snowflake"]
        dest_queries = [query for connection, query, _ in executed
                        if connection == "datateam1"]
        self.assertIn("CREATE TABLE", source_queries[0])
        self.assertIn("SKIP_HEADER=0", source_queries[1])
        self.assertIn("USE WAREHOUSE ADHOC_L_WH;", source_queries[3])
        self.assertIn("ZX_UNIFIED_PROFILE.PUBLIC.D_ZIP_DISTANCE", source_queries[3])
        self.assertIn("d.SOURCE_ZIP IN", source_queries[3])
        self.assertIn("SKIP_HEADER=1", dest_queries[-2])
        self.assertTrue(all(secret == "source-key" for connection, _, secret
                            in executed if connection == "snowflake"))
        self.assertTrue(all(secret == "datateam-key" for connection, _, secret
                            in executed if connection == "datateam1"))

    def test_zip_radius_snow_sql_timeout_is_bounded_and_noninteractive(self):
        def timeout(command, **kwargs):
            self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)
            self.assertEqual(kwargs["timeout"], 10)
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])

        with patch.dict(os.environ, {"CPA_ZIP_RADIUS_SQL_TIMEOUT_SECONDS": "10"}):
            with patch.object(zip_radius.subprocess, "run", side_effect=timeout):
                with self.assertRaisesRegex(RuntimeError, "timed out after 10s"):
                    zip_radius._run_snow_sql(
                        "snowflake", "SELECT 1;", LOG, "test-key", "test-secret",
                        passphrase="source-key",
                    )

    def test_doordash_md5_header_and_zip_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "md5_parts"
            destination = root / "md5_work"
            def fake_download(*args, **kwargs):
                source.mkdir(parents=True, exist_ok=True)
                with gzip.open(str(source / "data_0_0_0.csv.gz"), "wt") as out:
                    out.write("md5Hash\nabc123\n")
            with patch.object(processor, "run_command", side_effect=fake_download):
                lines = processor._download_and_combine(
                    "s3://bucket/md5", source, destination, "md5.csv", "GREEN", LOG,
                    final_header=["md5hash"],
                )
            self.assertEqual(lines, 2)
            csv_path = destination / "md5.csv"
            self.assertEqual(csv_path.read_text(), "md5hash\nabc123\n")
            zip_path = doordash._archive_delivery_csv(csv_path, LOG)
            import zipfile
            with zipfile.ZipFile(str(zip_path)) as archive:
                self.assertEqual(archive.namelist(), ["md5.csv"])
                self.assertEqual(archive.read("md5.csv"), b"md5hash\nabc123\n")

    def test_merge_fails_on_sql_error_and_keeps_md5_header(self):
        captured = []
        def fake_command(command, **kwargs):
            captured.append((command, Path(command[command.index("-f") + 1]).read_text()))
        with patch.object(merge, "run_command", side_effect=fake_command):
            merge._snowflake_merge(
                "s3://bucket/previous", "s3://bucket/current",
                "s3://bucket/merged", "DOORDASH_MD5HASH", 166, LOG,
            )
        command, sql = captured[0]
        self.assertIn("exit_on_error=true", command)
        self.assertIn('email AS "md5hash"', sql)
        self.assertEqual(
            merge._merged_s3_path("s3://bucket/ORANGE_FINAL", Path("out.csv")),
            "s3://bucket/ORANGE_FINAL_MERGED/OUT",
        )

    def test_md5_merge_download_is_atomic_and_uses_md5_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            current = Path(tmp) / "md5.csv"
            current.write_text("md5hash\noriginal\n")

            def part_with_header(header):
                def fake_download(command):
                    destination = Path(command[4])
                    destination.mkdir(parents=True, exist_ok=True)
                    with gzip.open(str(destination / "data_0_0_0.csv.gz"), "wt") as out:
                        out.write(header + "\nnewhash\n")
                return fake_download

            with patch.object(merge, "run_command", side_effect=part_with_header("md5hash")):
                count = merge._download_merged_export(
                    "s3://bucket/merged", current, "DOORDASH_MD5HASH", tmp, LOG,
                )
            self.assertEqual(count, 1)
            self.assertEqual(current.read_text(), "md5hash\nnewhash\n")

            with patch.object(merge, "run_command", side_effect=part_with_header("email")):
                with self.assertRaisesRegex(RuntimeError, "md5hash column"):
                    merge._download_merged_export(
                        "s3://bucket/merged", current, "DOORDASH_MD5HASH", tmp, LOG,
                    )
            self.assertEqual(current.read_text(), "md5hash\nnewhash\n")

    def test_notification_has_requested_radius_and_short_error(self):
        rows = dict(utils._request_detail_rows({"zip_radius": 25}))
        self.assertEqual(rows["ZIP Radius"], "25 mile(s)")
        reason = utils._short_error_reason(
            "Traceback (most recent call last):\n  File \"app.py\", line 3\n"
            "RuntimeError: Bad decrypt. Incorrect password?"
        )
        self.assertEqual(reason, "Bad decrypt. Incorrect password?")


if __name__ == "__main__":
    unittest.main()
