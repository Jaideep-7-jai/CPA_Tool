"""Small, offline checks for the request paths that failed in production."""

import ast
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
    def test_notifications_use_only_configured_cpa_and_tech_lists(self):
        with patch.object(utils, "CPAUSER_EMAIL", ["cpa@example.com"]):
            with patch.object(utils, "TECH_NOTIFICATION_RECIPIENTS", ["tech@example.com"]):
                with patch.dict(os.environ, {"CPA_EMAIL_TECH_RECIPIENTS": "unused@example.com",
                                          "CPA_EMAIL_CPA_RECIPIENTS": "unused@example.com"}):
                    self.assertEqual(utils._notification_recipients(
                        {"username": "cpauser"}, is_error=True),
                        ["cpa@example.com", "tech@example.com"])
                    self.assertEqual(utils._notification_recipients(
                        {"username": "techuser"}, is_error=True),
                        ["tech@example.com"])

    def test_request_job_timestamps_use_mysql_now(self):
        source = ast.parse(Path("app.py").read_text())
        update_node = next(node for node in source.body
                           if isinstance(node, ast.FunctionDef) and node.name == "update_request_db")
        commands = []
        class Cursor:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
            def execute(self, sql, values):
                commands.append((sql, values))
        connection = types.SimpleNamespace(cursor=lambda: Cursor(), close=lambda: None)
        clock = object()
        scope = {"get_db": lambda: connection, "_DB_NOW": clock, "_CHANNEL_COLUMNS": set()}
        exec(compile(ast.Module(body=[update_node], type_ignores=[]), "app.py", "exec"), scope)
        scope["update_request_db"]("request-1", overall_status="inprogress",
                                   started_at=clock)
        scope["update_request_db"]("request-1", overall_status="completed",
                                   finished_at=clock)
        for sql, values in commands:
            self.assertIn("=NOW()", sql)
            self.assertEqual(values[-1], "request-1")
            self.assertEqual(len(values), 2)

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

    def test_gender_complete_only_and_arcamax_profile_fields(self):
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
        with patch.object(processor, "run_command",
                          return_value="EMAIL_ADDRESS\nAGE\nSTATE\nZIP\nSEX\n"):
            fields = processor._arcamax_profile_columns(LOG, {"gender", "age"})
        self.assertEqual(fields["email"], "EMAIL_ADDRESS")
        self.assertEqual(fields["gender"], "SEX")
        self.assertEqual(fields["age"], "AGE")
        self.assertIn("a.SEX", processor._gender_expression("ARCAMAX", fields))
        self.assertIn("TRY_TO_NUMBER", processor._age_expression("ARCAMAX", fields))

    def test_arcamax_criteria_uses_new_profile_and_optional_open_date(self):
        captured = []
        def fake_run(command):
            sql = command[command.index("-q") + 1]
            if "INFORMATION_SCHEMA.COLUMNS" in sql:
                return "EMAIL\nAGE\nSTATE\nZIP\nSEX\n"
            captured.append(sql)
            return ""
        criteria = [{"type": "age", "comparison": "greater", "value": "40"},
                    {"type": "state", "comparison": "include", "values": ["CA"]},
                    {"type": "zips", "comparison": "include"},
                    {"type": "gender", "comparison": "include", "values": ["FEMALE"]}]
        with patch.object(processor, "run_command", side_effect=fake_run):
            with patch.object(processor, "_query_snowflake", return_value=23):
                count = processor._create_criteria_channel_table(
                    "TEST_ARCAMAX", "ARCAMAX", criteria, "TEST_ZIPS", True, 30, LOG)
        self.assertEqual(count, 23)
        sql = captured[0]
        self.assertIn("FROM GREEN.DT_DATA.APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE_DND_SF a", sql)
        self.assertNotIn("FROM APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE a", sql)
        self.assertIn("a.SEX", sql)
        self.assertIn("a.AGE", sql)
        self.assertIn("a.STATE IN ('CA')", sql)
        self.assertIn("a.ZIP IN (SELECT zip_code FROM TEST_ZIPS)", sql)
        self.assertIn("GREEN.DT_DATA.ARCAMAX_DELIVERY_LOGS", sql)
        self.assertIn("DATEADD(day, -30", sql)
        self.assertIn("OPEN_DATE", sql)
        self.assertEqual(processor._responder_join("ARCAMAX", False, None), "")

    def test_arcamax_profile_accepts_date_age_and_validates_required_columns(self):
        with patch.object(processor, "run_command",
                          return_value="EMAIL_ADDRESS\nBIRTHDAY\nZIP_CODE\nGENDER\n"):
            fields = processor._arcamax_profile_columns(LOG, {"age", "zips", "gender"})
        self.assertIn("DATEDIFF(year", processor._age_expression("ARCAMAX", fields))
        self.assertIn("a.GENDER", processor._gender_expression("ARCAMAX", fields))
        with patch.object(processor, "run_command", return_value="EMAIL\nZIP\n"):
            with self.assertRaisesRegex(RuntimeError, "missing required column.*gender"):
                processor._arcamax_profile_columns(LOG, {"gender"})

    def test_doordash_arcamax_zip_source_uses_new_profile(self):
        captured = []
        def fake_run(command):
            sql = command[command.index("-q") + 1]
            if "INFORMATION_SCHEMA.COLUMNS" in sql:
                return "EMAIL\nZIP\n"
            captured.append(sql)
            return ""
        with patch.object(processor, "run_command", side_effect=fake_run):
            with patch.object(processor, "_query_snowflake", return_value=7):
                count = processor._insert_into_perm_table(
                    "TEST_ARCAMAX", "ARCAMAX", "TEST_ZIPS", "include", True, 21, LOG)
        self.assertEqual(count, 7)
        self.assertIn("FROM GREEN.DT_DATA.APT_CUSTOM_ARCAMAX_CUSTOMER_TABLE_DND_SF a", captured[0])
        self.assertIn("a.ZIP IN (SELECT zip_code FROM TEST_ZIPS)", captured[0])
        self.assertIn("DATEADD(day, -21", captured[0])

    def test_legacy_zip_entry_points_are_archived(self):
        from old_module import zip_processor as legacy
        for name in ("process_zip_request", "process_green_blue_zip",
                     "process_arcamax_zip"):
            self.assertFalse(hasattr(processor, name))
            self.assertTrue(callable(getattr(legacy, name)))
        self.assertTrue(callable(processor.process_orange_zip))

    def test_orange_esp_join_has_space_before_on(self):
        statements = []
        with patch.object(processor, "run_command",
                          side_effect=lambda command: statements.append(command[command.index("-q") + 1])):
            with patch.object(processor, "_query_snowflake", return_value=5):
                processor._create_criteria_channel_table(
                    "TEST_ORANGE", "ORANGE",
                    [{"type": "gender", "comparison": "include", "values": ["MALE"]}],
                    None, False, None, LOG,
                )
        self.assertIn(") esp ON a.FEED_ID=esp.FEEDID ", statements[0])
        self.assertNotIn("espON", statements[0])

    def test_count_validation_rejects_snowflake_error_code(self):
        with patch.object(processor, "run_command", return_value=(
                "002003 (42S02): SQL compilation error:\n"
                "Object 'TEST_ORANGE' does not exist or not authorized.")) as command:
            with self.assertRaisesRegex(RuntimeError, "did not return one integer"):
                processor._query_snowflake("SELECT COUNT(*) FROM TEST_ORANGE", LOG)
        self.assertIn("exit_on_error=true", command.call_args[0][0])
        with patch.object(processor, "run_command", return_value="2003"):
            self.assertEqual(processor._query_snowflake("SELECT COUNT(*) FROM TEST_ORANGE", LOG), 2003)

    def test_orange_create_failure_stops_before_count(self):
        with patch.object(processor, "run_command",
                          side_effect=RuntimeError("SQL compilation error")) as command:
            with patch.object(processor, "_query_snowflake") as count:
                with self.assertRaisesRegex(RuntimeError, "SQL compilation error"):
                    processor._create_criteria_channel_table(
                        "TEST_ORANGE", "ORANGE",
                        [{"type": "gender", "comparison": "include", "values": ["MALE"]}],
                        None, True, 90, LOG,
                    )
        self.assertIn("exit_on_error=true", command.call_args[0][0])
        count.assert_not_called()

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
                with self.assertRaisesRegex(RuntimeError, "expected \\['md5hash'\\]"):
                    merge._download_merged_export(
                        "s3://bucket/merged", current, "DOORDASH_MD5HASH", tmp, LOG,
                    )
            self.assertEqual(current.read_text(), "md5hash\nnewhash\n")

    def test_merge_sql_contains_only_final_columns_and_pair_dedupe(self):
        scripts = {}
        def capture(command):
            scripts[next_channel[0]] = Path(command[command.index("-f") + 1]).read_text()
        next_channel = [None]
        with patch.object(merge, "run_command", side_effect=capture):
            for channel in ("GREEN", "ORANGE", "DOORDASH_EMAIL", "DOORDASH_MD5HASH"):
                next_channel[0] = channel
                merge._snowflake_merge("s3://test/previous", "s3://test/current",
                                        "s3://test/output", channel, 181, LOG)
        for channel, sql in scripts.items():
            self.assertNotIn("gender", sql.lower())
            self.assertNotIn("MERGED_PRIVATE", sql)
            self.assertIn("SKIP_HEADER=1", sql)
            if channel == "ORANGE":
                self.assertIn("PARTITION BY LOWER(TRIM(email)), LOWER(TRIM(account_name))", sql)
                self.assertIn('account_name AS "accountname"', sql)
                self.assertIn("email VARCHAR, account_name VARCHAR, source_order NUMBER", sql)
            else:
                self.assertIn("email VARCHAR, source_order NUMBER", sql)
                self.assertNotIn("account_name VARCHAR", sql)

    def test_orange_final_is_pair_for_suppression_and_default_responder_l90(self):
        for request_type in ("Suppression", "Mailing"):
            captured = []
            with patch.object(processor, "_query_copy_unload_rows",
                              side_effect=lambda sql, log: captured.append(sql) or 1):
                processor._export_complete_final_file(
                    "FINAL", "TEST_TABLE", "s3://test/orange", "ORANGE", LOG,
                    criteria_columns=["age"], request_type=request_type)
            self.assertIn('email_address AS "email", account_name AS "accountname"', captured[0])
            self.assertNotIn("GENDER", captured[0])
        join = processor._responder_join("ORANGE", False, None)
        self.assertIn("APT_CUSTOM_L120_ORANGE_UNIQ_RESPONDERS_UNIQ_DND", join)
        self.assertIn("DATEADD(day, -90", join)
        self.assertIn("DATEADD(day, -30", processor._responder_join("ORANGE", True, 30))
        self.assertEqual(processor._responder_join("GREEN", False, None), "")

    def test_previous_merge_source_rejects_other_request_type(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, sql, params): self.sql, self.params = sql, params
            def fetchone(self): return ("completed", "s3://test/source", "Mailing")
        class Connection:
            def cursor(self): return Cursor()
            def close(self): pass
        with patch.object(merge, "_db_connection", return_value=Connection()):
            with self.assertRaisesRegex(ValueError, "should be Suppression"):
                merge._previous_path(180, "ORANGE", LOG, "Suppression")

    def test_doordash_orange_uses_its_final_pair_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            current = Path(tmp) / "orange.csv"
            current.write_text("email_address|account_name\na@example.com|ESP1\n")
            with patch.object(merge, "_previous_path", return_value="s3://test/previous") as previous:
                with patch.object(merge, "_merge_to_current_file", return_value=(1, "s3://test/merged")) as execute:
                    with patch.object(processor, "update_channel_storage"):
                        with patch.object(merge, "_set_merge_status"):
                            with patch.object(merge, "update_orange_merge_source_storage"):
                                result = merge.merge_current_file(
                                    181, 180, "ORANGE", current,
                                    "s3://test/current", tmp, LOG,
                                    request_type="Doordash")
            previous.assert_called_once_with(180, "ORANGE", LOG, "Doordash")
            self.assertEqual(execute.call_args.args[3], "ORANGE")
            self.assertEqual(result["s3_path"], "s3://test/merged")

    def test_orange_suppression_delivers_unique_email_from_pair_final(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "orange.csv"
            raw.write_text("email|accountname\na@example.com|ESP1\n"
                           "a@example.com|ESP2\nb@example.com|ESP1\n")
            context = {"final_files_dir": root / "final", "channel_tmp": root,
                       "output_file": "out.csv"}
            context["final_files_dir"].mkdir()
            _, delivery, count = processor._write_orange_delivery(
                raw, context, "Suppression", LOG)
            self.assertEqual(count, 2)
            self.assertEqual(delivery.read_text(), "email\na@example.com\nb@example.com\n")

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
