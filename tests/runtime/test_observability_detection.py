import copy
import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from jobs.observability import run_observability_failure_smoke as detection  # noqa: E402


class ObservabilityThresholdConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.thresholds = detection.load_thresholds()

    def test_real_threshold_config_declares_every_required_threshold(self):
        configured = self.thresholds["thresholds"]
        self.assertEqual(self.thresholds["version"], 1)
        self.assertEqual(
            self.thresholds["scope"],
            "controlled_local_observability",
        )
        self.assertEqual(self.thresholds["runtime_profiles"], ["local-small"])
        self.assertEqual(configured["monitoring"]["minimum_events"], 5)
        self.assertEqual(
            set(
                configured["stage_duration"][
                    "maximum_seconds_by_stage"
                ]
            ),
            set(detection.EXPECTED_STAGES),
        )
        self.assertEqual(configured["volume"]["minimum_source_rows"], 1)
        self.assertEqual(
            set(configured["volume"]["minimum_layer_rows"]),
            set(detection.EXPECTED_LAYERS),
        )
        self.assertEqual(configured["volume"]["maximum_drop_percent"], 50.0)
        self.assertEqual(configured["quality"]["maximum_failures"], 0)
        self.assertEqual(configured["masking"]["maximum_failures"], 0)

    def test_invalid_threshold_documents_fail_strictly(self):
        invalid_documents = []

        wrong_version = copy.deepcopy(self.thresholds)
        wrong_version["version"] = 2
        invalid_documents.append(wrong_version)

        wrong_scope = copy.deepcopy(self.thresholds)
        wrong_scope["scope"] = "another_scope"
        invalid_documents.append(wrong_scope)

        unsupported_profile = copy.deepcopy(self.thresholds)
        unsupported_profile["runtime_profiles"] = ["local-medium"]
        invalid_documents.append(unsupported_profile)

        unknown_root = copy.deepcopy(self.thresholds)
        unknown_root["unexpected"] = True
        invalid_documents.append(unknown_root)

        missing_section = copy.deepcopy(self.thresholds)
        del missing_section["thresholds"]["masking"]
        invalid_documents.append(missing_section)

        missing_stage = copy.deepcopy(self.thresholds)
        del missing_stage["thresholds"]["stage_duration"][
            "maximum_seconds_by_stage"
        ]["gold"]
        invalid_documents.append(missing_stage)

        boolean_number = copy.deepcopy(self.thresholds)
        boolean_number["thresholds"]["monitoring"]["minimum_events"] = True
        invalid_documents.append(boolean_number)

        invalid_percent = copy.deepcopy(self.thresholds)
        invalid_percent["thresholds"]["volume"][
            "maximum_drop_percent"
        ] = 100.1
        invalid_documents.append(invalid_percent)

        for document in invalid_documents:
            with self.subTest(document=document):
                with self.assertRaises(detection.ThresholdConfigError):
                    detection.validate_threshold_config(document)


class ObservabilityEvaluatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.thresholds = detection.load_thresholds()

    def _healthy_observation(self):
        return {
            "source": {
                "name": "clientes",
                "exists": True,
                "schema_valid": True,
                "missing_columns": [],
                "observed_rows": 20,
                "reference_rows": 20,
            },
            "monitoring_events": 5,
            "stage_durations_seconds": {
                "generate_sample_data": 1.0,
                "bronze": 10.0,
                "raw_hubs": 10.0,
                "raw_links": 10.0,
                "raw_satellites": 10.0,
                "gold": 10.0,
            },
            "layer_rows": {
                "bronze": 20,
                "raw_vault_hubs": 10,
                "raw_vault_links": 10,
                "raw_vault_satellites": 10,
                "gold": 10,
            },
            "reference_layer_rows": {
                "bronze": 20,
                "raw_vault_hubs": 10,
                "raw_vault_links": 10,
                "raw_vault_satellites": 10,
                "gold": 10,
            },
            "quality_failures": 0,
            "masking_failures": 0,
        }

    def _evaluate_expected(self, observation, stage, rule):
        return detection.evaluate_observation(
            observation,
            self.thresholds,
            expected_stage=stage,
            expected_rule=rule,
        )

    def test_healthy_observation_has_no_detection(self):
        result = detection.evaluate_observation(
            self._healthy_observation(),
            self.thresholds,
        )
        self.assertEqual(result["detection_status"], "NOT_DETECTED")
        self.assertEqual(result["pipeline_status"], "SUCCESS")
        self.assertIsNone(result["failed_stage"])
        self.assertIsNone(result["rule_triggered"])

    def test_invalid_schema_is_attributed_to_bronze(self):
        observation = self._healthy_observation()
        observation["source"]["schema_valid"] = False
        observation["source"]["missing_columns"] = ["cliente_id"]
        result = self._evaluate_expected(
            observation,
            "bronze",
            "source.schema.required_columns",
        )
        self.assertEqual(result["detection_status"], "DETECTED")
        self.assertEqual(result["pipeline_status"], "FAILURE")
        self.assertEqual(result["failed_stage"], "bronze")
        self.assertEqual(
            result["rule_triggered"],
            "source.schema.required_columns",
        )
        self.assertNotIn("path", result["error_message"].lower())

    def test_missing_source_prefers_source_rule_over_volume_rules(self):
        observation = self._healthy_observation()
        observation["source"].update(
            {
                "exists": False,
                "schema_valid": None,
                "observed_rows": 0,
            }
        )
        result = self._evaluate_expected(
            observation,
            "bronze",
            "source.file.required",
        )
        self.assertEqual(result["failed_stage"], "bronze")
        self.assertEqual(result["rule_triggered"], "source.file.required")
        self.assertIn("volume.minimum_rows", result["triggered_rules"])
        self.assertIn(
            "volume.maximum_drop_percent",
            result["triggered_rules"],
        )

    def test_zero_volume_prefers_minimum_rows_and_records_drop(self):
        observation = self._healthy_observation()
        observation["source"]["observed_rows"] = 0
        result = self._evaluate_expected(
            observation,
            "bronze",
            "volume.minimum_rows",
        )
        self.assertEqual(result["failed_stage"], "bronze")
        self.assertEqual(result["rule_triggered"], "volume.minimum_rows")
        self.assertIn(
            "volume.maximum_drop_percent",
            result["triggered_rules"],
        )

    def test_expected_rule_never_biases_primary_detection(self):
        observation = self._healthy_observation()
        observation["source"].update(
            {
                "schema_valid": False,
                "missing_columns": ["cliente_id"],
                "observed_rows": 0,
            }
        )
        result = self._evaluate_expected(
            observation,
            "bronze",
            "volume.minimum_rows",
        )
        self.assertEqual(
            result["rule_triggered"],
            "source.schema.required_columns",
        )

    def test_header_only_source_preserves_schema_and_zero_volume(self):
        preflight = {
            "sources": [
                {
                    "source_name": "clientes",
                    "record_count": 0,
                    "schema_valid": False,
                }
            ],
            "failures": [
                {
                    "source_name": "clientes",
                    "missing_columns": ["cliente_id", "nome"],
                }
            ],
        }
        observation = detection.build_source_observation(
            preflight=preflight,
            source_name="clientes",
            source_exists=True,
            reference_rows=20,
            required_columns=["cliente_id", "nome"],
            observed_columns=["cliente_id", "nome"],
        )
        self.assertTrue(observation["schema_valid"])
        self.assertEqual(observation["missing_columns"], [])
        self.assertEqual(observation["observed_rows"], 0)
        result = self._evaluate_expected(
            {"source": observation},
            "bronze",
            "volume.minimum_rows",
        )
        self.assertEqual(result["rule_triggered"], "volume.minimum_rows")

    def test_stage_duration_threshold_attributes_the_measured_stage(self):
        observation = self._healthy_observation()
        observation["stage_durations_seconds"]["gold"] = 181.0
        result = self._evaluate_expected(
            observation,
            "gold",
            "stage.maximum_duration_seconds",
        )
        self.assertEqual(result["failed_stage"], "gold")
        self.assertEqual(
            result["rule_triggered"],
            "stage.maximum_duration_seconds",
        )

    def test_volume_drop_compares_only_the_same_layer_metric(self):
        observation = self._healthy_observation()
        observation["layer_rows"]["gold"] = 4
        observation["reference_layer_rows"]["gold"] = 10
        result = self._evaluate_expected(
            observation,
            "gold",
            "volume.maximum_drop_percent",
        )
        self.assertEqual(result["failed_stage"], "gold")
        self.assertEqual(
            result["rule_triggered"],
            "volume.maximum_drop_percent",
        )
        detection_item = next(
            item
            for item in result["detections"]
            if item["rule_id"] == "volume.maximum_drop_percent"
        )
        self.assertEqual(detection_item["metric"], "layer.gold.drop_percent")
        self.assertEqual(detection_item["observed"], 60.0)

    def test_monitoring_quality_and_masking_rules(self):
        cases = (
            (
                "monitoring_events",
                4,
                "observability",
                "monitoring.minimum_events",
            ),
            (
                "quality_failures",
                1,
                "data_quality",
                "quality.maximum_failures",
            ),
            (
                "masking_failures",
                1,
                "masking",
                "masking.maximum_failures",
            ),
        )
        for field, value, stage, rule in cases:
            with self.subTest(field=field):
                observation = self._healthy_observation()
                observation[field] = value
                result = self._evaluate_expected(
                    observation,
                    stage,
                    rule,
                )
                self.assertEqual(result["failed_stage"], stage)
                self.assertEqual(result["rule_triggered"], rule)


class ObservabilityPayloadAndExitCodeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.thresholds = detection.load_thresholds()

    def _invalid_schema_observation(self):
        return {
            "source": {
                "name": "clientes",
                "exists": True,
                "schema_valid": False,
                "missing_columns": ["cliente_id"],
                "observed_rows": 20,
                "reference_rows": 20,
            },
            "stage_durations_seconds": {"bronze": 1.0},
        }

    def _payload(self):
        observation = self._invalid_schema_observation()
        evaluation = detection.evaluate_observation(
            observation,
            self.thresholds,
            expected_stage="bronze",
            expected_rule="source.schema.required_columns",
        )
        started_at = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        return detection.build_failure_payload(
            scenario="invalid-schema",
            runtime_profile="local-small",
            batch_id="test-batch",
            started_at=started_at,
            finished_at=started_at + timedelta(seconds=1),
            evaluation=evaluation,
            observation=observation,
            observed_stage_status="FAILURE",
        )

    def test_payload_contains_required_fields_and_exit_one(self):
        payload = self._payload()
        detection.validate_payload_contract(payload)
        self.assertEqual(
            set(detection.REQUIRED_PAYLOAD_FIELDS) - set(payload),
            set(),
        )
        self.assertEqual(payload["process_exit_code"], 1)
        self.assertEqual(detection.exit_code_for_payload(payload), 1)
        self.assertEqual(payload["detection_status"], "DETECTED")
        self.assertEqual(payload["pipeline_status"], "FAILURE")
        self.assertEqual(payload["failed_stage"], "bronze")

    def test_wrong_rule_does_not_turn_incidental_detection_into_success(self):
        observation = {
            "stage_durations_seconds": {"bronze": 181.0},
        }
        evaluation = detection.evaluate_observation(
            observation,
            self.thresholds,
            expected_stage="bronze",
            expected_rule="source.schema.required_columns",
        )
        started_at = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        payload = detection.build_failure_payload(
            scenario="invalid-schema",
            runtime_profile="local-small",
            batch_id="wrong-rule",
            started_at=started_at,
            finished_at=started_at + timedelta(seconds=1),
            evaluation=evaluation,
            observation=observation,
            observed_stage_status="FAILURE",
        )
        self.assertEqual(
            payload["rule_triggered"],
            "stage.maximum_duration_seconds",
        )
        self.assertEqual(payload["process_exit_code"], 2)
        self.assertNotEqual(payload["process_exit_code"], 0)

    def test_detected_rule_requires_observed_bronze_failure(self):
        payload = self._payload()
        payload["observed_stage_status"] = "SUCCESS"
        payload["process_exit_code"] = detection.exit_code_for_payload(payload)
        self.assertEqual(payload["process_exit_code"], 2)
        detection.validate_payload_contract(payload)

    def test_payload_requires_timezone_and_rejects_local_paths(self):
        payload = self._payload()
        payload["started_at"] = "2026-07-25T12:00:00"
        with self.assertRaisesRegex(ValueError, "timezone"):
            detection.validate_payload_contract(payload)

        payload = self._payload()
        payload["error_message"] = (
            "Unexpected file "
            + "C:"
            + "\\Users\\example\\synthetic.csv"
        )
        with self.assertRaisesRegex(ValueError, "path"):
            detection.validate_payload_contract(payload)

    def test_payload_rejects_explicit_sensitive_values(self):
        payload = self._payload()
        payload["error_message"] = "Synthetic value 123.456.789-00"
        with self.assertRaisesRegex(ValueError, "forbidden value"):
            detection.validate_payload_contract(
                payload,
                forbidden_values=("123.456.789-00",),
            )

    def test_payload_rejects_unknown_fields_and_inconsistent_duration(self):
        payload = self._payload()
        payload["secret"] = "must-not-be-exported"
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            detection.validate_payload_contract(payload)

        payload = self._payload()
        payload["duration_seconds"] = 99.0
        with self.assertRaisesRegex(ValueError, "timestamps"):
            detection.validate_payload_contract(payload)

    def test_missing_required_payload_field_is_rejected(self):
        payload = self._payload()
        del payload["batch_id"]
        with self.assertRaisesRegex(ValueError, "batch_id"):
            detection.validate_payload_contract(payload)


class ObservabilityFailureSmokeMainTests(unittest.TestCase):
    def _runtime_result(self, observation, stage_status="FAILURE"):
        return {
            "observation": observation,
            "observed_stage_status": stage_status,
        }

    def _invalid_schema_observation(self):
        return {
            "source": {
                "name": "clientes",
                "exists": True,
                "schema_valid": False,
                "missing_columns": ["cliente_id"],
                "observed_rows": 20,
                "reference_rows": 20,
            },
            "stage_durations_seconds": {"bronze": 1.0},
        }

    def _run_main(
        self,
        runtime_side_effect,
        scenario="invalid-schema",
        batch_id="unit-test-batch",
    ):
        output = io.StringIO()
        argv = ["--scenario", scenario]
        if batch_id is not None:
            argv.extend(["--batch-id", batch_id])
        with patch.object(
            detection,
            "execute_runtime_scenario",
            side_effect=runtime_side_effect,
        ):
            with redirect_stdout(output):
                exit_code = detection.main(argv)
        rendered = output.getvalue()
        marker_lines = [
            line
            for line in rendered.splitlines()
            if line.startswith(detection.RESULT_MARKER)
        ]
        self.assertEqual(len(marker_lines), 1)
        self.assertEqual(
            rendered.strip().splitlines()[-1],
            marker_lines[0],
        )
        payload = json.loads(
            marker_lines[0][len(detection.RESULT_MARKER) :]
        )
        return exit_code, payload, rendered

    def test_main_emits_one_final_marker_and_exit_one(self):
        runtime_result = self._runtime_result(
            self._invalid_schema_observation()
        )
        exit_code, payload, _ = self._run_main(
            lambda _args, _thresholds: runtime_result
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["process_exit_code"], 1)
        self.assertEqual(payload["failed_stage"], "bronze")
        self.assertEqual(
            payload["rule_triggered"],
            "source.schema.required_columns",
        )

    def test_generated_batch_id_is_propagated_to_runtime_and_payload(self):
        observed = {}

        def capture_runtime(args, _thresholds):
            observed["batch_id"] = args.batch_id
            return self._runtime_result(
                self._invalid_schema_observation()
            )

        exit_code, payload, _ = self._run_main(
            capture_runtime,
            batch_id=None,
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(observed["batch_id"], payload["batch_id"])
        self.assertRegex(
            payload["batch_id"],
            r"^observability_failure_\d{8}T\d{6}Z$",
        )

    def test_missing_source_and_zero_volume_match_exact_contracts(self):
        cases = (
            (
                "missing-source",
                {
                    "source": {
                        "name": "clientes",
                        "exists": False,
                        "schema_valid": None,
                        "missing_columns": [],
                        "observed_rows": 0,
                        "reference_rows": 20,
                    },
                    "stage_durations_seconds": {"bronze": 1.0},
                },
                "source.file.required",
            ),
            (
                "zero-volume",
                {
                    "source": {
                        "name": "clientes",
                        "exists": True,
                        "schema_valid": True,
                        "missing_columns": [],
                        "observed_rows": 0,
                        "reference_rows": 20,
                    },
                    "stage_durations_seconds": {"bronze": 1.0},
                },
                "volume.minimum_rows",
            ),
        )
        for scenario, observation, expected_rule in cases:
            with self.subTest(scenario=scenario):
                exit_code, payload, _ = self._run_main(
                    lambda _args, _thresholds, current=observation: (
                        self._runtime_result(current)
                    ),
                    scenario=scenario,
                )
                self.assertEqual(exit_code, 1)
                self.assertEqual(payload["process_exit_code"], 1)
                self.assertEqual(payload["failed_stage"], "bronze")
                self.assertEqual(payload["rule_triggered"], expected_rule)

    def test_detected_rule_with_successful_stage_returns_two(self):
        runtime_result = self._runtime_result(
            self._invalid_schema_observation(),
            stage_status="SUCCESS",
        )
        exit_code, payload, _ = self._run_main(
            lambda _args, _thresholds: runtime_result
        )
        self.assertEqual(exit_code, 2)
        self.assertEqual(payload["process_exit_code"], 2)
        self.assertEqual(payload["observed_stage_status"], "SUCCESS")

    def test_source_threshold_gate_fails_a_successful_bronze_operation(self):
        thresholds = detection.load_thresholds()
        zero_volume = {
            "name": "clientes",
            "exists": True,
            "schema_valid": True,
            "missing_columns": [],
            "observed_rows": 0,
            "reference_rows": 20,
        }

        status = detection.resolve_bronze_stage_status(
            {"status": "SUCCESS"},
            zero_volume,
            thresholds,
        )

        self.assertEqual(status, "FAILURE")

    def test_source_threshold_gate_preserves_a_healthy_bronze_success(self):
        thresholds = detection.load_thresholds()
        healthy = {
            "name": "clientes",
            "exists": True,
            "schema_valid": True,
            "missing_columns": [],
            "observed_rows": 20,
            "reference_rows": 20,
        }

        status = detection.resolve_bronze_stage_status(
            {"status": "SUCCESS"},
            healthy,
            thresholds,
        )

        self.assertEqual(status, "SUCCESS")

    def test_non_detection_returns_two_and_never_zero(self):
        healthy = {
            "source": {
                "name": "clientes",
                "exists": True,
                "schema_valid": True,
                "missing_columns": [],
                "observed_rows": 20,
                "reference_rows": 20,
            },
            "stage_durations_seconds": {"bronze": 1.0},
        }
        exit_code, payload, _ = self._run_main(
            lambda _args, _thresholds: self._runtime_result(
                healthy,
                stage_status="SUCCESS",
            )
        )
        self.assertEqual(exit_code, 2)
        self.assertNotEqual(exit_code, 0)
        self.assertEqual(payload["detection_status"], "NOT_DETECTED")

    def test_harness_error_returns_two_with_sanitized_payload(self):
        leaked_path = "C:" + "\\Users\\example\\private\\source.csv"

        def fail_runtime(_args, _thresholds):
            raise RuntimeError(leaked_path)

        exit_code, payload, rendered = self._run_main(fail_runtime)
        self.assertEqual(exit_code, 2)
        self.assertNotEqual(exit_code, 0)
        self.assertEqual(payload["detection_status"], "ERROR")
        self.assertNotIn(leaked_path, rendered)
        detection.validate_payload_contract(payload)

    def test_threshold_error_message_is_generic_and_sanitized(self):
        leaked_path = "C:" + "\\Users\\example\\private\\thresholds.yml"
        message = detection._safe_harness_error(
            detection.ThresholdConfigError(leaked_path)
        )
        self.assertEqual(
            message,
            "Threshold configuration validation failed.",
        )
        self.assertNotIn(leaked_path, message)


if __name__ == "__main__":
    unittest.main(verbosity=2)
