import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = REPO_ROOT / "dags" / "banking_data_vault_pipeline_dag.py"


class AirflowKubernetesDagContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DAG_PATH.read_text(encoding="utf-8-sig")

    def test_official_dag_uses_spark_kubernetes_operator(self):
        self.assertIn("SparkKubernetesOperator", self.source)
        self.assertNotIn("SparkSubmitOperator", self.source)

    def test_official_dag_does_not_create_local_spark(self):
        self.assertNotIn("create_spark_session", self.source)
        self.assertNotIn("SparkSession", self.source)
        self.assertNotIn('"local[*]"', self.source)
        self.assertNotIn("'local[*]'", self.source)

    def test_expected_pipeline_and_gate_stages_are_declared(self):
        for stage in (
            "bronze",
            "hubs",
            "links",
            "satellites",
            "gold",
            "data-vault-gate",
            "masking-gate",
            "evidence",
        ):
            self.assertIn(f'"{stage}"', self.source)

    def test_operator_sets_observability_labels(self):
        self.assertIn('"data-master.io/runtime-profile"', self.source)
        self.assertIn('"data-master.io/stage": stage', self.source)

    def test_dag_declares_distinct_business_vault_and_gold_variables(self):
        self.assertIn('"BUSINESS_VAULT_PATH"', self.source)
        self.assertIn('"s3a://lakehouse/business_vault"', self.source)
        self.assertIn('"GOLD_PATH"', self.source)
        self.assertIn('"s3a://lakehouse/gold"', self.source)
        self.assertNotIn("Business Vault/Gold", self.source)


if __name__ == "__main__":
    unittest.main()
