# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only


"""Contrato entre o modelo visual canônico e as tabelas configuradas."""

import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "jobs" / "common" / "config.py"
MODEL_PATH = ROOT / "infra" / "model" / "data-domain-and-lineage.md"
README_PATH = ROOT / "README.md"


def _configured_keys(attribute_name):
    tree = ast.parse(CONFIG_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != "PipelineConfig":
            continue
        for statement in node.body:
            if not isinstance(statement, ast.Assign):
                continue
            targets = [
                target.id
                for target in statement.targets
                if isinstance(target, ast.Name)
            ]
            if (
                attribute_name not in targets
                or not isinstance(statement.value, ast.Dict)
            ):
                continue
            return {
                key.value
                for key in statement.value.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    raise AssertionError(f"{attribute_name} não encontrado em PipelineConfig")


class VisualModelContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = MODEL_PATH.read_text(encoding="utf-8")

    def test_presentation_order_and_mermaid_blocks(self):
        domain_position = self.model.index("## 1. Domínio bancário sintético")
        lineage_position = self.model.index(
            "## 2. Bronze → Raw Vault → Business Vault lógica → Gold"
        )
        self.assertLess(domain_position, lineage_position)
        self.assertEqual(2, self.model.count("```mermaid"))
        self.assertEqual(4, self.model.count("```"))

    def test_all_configured_bronze_tables_are_present_without_extras(self):
        configured = _configured_keys("BRONZE_TABLES")
        documented = set(re.findall(r"\bbronze__([a-z0-9_]+)\[", self.model))
        self.assertEqual(configured, documented)

    def test_all_raw_vault_objects_are_present_without_extras(self):
        for attribute_name, prefix in (
            ("HUB_TABLES", "hub"),
            ("LINK_TABLES", "link"),
            ("SATELLITE_TABLES", "sat"),
        ):
            with self.subTest(attribute_name=attribute_name):
                configured = _configured_keys(attribute_name)
                documented = set(
                    re.findall(rf"\b{prefix}_[a-z0-9_]+\b", self.model)
                )
                self.assertEqual(configured, documented)

    def test_all_seven_gold_tables_are_present_and_have_incoming_lineage(self):
        configured = _configured_keys("GOLD_TABLES")
        documented = set(
            re.findall(r'\["(gold_[a-z0-9_]+)"\]', self.model)
        )
        self.assertEqual(configured, documented)
        self.assertEqual(7, len(configured))
        for table_name in configured:
            node_id = f"goldnode__{table_name.removeprefix('gold_')}"
            self.assertRegex(self.model, rf"-->\s*{re.escape(node_id)}\b")

    def test_physical_and_logical_layers_are_visually_distinct(self):
        self.assertIn("classDef physical", self.model)
        self.assertIn("classDef logical", self.model)
        self.assertIn("stroke-dasharray", self.model)
        self.assertIn("Business Vault lógica - não materializada", self.model)

    def test_readme_points_to_the_canonical_visual_model(self):
        readme = README_PATH.read_text(encoding="utf-8")
        self.assertIn("infra/model/data-domain-and-lineage.md", readme)


if __name__ == "__main__":
    unittest.main()
