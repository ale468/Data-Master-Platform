import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from validate_public_repository import (  # noqa: E402
    yaml,
    validate_boundary,
    validate_formats,
    validate_markdown_links,
)


class PublicRepositoryValidationTests(unittest.TestCase):
    def test_boundary_accepts_allowlisted_public_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "jobs" / "example.py"
            path.parent.mkdir(parents=True)
            path.write_text("print('synthetic')\n", encoding="utf-8")

            self.assertEqual(validate_boundary(root, [path]), [])

    def test_boundary_rejects_forbidden_tree_without_echoing_content(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "spdd" / "record.md"
            path.parent.mkdir(parents=True)
            path.write_text("private material\n", encoding="utf-8")

            failures = validate_boundary(root, [path])

            self.assertIn("allowlist.top_level:spdd/record.md", failures)
            self.assertIn("denylist.path:spdd/record.md", failures)
            self.assertNotIn("private material", "\n".join(failures))

    def test_boundary_rejects_sensitive_and_binary_files_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            sensitive_path = root / "config" / "client.key"
            binary_path = root / "config" / "opaque.bin"
            sensitive_path.parent.mkdir(parents=True)
            sensitive_path.write_text("placeholder\n", encoding="utf-8")
            binary_path.write_bytes(b"\x00\x01\x02")

            failures = validate_boundary(root, [sensitive_path, binary_path])

            self.assertIn(
                "denylist.sensitive_extension:config/client.key",
                failures,
            )
            self.assertIn(
                "content.unreadable_or_binary:config/opaque.bin",
                failures,
            )

    def test_formats_parse_yaml_and_json(self):
        if yaml is None:
            self.skipTest("PyYAML is not installed in this host runtime")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            yaml_path = root / "config" / "valid.yml"
            json_path = root / "config" / "valid.json"
            yaml_path.parent.mkdir(parents=True)
            yaml_path.write_text("version: 1\nitems:\n  - synthetic\n", encoding="utf-8")
            json_path.write_text(json.dumps({"version": 1}), encoding="utf-8")

            self.assertEqual(
                validate_formats(root, [yaml_path, json_path]),
                [],
            )

    def test_formats_parse_multi_document_yaml(self):
        if yaml is None:
            self.skipTest("PyYAML is not installed in this host runtime")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            yaml_path = root / "config" / "resources.yml"
            yaml_path.parent.mkdir(parents=True)
            yaml_path.write_text(
                "apiVersion: v1\nkind: ServiceAccount\n"
                "---\napiVersion: v1\nkind: ConfigMap\n",
                encoding="utf-8",
            )

            self.assertEqual(validate_formats(root, [yaml_path]), [])

    def test_formats_reject_invalid_json(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "config" / "invalid.json"
            path.parent.mkdir(parents=True)
            path.write_text("{", encoding="utf-8")

            self.assertEqual(
                validate_formats(root, [path]),
                ["format.invalid:config/invalid.json"],
            )

    def test_workflow_with_github_expressions_is_parsed_and_validated(self):
        if yaml is None:
            self.skipTest("PyYAML is not installed in this host runtime")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                "\n".join(
                    [
                        "name: CI",
                        "on:",
                        "  workflow_dispatch:",
                        "jobs:",
                        "  validate:",
                        "    runs-on: ubuntu-latest",
                        "    steps:",
                        "      - run: echo '${{ runner.temp }}'",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            self.assertEqual(validate_formats(root, [workflow]), [])

    def test_markdown_links_accept_existing_relative_target(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            readme = root / "README.md"
            target = root / "docs" / "guide.md"
            target.parent.mkdir(parents=True)
            target.write_text("# Guide\n", encoding="utf-8")
            readme.write_text("[Guide](docs/guide.md)\n", encoding="utf-8")

            self.assertEqual(validate_markdown_links(root, [readme]), [])

    def test_markdown_links_reject_missing_target(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            readme = root / "README.md"
            readme.write_text("[Missing](docs/missing.md)\n", encoding="utf-8")

            self.assertEqual(
                validate_markdown_links(root, [readme]),
                ["markdown.missing_target:README.md"],
            )


if __name__ == "__main__":
    unittest.main()
