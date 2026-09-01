# Copyright (C) 2026 Alexandre Ferreira
# SPDX-License-Identifier: AGPL-3.0-only

import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from validate_provenance import (  # noqa: E402
    classify_source,
    validate_no_project_claims,
    validate_source_headers,
)


POLICY = {
    "copyright_notice": "Copyright (C) 2026 Alexandre Ferreira",
    "license_expression": "AGPL-3.0-only",
    "source_extensions": [".java", ".py", ".sh"],
    "source_filenames": [],
    "owned_source_patterns": ["jobs/**/*.py"],
    "review_required_patterns": [{"pattern": "infra/**", "reason": "upstream"}],
    "files_without_project_copyright": ["infra/upstream.sh"],
}


class ProvenanceComplianceTests(unittest.TestCase):
    def test_owned_source_accepts_minimal_header(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "jobs" / "demo" / "owned.py"
            path.parent.mkdir(parents=True)
            path.write_text(
                "# Copyright (C) 2026 Alexandre Ferreira\n"
                "# SPDX-License-Identifier: AGPL-3.0-only\n\n"
                "print('synthetic')\n",
                encoding="utf-8",
            )

            self.assertEqual(validate_source_headers(root, [path], POLICY), [])

    def test_owned_script_accepts_header_after_shebang(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "jobs" / "demo" / "owned.py"
            path.parent.mkdir(parents=True)
            path.write_text(
                "#!/usr/bin/env python3\n"
                "# Copyright (C) 2026 Alexandre Ferreira\n"
                "# SPDX-License-Identifier: AGPL-3.0-only\n",
                encoding="utf-8",
            )

            self.assertEqual(validate_source_headers(root, [path], POLICY), [])

    def test_owned_source_rejects_missing_header(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "jobs" / "demo" / "owned.py"
            path.parent.mkdir(parents=True)
            path.write_text("print('synthetic')\n", encoding="utf-8")

            self.assertEqual(
                validate_source_headers(root, [path], POLICY),
                ["header.missing_or_invalid:jobs/demo/owned.py"],
            )

    def test_new_source_type_requires_classification(self):
        self.assertEqual(classify_source("src/Example.java", POLICY), "unclassified")

    def test_preexisting_source_is_held_for_legacy_review(self):
        policy = dict(POLICY)
        policy["owned_source_patterns"] = []

        self.assertEqual(
            classify_source(
                "jobs/demo/legacy.py",
                policy,
                {"jobs/demo/legacy.py"},
            ),
            "legacy_review",
        )

    def test_review_required_file_rejects_project_claim(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "infra" / "upstream.sh"
            path.parent.mkdir(parents=True)
            path.write_text(
                "# Copyright (C) 2026 Alexandre Ferreira\n",
                encoding="utf-8",
            )

            self.assertEqual(
                validate_no_project_claims(root, POLICY),
                ["exception.project_copyright_present:infra/upstream.sh"],
            )


if __name__ == "__main__":
    unittest.main()
