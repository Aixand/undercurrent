#!/usr/bin/env python3
"""Regression checks for broken documentation and bounded preview generation."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/checks/check-docs.py"
SPEC = importlib.util.spec_from_file_location("aixand_docs_check", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECK
SPEC.loader.exec_module(CHECK)


class DocumentationChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.config = {"version": 1, "include": ["*.md", "docs/**/*.md"], "exclude": [], "allow_placeholders": False}
        self.put("README.md", "# Project\n\n[Start](docs/start.md#first-result)\n")
        self.put("docs/start.md", "# Start\n\n## First result\n\nUseful instructions.\n")

    def put(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def messages(self):
        issues, _ = CHECK.validate(self.root, self.config)
        return "\n".join(issue.message for issue in issues)

    def test_valid_relative_path_and_anchor(self):
        self.assertEqual("", self.messages())

    def test_missing_target_fails(self):
        self.put("README.md", "# Project\n\n[Missing](gone.md)\n")
        self.assertIn("missing link target", self.messages())

    def test_missing_anchor_fails(self):
        self.put("README.md", "# Project\n\n[Missing](docs/start.md#absent)\n")
        self.assertIn("missing heading anchor", self.messages())

    def test_duplicate_headings_and_unicode_anchors(self):
        self.put("docs/start.md", "# Start\n\n## Résumé\n\n## Résumé\n\n[Again](#résumé-1)\n")
        self.put("README.md", "# Project\n\n[Start](docs/start.md#r%C3%A9sum%C3%A9)\n")
        self.assertEqual("", self.messages())

    def test_percent_encoded_space_and_balanced_parentheses(self):
        self.put("docs/a (b).md", "# Name\n")
        self.put("README.md", "# Project\n\n[A](docs/a%20(b).md)\n[B](<docs/a (b).md>)\n")
        self.assertEqual("", self.messages())

    def test_title_and_escaped_parentheses(self):
        self.put("docs/a(b).md", "# Name\n")
        self.put("README.md", '# Project\n\n[A](docs/a\\(b\\).md "Title")\n')
        self.assertEqual("", self.messages())

    def test_reference_links_and_shortcuts(self):
        self.put("README.md", "# Project\n\n[Start][intro] and [intro].\n\n[intro]: docs/start.md#first-result\n")
        self.assertEqual("", self.messages())

    def test_undefined_full_reference_fails(self):
        self.put("README.md", "# Project\n\n[Start][missing]\n")
        self.assertIn("undefined link reference", self.messages())

    def test_unused_broken_definition_fails(self):
        self.put("README.md", "# Project\n\n[unused]: missing.md\n")
        self.assertIn("missing link target", self.messages())

    def test_missing_image_fails(self):
        self.put("README.md", "# Project\n\n![Screenshot](missing.png)\n")
        self.assertIn("missing link target", self.messages())

    def test_inline_and_fenced_code_are_not_links_or_commands(self):
        self.put("README.md", "# Project\n\n`[example](missing.md)`\n\n```sh\n[example](missing.md)\ntouch sentinel\n```\n")
        self.assertEqual("", self.messages())
        self.assertFalse((self.root / "sentinel").exists())

    def test_external_urls_are_not_fetched(self):
        self.put("README.md", "# Project\n\n[Site](https://example.invalid/path#missing)\n")
        self.assertEqual("", self.messages())

    def test_escaping_root_and_absolute_paths_fail(self):
        self.put("README.md", "# Project\n\n[A](../outside.md)\n[B](/tmp/host.md)\n")
        result = self.messages()
        self.assertIn("leaves project root", result)
        self.assertIn("unsupported absolute", result)

    def test_heading_structure_fails(self):
        self.put("docs/start.md", "# Start\n\n### Skipped\n\n# Duplicate title\n")
        result = self.messages()
        self.assertIn("heading skips", result)
        self.assertIn("expected one H1", result)

    def test_unclosed_fence_fails(self):
        self.put("docs/start.md", "# Start\n\n```python\nprint('x')\n")
        self.assertIn("unclosed code fence", self.messages())

    def test_closing_fence_must_match_opening_character(self):
        self.put("docs/start.md", "# Start\n\n```text\n~~~\n")
        self.assertIn("unclosed code fence", self.messages())

    def test_unresolved_tokens_in_code_fail(self):
        self.put("docs/start.md", "# Start\n\n```sh\n{{COMMAND}}\n```\n")
        self.assertIn("unresolved template token", self.messages())

    def test_unfinished_author_instruction_fails(self):
        self.put("docs/start.md", "# Start\n\n<!-- AUTHOR: Fill this in. -->\n")
        self.assertIn("unfinished author instruction", self.messages())

    def test_mermaid_needs_accessible_descriptions(self):
        self.put("docs/start.md", "# Start\n\n```mermaid\nflowchart LR\n A --> B\n```\n")
        result = self.messages()
        self.assertIn("Mermaid needs accTitle", result)
        self.assertIn("Mermaid needs accDescr", result)

    def test_multiline_accessible_description(self):
        page = self.put("docs/start.md", "# Start\n\n```mermaid\nflowchart LR\n accTitle: A to B\n accDescr {\n A connects to B.\n }\n A --> B\n```\n")
        parsed = CHECK.parse_page(page)
        self.assertEqual([], parsed.issues)
        self.assertEqual(1, len(parsed.diagrams))

    def test_blank_accessibility_values_fail(self):
        page = self.put("docs/chart.md", "# Chart\n\n```mermaid\nflowchart LR\n accTitle: \n accDescr {\n }\n A --> B\n```\n")
        issues = CHECK.parse_page(page).issues
        self.assertEqual({"Mermaid needs accTitle", "Mermaid needs accDescr"}, {issue.message for issue in issues})

    def test_invalid_configuration_fails(self):
        variants = [dict(self.config, version=True), dict(self.config, extra=True), dict(self.config, include=[]), dict(self.config, exclude=["../outside"]), dict(self.config, allow_placeholders="false")]
        for data in variants:
            with self.subTest(data=data):
                self.put("docs-checks.json", json.dumps(data))
                with self.assertRaises(ValueError):
                    CHECK.load_config(self.root)

    def test_unmatched_include_and_empty_selection_fail(self):
        self.config["include"] = ["missing/**/*.md"]
        self.assertIn("include matches no files", self.messages())
        self.assertIn("no documentation selected", self.messages())

    def test_excluded_page_is_not_a_selected_page(self):
        self.put("docs/draft.md", "# Draft\n\n{{UNFINISHED}}\n")
        self.config["exclude"] = ["docs/draft.md"]
        self.assertEqual("", self.messages())

    def test_excluded_directory_removes_descendants(self):
        self.put("docs/archive/old.md", "# Old\n\n{{UNFINISHED}}\n")
        self.config["exclude"] = ["docs/archive"]
        self.assertEqual("", self.messages())

    def test_missing_newline_fails(self):
        self.put("docs/start.md", "# Start")
        self.assertIn("missing final newline", self.messages())

    def diagram_pages(self):
        page = self.put("docs/chart.md", "# Chart\n\n```mermaid\nflowchart LR\n accTitle: Link\n accDescr: A connects to B.\n A --> B\n```\n")
        return [CHECK.parse_page(page)]

    def test_extraction_manifest_maps_source_and_line(self):
        output = self.root / "preview"
        self.assertEqual(1, CHECK.export_diagrams(self.root, self.diagram_pages(), output))
        data = json.loads((output / "manifest.json").read_text())
        self.assertEqual("extracted", data["status"])
        self.assertEqual("docs/chart.md", data["diagrams"][0]["source"])
        self.assertEqual(3, data["diagrams"][0]["line"])

    def test_preview_refuses_existing_directory(self):
        output = self.root / "preview"
        output.mkdir()
        sentinel = output / "keep.txt"
        sentinel.write_text("keep")
        with self.assertRaises(FileExistsError):
            CHECK.export_diagrams(self.root, self.diagram_pages(), output)
        self.assertEqual("keep", sentinel.read_text())

    def test_missing_renderer_creates_no_output(self):
        output = self.root / "preview"
        with self.assertRaises(ValueError):
            CHECK.export_diagrams(self.root, self.diagram_pages(), output, "missing-aixand-mmdc-command")
        self.assertFalse(output.exists())

    def test_renderer_uses_both_themes_without_a_shell(self):
        output = self.root / "preview"
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            self.assertNotIn("shell", kwargs)
            Path(command[command.index("-o") + 1]).write_bytes(b"fixture image")
            return subprocess.CompletedProcess(command, 0, "", "")
        with patch.object(CHECK.subprocess, "run", side_effect=run):
            CHECK.export_diagrams(self.root, self.diagram_pages(), output, sys.executable)
        self.assertEqual(["default", "dark"], [c[c.index("-t") + 1] for c in commands])
        self.assertEqual(["#ffffff", "#1e1e1e"], [c[c.index("-b") + 1] for c in commands])
        self.assertEqual("rendered", json.loads((output / "manifest.json").read_text())["status"])

    def test_render_failure_preserves_failed_manifest(self):
        output = self.root / "preview"
        with patch.object(CHECK.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "bad syntax")):
            with self.assertRaisesRegex(ValueError, "render failed"):
                CHECK.export_diagrams(self.root, self.diagram_pages(), output, sys.executable)
        self.assertEqual("failed", json.loads((output / "manifest.json").read_text())["status"])

    def test_invalid_docs_prevent_preview_mutation(self):
        self.put("docs-checks.json", json.dumps(self.config))
        self.put("README.md", "# Broken\n\n[No](missing.md)\n")
        output = self.root / "preview"
        result = subprocess.run([sys.executable, str(SCRIPT), "--root", str(self.root), "--extract-mermaid", str(output)], capture_output=True, text=True)
        self.assertEqual(1, result.returncode)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
