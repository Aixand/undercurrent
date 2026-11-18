"""Exercise regeneration and drift detection for the portable skill references."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = Path("scripts/docs/generate_skill_references.py")
SOURCE = Path("docs/agent/shared/mcp-control.md")
REFERENCES = [
    Path("docs/agent/skills") / skill / "references/mcp-control.md"
    for skill in ("undercurrent", "undercurrent-orchestrate")
]


class SkillReferences(unittest.TestCase):
    def test_check_reports_drift_without_writing_and_regeneration_repairs_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path in (SCRIPT, SOURCE, *REFERENCES):
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / path, root / path)

            def generate(*arguments):
                return subprocess.run(
                    [sys.executable, str(root / SCRIPT), *arguments],
                    cwd=root,
                    capture_output=True,
                    text=True,
                )

            result = generate("--check")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            changed, missing = (root / path for path in REFERENCES)
            changed.write_bytes(b"stale\n")
            missing.unlink()

            result = generate("--check")
            self.assertEqual(result.returncode, 1)
            for path in REFERENCES:
                self.assertIn(path.as_posix(), result.stdout)
            self.assertEqual(changed.read_bytes(), b"stale\n")
            self.assertFalse(missing.exists())

            result = generate()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for path in REFERENCES:
                self.assertEqual((root / path).read_bytes(), (ROOT / path).read_bytes())


if __name__ == "__main__":
    unittest.main()
