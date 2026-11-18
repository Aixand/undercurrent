"""Exercise missing docs, stale exceptions and substantive-entrypoint drift."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('documentation_coverage', ROOT / 'scripts/checks/check-doc-coverage.py')
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'docs').mkdir()
        (self.root / 'docs/page.md').write_text('# Page\n\nPublished guide.\n')
        (self.root / 'docs/publication.json').write_text(json.dumps({'schema': 1, 'pages': [{'file':'page.md'}]}))
        self.data = {'schema':1, 'manifests':['docs/publication.json'], 'documents':[]}
        self.write()

    def write(self):
        (self.root / 'docs/coverage.json').write_text(json.dumps(self.data))

    def test_new_document_requires_destination(self):
        (self.root / 'docs/hidden.md').write_text('# Hidden\n')
        with self.assertRaisesRegex(ValueError, 'Unclassified'): coverage.audit(self.root)

    def test_entrypoint_must_link_owner_and_remain_short(self):
        p = self.root / 'README.md'
        p.write_text('# Read\n\n[Guide](docs/page.md)\n')
        self.data['documents']=[dict(path='README.md', role='entrypoint', destinations=['docs/page.md'], reason='Repository entry', maxWords=20)]
        self.write()
        self.assertEqual(coverage.audit(self.root)['entrypoints'], 1)
        p.write_text('# Read\nNo link')
        with self.assertRaisesRegex(ValueError, 'owning-page link'): coverage.audit(self.root)
        p.write_text('[Guide](docs/page.md)\n' + 'hidden prose ' * 30)
        with self.assertRaisesRegex(ValueError, 'substantive prose'): coverage.audit(self.root)

    def test_stale_or_broad_exemptions_fail(self):
        self.data['documents']=[dict(path='*.md', role='unpublished', destinations=['docs/page.md'], reason='Skip')]
        self.write()
        with self.assertRaisesRegex(ValueError, 'Stale'): coverage.audit(self.root)

    def test_dependency_requires_copied_attachment(self):
        (self.root / 'vendor').mkdir()
        (self.root / 'vendor/README.md').write_text('Upstream text')
        self.data['documents']=[dict(path='vendor/README.md', role='dependency', destinations=['docs/page.md'], reason='Upstream documentation')]
        self.write()
        with self.assertRaisesRegex(ValueError, 'attachment'): coverage.audit(self.root)
        (self.root / 'docs/publication.json').write_text(json.dumps({'pages':[{'file':'page.md'}], 'assets':[{'file':'vendor/README.md', 'link':True}]}))
        self.assertEqual(coverage.audit(self.root)['dependency_records'], 1)


if __name__ == '__main__': unittest.main()
