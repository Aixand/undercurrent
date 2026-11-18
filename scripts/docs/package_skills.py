"""Build deterministic, self-contained skill downloads without changing source skills."""
import argparse
import io
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[2]
NAMES = ('undercurrent', 'undercurrent-orchestrate')


def archive(root, name):
    source = root / 'docs/agent/skills' / name
    files = {}
    for path in sorted(source.rglob('*')):
        if path.is_symlink(): raise ValueError('Skill package contains a symlink: ' + str(path))
        if path.is_file(): files[path.relative_to(source).as_posix()] = path.read_bytes()
    if 'SKILL.md' not in files: raise ValueError('Missing skill entrypoint: ' + name)
    files['LICENSE'] = (root / 'LICENSE').read_bytes()
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED) as package:
        for relative, content in sorted(files.items()):
            info = zipfile.ZipInfo(name + '/' + relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            package.writestr(info, content)
    return output.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    folder = ROOT / 'docs/agent/downloads'
    stale = []
    for name in NAMES:
        expected = archive(ROOT, name)
        target = folder / (name + '.zip')
        if args.check:
            if not target.is_file() or target.read_bytes() != expected: stale.append(name)
        else:
            folder.mkdir(parents=True, exist_ok=True)
            target.write_bytes(expected)
    if stale:
        raise SystemExit('Missing or stale skill downloads: ' + ', '.join(stale) + '. Run scripts/docs/package_skills.py.')
    print('PASS skill downloads: ' + ('checked' if args.check else 'generated') + ' both standalone packages')


if __name__ == '__main__': main()
