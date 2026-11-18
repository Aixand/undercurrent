#!/usr/bin/env python3
"""Require a publication or explicit, validated role for every repository document."""
import argparse
import json
from pathlib import Path
import re
import subprocess


def relative(value):
    if not isinstance(value, str) or not value or '\\' in value or value.startswith('/') or any(p in {'', '.', '..'} for p in value.split('/')):
        raise ValueError('Invalid repository path: ' + repr(value))
    return value


def inventory(root):
    try:
        names = subprocess.check_output(['git', '-C', str(root), 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], stderr=subprocess.DEVNULL).decode().split('\0')
        return sorted({name for name in names if name.endswith('.md') and (root / name).is_file()})
    except (OSError, subprocess.CalledProcessError):
        # Source archives and the focused fixture tests do not require Git metadata.
        ignored = {'.git', 'target', 'build', 'dist', '.venv', 'node_modules', '__pycache__'}
        return sorted(str(p.relative_to(root)) for p in root.rglob('*.md') if not ignored.intersection(p.relative_to(root).parts))


def audit(root, names=None):
    root = Path(root).resolve()
    config = json.loads((root / 'docs/coverage.json').read_text())
    if set(config) != {'schema', 'manifests', 'documents'} or config['schema'] != 1:
        raise ValueError('Invalid documentation coverage configuration')
    published = set()
    attachments = set()
    for name in config['manifests']:
        manifest = root / relative(name)
        data = json.loads(manifest.read_text())
        base = root if data.get('sourceRoot') == 'repository' else manifest.parent
        for page in data['pages']:
            source = (base / relative(page['file'])).relative_to(root).as_posix()
            if not (root / source).is_file(): raise ValueError('Missing published source: ' + source)
            published.add(source)
        attachments.update(asset['file'] for asset in data.get('assets', []) if asset.get('link'))
    registry = {}
    for row in config['documents']:
        if not isinstance(row, dict) or set(row) - {'path', 'role', 'destinations', 'reason', 'maxWords'}:
            raise ValueError('Invalid document role fields')
        name = relative(row['path'])
        if name in registry or name in published: raise ValueError('Duplicate document classification: ' + name)
        if not (root / name).is_file(): raise ValueError('Stale document role: ' + name)
        if not isinstance(row.get('reason'), str) or not row['reason'].strip(): raise ValueError('Missing document purpose: ' + name)
        targets = row.get('destinations')
        if not isinstance(targets, list) or not targets or any(relative(target) not in published for target in targets):
            raise ValueError('Document role needs published destinations: ' + name)
        role = row.get('role')
        if role in {'entrypoint', 'redirect'}:
            limit = row.get('maxWords', 300)
            if type(limit) is not int or not 1 <= limit <= 500: raise ValueError('Invalid entrypoint word limit: ' + name)
            text = (root / name).read_text()
            if len(text.split()) > limit: raise ValueError('Move substantive prose into published owner: ' + name)
            links = re.findall(r'\]\(([^)\s]+)', text)
            resolved = {(root / name).parent.joinpath(link.split('#')[0]).resolve() for link in links if '://' not in link and not link.startswith('#')}
            if not any((root / target).resolve() in resolved for target in targets): raise ValueError('Missing owning-page link: ' + name)
        elif role == 'dependency':
            if not name.startswith(('vendor/', 'licenses/')) or name not in attachments:
                raise ValueError('Dependency record needs an imported original attachment: ' + name)
        else:
            raise ValueError('Unknown document role: ' + name)
        registry[name] = row
    names = inventory(root) if names is None else sorted(names)
    unknown = sorted(set(names) - published - registry.keys())
    if unknown: raise ValueError('Unclassified documentation:\n' + '\n'.join(unknown))
    return {'documents': len(names), 'published': len(published), 'entrypoints': sum(r['role'] == 'entrypoint' for r in registry.values()),
            'redirects': sum(r['role'] == 'redirect' for r in registry.values()), 'dependency_records': sum(r['role'] == 'dependency' for r in registry.values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    try:
        result = audit(args.root)
    except (ValueError, KeyError, OSError, TypeError) as error:
        parser.exit(1, str(error) + '\n')
    print('PASS documentation coverage: ' + json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
