#!/usr/bin/env python3
"""Offline checks for plain repository Markdown; never execute documented commands."""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import html
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import unquote, urlsplit

TOKEN = re.compile(r"\{\{[A-Z][A-Z0-9_]*\}\}")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
HEADING = re.compile(r"^ {0,3}(#{1,6})\s+(.+?)\s*$")
REFERENCE = re.compile(r'^ {0,3}\[([^\]]+)\]:\s*(<[^>]+>|\S+)(?:\s+.*)?$')
INLINE = re.compile(r"!?\[[^\]\n]*\]\(")
REF_LINK = re.compile(r"!?\[([^\]\n]+)\]\[([^\]\n]*)\]")
SHORT_LINK = re.compile(r"(?<!!)\[([^\]\n]+)\](?![\[(])")


@dataclass
class Issue:
    path: Path
    line: int
    message: str


@dataclass
class Diagram:
    path: Path
    line: int
    source: str


@dataclass
class Page:
    path: Path
    text: str
    prose: list[tuple[int, str]] = field(default_factory=list)
    anchors: set[str] = field(default_factory=set)
    references: dict[str, tuple[str, int]] = field(default_factory=dict)
    diagrams: list[Diagram] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)


def normalize_label(value: str) -> str:
    return " ".join(value.split()).casefold()


def anchor(value: str) -> str:
    value = html.unescape(re.sub(r"<[^>]+>", "", value))
    value = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", value)
    value = re.sub(r"[^\w\- ]", "", value.lower())
    return value.replace(" ", "-")


def mask_inline_code(line: str) -> str:
    return re.sub(r"(`+)(.+?)\1", lambda m: " " * len(m[0]), line)


def parse_page(path: Path, allow_placeholders: bool = False) -> Page:
    page = Page(path, path.read_text(encoding="utf-8"))
    if not page.text.endswith("\n"):
        page.issues.append(Issue(path, 1, "missing final newline"))
    fence_char, fence_size, fence_line, language = "", 0, 0, ""
    code: list[str] = []
    prior_level, h1_count = 0, 0
    for number, line in enumerate(page.text.splitlines(), 1):
        if not allow_placeholders and TOKEN.search(line):
            page.issues.append(Issue(path, number, "unresolved template token"))
        match = FENCE.match(line)
        if fence_char:
            if match and match[1][0] == fence_char and len(match[1]) >= fence_size and not match[2].strip():
                if language == "mermaid":
                    source = "\n".join(code) + "\n"
                    for name in ("accTitle", "accDescr"):
                        present = re.search(rf"^[ \t]*{name}:[ \t]*\S", source, re.MULTILINE)
                        if name == "accDescr" and not present:
                            description = re.search(r"^[ \t]*accDescr[ \t]+\{([^}]*)\}", source, re.MULTILINE)
                            present = description and description[1].strip()
                        if not present:
                            page.issues.append(Issue(path, fence_line, f"Mermaid needs {name}"))
                    page.diagrams.append(Diagram(path, fence_line, source))
                fence_char, code = "", []
            else:
                code.append(line)
            continue
        if match:
            fence_char, fence_size, fence_line = match[1][0], len(match[1]), number
            language = match[2].strip()
            if not language:
                page.issues.append(Issue(path, number, "code fence needs a language"))
            continue
        prose = mask_inline_code(line)
        if "<!-- AUTHOR:" in prose and not allow_placeholders:
            page.issues.append(Issue(path, number, "unfinished author instruction"))
        ref = REFERENCE.match(prose)
        if ref:
            label = normalize_label(ref[1])
            if label in page.references:
                page.issues.append(Issue(path, number, f"duplicate link reference: {label}"))
            page.references[label] = (ref[2].strip("<>"), number)
            continue
        page.prose.append((number, prose))
        heading = HEADING.match(line)
        if heading:
            level = len(heading[1])
            if prior_level and level > prior_level + 1:
                page.issues.append(Issue(path, number, "heading skips a level"))
            if not prior_level and level != 1:
                page.issues.append(Issue(path, number, "first heading must be H1"))
            prior_level = level
            h1_count += level == 1
            base = anchor(re.sub(r"\s+#+\s*$", "", heading[2]))
            slug, suffix = base, 0
            while slug in page.anchors:
                suffix += 1
                slug = f"{base}-{suffix}"
            page.anchors.add(slug)
    if fence_char:
        page.issues.append(Issue(path, fence_line, "unclosed code fence"))
    if h1_count != 1:
        page.issues.append(Issue(path, 1, f"expected one H1; found {h1_count}"))
    return page


def inline_destinations(line: str):
    """Read flat inline labels with balanced/escaped parentheses in destinations."""
    for match in INLINE.finditer(line):
        pos = match.end()
        if pos < len(line) and line[pos] == "<":
            end = line.find(">", pos + 1)
            if end >= 0 and re.match(r'(?:\s+[\'"].*?[\'"])?\s*\)', line[end + 1:]):
                yield line[pos + 1:end]
            continue
        start, depth = pos, 0
        while pos < len(line):
            char = line[pos]
            if char == "\\":
                pos += 2
                continue
            if char == "(":
                depth += 1
            elif char == ")":
                if depth == 0:
                    yield line[start:pos]
                    break
                depth -= 1
            elif char.isspace() and depth == 0:
                if re.match(r'\s+[\'"].*?[\'"]\s*\)', line[pos:]):
                    yield line[start:pos]
                break
            pos += 1


def check_destination(root: Path, page: Page, line: int, destination: str, cache: dict[Path, Page]) -> list[Issue]:
    destination = re.sub(r"\\([\\()\[\] ])", r"\1", destination)
    try:
        url = urlsplit(destination)
    except ValueError as exc:
        return [Issue(page.path, line, f"invalid link: {exc}")]
    if url.scheme in {"http", "https", "mailto", "tel"} or destination.startswith("//"):
        return []
    if url.scheme or url.path.startswith("/"):
        return [Issue(page.path, line, "unsupported absolute or special link; use a portable relative path")]
    target = (page.path.parent / unquote(url.path)).resolve() if url.path else page.path
    if not target.is_relative_to(root):
        return [Issue(page.path, line, f"link leaves project root: {destination}")]
    if not target.exists():
        return [Issue(page.path, line, f"missing link target: {destination}")]
    if target.is_dir():
        target = target / "README.md"
        if not url.fragment:
            return []
        if not target.is_file():
            return [Issue(page.path, line, f"directory anchor has no README.md: {destination}")]
    if url.fragment and target.suffix.lower() == ".md":
        if target not in cache:
            cache[target] = parse_page(target, allow_placeholders=True)
        if unquote(url.fragment) not in cache[target].anchors:
            return [Issue(page.path, line, f"missing heading anchor: {destination}")]
    return []


def load_config(root: Path, config: Path | None = None) -> dict:
    path = config if config is not None else root / "docs-checks.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or set(data) - {"version", "include", "exclude", "allow_placeholders"}:
        raise ValueError("configuration must be an object with only supported keys")
    if type(data.get("version")) is not int or data["version"] != 1:
        raise ValueError("configuration version must be 1")
    for key in ("include", "exclude"):
        values = data.get(key)
        if not isinstance(values, list) or not all(isinstance(p, str) and p for p in values):
            raise ValueError(f"{key} must be a list of nonempty patterns")
        if any(Path(p).is_absolute() or ".." in Path(p).parts or "\\" in p for p in values):
            raise ValueError(f"{key} must contain project-relative forward-slash patterns")
    if not data["include"]:
        raise ValueError("include must contain at least one pattern")
    if type(data.get("allow_placeholders")) is not bool:
        raise ValueError("allow_placeholders must be boolean")
    return data


def validate(root: Path, config: dict) -> tuple[list[Issue], list[Page]]:
    root = root.resolve()
    excluded: set[Path] = set()
    for pattern in config["exclude"]:
        for path in root.glob(pattern):
            if path.is_dir():
                excluded.update(p.resolve() for p in path.rglob("*") if p.is_file())
            elif path.is_file():
                excluded.add(path.resolve())
    paths: set[Path] = set()
    issues: list[Issue] = []
    for pattern in config["include"]:
        matches = {p.resolve() for p in root.glob(pattern) if p.is_file()}
        if not matches:
            issues.append(Issue(root / "docs-checks.json", 1, f"include matches no files: {pattern}"))
        for path in matches - excluded:
            if not path.is_relative_to(root):
                issues.append(Issue(path, 1, "selected file leaves project root"))
            else:
                paths.add(path)
    if not paths:
        issues.append(Issue(root / "docs-checks.json", 1, "no documentation selected"))
    pages = [parse_page(path, config["allow_placeholders"]) for path in sorted(paths)]
    cache = {page.path: page for page in pages}
    for page in pages:
        issues.extend(page.issues)
        for number, line in page.prose:
            destinations = list(inline_destinations(line))
            for ref in REF_LINK.finditer(line):
                label = normalize_label(ref[2] or ref[1])
                if label not in page.references:
                    issues.append(Issue(page.path, number, f"undefined link reference: {label}"))
            for label in [normalize_label(m[2] or m[1]) for m in REF_LINK.finditer(line)]:
                if label in page.references:
                    destinations.append(page.references[label][0])
            for match in SHORT_LINK.finditer(line):
                ref = page.references.get(normalize_label(match[1]))
                if ref:
                    destinations.append(ref[0])
            for destination in destinations:
                issues.extend(check_destination(root, page, number, destination, cache))
        # Validate even unused definitions so dead reference destinations cannot rot.
        for destination, number in page.references.values():
            issues.extend(check_destination(root, page, number, destination, cache))
    return issues, pages


def export_diagrams(root: Path, pages: list[Page], output: Path, mmdc: str | None = None, browser_config: Path | None = None) -> int:
    renderer = None
    if mmdc is not None:
        renderer = shutil.which(mmdc)
        if renderer is None:
            raise ValueError(f"Mermaid renderer not found: {mmdc}")
    if browser_config is not None and not browser_config.is_file():
        raise ValueError("Puppeteer configuration must be an existing file")
    output.mkdir(parents=True, exist_ok=False)
    manifest = {"status": "extracted", "themes": ["default", "dark"] if renderer else [], "diagrams": []}
    manifest_path = output / "manifest.json"
    try:
        for page in pages:
            for diagram in page.diagrams:
                stem = f"{len(manifest['diagrams']) + 1:03d}-{page.path.stem}"
                source = output / f"{stem}.mmd"
                source.write_text(diagram.source, encoding="utf-8")
                entry = {"source": page.path.relative_to(root).as_posix(), "line": diagram.line, "input": source.name, "renders": []}
                manifest["diagrams"].append(entry)
                if renderer:
                    for theme in manifest["themes"]:
                        target = output / f"{stem}-{theme}.png"
                        background = "#1e1e1e" if theme == "dark" else "#ffffff"
                        command = [renderer, "-i", str(source), "-o", str(target), "-t", theme, "-b", background, "--size", "1400", "-s", "2"]
                        if browser_config is not None:
                            command += ["-p", str(browser_config)]
                        result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
                        if result.returncode or not target.is_file():
                            raise ValueError(f"render failed for {entry['source']}:{diagram.line} ({theme}): {result.stderr[-2000:]}")
                        entry["renders"].append(target.name)
        if renderer:
            manifest["status"] = "rendered"
    except (OSError, ValueError, subprocess.TimeoutExpired):
        manifest["status"] = "failed"
        raise
    finally:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return len(manifest["diagrams"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--extract-mermaid", type=Path)
    group.add_argument("--render-mermaid", type=Path)
    parser.add_argument("--mmdc", default="mmdc")
    parser.add_argument("--puppeteer-config", type=Path)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        config = load_config(root, args.config)
        issues, pages = validate(root, config)
        for issue in issues:
            print(f"{issue.path}:{issue.line}: {issue.message}", file=sys.stderr)
        if issues:
            print(f"FAILED: {len(issues)} issue(s)", file=sys.stderr)
            return 1
        count = sum(len(page.diagrams) for page in pages)
        print(f"PASS: {len(pages)} Markdown pages; {count} Mermaid blocks. External URLs and behavior not checked.")
        if args.extract_mermaid or args.render_mermaid:
            output = (args.extract_mermaid or args.render_mermaid).resolve()
            count = export_diagrams(root, pages, output, args.mmdc if args.render_mermaid else None, args.puppeteer_config)
            print(f"{'Rendered' if args.render_mermaid else 'Extracted'} {count} diagram(s) into {output}; visual review is separate.")
        return 0
    except (OSError, ValueError, UnicodeError, subprocess.TimeoutExpired) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
