# Documentation assets

Keep shared documentation images here and reference them with relative links.
The root README uses the [Undercurrent logo](branding/undercurrent-icon.svg)
and [architecture image](diagrams/undercurrent-architecture.png). Keep brand images in
`branding/` and technical diagrams in `diagrams/`. The logo is displayed at
240 × 240 pixels. Maintain the [SVG](branding/undercurrent-icon.svg) as the vector
logo and keep the [transparent PNG](branding/undercurrent-icon.png) consistent
when updating the branding.

Review the architecture image against the [architecture guide](../architecture/README.md)
when runtime boundaries change. It has no editable source in this repository;
keep labels readable at README width and preserve its full-resolution link.

Terminal recordings live in [`demos/`](demos/README.md), with their VHS tapes,
sample programs, generated GIFs, and static posters. Use the documented runner
to regenerate them against an isolated daemon and verify the recorded workflow.

The README's badge row uses Shields images. Its CI badge identifies and links to
the native platform workflow; it does not display live pass/fail results. Keep the
license and language badges aligned with [LICENSE](../../LICENSE),
[rust-toolchain.toml](../../rust-toolchain.toml), and
[Python requirements](../../python/pyproject.toml). Use live status or release
badges only when their public endpoints return usable results.

For behavior and ownership details, see the [architecture guide](../architecture/README.md).
