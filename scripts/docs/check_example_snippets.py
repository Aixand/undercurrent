"""Check published Rust/Python excerpts against their maintained runnable source."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
CRATES = {
    'foundation-values': ('foundation', 'foundation_values.rs'),
    'model-walkthrough': ('domain', 'model_walkthrough.rs'),
    'inspect-payload': ('protocol', 'inspect_payload.rs'),
    'inspect-cancellation': ('protocol', 'inspect_cancellation.rs'),
    'inspect-frames': ('wire', 'inspect_frames.rs'),
    'framing-roundtrip': ('transport-local', 'framing_roundtrip.rs'),
    'bounded-delivery': ('multiplex', 'bounded_delivery.rs'),
    'retained-result': ('transport-core', 'retained_result.rs'),
    'output-gap': ('application', 'output_gap.rs'),
    'inspect-config': ('config', 'inspect_config.rs'),
    'repository-roundtrip': ('store-sqlite', 'repository_roundtrip.rs'),
}


def normalized(text):
    return '\n'.join(line.strip() for line in text.strip().splitlines())


def main():
    pairs = [(f'docs/examples/crates/{page}.md', f'crates/undercurrent-{crate}/examples/{name}')
             for page, (crate, name) in CRATES.items()]
    pairs += [(f'docs/examples/runtime/{page}.md', 'docs/examples/runtime/workflows.py')
              for page in ['parallel-report', 'failed-prerequisite', 'output-checkpoint', 'daemon-reconnect', 'mcp-observer']]
    pairs += [('docs/examples/parallel-jobs.md', 'docs/assets/demos/fixtures/orchestrate.py'),
              ('docs/examples/carrier-api.md', 'docs/examples/integrations/carrier-api.py')]
    count = 0
    for page, source in pairs:
        blocks = re.findall(r'^```(?:rust|python)\n(.*?)^```', (ROOT / page).read_text(), re.M | re.S)
        assert blocks, 'Missing executable excerpt: ' + page
        actual = normalized((ROOT / source).read_text())
        for block in blocks:
            assert normalized(block) in actual, 'Excerpt differs from maintained source: ' + page
            count += 1
    print(f'PASS: {count} executable excerpts across {len(pairs)} walkthroughs')


if __name__ == '__main__':
    main()
