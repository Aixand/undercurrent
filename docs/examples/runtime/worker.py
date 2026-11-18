"""Bounded deterministic fixture processes for the runtime SDK walkthroughs."""
import argparse
import csv
import json
import os
from pathlib import Path
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('mode', choices=['check', 'total', 'report', 'lines', 'hold'])
a = p.parse_args()
if a.mode in {'check', 'total'}:
    Path(a.mode + '.ready').touch()
    deadline = time.monotonic() + 20
    while not Path('release').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Coordinator did not release fixture barrier')
        time.sleep(.03)
    rows = list(csv.DictReader(Path('orders.csv').open()))
    if a.mode == 'check':
        if any(int(r['quantity']) <= 0 for r in rows):
            print('Invalid quantity: report must not run', flush=True)
            raise SystemExit(7)
        result = {'valid': True, 'rows': len(rows)}
    else:
        result = {'cents': sum(int(r['quantity']) * int(r['unit_cents']) for r in rows)}
    Path(a.mode + '.json').write_text(json.dumps(result))
    print(a.mode + ': ' + json.dumps(result), flush=True)
elif a.mode == 'report':
    check = json.loads(Path('check.json').read_text())
    total = json.loads(Path('total.json').read_text())
    assert check['valid'] is True
    Path('report.txt').write_text(f"Rows: {check['rows']}; total cents: {total['cents']}\n")
    print('Report artifact written', flush=True)
elif a.mode == 'lines':
    for i in range(1, 4):
        print(f'entry-{i:03d}', flush=True)
    Path('first.ready').touch()
    deadline = time.monotonic() + 30
    while not Path('continue').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Coordinator did not release output fixture')
        time.sleep(.03)
    for i in range(4, 7):
        print(f'entry-{i:03d}', flush=True)
else:
    Path('native-pid.txt').write_text(str(os.getpid()))
    print('Worker ready', flush=True)
    deadline = time.monotonic() + 60
    while not Path('finish').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Owned fixture exceeded maximum lifetime')
        time.sleep(.05)
    print('Worker finished', flush=True)
