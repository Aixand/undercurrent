"""Run complete local SDK workflows; requires matching native tools and Python SDK.

From the Undercurrent root:
python docs/examples/runtime/workflows.py parallel-report --work /tmp/uc-report --bin-dir target/debug
Linux walkthroughs use disposable homes; no provider or external service runs.
"""
import argparse
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from undercurrent import Client, OutputGap

TERMINAL = {'exited', 'failed', 'stopped'}


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(path)


def show(label, **values):
    print(label + ': ' + json.dumps(values, sort_keys=True), flush=True)


async def until(predicate, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(.05)
    raise TimeoutError('Expected local workflow evidence did not arrive')


class Demo:
    def __init__(self, args):
        self.root = args.work.resolve()
        self.bin = args.bin_dir.resolve()
        self.home = self.root / 'home'
        self.daemon = None
        self.log = None

    def client(self):
        return Client(home=self.home, bridge=str(self.bin / 'uc-python-bridge'))

    async def start_daemon(self):
        self.log = (self.root / ('daemon-' + str(time.time_ns()) + '.log')).open('w')
        self.daemon = subprocess.Popen([str(self.bin / 'uc-daemon')],
            env=dict(os.environ, UNDERCURRENT_HOME=str(self.home)),
            stdout=self.log, stderr=subprocess.STDOUT)
        async def ready():
            if self.daemon.poll() is not None:
                raise RuntimeError('Private daemon exited; inspect its retained log')
            try:
                async with self.client() as client:
                    await client.list()
                return True
            except Exception:
                return False
        await until(ready)

    async def stop_daemon(self):
        if self.daemon and self.daemon.poll() is None:
            self.daemon.send_signal(signal.SIGTERM)
            await asyncio.to_thread(self.daemon.wait, 20)
        if self.log:
            self.log.close()

    async def start(self, client, mode, name):
        session = await client.start([sys.executable, '-u', str(Path(__file__).with_name('worker.py')), mode],
            name=name, cwd=str(self.root), detection=False)
        save(self.root / (name + '-session.json'), {'session_id': session.id, 'name': name})
        return session

    async def ended(self, client, session):
        return await client.wait(session, lifecycle=TERMINAL, timeout=25)

    async def cleanup(self):
        failures = []
        observed = []
        try:
            if not self.daemon or self.daemon.poll() is not None:
                await self.start_daemon()
            async with self.client() as client:
                for session in await client.list():
                    state = await client.inspect(session)
                    if state.lifecycle not in TERMINAL:
                        await client.stop(session, graceful_only=True)
                        state = await self.ended(client, session)
                    observed.append({'session_id': session.id, 'lifecycle': state.lifecycle})
        except Exception as error:
            failures.append(type(error).__name__)
        try:
            await self.stop_daemon()
        except Exception as error:
            failures.append(type(error).__name__)
        save(self.root / 'stopped-sessions.json', observed)
        save(self.root / 'cleanup.json', {'owned_sessions_stopped': not failures,
            'daemon_exited': bool(self.daemon and self.daemon.poll() is not None), 'failures': failures})
        if failures:
            raise RuntimeError('Cleanup uncertain; inspect retained evidence before retrying')


@asynccontextmanager
async def demo(args):
    app = Demo(args)
    if len(str(app.home)) > 60:
        raise ValueError('Choose a shorter work path for the local control socket')
    for binary in ['uc-daemon', 'uc-session-host', 'uc-python-bridge']:
        if not os.access(app.bin / binary, os.X_OK):
            raise ValueError('Missing matching native executable: ' + binary)
    app.root.mkdir(mode=0o700, parents=True, exist_ok=False)
    save(app.root / 'run.json', {'workflow': args.workflow, 'binary_directory': str(app.bin)})
    try:
        await app.start_daemon()
        yield app
    finally:
        await app.cleanup()


async def jobs(app, fail):
    (app.root / 'orders.csv').write_text('quantity,unit_cents\n' + ('-1' if fail else '2') + ',1200\n3,700\n')
    async with app.client() as client:
        # docs:start dependency-gate
        check, total = await asyncio.gather(
            app.start(client, 'check', 'check'), app.start(client, 'total', 'total'))
        async def both_ready():
            return all((app.root / (name + '.ready')).exists() for name in ['check', 'total'])
        await until(both_ready)
        snapshots = await asyncio.gather(*(client.inspect(s) for s in [check, total]))
        assert all(s.lifecycle == 'running' for s in snapshots)
        show('Parallel', workers=2, both_running=True)
        (app.root / 'release').touch()
        ended = await asyncio.gather(app.ended(client, check), app.ended(client, total))
        codes = [s.raw['lifecycle'].get('code') for s in ended]
        successful = all(s.lifecycle == 'exited' and s.raw['lifecycle'].get('code') == 0 for s in ended)
        if not successful:
            assert fail and codes == [7, 0]
            assert not (app.root / 'report.txt').exists()
            assert not any(s.name == 'report' for s in await client.list())
            show('Gate rejected', exit_codes=codes, report_started=False)
            return
        assert not fail
        report = await app.start(client, 'report', 'report')
        assert (await app.ended(client, report)).raw['lifecycle'].get('code') == 0
        artifact = (app.root / 'report.txt').read_text()
        assert artifact == 'Rows: 2; total cents: 4500\n'
        save(app.root / 'accepted.json', {'exit_codes': codes, 'artifact': artifact, 'sha256': hashlib.sha256(artifact.encode()).hexdigest()})
        show('Report verified', rows=2, total_cents=4500, prerequisite_exits=codes)
        # docs:end dependency-gate


async def archive(client, session, target, start):
    offset = start
    with target.open('ab') as output:
        while True:
            page = await client.read_output(session, from_offset=offset, max_bytes=16)
            assert page.from_offset == offset
            output.write(page.data)
            output.flush()
            os.fsync(output.fileno())
            # docs:start output-checkpoint
            offset = page.from_offset + len(page.data)
            save(target.with_suffix('.checkpoint.json'), {'session_id': session.id, 'offset': offset})
            if not page.has_more:
                return offset
            # docs:end output-checkpoint


async def output_checkpoint(app):
    target = app.root / 'archive.bin'
    async with app.client() as first:
        session = await app.start(first, 'lines', 'numbered-lines')
        async def produced():
            if not (app.root / 'first.ready').exists(): return False
            return b'entry-003' in (await first.read_output(session)).data
        await until(produced)
        offset = await archive(first, session, target, 0)
        reference = session.id
    show('Checkpoint saved', offset=offset, client_closed=True)
    (app.root / 'continue').touch()
    # docs:start resume-output
    async with app.client() as resumed:
        session = await resumed.inspect(reference)
        assert session.id == reference
        assert (await app.ended(resumed, session)).raw['lifecycle'].get('code') == 0
        checkpoint = json.loads(target.with_suffix('.checkpoint.json').read_text())
        assert checkpoint['session_id'] == reference and target.stat().st_size == checkpoint['offset']
        end = await archive(resumed, session, target, checkpoint['offset'])
    expected = ''.join(f'entry-{i:03d}\r\n' for i in range(1, 7)).encode()
    assert target.read_bytes() == expected
    show('Archive verified', bytes=end, lines=6, same_session=True, duplicates=0)
    # docs:end resume-output


async def daemon_reconnect(app):
    async with app.client() as initial:
        session = await app.start(initial, 'hold', 'surviving-worker')
        async def ready(): return (app.root / 'native-pid.txt').exists()
        await until(ready)
        identity = session.id
        native_pid = int((app.root / 'native-pid.txt').read_text())
        async def initial_output():
            page = await initial.read_output(session)
            return page if b'Worker ready' in page.data else None
        baseline = await until(initial_output)
        checkpoint = baseline.from_offset + len(baseline.data)
        (app.root / 'before-crash.bin').write_bytes(baseline.data)
    # docs:start daemon-reconnect
    # Crash this owned daemon; graceful shutdown intentionally stops its sessions.
    app.daemon.kill()
    await asyncio.to_thread(app.daemon.wait, 20)
    app.log.close()
    os.kill(native_pid, 0)
    show('Daemon interrupted', native_process_alive=True)
    await app.start_daemon()
    async with app.client() as replacement:
        recovered = await replacement.inspect(identity)
        assert recovered.id == identity and recovered.lifecycle == 'running'
        os.kill(native_pid, 0)
        (app.root / 'finish').touch()
        ended = await app.ended(replacement, recovered)
        assert ended.lifecycle == 'exited' and ended.raw['lifecycle'].get('code') == 0
        gap_start = None
        try:
            await replacement.read_output(recovered, from_offset=0)
        except OutputGap as gap:
            gap_start = gap.available_from
        page = await replacement.read_output(recovered, from_offset=checkpoint)
        output = (app.root / 'before-crash.bin').read_bytes() + page.data
        assert b'Worker ready' in output and b'Worker finished' in output
        (app.root / 'recovered-output.bin').write_bytes(output)
        show('Recovered', same_session=True, same_native_pid=True,
             archived_prefix_bytes=checkpoint, available_after_crash=gap_start, exit_code=0)
    # docs:end daemon-reconnect


async def mcp_observer(app, source):
    if not os.access(app.bin / 'uc-mcp', os.X_OK):
        raise ValueError('Build the matching uc-mcp binary for this workflow')
    sys.path.insert(0, str(source / 'scripts'))
    from mcp_probe import Client as MCP
    async with app.client() as client:
        session = await app.start(client, 'hold', 'observed-worker')
        async def ready(): return (app.root / 'native-pid.txt').exists()
        await until(ready)
        # docs:start readonly-mcp
        observer = await asyncio.to_thread(MCP, app.bin / 'uc-mcp', app.home, read_only=True)
        try:
            tools = (await asyncio.to_thread(observer.request, 'tools/list', {}))['result']['tools']
            names = {t['name'] for t in tools}
            assert {'inspect_session', 'read_output', 'list_sessions'} <= names
            assert not {'start_session', 'send_input', 'stop_session'} & names
            detail = await asyncio.to_thread(observer.call, 'inspect_session', session_id=session.id)
            assert detail['summary']['id'] == session.id
            denied = await asyncio.to_thread(observer.call_raw, 'stop_session', session_id=session.id)
            assert denied.get('error') or denied.get('result', {}).get('isError')
            assert (await client.inspect(session)).lifecycle == 'running'
            show('Observer verified', readonly=True, expected_session=True, mutation_rejected=True, process_running=True)
        finally:
            await asyncio.to_thread(observer.close)
        # docs:end readonly-mcp


async def main(args):
    async with demo(args) as app:
        if args.workflow in {'parallel-report', 'failed-prerequisite'}:
            await jobs(app, args.workflow == 'failed-prerequisite')
        elif args.workflow == 'output-checkpoint':
            await output_checkpoint(app)
        elif args.workflow == 'daemon-reconnect':
            await daemon_reconnect(app)
        else:
            await mcp_observer(app, args.source.resolve())
    show('Cleanup verified', owned_sessions_stopped=True, daemon_exited=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workflow', choices=['parallel-report', 'failed-prerequisite', 'output-checkpoint', 'daemon-reconnect', 'mcp-observer'])
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[3])
    asyncio.run(main(parser.parse_args()))
