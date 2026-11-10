#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real PTY persistent-group regression; build CLI/daemon binaries first. Linux only."""
import argparse
import fcntl
import json
import os
import pathlib
import pty
import select
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / "scripts"))
from _support.processes import identities, terminate_identity

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--shell', choices=['bash', 'zsh', 'fish'], default='bash')
parser.add_argument('--legacy-closed', action='store_true', help='Simulate a slot closed by the previous cleanup bug')
args = parser.parse_args()
shell = shutil.which(args.shell)
if shell is None:
    parser.error('requested shell is not installed')
BIN = pathlib.Path(os.environ.get('UC_E2E_BIN_DIR', pathlib.Path(__file__).resolve().parents[3] / 'target/debug'))
fixture = tempfile.TemporaryDirectory(prefix='uc-groups-')
root = pathlib.Path(fixture.name)
home = root / 'home'
home.mkdir()
# Suppress Zsh first-run setup without installing any UC hooks.
(home / ".zshrc").touch()
product = root / 'uc'
env = dict(os.environ, HOME=str(home), UNDERCURRENT_HOME=str(product), SHELL=shell, TERM='xterm-256color', PATH=str(BIN) + ':/usr/bin:/bin')
for k in list(env):
    if k.startswith('UC_') or k in ['UNDERCURRENT_CONFIG', 'ZDOTDIR', 'BASH_ENV', 'ENV']:
        env.pop(k, None)
env['XDG_CONFIG_HOME'] = str(home / '.config')
views = []
terminal_tails = {}

def cli(*args, check=True):
    p = subprocess.run([str(BIN / 'uc'), *args], env=env, cwd=root, text=True, capture_output=True, timeout=35)
    if check and p.returncode:
        raise Exception((args, p.returncode, p.stderr))
    return p

def view(*args):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 100, 0, 0))
    p = subprocess.Popen([str(BIN / 'uc'), *args], env=env, cwd=root, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
    os.close(slave)
    views.append((p, master))
    return (p, master)

def drain(v):
    while select.select([v[1]], [], [], 0)[0]:
        try:
            terminal_tails[v[1]] = (terminal_tails.get(v[1], b'') + os.read(v[1], 65536))[-4096:]
        except OSError:
            break

def groups():
    p = product / 'state/group/workspaces.json'
    if not p.exists():
        return {}
    return next(iter(json.loads(p.read_text())['namespaces'].values()), {})

def detail(session):
    name = next(row['name'] for row in json.loads(cli('--json', 'ls').stdout)['items'] if row['id'] == session)
    return json.loads(cli('--json', 'inspect', name).stdout)

def until(fn, v=None):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if v:
            drain(v)
        if fn():
            return
        if v and v[0].poll() is not None:
            raise Exception('view exited ' + str(v[0].returncode))
        time.sleep(0.1)
    raise Exception(('condition timeout', terminal_tails.get(v[1], b'') if v else b''))

def detach(v):
    os.write(v[1], b'\x1bud')
    until(lambda: v[0].poll() is not None)
    drain(v)

def kill_root_shell(session, directory, v=None):
    pid_file = root / 'root-shell.pid'
    pid_file.unlink(missing_ok=True)
    pid_variable = '$fish_pid' if args.shell == 'fish' else '$$'
    name = detail(session)['summary']['name']
    cli('send', name, 'echo ' + pid_variable + ' > "' + str(pid_file) + '"')
    until(lambda: pid_file.exists() and pid_file.read_text().strip(), v)
    root_pid = int(pid_file.read_text().strip())
    fd = os.pidfd_open(root_pid)
    try:
        assert pathlib.Path(f'/proc/{root_pid}/cwd').resolve() == directory
        assert ('UNDERCURRENT_HOME=' + str(product)).encode() in pathlib.Path(f'/proc/{root_pid}/environ').read_bytes().split(b'\0')
        signal.pidfd_send_signal(fd, signal.SIGKILL)
        assert select.select([fd], [], [], 5)[0], 'root shell did not exit'
    finally:
        os.close(fd)
    until(lambda: detail(session)['summary']['lifecycle']['state'] == 'failed', v)

def kill_fixture_daemon():
    for entry in pathlib.Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if (entry / 'exe').resolve() != BIN / 'uc-daemon':
                continue
            values = (entry / 'environ').read_bytes().split(b'\x00')
            if ('UNDERCURRENT_HOME=' + str(product)).encode() not in values:
                continue
            fd = os.pidfd_open(int(entry.name))
            try:
                if (entry / 'exe').resolve() != BIN / 'uc-daemon' or ('UNDERCURRENT_HOME=' + str(product)).encode() not in (entry / 'environ').read_bytes().split(b'\0'):
                    continue
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                assert select.select([fd], [], [], 5)[0], 'daemon did not exit'
            finally:
                os.close(fd)
            return
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    raise Exception('isolated daemon not found')
try:
    print('testing without shell hooks', flush=True)
    v = view('group', 'new', 'work', '--shell', shell)
    until(lambda: groups().get('work', {}).get('members'), v)
    time.sleep(0.5)
    dest = root / 'directory A'
    dest.mkdir()
    os.write(v[1], ('cd "' + str(dest) + '"\n').encode())
    first = groups()['work']['members'][0]['id']
    until(lambda: detail(first).get('restoration_directory') == str(dest), v)
    print('directory tracking verified', flush=True)
    nested = root / 'nested'
    nested.mkdir()
    os.write(v[1], (args.shell + ' -ic \'cd "' + str(nested) + '"\'\n').encode())
    time.sleep(0.5)
    drain(v)
    assert detail(first)['restoration_directory'] == str(dest)
    # A delayed root-shell cd must be retained with no group view attached.
    detached = root / 'directory while detached'
    detached.mkdir()
    os.write(v[1], ('sleep 1; cd "' + str(detached) + '"\n').encode())
    detach(v)
    until(lambda: detail(first).get('restoration_directory') == str(detached))
    dest = detached
    print('detach verified', flush=True)
    kill_fixture_daemon()
    cli('daemon', 'start')
    v = view('group', 'attach', 'work')
    time.sleep(0.5)
    drain(v)
    assert groups()['work']['members'][0]['id'] == first
    detach(v)
    print('daemon crash preserves original session', flush=True)
    old_slot = groups()['work']['members'][0]['restore']['slot']
    v = view('group', 'attach', 'work')
    time.sleep(0.3)
    kill_root_shell(first, dest, v)
    time.sleep(1.5)
    drain(v)
    assert groups()['work']['members'][0]['id'] == first, 'shell exit alone must not restore an open view'
    kill_fixture_daemon()
    cli('daemon', 'start')
    until(lambda: groups()['work']['members'][0]['id'] != first, v)
    os.write(v[1], b'\n')
    until(lambda: detail(groups()['work']['members'][0]['id']).get('restoration_directory') == str(dest), v)
    assert groups()['work']['members'][0]['restore']['slot'] == old_slot
    assert groups()['work']['selected'] == old_slot
    assert groups()['work']['members'][0]['restore']['cwd'] == str(dest)
    assert len(groups()['work']['members']) == 1
    first = groups()['work']['members'][0]['id']
    detach(v)
    print('lost shell automatically restored once', flush=True)
    actual = next((row['name'] for row in json.loads(cli('--json', 'ls').stdout)['items'] if row['id'] == first))
    cleaned_directory = root / 'directory before cleanup'
    cleaned_directory.mkdir()
    cli('send', actual, 'cd "' + str(cleaned_directory) + '"')
    until(lambda: detail(first).get('restoration_directory') == str(cleaned_directory))
    dest = cleaned_directory
    # A real shell failure preserves recovery eligibility after cleanup.
    # Immediate daemon shutdown now durably stops sessions instead.
    kill_root_shell(first, dest)
    cli('clean', '--yes')
    assert not json.loads(cli('--json', 'ls').stdout)['items']
    if args.legacy_closed:
        registry = product / 'state/group/workspaces.json'
        saved = json.loads(registry.read_text())
        next(iter(saved['namespaces'].values()))['work']['members'][0]['restore']['open'] = False
        saved['revision'] += 1
        registry.write_text(json.dumps(saved))
    v = view('group', 'attach', 'work')
    until(lambda: groups()['work']['members'][0]['id'] != first, v)
    first = groups()['work']['members'][0]['id']
    until(lambda: detail(first).get('restoration_directory') == str(dest), v)
    assert groups()['work']['members'][0]['restore']['slot'] == old_slot
    assert groups()['work']['selected'] == old_slot
    assert len(groups()['work']['members']) == 1
    detach(v)
    print('failed-shell cleanup preserves restoration directory', flush=True)
    for scope in ('daemon', 'session'):
        actual = detail(first)['summary']['name']
        if scope == 'daemon':
            cli('daemon', 'stop')
            cli('daemon', 'start')
        else:
            cli('stop', actual)
        assert detail(first)['summary']['lifecycle']['state'] == 'stopped'
        cli('clean', '--yes')
        assert not json.loads(cli('--json', 'ls').stdout)['items']
        v = view('group', 'attach', 'work')
        until(lambda: not groups()['work']['members'][0]['restore']['open'], v)
        detach(v)
        member = groups()['work']['members'][0]
        assert member['id'] == first
        assert member['restore']['cwd'] == str(dest)
        assert not json.loads(cli('--json', 'ls').stdout)['items']
        print(scope + ' stop remains closed after cleanup and attach', flush=True)
        cli('group', 'restore', 'work', 'work-shell-1')
        second = groups()['work']['members'][0]['id']
        assert second != first
        v = view('group', 'attach', 'work')
        until(lambda: detail(second).get('restoration_directory') == str(dest), v)
        assert groups()['work']['members'][0]['restore']['slot'] == old_slot
        assert groups()['work']['selected'] == old_slot
        detach(v)
        first = second
        print('explicit restoration after ' + scope + ' stop verified', flush=True)

    v = view('group', 'attach', 'work')
    trap_ready = root / 'trap-ready'
    os.write(v[1], ("trap '' HUP TERM; echo ready > \"" + str(trap_ready) + "\"\n").encode())
    until(trap_ready.exists, v)
    until(lambda: detail(groups()['work']['members'][0]['id']).get('restoration_directory') == str(dest), v)
    detach(v)
    assert groups()['work']['members'][0]['restore']['cwd'] == str(dest)
    # Graceful deletion can outlive the caller's deadline. Once its stop has
    # settled, the pending group intent must still survive a daemon crash.
    result = cli('--timeout', '200ms', 'group', 'delete', 'work', '--stop', '--graceful', check=False)
    assert result.returncode and groups()['work']['deleting'], result.stdout + result.stderr
    until(lambda: detail(first)['summary']['lifecycle']['state'] == 'stopped')
    assert groups()['work']['deleting']
    kill_fixture_daemon()
    cli('daemon', 'start')
    assert groups()['work']['deleting']
    cli('group', 'delete', 'work', '--stop')
    assert 'work' not in groups()
    print('delete with stop verified', flush=True)
    cli('reset', '--yes')
    assert not (product / 'state/group/workspaces.json').exists()
    print('reset verified', flush=True)
finally:
    for p, fd in views:
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)
        os.close(fd)
    try:
        cli('stop', '--all', '--force', check=False)
        cli('daemon', 'stop', check=False)
    except Exception:
        pass
    # A failed daemon recovery can leave a host with no public endpoint.
    # Reuse the shared identity-pinned cleanup for this fixture's home only.
    for identity in identities(home=product):
        terminate_identity(identity)
    until(lambda: not identities(home=product))
    fixture.cleanup()
