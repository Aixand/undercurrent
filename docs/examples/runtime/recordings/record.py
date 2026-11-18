"""Record real connected SDK workflows with VHS; source outputs stay beside this file.

From the Undercurrent checkout, use the selected SDK environment's Python:
python docs/examples/runtime/recordings/record.py --runtime-bin /path/to/bin
Requires vhs, ttyd, ffmpeg, ffprobe and Chromium. No authenticated providers run.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

WORKFLOWS = {
    'output-checkpoint': ('output-checkpoint', 'Archive verified', 'Archive six lines across two SDK clients; verify exact bytes'),
    'daemon-reconnect': ('daemon-reconnect', 'Recovered', 'Recover the original process after a private daemon crash'),
    'mcp-observer': ('mcp-observer', 'Observer verified', 'Real read-only MCP discovery, inspect and rejected mutation'),
}
DEPENDENCIES = {slug: ['workflows.py', 'worker.py'] for slug in WORKFLOWS}
PRESET = '''Set Shell "bash"
Set Width 1280
Set Height 800
Set FontFamily "Noto Sans Mono"
Set FontSize 20
Set LineHeight 1.2
Set Padding 24
Set Margin 0
Set BorderRadius 0
Set CursorBlink false
Set Framerate 20
Set TypingSpeed 15ms
Set WaitTimeout 90s
Set Theme { "name": "Connected SDKs", "background": "#0b1628", "foreground": "#dce9f5", "black": "#142238", "red": "#ff7e8a", "green": "#83d9a1", "yellow": "#f4d17a", "blue": "#3da5ff", "magenta": "#bca2ff", "cyan": "#65bcbe", "white": "#dce9f5", "brightBlack": "#71869f", "brightRed": "#ff9ca6", "brightGreen": "#a8e6bc", "brightYellow": "#ffe1a0", "brightBlue": "#7abfff", "brightMagenta": "#d4bdff", "brightCyan": "#9ee8ff", "brightWhite": "#ffffff", "cursor": "#65bcbe" }
'''


def run(command, *, cwd, env, timeout=180):
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('Recording command failed: ' + str(command[0]) + '\n' + result.stdout[-3000:] + result.stderr[-3000:])
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-bin', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--browser', type=Path)
    parser.add_argument('--only', choices=WORKFLOWS)
    args = parser.parse_args()
    for tool in ['vhs', 'ttyd', 'ffmpeg', 'ffprobe']:
        if shutil.which(tool) is None:
            raise RuntimeError('Missing recording tool: ' + tool)
    folder = Path(__file__).resolve().parent
    source = folder.parent
    candidates = sorted((Path.home() / '.cache/ms-playwright').glob('chromium-*/chrome-linux64/chrome'))
    browser = args.browser or (candidates[-1] if candidates else None)
    env = dict(os.environ, PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'],
               RUNTIME_BIN=str(args.runtime_bin.resolve()), UC_SOURCE=str(args.source.resolve()), PYTHONDONTWRITEBYTECODE='1')
    if browser:
        env['VHS_BROWSER_PATH'] = str(browser.resolve())
    for slug in ([args.only] if args.only else WORKFLOWS):
        script, result_marker, description = WORKFLOWS[slug]
        workspace = Path(tempfile.mkdtemp(prefix='uc-vhs-wf-'))
        (workspace / 'runtime').mkdir()
        for item in source.glob('*.py'):
            shutil.copy2(item, workspace / 'runtime' / item.name)
        tape = f'Output {slug}.gif\n' + PRESET + f'''
Hide
Type@1ms "PS1='$ '; unset PROMPT_COMMAND; set +o history; clear"
Enter
Wait+Line /^\\$ ?$/
Show
Type "# {description}"
Enter
Type 'python runtime/workflows.py {script} --work demo --bin-dir "$RUNTIME_BIN" --source "$UC_SOURCE"'
Enter
Wait+Screen /{result_marker}/
Wait+Line /^\\$ ?$/
Sleep 5s
Type `python -c 'import json; c=json.load(open("demo/cleanup.json")); assert c["owned_sessions_stopped"] and c["daemon_exited"] and not c["failures"]; print("Owned processes stopped; evidence retained.")'`
Enter
Wait+Screen /Owned processes stopped; evidence retained./
Wait+Line /^\\$ ?$/
Sleep 4s
Screenshot {slug}-poster.png
Sleep 500ms
'''
        # A short workspace copy keeps commands readable; source hashes establish its identity.
        (workspace / f'{slug}.tape').write_text(tape)
        run(['vhs', 'validate', f'{slug}.tape'], cwd=workspace, env=env)
        run(['vhs', f'{slug}.tape'], cwd=workspace, env=env, timeout=240)
        cleanup = json.loads((workspace / 'demo/cleanup.json').read_text())
        assert cleanup == {'daemon_exited': True, 'failures': [], 'owned_sessions_stopped': True}
        run(['ffmpeg', '-y', '-v', 'error', '-i', f'{slug}.gif', '-an', '-c:v', 'libopenh264',
             '-b:v', '1400k', '-r', '20', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', f'{slug}.mp4'], cwd=workspace, env=env)
        media = json.loads(run(['ffprobe', '-v', 'error', '-show_entries', 'stream=width,height,duration,codec_name',
                               '-of', 'json', f'{slug}.mp4'], cwd=workspace, env=env))['streams'][0]
        assert media['codec_name'] == 'h264' and media['width'] == 1280 and media['height'] == 800
        assert 8 < float(media['duration']) < 180
        for suffix in ['.mp4', '-poster.png', '.tape']:
            shutil.copy2(workspace / (slug + suffix), folder / (slug + suffix))
        capture = {'schema': 1, 'scope': 'Actual local Undercurrent SDK/processes and bounded fixtures; no model calls',
                   'workflow': slug, 'recordedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                   'sourceFiles': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (source / name for name in DEPENDENCIES[slug])},
                   'tapeSha256': hashlib.sha256(tape.encode()).hexdigest(), 'media': media, 'cleanup': cleanup,
                   'method': 'VHS executes a byte-identical short-path source copy; output is real, never preprinted or simulated',
                   'runtimeBin': str(args.runtime_bin.resolve()), 'evidence': str(workspace / 'demo')}
        (folder / (slug + '-capture.json')).write_text(json.dumps(capture, indent=2) + '\n')
        print(f'Recorded {slug}: {media["duration"]} seconds; {workspace}', flush=True)


if __name__ == '__main__':
    main()
