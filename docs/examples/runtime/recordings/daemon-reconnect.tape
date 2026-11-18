Output daemon-reconnect.gif
Set Shell "bash"
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

Hide
Type@1ms "PS1='$ '; unset PROMPT_COMMAND; set +o history; clear"
Enter
Wait+Line /^\$ ?$/
Show
Type "# Recover the original process after a private daemon crash"
Enter
Type 'python runtime/workflows.py daemon-reconnect --work demo --bin-dir "$RUNTIME_BIN" --source "$UC_SOURCE"'
Enter
Wait+Screen /Recovered/
Wait+Line /^\$ ?$/
Sleep 5s
Type `python -c 'import json; c=json.load(open("demo/cleanup.json")); assert c["owned_sessions_stopped"] and c["daemon_exited"] and not c["failures"]; print("Owned processes stopped; evidence retained.")'`
Enter
Wait+Screen /Owned processes stopped; evidence retained./
Wait+Line /^\$ ?$/
Sleep 4s
Screenshot daemon-reconnect-poster.png
Sleep 500ms
