# The runner supplies the Python package, matching bridge, and isolated daemon.
Output docs/assets/demos/python-control.gif
Require uc
Require python3
Source docs/assets/demos/theme.tape
Set Height 1000
Set FontSize 22
Source docs/assets/demos/setup.tape

Type "# Start, send input, verify the response, stop"
Enter
Type "cat control.py"
Enter
Wait+Line /demo \$$/
Sleep 10s
Hide
Type@1ms "clear"
Enter
Wait+Line /demo \$$/
Show

Type "python3 control.py"
Enter
Wait+Screen /APP_ACK: hello from Python/
Wait+Screen /Final state: (stopped|exited)/
Wait+Line /demo \$$/
Sleep 3s
Type "uc logs python-demo --max-bytes 4096"
Enter
Wait+Line /demo \$$/
Sleep 4s
Screenshot docs/assets/demos/python-control-poster.png
Sleep 500ms
Hide
Type@1ms "touch recording-complete"
Enter
Wait+Line /demo \$$/
