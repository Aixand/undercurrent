# Run through scripts/docs/render_demos.py to isolate and clean up runtime state.
Output docs/assets/demos/overview.gif
Require uc
Require python3
Source docs/assets/demos/theme.tape
Source docs/assets/demos/setup.tape

Type "# Start a task in a named session"
Enter
Type "uc start demo -- python3 -u worker.py"
Enter
Wait+Line /demo \$$/
Sleep 1500ms
Type "uc attach demo"
Enter
Wait+Screen /tick 0[3-9] \| working/
Sleep 4s
Alt+u
Type@100ms "d"
Wait+Line /demo \$$/
Hide
Type@1ms "python3 checkpoint.py detached"
Enter
Wait+Line /demo \$$/
Type@1ms "clear"
Enter
Wait+Line /demo \$$/
Show

Type "# The task keeps running after detach"
Enter
Type "uc ls --name demo"
Enter
Wait+Line /demo \$$/
Sleep 4s
Type "uc logs demo --max-bytes 4096"
Enter
Wait+Line /demo \$$/
Sleep 2s
Hide
Type@1ms "python3 checkpoint.py continued"
Enter
Wait+Line /demo \$$/
Type@1ms "clear"
Enter
Wait+Line /demo \$$/
Show

Type "# Return to the same running task"
Enter
Type "uc attach demo"
Enter
Wait+Screen /UC.*demo.*attached/
Sleep 4s
Screenshot docs/assets/demos/overview-poster.png
Sleep 500ms
Alt+u
Type@100ms "d"
Wait+Line /demo \$$/
Type "uc stop demo"
Enter
Wait+Line /demo \$$/
Sleep 2s
Hide
Type@1ms "touch recording-complete"
Enter
Wait+Line /demo \$$/
