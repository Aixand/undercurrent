# The runner installs ask.yaml only in this recording's private home.
Output docs/assets/demos/interaction.gif
Require uc
Require python3
Source docs/assets/demos/theme.tape
Set Width 1600
Source docs/assets/demos/setup.tape

Type "# A sample importer waits for confirmation"
Enter
Type "uc start review --profile ask -- python3 -u approval.py"
Enter
Wait+Line /demo \$$/
Type "uc wait review --interaction requested --timeout 15s"
Enter
Wait+Line /demo \$$/
Sleep 3s
Type "uc logs review --max-bytes 4096"
Enter
Wait+Line /demo \$$/
Sleep 4s
Screenshot docs/assets/demos/interaction-poster.png
Sleep 500ms
Hide
Type@1ms "python3 checkpoint.py requested"
Enter
Wait+Line /demo \$$/
Type@1ms "clear"
Enter
Wait+Line /demo \$$/
Show
Type "# Send the answer, then wait for completion"
Enter
Type "uc send review yes --raw --newline"
Enter
Wait+Line /demo \$$/
Type "uc wait review --lifecycle exited --timeout 15s"
Enter
Wait+Line /demo \$$/
Type "uc logs review --max-bytes 4096"
Enter
Wait+Line /demo \$$/
Sleep 4s
Hide
Type@1ms "touch recording-complete"
Enter
Wait+Line /demo \$$/
