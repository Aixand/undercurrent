Output docs/assets/demos/orchestration.gif
Require uc
Require python3
Source docs/assets/demos/theme.tape
Set Width 1600
Set Height 900
Source docs/assets/demos/setup.tape

Type "# Two parallel jobs, then a dependent report"
Enter
Type "cat orders.csv"
Enter
Wait+Line /demo \$$/
Sleep 3s
Type "python3 orchestrate.py"
Enter
Wait+Screen /COMPLETE: report.txt is ready/
Wait+Line /demo \$$/
Sleep 4s
Screenshot docs/assets/demos/orchestration-poster.png
Sleep 500ms
Hide
Type@1ms "clear"
Enter
Wait+Line /demo \$$/
Show
Type "# Inspect the results and all three sessions"
Enter
Type "cat report.txt"
Enter
Wait+Line /demo \$$/
Sleep 3s
Type "uc ls --label demo=orchestration"
Enter
Wait+Line /demo \$$/
Sleep 4s
Hide
Type@1ms "touch recording-complete"
Enter
Wait+Line /demo \$$/
