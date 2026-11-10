Hide
Type@1ms `test -f "$UC_DEMO_WORKSPACE/.recording-owned" && cd "$UC_DEMO_WORKSPACE" || exit 1`
Enter
Sleep 200ms
Type@1ms `PS1='demo $ '; unset PROMPT_COMMAND; set +o history; clear`
Enter
Wait+Line /demo \$$/
Show
