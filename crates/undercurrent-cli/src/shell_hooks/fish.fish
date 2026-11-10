# >>> uc detection hooks >>>
# Installed by `uc init fish`; inactive outside managed UC sessions.
if test -n "$UC_SESSION"; and not set -q __UC_SHELL_HOOKS_ACTIVE
    set -g __UC_SHELL_HOOKS_ACTIVE 1
    set -g __uc_capture_helper "$UC_SHELL_HELPER"
    function __uc_capture_environment
        set -l saved_status $status
        if test -n "$__uc_capture_helper"
            UC_CAPTURE_SHELL_PID=$fish_pid "$__uc_capture_helper" --capture-shell-environment >/dev/null 2>&1
        end
        return $saved_status
    end

    # Prompt decoration is opt-in; UC_PROMPT overrides the installed default.
    set -l __uc_prompt_mode off
    if set -q UC_PROMPT
        set __uc_prompt_mode "$UC_PROMPT"
    end
    set -g __uc_prompt_prefix ''
    switch "$__uc_prompt_mode"
        case simple
            set -g __uc_prompt_prefix '[uc] '
        case session
            set -l session_name (command uc context --name 2>/dev/null)
            if test $status -eq 0; and test -n "$session_name"
                set session_name (string sub -l 80 -- (string replace -ar '[^a-zA-Z0-9_.-]' '_' -- "$session_name"))
                set -g __uc_prompt_prefix "[uc:$session_name] "
            else
                set -g __uc_prompt_prefix '[uc] '
            end
    end
    if test -n "$__uc_prompt_prefix"
        # Copying autoloads the existing prompt before installing the wrapper.
        functions -c fish_prompt __uc_original_fish_prompt
        function __uc_restore_status
            return $argv[1]
        end
        function fish_prompt
            set -l saved_status $status
            printf '%s' "$__uc_prompt_prefix"
            # Preserve the previous command status for the original prompt.
            __uc_restore_status $saved_status
            __uc_original_fish_prompt
        end
    end

    function __uc_preexec --on-event fish_preexec
        __uc_capture_environment
        printf '\e]9091;uc=running\a' 2>/dev/null
    end
    function __uc_precmd --on-event fish_prompt
        set -l saved_status $status
        __uc_capture_environment
        printf '\e]9091;uc=ready\a' 2>/dev/null
        return $saved_status
    end

    __uc_precmd
end
# <<< uc detection hooks <<<
