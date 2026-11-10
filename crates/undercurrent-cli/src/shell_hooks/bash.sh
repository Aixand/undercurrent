# >>> uc detection hooks >>>
# Installed by `uc init bash`; inactive outside managed UC sessions.
if [ -n "${UC_SESSION:-}" ] && [ -z "${__UC_SHELL_HOOKS_ACTIVE:-}" ]; then
    __UC_SHELL_HOOKS_ACTIVE=1

    __uc_capture_helper=${UC_SHELL_HELPER:-}
    __uc_capture_environment() {
        local __uc_capture_status=$?
        if [ -n "$__uc_capture_helper" ]; then
            UC_CAPTURE_SHELL_PID=$$ "$__uc_capture_helper" --capture-shell-environment >/dev/null 2>&1
        fi
        return "$__uc_capture_status"
    }

    # Prompt decoration is opt-in and never runs a daemon query per prompt.
    __uc_prompt_prefix=''
    case "${UC_PROMPT:-off}" in
        simple) __uc_prompt_prefix='[uc] ' ;;
        session)
            __uc_prompt_name=$(command uc context --name 2>/dev/null) || __uc_prompt_name=''
            # Restrict prompt metacharacters and terminal controls to plain text.
            __uc_prompt_name=$(printf '%s' "$__uc_prompt_name" | LC_ALL=C tr -c 'a-zA-Z0-9_.-' '_' | cut -c 1-80)
            __uc_prompt_prefix="[uc${__uc_prompt_name:+:$__uc_prompt_name}] "
            ;;
    esac

    __uc_update_prompt() {
        if [ -n "$__uc_prompt_prefix" ]; then
            case "$PS1" in
                "$__uc_prompt_prefix"*) ;;
                *) PS1="$__uc_prompt_prefix$PS1" ;;
            esac
        fi
    }

    __uc_precmd() {
        local __uc_saved_status=$?
        __uc_capture_environment
        printf '\033]9091;uc=ready\007' 2>/dev/null
        return "$__uc_saved_status"
    }

    case "${PS0:-}" in
        *"9091;uc=running"*) ;;
        *) PS0="${PS0:-}"'$( __uc_capture_environment )'$'\033]9091;uc=running\007' ;;
    esac

    if declare -p PROMPT_COMMAND 2>/dev/null | command grep -q 'declare -a'; then
        PROMPT_COMMAND+=(__uc_precmd __uc_update_prompt)
    else
        PROMPT_COMMAND="${PROMPT_COMMAND:+$PROMPT_COMMAND; }__uc_precmd; __uc_update_prompt"
    fi
    __uc_update_prompt
    __uc_precmd
fi
# <<< uc detection hooks <<<
