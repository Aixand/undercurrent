# >>> uc detection hooks >>>
# Installed by `uc init powershell`; inactive outside managed UC sessions.
if ($env:UC_SESSION -and -not (Get-Variable __UC_SHELL_HOOKS_ACTIVE -Scope Global -ErrorAction Ignore)) {
    $global:__UC_SHELL_HOOKS_ACTIVE = $true
    $global:__uc_capture_helper = $env:UC_SHELL_HELPER

    function global:__uc_capture_environment {
        $savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ValueOnly -ErrorAction Ignore
        $savedShellPid = $env:UC_CAPTURE_SHELL_PID
        try {
            if ($global:__uc_capture_helper) {
                $env:UC_CAPTURE_SHELL_PID = [string]$PID
                & $global:__uc_capture_helper --capture-shell-environment *> $null
            }
        } catch {
            # Hook failures must not print exports or interrupt the user's prompt.
        } finally {
            $env:UC_CAPTURE_SHELL_PID = $savedShellPid
            $global:LASTEXITCODE = $savedExitCode
        }
    }

    $ucPromptMode = 'off'
    if ($env:UC_PROMPT) { $ucPromptMode = $env:UC_PROMPT }
    $global:__uc_prompt_prefix = ''
    if ($ucPromptMode -eq 'simple') { $global:__uc_prompt_prefix = '[uc] ' }
    if ($ucPromptMode -eq 'session') {
        $global:__uc_prompt_prefix = '[uc] '
        $ucSavedExitCode = Get-Variable LASTEXITCODE -Scope Global -ValueOnly -ErrorAction Ignore
        try {
            $ucName = (& uc context --name 2>$null) -join ''
            if ($LASTEXITCODE -eq 0 -and $ucName) {
                $ucName = $ucName -replace '[^a-zA-Z0-9_.-]', '_'
                $ucName = $ucName.Substring(0, [Math]::Min(80, $ucName.Length))
                $global:__uc_prompt_prefix = '[uc:' + $ucName + '] '
            }
        } catch {} finally { $global:LASTEXITCODE = $ucSavedExitCode }
    }

    $global:__uc_original_prompt = (Get-Command prompt -CommandType Function).ScriptBlock
    function global:prompt {
        # Capture the native exit code even before the first native command ran.
        $savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ValueOnly -ErrorAction Ignore
        $originalPrompt = & $global:__uc_original_prompt
        try {
            __uc_capture_environment
            [Console]::Write(([char]27 + ']9091;uc=ready' + [char]7))
        } finally { $global:LASTEXITCODE = $savedExitCode }
        $global:__uc_prompt_prefix + ($originalPrompt -join '')
    }

    $ucReadLine = Get-Command PSConsoleHostReadLine -CommandType Function -ErrorAction Ignore
    if ($ucReadLine) {
        $global:__uc_original_readline = $ucReadLine.ScriptBlock
        function global:PSConsoleHostReadLine {
            $line = & $global:__uc_original_readline
            if ($line) {
                __uc_capture_environment
                [Console]::Write(([char]27 + ']9091;uc=running' + [char]7))
            }
            $line
        }
    }
}
# <<< uc detection hooks <<<
