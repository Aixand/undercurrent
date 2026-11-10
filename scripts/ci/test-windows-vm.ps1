# SPDX-License-Identifier: Apache-2.0
#requires -Version 5.1
param(
    [Parameter(Mandatory = $true)][string] $Target,
    [string] $Identity,
    [int] $Port = 22,
    [string] $KnownHosts,
    [string] $Stage = 'all',
    [string] $Root = (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent),
    [string] $RequireBuild = '20348.5622'
)
$ErrorActionPreference = 'Stop'
$arguments = @((Join-Path $PSScriptRoot 'windows-vm.py'), '--target', $Target,
    '--stage', $Stage, '--root', $Root, '--require-build', $RequireBuild, '--port', $Port)
if ($Identity) { $arguments += @('--identity', $Identity) }
if ($KnownHosts) { $arguments += @('--known-hosts', $KnownHosts) }
if (Get-Command uv -ErrorAction SilentlyContinue) {
    & uv run --python 3.13 --no-project python @arguments
} else {
    & python @arguments
}
exit $LASTEXITCODE
