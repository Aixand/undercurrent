# SPDX-License-Identifier: Apache-2.0
# Run with Windows PowerShell 5.1; no Rust build or installed product required.
#requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$workRoot = Join-Path ([IO.Path]::GetTempPath()) ('uc dev installer test ' + [Guid]::NewGuid().ToString('N'))
$originalEnvironment = @{}
$variables = @('PATH', 'UNDERCURRENT_DEV_INSTALL_DIR', 'UNDERCURRENT_CACHE_ROOT', 'CARGO_TARGET_DIR', 'UC_TEST_ROOT', 'UC_TEST_STOP_FAIL', 'UC_TEST_BUILD_FAIL')
foreach ($name in $variables) { $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$running = $null

function Get-FileDigest([string] $Path) {
    # Work in Windows PowerShell even when launched with PowerShell 7's module path.
    $hash = [Security.Cryptography.SHA256]::Create()
    $stream = [IO.File]::OpenRead($Path)
    try { return [BitConverter]::ToString($hash.ComputeHash($stream)) }
    finally { $stream.Dispose(); $hash.Dispose() }
}

function Run-Dev([int] $Expected, [string[]] $Arguments) {
    # Windows PowerShell wraps native stderr as errors, including expected failures.
    $ErrorActionPreference = 'Continue'
    $output = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $repository 'dev.ps1') @Arguments 2>&1
    $ErrorActionPreference = 'Stop'
    if ($LASTEXITCODE -ne $Expected) {
        throw "Expected exit $Expected for $Arguments, got ${LASTEXITCODE}: $output"
    }
}

try {
    $fakeBin = Join-Path $workRoot 'fake-bin'
    $fixtures = Join-Path $workRoot 'fixtures'
    $null = [IO.Directory]::CreateDirectory($fakeBin)
    $null = [IO.Directory]::CreateDirectory($fixtures)
    $source = @'
using System;
using System.IO;
using System.Reflection;
using System.Threading;
class Fixture {
    static int Main(string[] args) {
        string name = Path.GetFileNameWithoutExtension(Assembly.GetExecutingAssembly().Location);
        if (args.Length == 1 && args[0] == "--version") {
            Console.WriteLine(name + " 0.1.0-beta"); return 0;
        }
        if (args.Length == 2 && args[0] == "daemon" && args[1] == "stop") {
            return Environment.GetEnvironmentVariable("UC_TEST_STOP_FAIL") == "1" ? 1 : 0;
        }
        Thread.Sleep(60000); return 0;
    }
}
'@
    Add-Type -TypeDefinition $source -OutputAssembly (Join-Path $fixtures 'uc.exe') -OutputType ConsoleApplication
    foreach ($name in @('uc-daemon', 'uc-session-host', 'uc-mcp')) {
        Copy-Item -LiteralPath (Join-Path $fixtures 'uc.exe') -Destination (Join-Path $fixtures "$name.exe")
    }
    $runtimeFixture = Join-Path $fixtures 'uc-conpty'
    $null = [IO.Directory]::CreateDirectory($runtimeFixture)
    foreach ($name in @('conpty.dll', 'OpenConsole.exe')) {
        Copy-Item -LiteralPath (Join-Path $repository "vendor/portable-pty/conpty/x64/$name") -Destination $runtimeFixture
    }
    Copy-Item -LiteralPath (Join-Path $repository 'vendor/portable-pty/conpty/LICENSE') -Destination $runtimeFixture
    Set-Content -LiteralPath (Join-Path $fakeBin 'rustc.cmd') -Value '@echo host: x86_64-pc-windows-msvc' -Encoding ASCII
    Set-Content -LiteralPath (Join-Path $fakeBin 'cargo.cmd') -Encoding ASCII -Value @'
@echo off
if "%UC_TEST_BUILD_FAIL%"=="1" exit /b 1
echo %*>>"%UC_TEST_ROOT%\cargo.log"
mkdir "%CARGO_TARGET_DIR%\x86_64-pc-windows-msvc\release" 2>nul
copy /y "%UC_TEST_ROOT%\fixtures\*.exe" "%CARGO_TARGET_DIR%\x86_64-pc-windows-msvc\release\" >nul
mkdir "%CARGO_TARGET_DIR%\x86_64-pc-windows-msvc\release\uc-conpty" 2>nul
copy /y "%UC_TEST_ROOT%\fixtures\uc-conpty\*" "%CARGO_TARGET_DIR%\x86_64-pc-windows-msvc\release\uc-conpty\" >nul
exit /b 0
'@
    $env:PATH = "$fakeBin;$env:PATH"
    $env:UC_TEST_ROOT = $workRoot
    $env:UC_TEST_STOP_FAIL = '0'
    $env:UC_TEST_BUILD_FAIL = '0'
    $env:UNDERCURRENT_DEV_INSTALL_DIR = Join-Path $workRoot 'install bin'
    $env:UNDERCURRENT_CACHE_ROOT = Join-Path $workRoot 'cache'
    $env:CARGO_TARGET_DIR = Join-Path $workRoot 'target'
    Run-Dev 0 @('--help')
    Run-Dev 2 @('install')
    Run-Dev 2 @('uninstall', '--prod')
    Run-Dev 0 @('install', '--prod')
    foreach ($name in @('uc', 'uc-daemon', 'uc-session-host', 'uc-mcp')) {
        if (-not (Test-Path -LiteralPath (Join-Path $env:UNDERCURRENT_DEV_INSTALL_DIR "$name.exe"))) { throw "Missing $name" }
    }
    foreach ($name in @('conpty.dll', 'OpenConsole.exe', 'LICENSE')) {
        $installed = Join-Path $env:UNDERCURRENT_DEV_INSTALL_DIR "uc-conpty/$name"
        if ((Get-FileDigest $installed) -ne (Get-FileDigest (Join-Path $runtimeFixture $name))) {
            throw "ConPTY runtime file changed: $name"
        }
    }
    $buildLog = Get-Content -LiteralPath (Join-Path $workRoot 'cargo.log') -Raw
    if ($buildLog -notmatch '--locked --release --target x86_64-pc-windows-msvc' -or
        $buildLog -notmatch '--package undercurrent-cli --package undercurrent-daemon --package undercurrent-mcp --bins') { throw 'Unexpected Cargo arguments' }
    Run-Dev 1 @('install', '--prod')
    $env:UC_TEST_STOP_FAIL = '1'
    Run-Dev 1 @('reinstall')
    $env:UC_TEST_STOP_FAIL = '0'
    $env:UC_TEST_BUILD_FAIL = '1'
    Run-Dev 1 @('reinstall')
    $env:UC_TEST_BUILD_FAIL = '0'
    Run-Dev 0 @('reinstall')
    Run-Dev 0 @('reinstall', '--prod')
    $statePath = Join-Path $env:UNDERCURRENT_DEV_INSTALL_DIR 'preserved-state.txt'
    Set-Content -LiteralPath $statePath -Value 'preserve-me'
    $running = Start-Process -FilePath (Join-Path $env:UNDERCURRENT_DEV_INSTALL_DIR 'uc-daemon.exe') -WindowStyle Hidden -PassThru
    Run-Dev 1 @('uninstall')
    Run-Dev 1 @('reinstall')
    Stop-Process -Id $running.Id
    $running.WaitForExit()
    $running = $null
    Run-Dev 0 @('uninstall')
    Run-Dev 0 @('uninstall')
    if ((Get-Content -LiteralPath $statePath) -ne 'preserve-me') { throw 'State changed' }
    if (Get-ChildItem -LiteralPath $env:UNDERCURRENT_DEV_INSTALL_DIR -Filter '*.exe') { throw 'Uninstall left executables' }
    if (Test-Path -LiteralPath (Join-Path $env:UNDERCURRENT_DEV_INSTALL_DIR 'uc-conpty')) { throw 'Uninstall left ConPTY runtime' }
    $env:UNDERCURRENT_DEV_INSTALL_DIR = 'C:\'
    Run-Dev 1 @('uninstall')
    $env:UNDERCURRENT_DEV_INSTALL_DIR = 'C:relative'
    Run-Dev 1 @('uninstall')
    Write-Output 'Undercurrent Windows developer installer tests passed'
}
finally {
    if ($null -ne $running -and -not $running.HasExited) { Stop-Process -Id $running.Id }
    foreach ($name in $variables) { [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name], 'Process') }
    $resolvedWork = [IO.Path]::GetFullPath($workRoot)
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if (-not $resolvedWork.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
        (Split-Path $resolvedWork -Leaf) -notlike 'uc dev installer test *') { throw 'Unsafe test cleanup path' }
    if (Test-Path -LiteralPath $resolvedWork) { Remove-Item -LiteralPath $resolvedWork -Recurse -Force }
}
