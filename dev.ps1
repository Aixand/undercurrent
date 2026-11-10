# SPDX-License-Identifier: Apache-2.0
# Keep the command-line interface aligned with dev.sh (including --prod).
#requires -Version 5.1

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$productOperations = @('doctor', 'develop', 'check', 'test', 'build', 'package', 'verify', 'inspect', 'commands', 'plan', 'upgrade', 'rollback', 'prune', 'export-release', 'release-check')
$productOptions = @('--artifact', '--prefix', '--product', '--scope', '--owner', '--register-path', '--command-policy')
$productRoute = $args.Count -gt 0 -and ($args[0] -in $productOperations -or
    @($args | Where-Object { $_ -in $productOptions -or $_ -match '^--(artifact|prefix|product|scope|owner|command-policy)=' }).Count -gt 0)
if ($productRoute) {
    $runner = if ($env:AIXAND_DEV_PYTHON) { $env:AIXAND_DEV_PYTHON } else { $env:UNDERCURRENT_DEV_PYTHON }
    $prefixArguments = @()
    if (-not $runner) {
        if (Get-Command python -ErrorAction SilentlyContinue) { $runner = 'python' }
        elseif (Get-Command py -ErrorAction SilentlyContinue) { $runner = 'py'; $prefixArguments = @('-3') }
        else { throw 'Product developer commands require Python 3.11+; set AIXAND_DEV_PYTHON.' }
    }
    & $runner @prefixArguments -B (Join-Path $PSScriptRoot 'scripts/dev/cli.py') --project $PSScriptRoot @args
    exit $LASTEXITCODE
}
$artifacts = @('uc', 'uc-daemon', 'uc-session-host', 'uc-mcp')
$runtimeFiles = @('uc-conpty/conpty.dll', 'uc-conpty/OpenConsole.exe', 'uc-conpty/LICENSE')
$installedFiles = @($artifacts | ForEach-Object { "$_.exe" }) + $runtimeFiles

function Show-Usage {
    @'
Usage:
  .\dev.ps1 doctor
  .\dev.ps1 develop
  .\dev.ps1 check
  .\dev.ps1 test
  .\dev.ps1 build --release
  .\dev.ps1 package --output dist/release
  .\dev.ps1 verify --artifact dist/release --install
  .\dev.ps1 install --artifact dist/release --prefix C:\Tools\Undercurrent --accept-license SHA256
  .\dev.ps1 uninstall --product undercurrent --prefix C:\Tools\Undercurrent --stopped
  .\dev.ps1 install --prod
  .\dev.ps1 reinstall [--prod]
  .\dev.ps1 uninstall

Build and install native Windows release executables using the repository Rust
toolchain. Requires Rust and the native linker/build tools for that toolchain.
The Rust application must support Windows for the build to succeed.

Environment:
  UNDERCURRENT_DEV_INSTALL_DIR  Binary directory (default: $CARGO_HOME\bin or ~/.cargo/bin)
  UNDERCURRENT_CACHE_ROOT       Build-cache root (default: ~/data/cache)
  CARGO_TARGET_DIR              Exact Cargo target directory override

Reinstall stops the daemon before building. Replacement and uninstall refuse
running installed executables. Uninstall preserves configuration and state.
Use --artifact, --product, or --prefix for revision-2 install/reinstall/uninstall.
Bare reinstall/uninstall and install --prod retain the source-development route.
Use 'python scripts/dev/cli.py help' for the complete product command reference.
'@
}

function Resolve-BoundedPath([string] $Value, [string] $Name) {
    # IsPathRooted alone also accepts drive-relative and root-relative paths.
    if ($Value -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\/]+[\\/][^\\/]+)' -or
        $Value -match '[\r\n]') {
        throw "$Name must be an absolute Windows path without newlines"
    }
    $resolved = [IO.Path]::GetFullPath($Value)
    if ($resolved.TrimEnd('\', '/') -eq [IO.Path]::GetPathRoot($resolved).TrimEnd('\', '/')) {
        throw "refusing to use a filesystem root for $Name"
    }
    return $resolved.TrimEnd('\', '/')
}

function Assert-SafeInstallDirectory([string] $Directory = $installDirectory) {
    # Reject junctions as well as symlinks, including ancestors.
    $candidate = $Directory
    while ($candidate) {
        $item = Get-Item -LiteralPath $candidate -Force -ErrorAction SilentlyContinue
        if ($null -ne $item) {
            if (-not $item.PSIsContainer -or
                ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
                throw "refusing to use an unsafe install directory: $candidate"
            }
        }
        $candidate = Split-Path -Path $candidate -Parent
    }
}

function Assert-Destinations([bool] $Replace) {
    Assert-SafeInstallDirectory
    Assert-SafeInstallDirectory (Join-Path $installDirectory 'uc-conpty')
    foreach ($file in $installedFiles) {
        $destination = Join-Path $installDirectory $file
        $item = Get-Item -LiteralPath $destination -Force -ErrorAction SilentlyContinue
        if ($null -ne $item) {
            if (-not $Replace) {
                throw "$destination already exists; use '.\dev.ps1 reinstall' to replace it"
            }
            if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
                throw "refusing to replace a non-regular or linked path: $destination"
            }
        }
    }
}

function Assert-ArtifactsStopped {
    foreach ($artifact in $artifacts) {
        $destination = Join-Path $installDirectory "$artifact.exe"
        if (-not (Test-Path -LiteralPath $destination -PathType Leaf)) { continue }
        # Query full executable paths; matching process names alone could stop or
        # block an unrelated installation. Fail closed if a path is inaccessible.
        $processes = @(Get-CimInstance -ClassName Win32_Process -Filter "Name = '$artifact.exe'")
        foreach ($process in $processes) {
            if ([string]::IsNullOrEmpty($process.ExecutablePath)) {
                throw "could not verify that $artifact is stopped (PID $($process.ProcessId))"
            }
            if ([IO.Path]::GetFullPath($process.ExecutablePath) -ieq $destination) {
                throw "$artifact is still running as PID $($process.ProcessId); stop the daemon and sessions before replacing or uninstalling"
            }
        }
    }
    # OpenConsole is also used by other terminals. Check only this installation's
    # exact files; querying unrelated elevated OpenConsole processes can fail.
    foreach ($file in @('uc-conpty/conpty.dll', 'uc-conpty/OpenConsole.exe')) {
        $destination = Join-Path $installDirectory $file
        if (-not (Test-Path -LiteralPath $destination -PathType Leaf)) { continue }
        try {
            $probe = [IO.File]::Open($destination, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            $probe.Dispose()
        } catch {
            throw "runtime file is in use or inaccessible: $destination"
        }
    }
}

function Assert-ArtifactSet([string] $Directory) {
    Assert-SafeInstallDirectory (Join-Path $Directory 'uc-conpty')
    foreach ($file in $runtimeFiles) {
        $path = Join-Path $Directory $file
        $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        if ($null -eq $item -or $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "required ConPTY runtime file is missing or unsafe: $path"
        }
    }
    foreach ($artifact in $artifacts) {
        $path = Join-Path $Directory "$artifact.exe"
        $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        if ($null -eq $item -or $item.PSIsContainer -or
            ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "required release artifact is missing or unsafe: $path"
        }
        $version = & $path --version
        if ($LASTEXITCODE -ne 0) { throw "release artifact version probe failed: $path" }
        if (($version -join "`n") -notlike "$artifact *") {
            throw "release artifact returned an unexpected version identity: $version"
        }
    }
}

if ($args.Count -eq 1 -and $args[0] -in @('--help', '-h', 'help')) {
    Show-Usage
    exit 0
}
$valid = $args.Count -ge 1 -and (
    ($args[0] -eq 'install' -and $args.Count -eq 2 -and $args[1] -eq '--prod') -or
    ($args[0] -eq 'reinstall' -and ($args.Count -eq 1 -or ($args.Count -eq 2 -and $args[1] -eq '--prod'))) -or
    ($args[0] -eq 'uninstall' -and $args.Count -eq 1)
)
if (-not $valid) {
    Show-Usage
    exit 2
}

try {
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
        throw 'dev.ps1 requires Windows; use dev.sh on Linux or macOS'
    }
    $cargoRoot = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $HOME '.cargo' }
    $installPath = if ($env:UNDERCURRENT_DEV_INSTALL_DIR) { $env:UNDERCURRENT_DEV_INSTALL_DIR } else { Join-Path $cargoRoot 'bin' }
    $installDirectory = Resolve-BoundedPath $installPath 'UNDERCURRENT_DEV_INSTALL_DIR / CARGO_HOME'
    $command = $args[0]
    $replace = $command -ne 'install'
    Assert-Destinations $replace

    if ($command -eq 'uninstall') {
        Assert-ArtifactsStopped
        $removed = $false
        foreach ($file in $installedFiles) {
            $destination = Join-Path $installDirectory $file
            if (Test-Path -LiteralPath $destination) {
                Remove-Item -LiteralPath $destination -Force
                $removed = $true
            }
        }
        $runtimeDirectory = Join-Path $installDirectory 'uc-conpty'
        if ((Test-Path -LiteralPath $runtimeDirectory -PathType Container) -and
            -not (Get-ChildItem -LiteralPath $runtimeDirectory -Force)) {
            [IO.Directory]::Delete($runtimeDirectory)
        }
        if ($removed) { Write-Output "Uninstalled Undercurrent development binaries from $installDirectory" }
        else { Write-Output "Undercurrent development binaries are not installed in $installDirectory" }
        Write-Output 'Undercurrent configuration, database, logs, and session state were preserved.'
        exit 0
    }

    $null = Get-Command cargo -CommandType Application -ErrorAction Stop
    $cachePath = if ($env:UNDERCURRENT_CACHE_ROOT) { $env:UNDERCURRENT_CACHE_ROOT } else { Join-Path $HOME 'data/cache' }
    $cacheRoot = Resolve-BoundedPath $cachePath 'UNDERCURRENT_CACHE_ROOT'
    $targetPath = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $cacheRoot 'undercurrent/dev-installer-target' }
    $targetDirectory = Resolve-BoundedPath $targetPath 'CARGO_TARGET_DIR'
    $null = [IO.Directory]::CreateDirectory($installDirectory)
    Assert-SafeInstallDirectory
    $installedCli = Join-Path $installDirectory 'uc.exe'
    if ($replace -and (Test-Path -LiteralPath $installedCli -PathType Leaf)) {
        & $installedCli daemon stop
        if ($LASTEXITCODE -ne 0) { throw 'could not stop the daemon; reinstall aborted' }
    }
    Assert-ArtifactsStopped

    # Explicitly select the native host, even when Cargo configuration sets a
    # cross-compilation target. Cargo puts these outputs in <target>/<host>/release.
    Push-Location -LiteralPath $PSScriptRoot
    try {
        $rustInfo = & rustc -vV
        if ($LASTEXITCODE -ne 0) { throw 'could not determine the Rust host toolchain' }
        $hostLine = @($rustInfo | Where-Object { $_ -match '^host: ' })
        if ($hostLine.Count -ne 1) { throw 'could not determine the Rust host target' }
        $hostTarget = $hostLine[0].Substring(6).Trim()
        if ($hostTarget -notmatch '^[a-zA-Z0-9_]+-pc-windows-(msvc|gnu|gnullvm)$') {
            throw "a native Windows Rust toolchain is required; found $hostTarget"
        }
        & cargo build --manifest-path (Join-Path $PSScriptRoot 'Cargo.toml') --locked --release `
            --target $hostTarget --target-dir $targetDirectory `
            --package undercurrent-cli --package undercurrent-daemon --package undercurrent-mcp --bins
        if ($LASTEXITCODE -ne 0) { throw 'release build failed; installed binaries were not replaced' }
    }
    finally { Pop-Location }

    $releaseDirectory = Join-Path (Join-Path $targetDirectory $hostTarget) 'release'
    Assert-ArtifactSet $releaseDirectory
    Assert-Destinations $replace
    Assert-ArtifactsStopped
    $stagingDirectory = Join-Path $installDirectory ('.undercurrent-dev-install.' + [Guid]::NewGuid().ToString('N'))
    $null = [IO.Directory]::CreateDirectory($stagingDirectory)
    $null = [IO.Directory]::CreateDirectory((Join-Path $stagingDirectory 'uc-conpty'))
    try {
        foreach ($file in $installedFiles) {
            Copy-Item -LiteralPath (Join-Path $releaseDirectory $file) -Destination (Join-Path $stagingDirectory $file)
        }
        Assert-ArtifactSet $stagingDirectory
        Assert-Destinations $replace
        Assert-ArtifactsStopped
        $null = [IO.Directory]::CreateDirectory((Join-Path $installDirectory 'uc-conpty'))
        foreach ($file in $installedFiles) {
            Move-Item -LiteralPath (Join-Path $stagingDirectory $file) `
                -Destination (Join-Path $installDirectory $file) -Force
        }
        Assert-ArtifactSet $installDirectory
    }
    finally {
        # Delete only known staged files; never recursively remove a computed path.
        foreach ($file in $installedFiles) {
            $stagedPath = Join-Path $stagingDirectory $file
            if (Test-Path -LiteralPath $stagedPath -PathType Leaf) {
                Remove-Item -LiteralPath $stagedPath -Force
            }
        }
        [IO.Directory]::Delete((Join-Path $stagingDirectory 'uc-conpty'))
        [IO.Directory]::Delete($stagingDirectory)
    }
    $verb = if ($replace) { 'Reinstalled' } else { 'Installed' }
    Write-Output "$verb Undercurrent release binaries in $installDirectory"
    Write-Output 'The daemon remains stopped; start it explicitly with: uc daemon start'
}
catch {
    [Console]::Error.WriteLine("undercurrent dev installer: $($_.Exception.Message)")
    exit 1
}
