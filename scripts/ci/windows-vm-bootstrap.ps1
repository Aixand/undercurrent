# SPDX-License-Identifier: Apache-2.0
# Runs only inside the disposable Server 2022 guest created by prepare-windows-vm.py.
#requires -Version 5.1
#requires -RunAsAdministrator
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$os = Get-CimInstance Win32_OperatingSystem
if ($os.ProductType -eq 1 -or $os.BuildNumber -ne '20348') {
    throw 'Bootstrap is restricted to a disposable Windows Server 2022 guest'
}
$work = 'C:\undercurrent-ci'
$null = New-Item -ItemType Directory -Force -Path $work
Start-Transcript -Path (Join-Path $work 'bootstrap.log') -Append

function Set-Progress([string] $Stage, [string] $Status = 'running') {
    @{ stage = $Stage; status = $Status; timestamp = [DateTime]::UtcNow.ToString('o') } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $work 'bootstrap-status.json') -Encoding UTF8
}
function Download([string] $Url, [string] $Name) {
    $destination = Join-Path $work $Name
    if (-not (Test-Path -LiteralPath $destination)) {
        & curl.exe --fail --location --retry 3 --silent --show-error --output $destination $Url
        if ($LASTEXITCODE -ne 0) { throw "Download failed: $Name" }
    }
    return $destination
}
function Install([string] $File, [string[]] $Arguments) {
    $process = Start-Process -FilePath $File -ArgumentList $Arguments -WindowStyle Hidden -Wait -PassThru
    if ($process.ExitCode -notin @(0, 3010)) { throw "Installer failed: $File ($($process.ExitCode))" }
}
try {
    # Preserve this reproduction image's patch level until explicitly serviced.
    $null = New-Item -Path 'HKLM:\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU' -Force
    Set-ItemProperty 'HKLM:\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU' -Name NoAutoUpdate -Value 1 -Type DWord
    Set-Progress 'ssh'
    $sshArchive = Download 'https://github.com/PowerShell/Win32-OpenSSH/releases/download/10.0.0.0p2-Preview/OpenSSH-Win64.zip' 'openssh.zip'
    Expand-Archive -LiteralPath $sshArchive -DestinationPath 'C:\Program Files' -Force
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Program Files\OpenSSH-Win64\install-sshd.ps1'
    if ($LASTEXITCODE -ne 0) { throw 'SSH service installation failed' }
    $sshState = Join-Path $env:ProgramData 'ssh'
    $null = New-Item -ItemType Directory -Force -Path $sshState
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'authorized_keys') -Destination (Join-Path $sshState 'administrators_authorized_keys')
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'ssh_host_ed25519_key') -Destination $sshState
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'ssh_host_ed25519_key.pub') -Destination $sshState
    foreach ($name in @('administrators_authorized_keys', 'ssh_host_ed25519_key')) {
        & icacls.exe (Join-Path $sshState $name) /inheritance:r /grant:r '*S-1-5-18:F' '*S-1-5-32-544:F'
        if ($LASTEXITCODE -ne 0) { throw 'SSH file permissions failed' }
    }
    @'
Port 22
HostKey __PROGRAMDATA__/ssh/ssh_host_ed25519_key
PubkeyAuthentication yes
PasswordAuthentication no
AllowUsers administrator
AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys
Subsystem sftp sftp-server.exe
'@ | Set-Content -LiteralPath (Join-Path $sshState 'sshd_config') -Encoding ASCII
    if (-not (Get-NetFirewallRule -Name 'UndercurrentCI-SSH' -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -Name 'UndercurrentCI-SSH' -DisplayName 'Undercurrent local CI SSH' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 22 | Out-Null
    }
    Set-Service sshd -StartupType Automatic
    Start-Service sshd
    Set-Progress 'python'
    $python = Download 'https://www.python.org/ftp/python/3.13.15/python-3.13.15-amd64.exe' 'python.exe'
    Install $python @('/quiet', 'InstallAllUsers=1', 'TargetDir=C:\Python313', 'PrependPath=1', 'Include_test=0')
    Set-Progress 'git'
    $git = Download 'https://github.com/git-for-windows/git/releases/download/0.1.0-beta.55.0.windows.5/Git-2.55.0.5-64-bit.exe' 'git.exe'
    Install $git @('/VERYSILENT', '/NORESTART', '/NOCANCEL', '/SP-')
    Set-Progress 'powershell'
    $pwsh = Download 'https://github.com/PowerShell/PowerShell/releases/download/v7.6.6/PowerShell-7.6.6-win-x64.msi' 'powershell.msi'
    Install 'msiexec.exe' @('/i', $pwsh, '/qn', '/norestart', 'ADD_PATH=1')
    Set-Progress 'msvc'
    $vs = Download 'https://aka.ms/vs/17/release/vs_buildtools.exe' 'vs-buildtools.exe'
    Install $vs @('--quiet', '--wait', '--norestart', '--nocache', '--add', 'Microsoft.VisualStudio.Workload.VCTools', '--includeRecommended')
    Set-Progress 'rust'
    $rust = Download 'https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe' 'rustup-init.exe'
    Install $rust @('-y', '--default-toolchain', '1.95.0', '--profile', 'minimal')
    & "$env:USERPROFILE\.cargo\bin\rustup.exe" component add --toolchain 1.95.0 clippy rustfmt
    if ($LASTEXITCODE -ne 0) { throw 'Rust components failed' }
    & 'C:\Python313\python.exe' -m pip install --disable-pip-version-check pywinpty==3.0.5
    if ($LASTEXITCODE -ne 0) { throw 'ConPTY fixture dependency failed' }
    Set-Progress 'ready' 'passed'
} catch {
    Set-Progress 'bootstrap' 'failed'
    Write-Error $_
} finally {
    Stop-Transcript
}
