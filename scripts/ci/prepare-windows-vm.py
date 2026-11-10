# /// script
# requires-python = ">=3.13"
# dependencies = ["pycdlib==1.14.0"]
# ///
# SPDX-License-Identifier: Apache-2.0
"""Prepare unattended Server 2022 media and a disposable QEMU local VM."""

import argparse
import csv
import io
import json
from pathlib import Path
import secrets
import shutil
import subprocess
from xml.sax.saxutils import escape

import pycdlib

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--installer", type=Path, required=True)
    parser.add_argument("--qemu", type=Path, required=True)
    parser.add_argument("--memory-mb", type=int, default=4096)
    parser.add_argument("--cpus", type=int, default=4)
    parser.add_argument("--accelerator", choices=("whpx", "tcg"), default="whpx")
    args = parser.parse_args()
    directory = args.directory.resolve()
    disk = directory / "windows-server-2022.qcow2"
    if disk.exists():
        parser.error("VM disk already exists; reuse launch.json or choose a new directory")
    if args.cpus < 1 or args.memory_mb < 2048:
        parser.error("use at least one CPU and 2048 MiB of memory")
    directory.mkdir(parents=True, exist_ok=True)
    account = next(csv.reader(io.StringIO(subprocess.check_output(
        ["whoami", "/user", "/fo", "csv", "/nh"], text=True))))
    subprocess.run(["icacls", str(directory), "/inheritance:r", "/grant:r",
                    f"*{account[1]}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F"], check=True)
    if not args.installer.is_file():
        parser.error("Windows Server installation ISO is missing")
    media = directory / "bootstrap"
    media.mkdir(exist_ok=True)
    password_file = directory / "guest-password.txt"
    if not password_file.exists():
        password_file.write_text("Uc!" + secrets.token_hex(20), encoding="utf-8")
    password = escape(password_file.read_text(encoding="utf-8"))
    private_key = directory / "id_ed25519"
    host_key = media / "ssh_host_ed25519_key"
    for path in (private_key, host_key):
        if not path.exists():
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(path)], check=True)
    shutil.copyfile(str(private_key) + ".pub", media / "authorized_keys")
    host_public = Path(str(host_key) + ".pub").read_text().split()
    (directory / "known_hosts").write_text(
        "[127.0.0.1]:2222 " + " ".join(host_public[:2]) + "\n", encoding="ascii")
    shutil.copyfile(HERE / "windows-vm-bootstrap.ps1", media / "bootstrap.ps1")
    # This answer file formats only disk 0 INSIDE the new VM. The launcher exposes
    # exactly one fresh virtual disk and never passes through a host disk.
    answer = rf'''<?xml version="1.0" encoding="utf-8"?>
<unattend xmlns="urn:schemas-microsoft-com:unattend" xmlns:wcm="http://schemas.microsoft.com/WMIConfig/2002/State">
  <settings pass="windowsPE">
    <component name="Microsoft-Windows-International-Core-WinPE" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <SetupUILanguage><UILanguage>en-US</UILanguage></SetupUILanguage>
      <InputLocale>en-US</InputLocale><SystemLocale>en-US</SystemLocale><UILanguage>en-US</UILanguage><UserLocale>en-US</UserLocale>
    </component>
    <component name="Microsoft-Windows-Setup" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <DiskConfiguration><Disk wcm:action="add"><DiskID>0</DiskID><WillWipeDisk>true</WillWipeDisk>
        <CreatePartitions><CreatePartition wcm:action="add"><Order>1</Order><Type>Primary</Type><Extend>true</Extend></CreatePartition></CreatePartitions>
        <ModifyPartitions><ModifyPartition wcm:action="add"><Order>1</Order><PartitionID>1</PartitionID><Format>NTFS</Format><Label>Windows</Label><Letter>C</Letter><Active>true</Active></ModifyPartition></ModifyPartitions>
      </Disk></DiskConfiguration>
      <ImageInstall><OSImage><InstallFrom><MetaData wcm:action="add"><Key>/IMAGE/INDEX</Key><Value>4</Value></MetaData></InstallFrom>
        <InstallTo><DiskID>0</DiskID><PartitionID>1</PartitionID></InstallTo><WillShowUI>OnError</WillShowUI>
      </OSImage></ImageInstall>
      <UserData><AcceptEula>true</AcceptEula><FullName>Undercurrent CI</FullName><Organization>Local validation</Organization></UserData>
    </component>
  </settings>
  <settings pass="specialize">
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS"><ComputerName>UC-CI-2022</ComputerName><TimeZone>UTC</TimeZone></component>
    <component name="Microsoft-Windows-Deployment" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <RunSynchronous><RunSynchronousCommand wcm:action="add"><Order>1</Order><Path>reg.exe add HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System /v EnableLUA /t REG_DWORD /d 0 /f</Path></RunSynchronousCommand></RunSynchronous>
    </component>
  </settings>
  <settings pass="oobeSystem">
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="amd64" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <OOBE><HideEULAPage>true</HideEULAPage><HideOnlineAccountScreens>true</HideOnlineAccountScreens><HideWirelessSetupInOOBE>true</HideWirelessSetupInOOBE><ProtectYourPC>3</ProtectYourPC></OOBE>
      <UserAccounts><AdministratorPassword><Value>{password}</Value><PlainText>true</PlainText></AdministratorPassword></UserAccounts>
      <AutoLogon><Password><Value>{password}</Value><PlainText>true</PlainText></Password><Username>Administrator</Username><Enabled>true</Enabled><LogonCount>1</LogonCount></AutoLogon>
      <FirstLogonCommands><SynchronousCommand wcm:action="add"><Order>1</Order><Description>Configure local CI guest</Description>
        <CommandLine>powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Get-PSDrive -PSProvider FileSystem | ForEach-Object {{ $p = Join-Path $_.Root 'bootstrap.ps1'; if (Test-Path -LiteralPath $p) {{ &amp; $p }} }}"</CommandLine>
      </SynchronousCommand></FirstLogonCommands>
    </component>
  </settings>
</unattend>
'''
    (media / "Autounattend.xml").write_text(answer, encoding="utf-8")
    iso = pycdlib.PyCdlib()
    iso.new(interchange_level=3, joliet=3, vol_ident="UCBOOT")
    for index, file in enumerate(sorted(media.iterdir())):
        iso.add_file(str(file), iso_path=f"/FILE{index:04}.DAT;1", joliet_path="/" + file.name)
    iso_path = directory / "bootstrap.iso"
    iso.write(str(iso_path))
    iso.close()
    subprocess.run([str(args.qemu / "qemu-img.exe"), "create", "-f", "qcow2", str(disk), "100G"], check=True)
    command = [str(args.qemu / "qemu-system-x86_64.exe"), "-machine", "q35",
               "-accel", args.accelerator,
               "-cpu", "Westmere-0.1.0-beta,-vmx,-svm,-arat" if args.accelerator == "whpx" else "max",
               "-smp", f"{args.cpus},sockets=1,cores={args.cpus},threads=1", "-m", str(args.memory_mb),
               "-drive", f"file={disk.as_posix()},format=qcow2,if=ide,index=0",
               "-drive", f"file={args.installer.resolve().as_posix()},media=cdrom,if=ide,index=1",
               "-drive", f"file={iso_path.as_posix()},media=cdrom,if=ide,index=2",
               "-boot", "order=c,once=d", "-no-reboot", "-display", "none",
               "-netdev", "user,id=network,hostfwd=tcp:127.0.0.1:2222-:22",
               "-device", "e1000,netdev=network",
               "-qmp", "tcp:127.0.0.1:4444,server=on,wait=off"]
    (directory / "launch.json").write_text(json.dumps(command, indent=2) + "\n", encoding="utf-8")
    print(f"Prepared VM launch arguments: {directory / 'launch.json'}")
    print("SSH endpoint: Administrator@127.0.0.1, port 2222")
    print(f"SSH identity: {private_key}; verified host keys: {directory / 'known_hosts'}")


if __name__ == "__main__":
    main()
