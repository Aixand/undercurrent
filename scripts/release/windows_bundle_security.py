# SPDX-License-Identifier: Apache-2.0
"""Owner-only ACLs for local Windows release-bundle files.

The verifier never repairs an input ACL. Only newly assembled/copied bundle
objects are restricted, through handles that reject reparse points and hardlinks.
"""
from contextlib import contextmanager
import ctypes as c
from ctypes import wintypes as w
import os
import msvcrt
from pathlib import Path

k = c.WinDLL("kernel32", use_last_error=True)
a = c.WinDLL("advapi32", use_last_error=True)
P = c.c_void_p


def bind(dll, name, result, *arguments):
    fn = getattr(dll, name)
    fn.restype, fn.argtypes = result, arguments
    return fn


CreateFile = bind(k, "CreateFileW", w.HANDLE, w.LPCWSTR, w.DWORD, w.DWORD, P, w.DWORD, w.DWORD, w.HANDLE)
CloseHandle = bind(k, "CloseHandle", w.BOOL, w.HANDLE)
LocalFree = bind(k, "LocalFree", P, P)
GetCurrentProcess = bind(k, "GetCurrentProcess", w.HANDLE)
OpenProcessToken = bind(a, "OpenProcessToken", w.BOOL, w.HANDLE, w.DWORD, c.POINTER(w.HANDLE))
GetTokenInformation = bind(a, "GetTokenInformation", w.BOOL, w.HANDLE, c.c_int, P, w.DWORD, c.POINTER(w.DWORD))
ConvertSid = bind(a, "ConvertSidToStringSidW", w.BOOL, P, c.POINTER(P))
ConvertDescriptor = bind(a, "ConvertStringSecurityDescriptorToSecurityDescriptorW", w.BOOL, w.LPCWSTR, w.DWORD, c.POINTER(P), P)
GetDacl = bind(a, "GetSecurityDescriptorDacl", w.BOOL, P, c.POINTER(w.BOOL), c.POINTER(P), c.POINTER(w.BOOL))
GetControl = bind(a, "GetSecurityDescriptorControl", w.BOOL, P, c.POINTER(w.WORD), c.POINTER(w.DWORD))
GetSecurityInfo = bind(a, "GetSecurityInfo", w.DWORD, w.HANDLE, c.c_int, w.DWORD, c.POINTER(P), P, c.POINTER(P), P, c.POINTER(P))
SetSecurityInfo = bind(a, "SetSecurityInfo", w.DWORD, w.HANDLE, c.c_int, w.DWORD, P, P, P, P)
EqualSid = bind(a, "EqualSid", w.BOOL, P, P)
GetAce = bind(a, "GetAce", w.BOOL, P, w.DWORD, c.POINTER(P))


class FileInformation(c.Structure):
    _fields_ = [("attributes", w.DWORD), ("created", w.FILETIME), ("accessed", w.FILETIME),
                ("written", w.FILETIME), ("volume", w.DWORD), ("size_high", w.DWORD),
                ("size_low", w.DWORD), ("links", w.DWORD), ("index_high", w.DWORD), ("index_low", w.DWORD)]


class SecurityAttributes(c.Structure):
    _fields_ = [("length", w.DWORD), ("descriptor", P), ("inherit", w.BOOL)]


GetFileInformation = bind(k, "GetFileInformationByHandle", w.BOOL, w.HANDLE, c.POINTER(FileInformation))
CreateDirectory = bind(k, "CreateDirectoryW", w.BOOL, w.LPCWSTR, c.POINTER(SecurityAttributes))


def check(ok):
    if not ok:
        raise c.WinError(c.get_last_error())


@contextmanager
def security():
    token = w.HANDLE()
    check(OpenProcessToken(GetCurrentProcess(), 8, c.byref(token)))  # TOKEN_QUERY
    try:
        size = w.DWORD()
        GetTokenInformation(token, 1, None, 0, c.byref(size))  # TokenUser
        if not size.value:
            raise c.WinError(c.get_last_error())
        user = c.create_string_buffer(size.value)
        check(GetTokenInformation(token, 1, user, size, c.byref(size)))
        sid = c.cast(user, c.POINTER(P))[0]
        text = P()
        check(ConvertSid(sid, c.byref(text)))
        try:
            sddl = "O:" + c.wstring_at(text) + "D:P(A;;FA;;;" + c.wstring_at(text) + ")"
        finally:
            LocalFree(text)
        descriptor = P()
        check(ConvertDescriptor(sddl, 1, c.byref(descriptor), None))
        try:
            dacl, present, defaulted = P(), w.BOOL(), w.BOOL()
            check(GetDacl(descriptor, c.byref(present), c.byref(dacl), c.byref(defaulted)))
            if not present or not dacl:
                raise PermissionError("owner DACL unavailable")
            yield sid, descriptor, dacl
        finally:
            LocalFree(descriptor)
    finally:
        CloseHandle(token)


@contextmanager
def opened(path: Path, write_acl=False):
    # READ_CONTROL | FILE_READ_ATTRIBUTES; no delete sharing. Open the object
    # itself, including directories, then reject reparse points before ACL work.
    handle = CreateFile(str(path.absolute()), 0x20080 | (0x40000 if write_acl else 0),
                        3, None, 3, 0x02200000, None)
    if handle == w.HANDLE(-1).value:
        error = c.WinError(c.get_last_error())
        error.filename = str(path)
        raise error
    try:
        info = FileInformation()
        check(GetFileInformation(handle, c.byref(info)))
        if info.attributes & 0x400 or (not info.attributes & 0x10 and info.links != 1):
            raise PermissionError("bundle reparse points and hardlinks are forbidden")
        yield handle
    finally:
        CloseHandle(handle)


def inspect(handle, sid, require_private):
    owner, dacl, descriptor = P(), P(), P()
    result = GetSecurityInfo(handle, 1, 5, c.byref(owner), None, c.byref(dacl), None, c.byref(descriptor))
    if result:
        raise c.WinError(result)
    try:
        if not owner or not EqualSid(owner, sid):
            raise PermissionError("bundle object is not owned by the current user")
        if not require_private:
            return
        control, revision = w.WORD(), w.DWORD()
        check(GetControl(descriptor, c.byref(control), c.byref(revision)))
        # ACL header: revision, reserved, size, ACE count, reserved.
        if not control.value & 0x1000 or not dacl or c.c_ushort.from_address(dacl.value + 4).value != 1:
            raise PermissionError("bundle requires a protected owner-only DACL")
        ace = P()
        check(GetAce(dacl, 0, c.byref(ace)))
        # Exactly one non-inherited ACCESS_ALLOWED_ACE granting FILE_ALL_ACCESS
        # to the current user. Unknown ACE types and rights fail closed.
        if (c.c_ubyte.from_address(ace.value).value != 0
                or c.c_ubyte.from_address(ace.value + 1).value != 0
                or w.DWORD.from_address(ace.value + 4).value != 0x1f01ff
                or not EqualSid(ace.value + 8, sid)):
            raise PermissionError("bundle ACL grants unexpected access")
    finally:
        LocalFree(descriptor)


def verify_private(path: Path):
    with security() as (sid, _descriptor, _dacl), opened(path) as handle:
        inspect(handle, sid, True)


def restrict_private(path: Path):
    with security() as (sid, _descriptor, dacl), opened(path, True) as handle:
        inspect(handle, sid, False)
        result = SetSecurityInfo(handle, 1, 0x80000004, None, None, dacl, None)
        if result:
            raise c.WinError(result)
        inspect(handle, sid, True)


def create_private_directory(path: Path):
    """Create a fresh directory with its protected ACL in the creation call."""
    with security() as (_sid, descriptor, _dacl):
        attributes = SecurityAttributes(c.sizeof(SecurityAttributes), descriptor, False)
        check(CreateDirectory(str(path.absolute()), c.byref(attributes)))
    verify_private(path)


def create_private_parents(path: Path):
    """Create missing directories with explicit ownership; leave ancestors alone."""
    if path.is_dir():
        return
    create_private_parents(path.parent)
    try:
        create_private_directory(path)
    except FileExistsError:
        verify_private(path)


@contextmanager
def private_writer(path: Path, overwrite=False):
    """Create an account-owned file, validating an existing file before truncation."""
    create_private_parents(path.parent)
    with security() as (sid, descriptor, dacl):
        attributes = SecurityAttributes(c.sizeof(SecurityAttributes), descriptor, False)
        # GENERIC_WRITE | READ_CONTROL | WRITE_DAC, no concurrent writers or
        # deletion, and never follow a leaf reparse point. OPEN_ALWAYS supplies
        # the security descriptor only when it actually creates a fresh file.
        handle = CreateFile(str(path.absolute()), 0x40060000, 1, c.byref(attributes),
                            4 if overwrite else 1, 0x00200000, None)
        if handle == w.HANDLE(-1).value:
            raise c.WinError(c.get_last_error())
        try:
            info = FileInformation()
            check(GetFileInformation(handle, c.byref(info)))
            if info.attributes & (0x400 | 0x10) or info.links != 1:
                raise PermissionError("bundle writer requires one regular file")
            inspect(handle, sid, False)
            result = SetSecurityInfo(handle, 1, 0x80000004, None, None, dacl, None)
            if result:
                raise c.WinError(result)
            inspect(handle, sid, True)
            fd = msvcrt.open_osfhandle(handle, os.O_WRONLY | os.O_BINARY)
            handle = None  # the CRT descriptor now owns the Windows handle
            try:
                writer = os.fdopen(fd, "wb")
            except BaseException:
                os.close(fd)
                raise
            with writer:
                writer.truncate(0)
                yield writer
        finally:
            if handle is not None:
                CloseHandle(handle)
