"""Windows version resources for owned, unsigned release files and command shims."""

from __future__ import annotations

import ctypes
from pathlib import Path
import re
import struct
import tempfile

from core import require


def version_parts(version):
    match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+))?", version)
    require(match is not None, "Windows product versions must start with major.minor")
    parts = tuple(int(value or 0) for value in match.groups())
    require(
        all(value <= 65535 for value in parts),
        "Windows version component exceeds 65535",
    )
    return parts


def resource(version, product, command, *, launcher=False):
    def block(key, value=b"", children=(), text=False):
        prefix = b"\0" * 6 + (key + "\0").encode("utf-16le")
        prefix += b"\0" * (-len(prefix) % 4)
        data = prefix + value
        for child in children:
            data += b"\0" * (-len(data) % 4) + child
        length = len(value) // 2 if text else len(value)
        return struct.pack("<HHH", len(data), length, int(text)) + data[6:]

    major, minor, patch, revision = version_parts(version)
    fixed = struct.pack(
        "<13I",
        0xFEEF04BD,
        0x10000,
        major << 16 | minor,
        patch << 16 | revision,
        major << 16 | minor,
        patch << 16 | revision,
        0x3F,
        2 if re.search(r"[A-Za-z-]", version) else 0,
        0x40004,
        1,
        0,
        0,
        0,
    )
    values = {
        "CompanyName": "Aixand",
        "ProductName": product,
        "FileDescription": command + (" command launcher" if launcher else ""),
        "ProductVersion": version,
        "FileVersion": version,
        "InternalName": command,
        "OriginalFilename": command + ".exe",
    }
    strings = [
        block(key, (value + "\0").encode("utf-16le"), text=True)
        for key, value in values.items()
    ]
    return block(
        "VS_VERSION_INFO",
        fixed,
        [
            block(
                "StringFileInfo",
                children=[block("040904b0", children=strings, text=True)],
                text=True,
            ),
            block(
                "VarFileInfo",
                children=[block("Translation", struct.pack("<HH", 0x409, 1200))],
                text=True,
            ),
        ],
    )


def stamp(path, version, product, command, *, launcher=False):
    """Update only a build copy; callers seal/hash its final bytes afterwards."""
    from ctypes import wintypes

    image = Path(path).read_bytes()
    require(image[:2] == b"MZ", "Windows version metadata requires a PE executable")
    pe = struct.unpack_from("<I", image, 0x3C)[0]
    require(image[pe : pe + 4] == b"PE\0\0", "Invalid PE executable")
    optional = pe + 24
    magic = struct.unpack_from("<H", image, optional)[0]
    require(magic in {0x10B, 0x20B}, "Unknown PE optional header")
    security = optional + (112 if magic == 0x20B else 96) + 8 * 4
    require(
        struct.unpack_from("<II", image, security) == (0, 0),
        "Add Windows version metadata before signing executable files",
    )
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.BeginUpdateResourceW.argtypes = [wintypes.LPCWSTR, wintypes.BOOL]
    api.BeginUpdateResourceW.restype = wintypes.HANDLE
    api.UpdateResourceW.argtypes = [
        wintypes.HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.WORD,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    api.UpdateResourceW.restype = wintypes.BOOL
    api.EndUpdateResourceW.argtypes = [wintypes.HANDLE, wintypes.BOOL]
    api.EndUpdateResourceW.restype = wintypes.BOOL
    data = resource(version, product, command, launcher=launcher)
    handle = api.BeginUpdateResourceW(str(Path(path).absolute()), False)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        buffer = ctypes.create_string_buffer(data)
        if not api.UpdateResourceW(handle, 16, 1, 0, buffer, len(data)):
            raise ctypes.WinError(ctypes.get_last_error())
    except BaseException:
        api.EndUpdateResourceW(handle, True)
        raise
    if not api.EndUpdateResourceW(handle, False):
        raise ctypes.WinError(ctypes.get_last_error())


def launcher_bytes(template, version, product, command):
    # Never edit an active executable or an immutable artifact in place.
    with tempfile.TemporaryDirectory(prefix="aixand-command-") as temporary:
        path = Path(temporary) / (command + ".exe")
        path.write_bytes(template.read_bytes())
        stamp(path, version, product, command, launcher=True)
        return path.read_bytes()
