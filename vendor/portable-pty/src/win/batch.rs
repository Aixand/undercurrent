// SPDX-License-Identifier: MIT
// Argument encoding adapted from the Rust standard library, Rust 1.95.0:
// library/std/src/sys/args/windows.rs (Rust Project Developers).
// Changes: bounded command length, script-path escaping, no raw-argument mode.
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;

pub(crate) fn command_line(script: &OsStr, args: &[OsString]) -> anyhow::Result<Vec<u16>> {
    let script = script
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid batch path"))?;
    let script = script.strip_prefix(r"\\?\").unwrap_or(script);
    anyhow::ensure!(
        std::path::Path::new(script).is_absolute()
            && !script.contains('"')
            && !script.ends_with('\\'),
        "invalid batch path"
    );
    let mut line: Vec<u16> = "cmd.exe /e:ON /v:OFF /d /c \"".encode_utf16().collect();
    append(&mut line, OsStr::new(script), true)?;
    for arg in args {
        line.push(b' ' as u16);
        append(&mut line, arg, false)?;
    }
    line.push(b'"' as u16);
    anyhow::ensure!(
        line.len() < 8191,
        "batch command exceeds command interpreter limit"
    );
    line.push(0);
    Ok(line)
}

fn append(line: &mut Vec<u16>, arg: &OsStr, force_quote: bool) -> anyhow::Result<()> {
    let units: Vec<u16> = arg.encode_wide().collect();
    anyhow::ensure!(
        !units.iter().any(|c| matches!(*c, 0 | 10 | 13)),
        "invalid batch argument"
    );
    let quote = force_quote
        || units.is_empty()
        || units.last() == Some(&(b'\\' as u16))
        || units.iter().any(|&c| {
            c < 128 && !(c as u8).is_ascii_alphanumeric() && !b"#$*+-./:?@\\_".contains(&(c as u8))
        })
        || units.iter().any(|&c| (128..=159).contains(&c));
    if quote {
        line.push(b'"' as u16);
    }
    let mut slashes = 0;
    for c in units {
        if c == b'\\' as u16 {
            slashes += 1;
        } else {
            if c == b'"' as u16 {
                line.extend(std::iter::repeat_n(b'\\' as u16, slashes));
                line.push(b'"' as u16);
            } else if c == b'%' as u16 {
                // Empty dynamic-CD substring prevents expansion of caller values.
                line.extend("%%cd:~,".encode_utf16());
            }
            slashes = 0;
        }
        line.push(c);
    }
    if quote {
        line.extend(std::iter::repeat_n(b'\\' as u16, slashes));
        line.push(b'"' as u16);
    }
    Ok(())
}
