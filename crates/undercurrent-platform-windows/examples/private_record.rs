// SPDX-License-Identifier: Apache-2.0
//! Run on native Windows from the repository root with:
//! `cargo run --locked -p aixand-undercurrent-platform-windows --example private_record`.
//!
//! Uses only temporary state and no daemon. See `docs/development/contributing.md` for the
//! expected result, prerequisites, and cleanup after abnormal termination.

#[cfg(windows)]
fn main() -> std::io::Result<()> {
    use std::io::{Read, Write};
    use undercurrent_platform_windows::{
        PrivateRemovalFile, create_private_file, open_private_file, prepare_private_directory,
        replace_private_file,
    };

    let temporary = tempfile::tempdir()?;
    eprintln!(
        "temporary example directory: {}",
        temporary.path().display()
    );
    // Canonicalize only our freshly created fixture root, never existing product state.
    let root = temporary.path().canonicalize()?.join("private-state");
    let directory = prepare_private_directory(&root)?;
    let staging = directory.path().join("record.staging");
    let record = directory.path().join("record");
    let expected = b"new private record";

    let mut file = create_private_file(&staging)?.into_file();
    file.write_all(expected)?;
    file.sync_all()?;
    drop(file);
    // This fixture has one writer. Real record owners must serialize publication.
    replace_private_file(&staging, &record)?;

    let pinned = open_private_file(&record, false, false)?;
    let mut actual = Vec::new();
    pinned.file().read_to_end(&mut actual)?;
    if actual.as_slice() != expected || staging.try_exists()? {
        return Err(std::io::Error::other("private record verification failed"));
    }
    drop(pinned);
    println!("published and verified: new private record");

    PrivateRemovalFile::open(&record)?.remove()?;
    if record.try_exists()? {
        return Err(std::io::Error::other("record still exists after removal"));
    }
    drop(directory);
    temporary.close()?;
    println!("removed record and temporary directory");
    Ok(())
}

#[cfg(not(windows))]
fn main() {
    eprintln!("private_record requires native Windows; no example work was performed");
    std::process::exit(1);
}
