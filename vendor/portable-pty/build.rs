// Copyright (c) Undercurrent contributors. Licensed under the MIT license.
// Stage Microsoft's pinned ConPTY runtime beside Cargo executables and tests.
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn copy_if_changed(source: &Path, destination: &Path) {
    let bytes = fs::read(source).expect("read vendored ConPTY asset");
    if fs::read(destination).ok().as_deref() != Some(bytes.as_slice()) {
        fs::write(destination, bytes)
            .expect("stage ConPTY asset; stop processes using an older copy");
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=conpty");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }
    let arch = match env::var("CARGO_CFG_TARGET_ARCH").unwrap().as_str() {
        "x86_64" => "x64",
        "x86" => "x86",
        "aarch64" => "arm64",
        other => panic!("no vendored ConPTY runtime for {}", other),
    };
    let source = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("conpty");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    // Cargo puts build-script output in <profile>/build/<package>/out, including
    // when --target or a custom target directory/profile is used.
    let build = out.parent().unwrap().parent().unwrap();
    assert_eq!(
        build.file_name().unwrap(),
        "build",
        "unexpected Cargo output layout"
    );
    let profile = build.parent().unwrap();
    for executable_directory in [
        profile.to_path_buf(),
        profile.join("deps"),
        profile.join("examples"),
    ] {
        let destination = executable_directory.join("uc-conpty");
        fs::create_dir_all(&destination).expect("create ConPTY asset directory");
        for name in ["conpty.dll", "OpenConsole.exe"] {
            copy_if_changed(&source.join(arch).join(name), &destination.join(name));
        }
        copy_if_changed(&source.join("LICENSE"), &destination.join("LICENSE"));
    }
}
