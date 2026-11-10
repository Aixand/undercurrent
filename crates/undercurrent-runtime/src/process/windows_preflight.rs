// SPDX-License-Identifier: Apache-2.0
//! Windows native launch preparation with retained file and ancestor identities.
use super::{LaunchPreparationError as Error, NativeProcessRuntimeLimits, PreparedLaunchHandles};
use crate::process::environment::load_environment;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use undercurrent_domain::{
    EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits, LaunchMode, LaunchSpec,
    PathReference, PayloadDigest,
};
use undercurrent_platform_windows::PinnedPath;

const MAX_PATH: usize = 32768 * 4;
const MAX_EXECUTABLE: u64 = 512 * 1024 * 1024;

fn is_batch(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
        })
}

fn batch_interpreter(
    script: &PinnedPath,
    args: &[String],
) -> Result<(PinnedPath, Option<PinnedPath>), Error> {
    // cmd does not accept verbatim paths. Before removing the prefix, retain the
    // ordinary spelling too and prove it identifies the same script. This rejects
    // verbatim-only trailing-dot/space aliases instead of launching another file.
    let ordinary = if let Some(path) = text(script.path())?.strip_prefix(r"\\?\") {
        let ordinary =
            PinnedPath::executable(Path::new(path)).map_err(|_| Error::ExecutableUnavailable)?;
        if ordinary
            .identity()
            .map_err(|_| Error::ExecutableIdentityUnavailable)?
            != script
                .identity()
                .map_err(|_| Error::ExecutableIdentityUnavailable)?
        {
            return Err(Error::ExecutableIdentityUnavailable);
        }
        Some(ordinary)
    } else {
        None
    };
    let path = undercurrent_platform_windows::command_interpreter()
        .map_err(|_| Error::ExecutableUnavailable)?;
    let (held, _, _) = executable(&path)?;
    let mut command = portable_pty::CommandBuilder::new(held.path());
    command.args(args);
    command
        .windows_batch_script(script.path().as_os_str())
        .map_err(|_| Error::ExecutableNotRunnable)?;
    Ok((held, ordinary))
}

fn native_only(launch: &LaunchSpec) -> Result<(), Error> {
    if launch.command().launch_mode() != LaunchMode::Native {
        return Err(Error::SealedLaunchUnsupported);
    }
    if launch.command().expected_executable_digest().is_some() {
        return Err(Error::ExecutableDigestMismatch);
    }
    Ok(())
}

struct Directory {
    binding: Option<ExecutionEnvironmentBinding>,
    path: Option<PathReference>,
    identity: Option<PayloadDigest>,
    handles: Vec<PinnedPath>,
}

fn directories(launch: &LaunchSpec, verify: bool) -> Result<Directory, Error> {
    let Some(binding) = launch.environment_binding() else {
        if launch.working_directory().is_some() || launch.working_directory_identity().is_some() {
            return Err(Error::MissingEnvironmentBinding);
        }
        return Ok(Directory {
            binding: None,
            path: None,
            identity: None,
            handles: vec![],
        });
    };
    let root = PinnedPath::directory(Path::new(binding.working_directory_root().as_str()))
        .map_err(|_| Error::InvalidEnvironmentBinding)?;
    let root_identity = identity(&root, b"directory")?;
    if verify
        && binding
            .root_identity()
            .ok_or(Error::MissingBindingRootIdentity)?
            != root_identity
    {
        return Err(Error::StaleBindingRootIdentity);
    }
    let requested = launch.working_directory().map(|p| Path::new(p.as_str()));
    let relative = match requested {
        None => Path::new(""),
        Some(path) if path.is_absolute() => path
            .strip_prefix(root.path())
            .map_err(|_| Error::WorkingDirectoryOutsideBinding)?,
        Some(path) => path,
    };
    if relative
        .components()
        .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(Error::WorkingDirectoryOutsideBinding);
    }
    let directory = PinnedPath::directory(&root.path().join(relative))
        .map_err(|_| Error::WorkingDirectoryUnavailable)?;
    let directory_identity = identity(&directory, b"directory")?;
    if verify
        && launch
            .working_directory_identity()
            .ok_or(Error::MissingWorkingDirectoryIdentity)?
            != directory_identity
    {
        return Err(Error::StaleWorkingDirectoryIdentity);
    }
    let root_reference =
        PathReference::new(text(root.path())?, MAX_PATH).map_err(|_| Error::Domain)?;
    let path = PathReference::new(text(directory.path())?, MAX_PATH).map_err(|_| Error::Domain)?;
    Ok(Directory {
        binding: Some(
            ExecutionEnvironmentBinding::new(binding.digest(), root_reference)
                .with_root_identity(Some(root_identity)),
        ),
        path: Some(path),
        identity: Some(directory_identity),
        handles: vec![root, directory],
    })
}

fn text(path: &Path) -> Result<&str, Error> {
    path.to_str()
        .filter(|s| s.len() <= MAX_PATH)
        .ok_or(Error::ExecutableReference)
}

fn identity(path: &PinnedPath, kind: &[u8]) -> Result<PayloadDigest, Error> {
    let id = path
        .identity()
        .map_err(|_| Error::ExecutableIdentityUnavailable)?;
    let mut digest = Sha256::new();
    digest.update(b"undercurrent.windows-file-identity.v0.1.0-beta\0");
    digest.update(kind);
    digest.update(id.volume.to_le_bytes());
    digest.update(id.index.to_le_bytes());
    digest.update(id.created.to_le_bytes());
    finish(digest)
}
fn finish(digest: Sha256) -> Result<PayloadDigest, Error> {
    PayloadDigest::from_hex(&format!("{:x}", digest.finalize())).map_err(|_| Error::Domain)
}

fn executable(path: &Path) -> Result<(PinnedPath, PayloadDigest, PayloadDigest), Error> {
    let held = PinnedPath::launch_file(path).map_err(|_| Error::ExecutableUnavailable)?;
    let metadata = held.file().metadata().map_err(|_| Error::ExecutableRead)?;
    if !metadata.is_file() {
        return Err(Error::ExecutableNotRegular);
    }
    if metadata.len() > MAX_EXECUTABLE {
        return Err(Error::ExecutableTooLarge);
    }
    let mut reader = held.file();
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    if !is_batch(held.path()) {
        let mut header = [0; 2];
        reader
            .read_exact(&mut header)
            .map_err(|_| Error::ExecutableNotRunnable)?;
        if header != *b"MZ" {
            return Err(Error::ExecutableNotRunnable);
        }
        digest.update(header);
        total = 2;
    }
    let mut buffer = vec![0; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| Error::ExecutableRead)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or(Error::ExecutableTooLarge)?;
        if total > MAX_EXECUTABLE {
            return Err(Error::ExecutableTooLarge);
        }
        digest.update(&buffer[..read]);
    }
    let identity = identity(&held, b"executable")?;
    Ok((held, finish(digest)?, identity))
}

fn resolve(
    reference: &str,
    directory: Option<&PathReference>,
    environment: &BTreeMap<String, String>,
) -> Result<(PinnedPath, PayloadDigest, PayloadDigest), Error> {
    let reference = if let Some(tool) = reference.strip_prefix("tool://") {
        let (name, version) = tool.split_once('/').ok_or(Error::ExecutableReference)?;
        if name.is_empty()
            || version.is_empty()
            || name.contains(['/', '\\'])
            || version.contains(['/', '\\'])
        {
            return Err(Error::ExecutableReference);
        }
        name
    } else {
        reference
    };
    if reference.is_empty() || reference.contains("://") {
        return Err(Error::ExecutableReference);
    }
    let path = Path::new(reference);
    let candidates = |path: PathBuf| -> Vec<PathBuf> {
        if path.extension().is_none() {
            vec![
                path.with_extension("exe"),
                path.with_extension("com"),
                path.with_extension("cmd"),
                path.with_extension("bat"),
                path,
            ]
        } else {
            vec![path]
        }
    };
    let paths = if path.is_absolute() {
        candidates(path.to_owned())
    } else if path.components().count() > 1 || reference.contains(['/', '\\', ':']) {
        if path.components().any(|c| {
            matches!(
                c,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        }) {
            return Err(Error::ExecutableReference);
        }
        candidates(
            Path::new(directory.ok_or(Error::MissingEnvironmentBinding)?.as_str()).join(path),
        )
    } else {
        let search = environment
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
            .map(|(_, value)| std::ffi::OsString::from(value))
            .or_else(|| std::env::var_os("PATH"))
            .ok_or(Error::ExecutableUnavailable)?;
        std::env::split_paths(&search)
            .filter(|p| p.is_absolute())
            .flat_map(|p| candidates(p.join(path)))
            .collect()
    };
    for path in paths {
        match executable(&path) {
            Ok(executable) => return Ok(executable),
            Err(
                Error::ExecutableUnavailable
                | Error::ExecutableNotRunnable
                | Error::ExecutableNotRegular,
            ) => {}
            Err(error) => return Err(error),
        }
    }
    Err(Error::ExecutableUnavailable)
}

fn receipt(
    requested: &str,
    selected: &str,
    content: PayloadDigest,
    object: PayloadDigest,
    directory: &Directory,
) -> Result<PayloadDigest, Error> {
    let mut digest = Sha256::new();
    digest.update(b"undercurrent.windows-native-resolution.v0.1.0-beta\0");
    for field in [
        requested.as_bytes(),
        selected.as_bytes(),
        content.as_bytes(),
        object.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_le_bytes());
        digest.update(field);
    }
    for value in [
        directory
            .binding
            .as_ref()
            .map(ExecutionEnvironmentBinding::digest),
        directory
            .binding
            .as_ref()
            .and_then(ExecutionEnvironmentBinding::root_identity),
        directory.identity,
    ] {
        match value {
            Some(value) => {
                digest.update([1]);
                digest.update(value.as_bytes());
            }
            None => digest.update([0]),
        }
    }
    finish(digest)
}

pub(super) fn prepare_launch(
    launch: &LaunchSpec,
    limits: NativeProcessRuntimeLimits,
) -> Result<LaunchSpec, Error> {
    native_only(launch)?;
    let environment = load_environment(launch, limits).map_err(|_| Error::Environment)?;
    let directory = directories(launch, false)?;
    let (held, content, object) = resolve(
        launch.command().program(),
        directory.path.as_ref(),
        &environment,
    )?;
    let selected = text(held.path())?;
    if is_batch(held.path()) {
        batch_interpreter(&held, launch.command().arguments())?;
    }
    let receipt = receipt(
        launch.command().program(),
        selected,
        content,
        object,
        &directory,
    )?;
    let command_bytes = launch
        .command()
        .arguments()
        .iter()
        .try_fold(selected.len(), |n, arg| n.checked_add(arg.len()))
        .ok_or(Error::Domain)?;
    let domain_limits = ExecutionLimits {
        max_arguments: launch.command().arguments().len().max(1),
        max_command_bytes: command_bytes.max(1),
        max_environment_entries: limits.environment_entries,
        max_environment_bytes: limits.environment_bytes,
        max_path_bytes: MAX_PATH,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    };
    let command = launch
        .command()
        .clone()
        .pin_executable_with_identity(selected.to_owned(), content, object, receipt, domain_limits)
        .map_err(|_| Error::Domain)?;
    let environment =
        EnvironmentVariables::new(environment, domain_limits).map_err(|_| Error::Environment)?;
    Ok(launch
        .clone()
        .with_command(command)
        .with_resolved_environment(environment, domain_limits)
        .map_err(|_| Error::Domain)?
        .with_resolved_paths(directory.path, directory.binding)
        .with_working_directory_identity(directory.identity))
}

pub(super) fn verify_prepared_launch(launch: &LaunchSpec) -> Result<PreparedLaunchHandles, Error> {
    native_only(launch)?;
    if launch.environment_file().is_some() {
        return Err(Error::Environment);
    }
    let resolution = launch
        .command()
        .executable_resolution()
        .ok_or(Error::MissingExecutableReceipt)?;
    let expected_identity = resolution
        .object_identity()
        .ok_or(Error::MissingExecutableIdentity)?;
    let mut directory = directories(launch, true)?;
    if receipt(
        resolution.requested_reference(),
        launch.command().program(),
        resolution.content_digest(),
        expected_identity,
        &directory,
    )? != resolution.receipt_digest()
    {
        return Err(Error::StaleExecutableReceipt);
    }
    // Native restarts allow installed updates at the accepted path, without PATH search.
    let (program, _, _) = executable(Path::new(launch.command().program()))?;
    let (program_path, windows_batch_script) = if is_batch(program.path()) {
        let (interpreter, ordinary) = batch_interpreter(&program, launch.command().arguments())?;
        if let Some(ordinary) = ordinary {
            directory.handles.push(ordinary);
        }
        let path = interpreter.path().to_owned();
        directory.handles.push(interpreter);
        (path, Some(program.path().to_owned()))
    } else {
        (program.path().to_owned(), None)
    };
    directory.handles.push(program);
    Ok(PreparedLaunchHandles {
        program_path,
        working_directory_path: directory.path.map(|p| PathBuf::from(p.as_str())),
        _windows_paths: directory.handles,
        windows_batch_script,
    })
}
