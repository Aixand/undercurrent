// SPDX-License-Identifier: Apache-2.0

//! Locked, versioned CLI metadata. Never opens the daemon database.

use super::{error, io_error};
use crate::CliError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(unix)]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use undercurrent_protocol::StartSessionRequest;

const MAX_BYTES: u64 = 4 * 1024 * 1024;
pub(super) const MAX_MEMBERS: usize = 300;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Member {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub restore: Option<Restore>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Restore {
    pub slot: String,
    pub shell: String,
    pub cwd: PathBuf,
    pub token: String,
    pub open: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub token: String,
    pub request: StartSessionRequest,
    #[serde(default)]
    pub restore: Option<Restore>,
    #[serde(default)]
    pub replaces: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Group {
    pub id: String,
    pub name: String,
    pub cwd: PathBuf,
    pub shell: String,
    pub members: Vec<Member>,
    pub selected: Option<String>,
    pub next_index: u64,
    pub pending: Option<Pending>,
    #[serde(default)]
    pub persistent: bool,
    #[serde(default)]
    pub deleting: bool,
}

impl Member {
    pub fn key(&self) -> &str {
        self.restore.as_ref().map_or(&self.id, |r| &r.slot)
    }
}
impl Group {
    pub fn selected_session(&self) -> Option<String> {
        self.members
            .iter()
            .find(|m| Some(m.key()) == self.selected.as_deref())
            .map(|m| m.id.clone())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema_version: undercurrent_protocol::ReleaseVersion,
    revision: u64,
    namespaces: BTreeMap<String, BTreeMap<String, Group>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            schema_version: undercurrent_protocol::ReleaseVersion::Current,
            revision: 0,
            namespaces: BTreeMap::new(),
        }
    }
}

pub(super) struct Store {
    pub directory: PathBuf,
    pub namespace: String,
    #[cfg(windows)]
    _directory_pin: undercurrent_platform_windows::PinnedPath,
}

impl Store {
    #[cfg(windows)]
    pub fn stamp(&self) -> Result<Option<(u64, i64, i64, u64)>, CliError> {
        use std::os::windows::fs::MetadataExt;
        let file = match open_read(&self.directory.join("workspaces.json")) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error("could not inspect group registry", e)),
        };
        let identity = undercurrent_platform_windows::file_identity(&file)
            .map_err(|e| io_error("could not identify group registry", e))?;
        let meta = file
            .metadata()
            .map_err(|e| io_error("could not inspect group registry", e))?;
        Ok(Some((
            identity.index,
            meta.last_write_time() as i64,
            i64::from(identity.volume),
            meta.len(),
        )))
    }
    pub fn new(state: &Path, endpoint: &Path) -> Result<Self, CliError> {
        let directory = state.join("group");
        #[cfg(unix)]
        let namespace = {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&directory)
                .map_err(|e| io_error("could not create group state directory", e))?;
            let meta = fs::symlink_metadata(&directory)
                .map_err(|e| io_error("could not inspect group state", e))?;
            #[cfg(unix)]
            if !meta.is_dir()
                || meta.uid() != nix::unistd::geteuid().as_raw()
                || meta.mode() & 0o077 != 0
            {
                return Err(error(
                    "group state directory must be owner-only and not a symlink",
                ));
            }
            use std::os::unix::ffi::OsStrExt;
            let namespace = format!("{:x}", Sha256::digest(endpoint.as_os_str().as_bytes()));
            namespace
        };
        #[cfg(windows)]
        let _directory_pin = undercurrent_platform_windows::prepare_private_directory(&directory)
            .map_err(|e| io_error("could not secure group directory", e))?;
        #[cfg(windows)]
        let namespace = {
            use std::os::windows::ffi::OsStrExt;
            let bytes: Vec<u8> = endpoint
                .as_os_str()
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect();
            format!("{:x}", Sha256::digest(bytes))
        };
        Ok(Self {
            directory,
            namespace,
            #[cfg(windows)]
            _directory_pin,
        })
    }

    fn lock(&self, name: &str) -> Result<File, CliError> {
        let file = open_lock(&self.directory.join(name))
            .map_err(|e| io_error("could not open group lock", e))?;
        check_file(&file)?;
        file.try_lock().map_err(|_| {
            error("group is busy; another group view or metadata update owns its lock")
        })?;
        Ok(file)
    }

    pub fn group_lock(&self, id: &str) -> Result<File, CliError> {
        uuid::Uuid::parse_str(id).map_err(|_| error("invalid group identity"))?;
        self.lock(&format!("{}-{id}.lock", self.namespace))
    }

    fn read(&self) -> Result<Registry, CliError> {
        let file = match open_read(&self.directory.join("workspaces.json")) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Registry::default()),
            Err(e) => return Err(io_error("could not read group registry", e)),
        };
        check_file(&file)?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| io_error("could not read group registry", e))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(error("group registry exceeds its size limit"));
        }
        let registry: Registry = serde_json::from_slice(&bytes).map_err(|_| {
            error("group registry is malformed; preserve it and repair it before continuing")
        })?;
        validate(&registry)?;
        Ok(registry)
    }

    #[cfg(unix)]
    pub fn stamp(&self) -> Result<Option<(u64, i64, i64, u64)>, CliError> {
        match fs::symlink_metadata(self.directory.join("workspaces.json")) {
            Ok(meta) => Ok(Some((
                meta.ino(),
                meta.mtime(),
                meta.mtime_nsec(),
                meta.len(),
            ))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_error("could not inspect group registry", e)),
        }
    }

    pub fn groups(&self) -> Result<Vec<Group>, CliError> {
        Ok(self
            .read()?
            .namespaces
            .remove(&self.namespace)
            .unwrap_or_default()
            .into_values()
            .collect())
    }

    pub fn group(&self, name: &str) -> Result<Group, CliError> {
        self.groups()?
            .into_iter()
            .find(|g| g.name == name)
            .ok_or_else(|| error("group not found; use uc group new GROUP to create it"))
    }

    pub fn update<T>(
        &self,
        change: impl FnOnce(&mut BTreeMap<String, Group>) -> Result<T, CliError>,
    ) -> Result<T, CliError> {
        let _lock = self.lock("registry.lock")?;
        let mut registry = self.read()?;
        let value = change(
            registry
                .namespaces
                .entry(self.namespace.clone())
                .or_default(),
        )?;
        registry.revision = registry
            .revision
            .checked_add(1)
            .ok_or_else(|| error("group revision exhausted"))?;
        validate(&registry)?;
        let bytes =
            serde_json::to_vec(&registry).map_err(|_| error("could not encode group registry"))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(error("group registry exceeds its size limit"));
        }
        let temporary = self
            .directory
            .join(format!(".{}.tmp", uuid::Uuid::now_v7()));
        let result = (|| {
            let mut file = open_new(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            publish(&temporary, &self.directory.join("workspaces.json"))
        })();
        if let Err(e) = result {
            let _ = fs::remove_file(&temporary);
            return Err(io_error(
                "could not save group registry; accepted processes remain running",
                e,
            ));
        }
        Ok(value)
    }
}

// Names are also used as generated session-name prefixes, never as paths.
pub(super) fn validate_name(name: &str) -> Result<(), CliError> {
    if name.is_empty()
        || name.len() > 48
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(error(
            "group names must be 1–48 ASCII letters, digits, underscores, or hyphens",
        ));
    }
    Ok(())
}

fn check_file(file: &File) -> Result<(), CliError> {
    let meta = file
        .metadata()
        .map_err(|e| io_error("could not inspect group file", e))?;
    #[cfg(windows)]
    if !meta.is_file() {
        return Err(error("group files must be regular owner-only files"));
    }
    #[cfg(unix)]
    if !meta.is_file() || meta.uid() != nix::unistd::geteuid().as_raw() || meta.mode() & 0o077 != 0
    {
        return Err(error("group files must be regular owner-only files"));
    }
    Ok(())
}

fn validate(registry: &Registry) -> Result<(), CliError> {
    if registry.schema_version != undercurrent_protocol::ReleaseVersion::Current {
        return Err(error("unsupported group registry schema"));
    }
    if registry.namespaces.len() > 64 {
        return Err(error("group endpoint capacity exceeded"));
    }
    let mut count = 0usize;
    for groups in registry.namespaces.values() {
        if groups.len() > 128 {
            return Err(error("group capacity exceeded"));
        }
        let mut ids = BTreeSet::new();
        let mut group_ids = BTreeSet::new();
        for (name, group) in groups {
            validate_name(name)?;
            if name != &group.name
                || uuid::Uuid::parse_str(&group.id).is_err()
                || !group_ids.insert(&group.id)
                || !group.cwd.is_absolute()
                || group.shell.is_empty()
                || group.shell.len() > 4096
            {
                return Err(error("group registry contains invalid group metadata"));
            }
            if group.members.len() > MAX_MEMBERS {
                return Err(error("group member capacity exceeded"));
            }
            let mut slots = BTreeSet::new();
            for member in &group.members {
                if let Some(restore) = &member.restore {
                    validate_restore(restore)?;
                    if !slots.insert(&restore.slot) {
                        return Err(error("duplicate group terminal slot"));
                    }
                }
                if member.id.is_empty()
                    || member.id.len() > 512
                    || member.name.len() > 512
                    || !ids.insert(&member.id)
                {
                    return Err(error("a session must belong to at most one group"));
                }
                count += 1;
            }
            if group
                .selected
                .as_ref()
                .is_some_and(|id| !group.members.iter().any(|m| m.key() == id))
            {
                return Err(error("group selection is not a member"));
            }
            if let Some(restore) = group.pending.as_ref().and_then(|p| p.restore.as_ref()) {
                validate_restore(restore)?;
            }
            if let Some(pending) = &group.pending
                && let Some(prior) = &pending.replaces
            {
                let member = group.members.iter().find(|m| &m.id == prior);
                if member.and_then(|m| m.restore.as_ref()).map(|r| &r.slot)
                    != pending.restore.as_ref().map(|r| &r.slot)
                    || pending.restore.is_none()
                {
                    return Err(error("pending restoration references a missing slot"));
                }
            }
            if let Some(pending) = &group.pending
                && (uuid::Uuid::parse_str(&pending.token).is_err()
                    || pending.request.name.is_empty()
                    || pending.request.name.len() > 128
                    || !pending
                        .request
                        .name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
                    || pending
                        .request
                        .labels
                        .get("uc.group.creation")
                        .or_else(|| pending.request.labels.get("uc.tui.creation"))
                        != Some(&pending.token))
            {
                return Err(error("group creation receipt is invalid"));
            }
        }
    }
    if count > 4096 {
        return Err(error("group registry member capacity exceeded"));
    }
    Ok(())
}

pub(crate) fn reset_locks(state: &Path) -> Result<Vec<File>, CliError> {
    let mut locks = Vec::new();
    for name in ["group", "tui"] {
        let directory = state.join(name);
        let _meta = match fs::symlink_metadata(&directory) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_error("could not inspect group state", e)),
        };
        #[cfg(unix)]
        if !_meta.is_dir()
            || _meta.uid() != nix::unistd::geteuid().as_raw()
            || _meta.mode() & 0o077 != 0
        {
            return Err(error("unsafe group directory during reset"));
        }
        #[cfg(windows)]
        let _directory_pin = undercurrent_platform_windows::prepare_private_directory(&directory)
            .map_err(|e| io_error("unsafe group directory during reset", e))?;
        let mut store = Store {
            #[cfg(windows)]
            _directory_pin,
            directory,
            namespace: String::new(),
        };
        locks.push(store.lock("registry.lock")?);
        for (namespace, groups) in store.read()?.namespaces {
            store.namespace = namespace;
            for group in groups.values() {
                locks.push(store.group_lock(&group.id)?);
            }
        }
    }
    Ok(locks)
}

fn validate_restore(value: &Restore) -> Result<(), CliError> {
    if uuid::Uuid::parse_str(&value.slot).is_err()
        || uuid::Uuid::parse_str(&value.token).is_err()
        || !value.cwd.is_absolute()
        || value.cwd.as_os_str().len() > 4096
        || value.shell.is_empty()
        || value.shell.len() > 4096
    {
        return Err(error("invalid terminal restoration metadata"));
    }
    Ok(())
}

fn open_read(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::open_private_file(path, false, false).map(|p| p.into_file())
    }
}
fn open_new(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::create_private_file(path).map(|p| p.into_file())
    }
}
fn open_lock(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::open_private_lock_file(path, true).map(|p| p.into_file())
    }
}
fn publish(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::rename(from, to)?;
        File::open(to.parent().expect("registry has parent"))?.sync_all()
    }
    #[cfg(windows)]
    {
        undercurrent_platform_windows::replace_private_file(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(name: &str) -> Group {
        Group {
            id: uuid::Uuid::now_v7().to_string(),
            name: name.into(),
            cwd: std::env::temp_dir(),
            shell: "/bin/sh".into(),
            members: vec![],
            selected: None,
            next_index: 1,
            pending: None,
            persistent: false,
            deleting: false,
        }
    }

    #[test]
    fn durable_empty_groups_endpoint_scoping_and_exclusive_group_locks() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), Path::new("/one.sock")).unwrap();
        let saved = group("backend");
        store
            .update(|groups| {
                groups.insert(saved.name.clone(), saved.clone());
                Ok(())
            })
            .unwrap();
        let reopened = Store::new(root.path(), Path::new("/one.sock")).unwrap();
        assert_eq!(reopened.group("backend").unwrap().id, saved.id);
        assert!(
            Store::new(root.path(), Path::new("/two.sock"))
                .unwrap()
                .groups()
                .unwrap()
                .is_empty()
        );
        let lock = store.group_lock(&saved.id).unwrap();
        assert!(reopened.group_lock(&saved.id).is_err());
        drop(lock);
        assert!(reopened.group_lock(&saved.id).is_ok());
    }

    #[test]
    fn rejected_mutations_never_overwrite_prior_metadata_or_duplicate_membership() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), Path::new("/one.sock")).unwrap();
        let mut first = group("first");
        first.members.push(Member {
            id: "session-1".into(),
            name: "worker".into(),
            restore: None,
        });
        store
            .update(|groups| {
                groups.insert(first.name.clone(), first.clone());
                Ok(())
            })
            .unwrap();
        let path = root.path().join("group/workspaces.json");
        let before = fs::read(&path).unwrap();
        let mut second = group("second");
        second.members = first.members.clone();
        assert!(
            store
                .update(|groups| {
                    groups.insert(second.name.clone(), second);
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(
            store
                .update(|groups| {
                    groups.clear();
                    Err::<(), _>(error("rejected"))
                })
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn corrupt_newer_schema_and_symlinks_are_preserved_and_rejected() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), Path::new("/one.sock")).unwrap();
        store.update(|_| Ok(())).unwrap();
        let path = root.path().join("group/workspaces.json");
        for contents in [
            "broken",
            r#"{"schema_version":3,"revision":1,"namespaces":{}}"#,
        ] {
            fs::write(&path, contents).unwrap();
            assert!(store.groups().is_err());
            assert!(store.update(|_| Ok(())).is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), contents);
        }
        fs::remove_file(&path).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.path().join("other"), &path).unwrap();
        #[cfg(windows)]
        {
            fs::write(root.path().join("other"), b"external").unwrap();
            fs::hard_link(root.path().join("other"), &path).unwrap();
        }
        assert!(store.groups().is_err());
        #[cfg(unix)]
        assert!(!root.path().join("other").exists());
        #[cfg(windows)]
        assert_eq!(fs::read(root.path().join("other")).unwrap(), b"external");
    }

    #[test]
    fn reset_cannot_cross_an_active_group_view_lock() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), Path::new("/one.sock")).unwrap();
        let group = group("active");
        store
            .update(|groups| {
                groups.insert("active".into(), group.clone());
                Ok(())
            })
            .unwrap();
        let view = store.group_lock(&group.id).unwrap();
        assert!(reset_locks(root.path()).is_err());
        drop(view);
        let locks = reset_locks(root.path()).unwrap();
        assert!(store.update(|_| Ok(())).is_err());
        drop(locks);
        store.update(|_| Ok(())).unwrap();
    }
}
