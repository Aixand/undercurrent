// SPDX-License-Identifier: Apache-2.0
//! Handle-pinned local paths and owner-only state creation.
use std::fs::File;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Component, Path, PathBuf, Prefix};
use windows_sys::Win32::Foundation::{
    GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo, SDDL_REVISION_1,
    SE_FILE_OBJECT, SetSecurityInfo,
};
use windows_sys::Win32::Security::{
    DACL_SECURITY_INFORMATION, EqualSid, GetSecurityDescriptorDacl, GetSecurityDescriptorOwner,
    OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, SECURITY_ATTRIBUTES,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_ALWAYS, OPEN_EXISTING,
    READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::System::SystemServices::MAXIMUM_ALLOWED;

fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "unsafe private path identity or ownership",
    )
}

fn wide(path: &Path) -> io::Result<Vec<u16>> {
    let bytes: Vec<_> = path.as_os_str().encode_wide().collect();
    if bytes.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"));
    }
    Ok(bytes.into_iter().chain(Some(0)).collect())
}

/// File object identity from a retained handle, not a content digest or pathname.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileObjectIdentity {
    pub volume: u32,
    pub index: u64,
    pub created: u64,
}

#[allow(unsafe_code)]
fn information(file: &File) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: file retains a valid handle and info is a correctly aligned output.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

pub fn file_identity(file: &File) -> io::Result<FileObjectIdentity> {
    let info = information(file)?;
    Ok(FileObjectIdentity {
        volume: info.dwVolumeSerialNumber,
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        created: (u64::from(info.ftCreationTime.dwHighDateTime) << 32)
            | u64::from(info.ftCreationTime.dwLowDateTime),
    })
}

struct OwnerSecurity(*mut std::ffi::c_void);
impl OwnerSecurity {
    #[allow(unsafe_code)]
    fn already_private(&self, file: &File) -> io::Result<bool> {
        use windows_sys::Win32::Security::{GetSecurityDescriptorControl, SE_DACL_PROTECTED};
        let mut owner = std::ptr::null_mut();
        let mut acl = std::ptr::null_mut();
        let mut actual = std::ptr::null_mut();
        // SAFETY: the retained file grants READ_CONTROL and all output slots are writable.
        let result = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                &mut acl,
                std::ptr::null_mut(),
                &mut actual,
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let mut expected_owner = std::ptr::null_mut();
        let mut expected_acl = std::ptr::null_mut();
        let mut defaulted = 0;
        let mut present = 0;
        let mut control = 0;
        let mut revision = 0;
        // SAFETY: all descriptor and contained SID/ACL pointers remain live. The
        // queried ACL sizes bound the byte slices, and null ACLs are rejected.
        let matches = unsafe {
            GetSecurityDescriptorOwner(self.0, &mut expected_owner, &mut defaulted) != 0
                && GetSecurityDescriptorDacl(
                    self.0,
                    &mut present,
                    &mut expected_acl,
                    &mut defaulted,
                ) != 0
                && GetSecurityDescriptorControl(actual, &mut control, &mut revision) != 0
                && control & SE_DACL_PROTECTED != 0
                && present != 0
                && !owner.is_null()
                && !expected_owner.is_null()
                && EqualSid(owner, expected_owner) != 0
                && !acl.is_null()
                && !expected_acl.is_null()
                && std::slice::from_raw_parts(acl.cast::<u8>(), (*acl).AclSize as usize)
                    == std::slice::from_raw_parts(
                        expected_acl.cast::<u8>(),
                        (*expected_acl).AclSize as usize,
                    )
        };
        // SAFETY: GetSecurityInfo allocated actual using LocalAlloc.
        unsafe {
            LocalFree(actual);
        }
        Ok(matches)
    }
    #[allow(unsafe_code)]
    fn new(directory: bool) -> io::Result<Self> {
        let sid = crate::current_user_sid()?;
        let inheritance = if directory { "OICI" } else { "" };
        let text: Vec<u16> = format!("O:{sid}D:P(A;{inheritance};FA;;;{sid})")
            .encode_utf16()
            .chain(Some(0))
            .collect();
        let mut raw = std::ptr::null_mut();
        // SAFETY: text is a terminated SDDL string and raw receives an allocation.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                text.as_ptr(),
                SDDL_REVISION_1,
                &mut raw,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(raw))
    }
    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }
    #[allow(unsafe_code)]
    fn verify_owner(&self, file: &File) -> io::Result<()> {
        let mut owner = std::ptr::null_mut();
        let mut actual = std::ptr::null_mut();
        // SAFETY: file is live and all supplied output pointers are writable.
        let result = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut actual,
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let mut expected = std::ptr::null_mut();
        let mut defaulted = 0;
        // SAFETY: self retains the descriptor created with an explicit owner SID.
        let queried = unsafe { GetSecurityDescriptorOwner(self.0, &mut expected, &mut defaulted) };
        // SAFETY: on success both SID pointers belong to live security descriptors.
        let same_owner = queried != 0
            && !owner.is_null()
            && !expected.is_null()
            && unsafe { EqualSid(owner, expected) } != 0;
        // SAFETY: GetSecurityInfo allocated actual using LocalAlloc.
        unsafe {
            LocalFree(actual);
        }
        if !same_owner {
            return Err(denied());
        }
        Ok(())
    }
    #[allow(unsafe_code)]
    fn restrict_owned(&self, file: &File) -> io::Result<()> {
        self.verify_owner(file)?;
        let mut defaulted = 0;
        let mut present = 0;
        let mut acl = std::ptr::null_mut();
        // SAFETY: self holds the descriptor and all output pointers are valid.
        if unsafe { GetSecurityDescriptorDacl(self.0, &mut present, &mut acl, &mut defaulted) } == 0
            || present == 0
            || acl.is_null()
        {
            return Err(denied());
        }
        // SAFETY: acl remains owned by self throughout the synchronous API call.
        let result = unsafe {
            SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                acl,
                std::ptr::null_mut(),
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(())
    }
}
impl Drop for OwnerSecurity {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // SAFETY: self uniquely owns the allocation returned by SDDL conversion.
        unsafe {
            LocalFree(self.0);
        }
    }
}

fn open(
    path: &Path,
    directory: bool,
    private: Option<&OwnerSecurity>,
    write: bool,
    creation: u32,
) -> io::Result<File> {
    open_with_sharing(path, directory, private, write, creation, false, false)
}

#[allow(unsafe_code)]
fn open_with_sharing(
    path: &Path,
    directory: bool,
    private: Option<&OwnerSecurity>,
    write: bool,
    creation: u32,
    shared_write: bool,
    delete: bool,
) -> io::Result<File> {
    if directory && let Some(security) = private {
        // An existing directory pin denies delete sharing. Reopening with
        // MAXIMUM_ALLOWED can conflict with it even when no ACL change is
        // needed, so keep the narrow handle if the owner and protected ACL match.
        // See [directory restriction](../docs/internals/filesystem-security.md#restrict-an-existing-directory-without-changing-its-children).
        let observed = open_with_sharing(path, true, None, false, OPEN_EXISTING, false, false)?;
        if security.already_private(&observed)? {
            return Ok(observed);
        }
        drop(observed);
    }
    let wide_path = wide(path)?;
    let attributes = private.map(OwnerSecurity::attributes);
    // Metadata-only access does not participate in Windows sharing checks.
    // Directory pins need data-read (directory-list) access too, otherwise a
    // handle without FILE_SHARE_DELETE still permits renaming the directory.
    let mut access = GENERIC_READ
        | if write { GENERIC_WRITE } else { 0 }
        | if delete {
            windows_sys::Win32::Storage::FileSystem::DELETE
        } else {
            0
        }
        | if private.is_some() {
            READ_CONTROL | WRITE_DAC
        } else {
            0
        };
    if directory && private.is_some() {
        // SetSecurityInfo deliberately does not propagate ACE changes to existing
        // children when the handle was opened with MAXIMUM_ALLOWED. Restrict only
        // this directory; do not traverse untrusted descendants or junctions.
        // https://learn.microsoft.com/windows/win32/api/aclapi/nf-aclapi-setsecurityinfo
        access = MAXIMUM_ALLOWED;
    }
    // Keep directories and executables from being renamed/deleted while pinned.
    let sharing = FILE_SHARE_READ
        | if directory || shared_write {
            FILE_SHARE_WRITE
        } else {
            0
        };
    // SAFETY: all pointers refer to live terminated paths or security attributes;
    // no template handle is used. The returned handle is checked before ownership.
    let raw = unsafe {
        CreateFileW(
            wide_path.as_ptr(),
            access,
            sharing,
            attributes.as_ref().map_or(std::ptr::null(), |a| a),
            creation,
            FILE_FLAG_OPEN_REPARSE_POINT
                | if directory {
                    FILE_FLAG_BACKUP_SEMANTICS
                } else {
                    0
                },
            std::ptr::null_mut(),
        )
    };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: raw is a uniquely owned handle returned by CreateFileW.
    let file = unsafe { File::from_raw_handle(raw) };
    let info = information(&file)?;
    // Reject private hardlinks before changing the DACL: aliases share the same
    // security descriptor, so restriction through one could affect another path.
    // See [private file checks](../docs/internals/filesystem-security.md#select-a-file-helper).
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0) != directory
        || (!directory && private.is_some() && info.nNumberOfLinks != 1)
    {
        return Err(denied());
    }
    if let Some(security) = private {
        security.restrict_owned(&file)?;
    }
    if directory && let Some(security) = private {
        // MAXIMUM_ALLOWED is used only for the non-propagating ACL change.
        // Retained directory pins need no DELETE access; retaining the broader
        // handle would conflict with later pins under the new private ACL.
        let identity = file_identity(&file)?;
        drop(file);
        let retained = open_with_sharing(path, true, None, false, OPEN_EXISTING, false, false)?;
        if file_identity(&retained)? != identity || !security.already_private(&retained)? {
            return Err(denied());
        }
        return Ok(retained);
    }
    Ok(file)
}

/// Retained handles prevent pathname replacement. Ordinary pins retain every
/// ancestor; launch-only pins may use an exact normalized leaf name and Windows'
/// prohibition on renaming a directory containing an open file.
#[derive(Debug)]
pub struct PinnedPath {
    path: PathBuf,
    handles: Vec<File>,
}
impl PinnedPath {
    /// Pins an existing directory owned by the current account without changing
    /// its ACL or creating missing directories.
    pub fn owned_directory(path: &Path) -> io::Result<Self> {
        let pinned = Self::directory(path)?;
        OwnerSecurity::new(true)?.verify_owner(pinned.file())?;
        Ok(pinned)
    }
    /// Pins an existing local directory and its ancestors without owner checks
    /// or ACL changes. Reparse points and unsupported path forms are rejected.
    pub fn directory(path: &Path) -> io::Result<Self> {
        Self::open(path, true, None, false, OPEN_EXISTING)
    }
    /// Pins an existing non-directory file against writes and replacement.
    /// Does not validate executable format, account ownership, or content digest.
    pub fn executable(path: &Path) -> io::Result<Self> {
        Self::open(path, false, None, false, OPEN_EXISTING)
    }

    /// Pins a launch file even when an ancestor allows traversal but not listing,
    /// as with Microsoft Store packages. The fallback never follows a leaf reparse
    /// point and requires an exact normalized name, rejecting ancestor junctions,
    /// short names, and case aliases rather than guessing their equivalence.
    pub fn launch_file(path: &Path) -> io::Result<Self> {
        match Self::executable(path) {
            Ok(pinned) => Ok(pinned),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                Self::launch_leaf(path)
            }
            Err(error) => Err(error),
        }
    }

    fn launch_leaf(path: &Path) -> io::Result<Self> {
        let components = local_components(path)?;
        let mut normalized = PathBuf::new();
        normalized.push(components[0]);
        normalized.push(components[1]);
        let root = open(&normalized, true, None, false, OPEN_EXISTING)?;
        for component in components.iter().skip(2) {
            normalized.push(component);
        }
        // A data-read handle with no write/delete sharing pins the leaf's bytes
        // and prevents both its removal and ancestor renames (including RenameEx).
        // Nonempty ancestors cannot be changed into reparse points (MS-FSA).
        let file = open(&normalized, false, None, false, OPEN_EXISTING)?;
        let actual = final_path(&file)?;
        if !same_local_name(&normalized, &actual)? {
            return Err(denied());
        }
        Ok(Self {
            path: normalized,
            handles: vec![root, file],
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn file(&self) -> &File {
        self.handles
            .last()
            .expect("validated path has a leaf handle")
    }
    pub fn identity(&self) -> io::Result<FileObjectIdentity> {
        file_identity(self.file())
    }
    /// Retains the opened object, releasing ancestor pathname locks. Use only
    /// when subsequent operations act on this handle, not on its old pathname.
    pub fn into_file(mut self) -> File {
        self.handles
            .pop()
            .expect("validated path has a leaf handle")
    }

    fn open(
        path: &Path,
        directory: bool,
        private: Option<&OwnerSecurity>,
        write: bool,
        creation: u32,
    ) -> io::Result<Self> {
        let components = local_components(path)?;
        let mut current = PathBuf::new();
        current.push(components[0]);
        current.push(components[1]);
        let mut handles = vec![open(&current, true, None, false, OPEN_EXISTING)?];
        for (index, component) in components.iter().enumerate().skip(2) {
            current.push(component);
            if index + 1 == components.len() {
                handles.push(open(&current, directory, private, write, creation)?);
            } else {
                handles.push(open(&current, true, None, false, OPEN_EXISTING)?);
            }
        }
        if components.len() == 2 && (!directory || private.is_some()) {
            return Err(denied());
        }
        Ok(Self {
            path: current,
            handles,
        })
    }
}

fn local_components(path: &Path) -> io::Result<Vec<Component<'_>>> {
    let components: Vec<_> = path.components().collect();
    if components.len() < 2
        || !matches!(components[0], Component::Prefix(p) if matches!(p.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_)))
        || !matches!(components[1], Component::RootDir)
        || components[2..].iter().any(|c| !matches!(c, Component::Normal(name) if !name.encode_wide().any(|c| c == u16::from(b':'))))
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "expected normalized absolute local path"));
    }
    Ok(components)
}

fn same_local_name(expected: &Path, actual: &Path) -> io::Result<bool> {
    let expected = local_components(expected)?;
    let actual = local_components(actual)?;
    let disk = |component: Component<'_>| match component {
        Component::Prefix(prefix) => match prefix.kind() {
            Prefix::Disk(disk) | Prefix::VerbatimDisk(disk) => Some(disk.to_ascii_uppercase()),
            _ => None,
        },
        _ => None,
    };
    Ok(disk(expected[0]) == disk(actual[0]) && expected[1..] == actual[1..])
}

#[allow(unsafe_code)]
fn final_path(file: &File) -> io::Result<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_NAME_NORMALIZED, GetFinalPathNameByHandleW, VOLUME_NAME_DOS,
    };
    let mut buffer = vec![0u16; 32768];
    // SAFETY: file is retained and buffer is writable for the advertised capacity.
    let length = unsafe {
        GetFinalPathNameByHandleW(
            file.as_raw_handle(),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize >= buffer.len() {
        return Err(io::Error::other("normalized launch path exceeds bound"));
    }
    buffer.truncate(length as usize);
    Ok(std::ffi::OsString::from_wide(&buffer).into())
}

/// Creates missing directories with private ACLs before they become visible.
/// Existing ancestors are left alone; only the requested directory is restricted.
pub fn prepare_private_directory(path: &Path) -> io::Result<PinnedPath> {
    let security = OwnerSecurity::new(true)?;
    if path.exists() {
        return PinnedPath::open(path, true, Some(&security), false, OPEN_EXISTING);
    }
    let parent = path.parent().ok_or_else(denied)?;
    let held_parent = if parent.exists() {
        PinnedPath::directory(parent)?
    } else {
        prepare_private_directory(parent)?
    };
    create_directory(path, &security)?;
    let result = PinnedPath::open(path, true, Some(&security), false, OPEN_EXISTING);
    drop(held_parent);
    result
}

#[allow(unsafe_code)]
fn create_directory(path: &Path, security: &OwnerSecurity) -> io::Result<()> {
    let path = wide(path)?;
    let attributes = security.attributes();
    // SAFETY: the path is terminated and security remains live through creation.
    if unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } == 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    Ok(())
}

/// Opens or creates a single-link state file without following reparse points.
/// Its owner is verified before tightening the ACL and before exposing contents.
/// Even a read-only open can tighten permissions. `create` selects open-or-create;
/// neither mode truncates the file. Retain the pin for later pathname operations.
pub fn open_private_file(path: &Path, write: bool, create: bool) -> io::Result<PinnedPath> {
    let security = OwnerSecurity::new(false)?;
    PinnedPath::open(
        path,
        false,
        Some(&security),
        write,
        if create { OPEN_ALWAYS } else { OPEN_EXISTING },
    )
}

/// Exclusively creates a private read/write state file, failing if it exists.
/// The parent must already exist; the caller owns writing and flushing contents.
pub fn create_private_file(path: &Path) -> io::Result<PinnedPath> {
    let security = OwnerSecurity::new(false)?;
    PinnedPath::open(path, false, Some(&security), true, CREATE_NEW)
}

/// Opens the stable, private file used for an OS-released advisory lock. Writers
/// share access so contenders can probe the lock; no handle permits deletion.
/// This only opens the object: the caller must acquire and retain its file lock.
pub fn open_private_lock_file(path: &Path, create: bool) -> io::Result<PinnedPath> {
    let name = path.file_name().ok_or_else(denied)?;
    if name.encode_wide().any(|c| c == u16::from(b':')) {
        return Err(denied());
    }
    let parent = path.parent().ok_or_else(denied)?;
    let mut pinned = PinnedPath::directory(parent)?;
    let security = OwnerSecurity::new(false)?;
    let file = open_with_sharing(
        path,
        false,
        Some(&security),
        true,
        if create { OPEN_ALWAYS } else { OPEN_EXISTING },
        true,
        false,
    )?;
    pinned.handles.push(file);
    pinned.path = path.to_owned();
    Ok(pinned)
}

/// Pins an exact private regular file for removal, without permitting concurrent
/// writers or rename. Dropping this value before remove() leaves the file intact.
pub struct PrivateRemovalFile(PinnedPath);
impl PrivateRemovalFile {
    pub fn open(path: &Path) -> io::Result<Self> {
        let name = path.file_name().ok_or_else(denied)?;
        if name.encode_wide().any(|c| c == u16::from(b':')) {
            return Err(denied());
        }
        let mut pinned = PinnedPath::owned_directory(path.parent().ok_or_else(denied)?)?;
        let security = OwnerSecurity::new(false)?;
        let file = open_with_sharing(
            path,
            false,
            Some(&security),
            false,
            OPEN_EXISTING,
            false,
            true,
        )?;
        pinned.handles.push(file);
        pinned.path = path.to_owned();
        Ok(Self(pinned))
    }
    pub fn file(&self) -> &File {
        self.0.file()
    }
    #[allow(unsafe_code)]
    pub fn remove(self) -> io::Result<()> {
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_DISPOSITION_INFO, FileDispositionInfo, SetFileInformationByHandle,
        };
        let info = FILE_DISPOSITION_INFO { DeleteFile: true };
        // SAFETY: the retained handle was opened with DELETE; info has the exact
        // advertised layout. Windows marks this object, never a reopened path.
        if unsafe {
            SetFileInformationByHandle(
                self.file().as_raw_handle(),
                FileDispositionInfo,
                (&info as *const FILE_DISPOSITION_INFO).cast(),
                std::mem::size_of_val(&info) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        drop(self);
        Ok(())
    }
}

/// Publishes an already-flushed journal file using a same-directory, write-through
/// rename. No copy/delete fallback or delayed-reboot operation is permitted.
///
/// The caller must flush and close the staged file and serialize competing
/// publishers. Leaf validation handles are released before the pathname rename;
/// only the parent pin spans it. This is not a multi-file transaction or an
/// atomic validation-and-publication operation against same-account mutation.
#[allow(unsafe_code)]
pub fn replace_private_file(from: &Path, to: &Path) -> io::Result<()> {
    if from.parent() != to.parent() || from.file_name().is_none() || to.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "journal replacement must stay in one directory",
        ));
    }
    let _parent = PinnedPath::directory(from.parent().ok_or_else(denied)?)?;
    // Validate ownership and single-link identity before exposing journal contents.
    drop(open_private_file(from, false, false)?);
    if to.exists() {
        drop(open_private_file(to, false, false)?);
    }
    let from = wide(from)?;
    let to = wide(to)?;
    // SAFETY: both paths are terminated, the parent is pinned, and no pointers
    // survive this synchronous call. The caller has closed its staged file.
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[allow(unsafe_code)]
    fn acl_bytes(path: &Path) -> Vec<u8> {
        let file = File::open(path).unwrap();
        let mut acl = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: the file handle is live; output pointers are writable.
        let result = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut acl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        assert_eq!(result, 0);
        // SAFETY: successful query returns the valid DACL inside descriptor.
        let bytes = unsafe {
            std::slice::from_raw_parts(acl.cast::<u8>(), (*acl).AclSize as usize).to_vec()
        };
        // SAFETY: GetSecurityInfo allocated the descriptor.
        unsafe {
            LocalFree(descriptor);
        }
        bytes
    }

    #[test]
    #[allow(unsafe_code)]
    fn restricting_a_directory_does_not_rewrite_existing_descendant_acls() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("private");
        drop(prepare_private_directory(&directory).unwrap());
        // Supply exact user ownership even when the token's default owner is
        // Administrators, then deliberately broaden this fixture's DACL.
        use std::os::windows::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .access_mode(MAXIMUM_ALLOWED)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&directory)
            .unwrap();
        assert_eq!(
            // SAFETY: this newly created test directory is retained and writable;
            // the null DACL deliberately grants public access before restriction.
            unsafe {
                SetSecurityInfo(
                    file.as_raw_handle(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        drop(file);
        let outside = root.path().join("outside");
        std::fs::write(&outside, b"must remain unchanged").unwrap();
        std::fs::hard_link(&outside, directory.join("untrusted-hardlink")).unwrap();
        let before = acl_bytes(&outside);
        let held = prepare_private_directory(&directory).unwrap();
        assert_eq!(acl_bytes(&outside), before);
        assert!(open_private_file(&directory.join("untrusted-hardlink"), true, false).is_err());
        drop(held);
    }

    #[test]
    fn junctions_are_rejected_without_mutating_the_target() {
        use std::os::windows::process::CommandExt;
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = root.path().join("junction");
        let status = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command",
                "$ErrorActionPreference='Stop'; $null=New-Item -ItemType Junction -Path $env:UC_LINK -Target $env:UC_TARGET"])
            .env("UC_LINK", &link).env("UC_TARGET", &target)
            .creation_flags(0x08000000) // CREATE_NO_WINDOW
            .status().unwrap();
        assert!(status.success());
        assert!(prepare_private_directory(&link).is_err());
        assert!(PinnedPath::directory(&link).is_err());
        std::fs::write(target.join("program.exe"), b"program").unwrap();
        assert!(PinnedPath::launch_file(&link.join("program.exe")).is_err());
        assert!(PinnedPath::launch_leaf(&link.join("program.exe")).is_err());
        // Remove only the junction itself; its target remains available.
        std::fs::remove_dir(&link).unwrap();
        assert!(target.is_dir());
    }

    #[test]
    fn pinned_ancestors_cannot_be_renamed_and_identity_survives_reopen() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("private/nested");
        let held = prepare_private_directory(&directory).unwrap();
        let identity = held.identity().unwrap();
        assert!(std::fs::rename(root.path().join("private"), root.path().join("moved")).is_err());
        drop(held);
        let again = PinnedPath::directory(&directory).unwrap();
        assert_eq!(again.identity().unwrap(), identity);
        drop(again);
        std::fs::rename(root.path().join("private"), root.path().join("moved")).unwrap();
    }

    #[test]
    fn launch_names_preserve_case_and_reject_aliases() {
        assert!(
            same_local_name(
                Path::new(r"c:\Apps\tool.exe"),
                Path::new(r"\\?\C:\Apps\tool.exe")
            )
            .unwrap()
        );
        assert!(
            !same_local_name(
                Path::new(r"C:\Apps\tool.exe"),
                Path::new(r"\\?\C:\apps\tool.exe")
            )
            .unwrap()
        );
        assert!(
            !same_local_name(
                Path::new(r"C:\APPS~1\tool.exe"),
                Path::new(r"\\?\C:\Apps Long\tool.exe")
            )
            .unwrap()
        );
    }

    #[test]
    #[allow(unsafe_code)]
    fn launch_leaf_blocks_content_replacement_and_ancestor_rename_ex() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            DELETE, FILE_RENAME_INFO, FILE_SHARE_DELETE, FileRenameInfoEx,
            SetFileInformationByHandle,
        };
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        let destination = root.path().join("moved");
        assert!(parent.starts_with(root.path()) && destination.starts_with(root.path()));
        std::fs::create_dir_all(parent.join("nested")).unwrap();
        // launch_leaf intentionally rejects short-name and case aliases.
        // Windows runners may expose TEMP through an 8.3 alias.
        let leaf = parent
            .join("nested")
            .canonicalize()
            .unwrap()
            .join("program.exe");
        std::fs::write(&leaf, b"program").unwrap();
        let pin = PinnedPath::launch_leaf(&leaf).unwrap();
        assert_eq!(
            pin.identity().unwrap(),
            file_identity(&File::open(&leaf).unwrap()).unwrap()
        );
        assert!(std::fs::write(&leaf, b"replacement").is_err());
        assert!(std::fs::remove_file(&leaf).is_err());
        assert!(std::fs::rename(&leaf, parent.join("other.exe")).is_err());
        assert!(std::fs::rename(&parent, &destination).is_err());
        let directory = std::fs::OpenOptions::new()
            .access_mode(DELETE | GENERIC_READ)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&parent)
            .unwrap();
        // FileRenameInfoEx accepts an absolute DOS name, not the verbatim Win32
        // prefix that tempfile may use for the temporary directory.
        let destination_name = destination.to_str().unwrap();
        let name: Vec<u16> = destination_name
            .strip_prefix(r"\\?\")
            .unwrap_or(destination_name)
            .encode_utf16()
            .collect();
        let bytes = std::mem::offset_of!(FILE_RENAME_INFO, FileName) + (name.len() + 1) * 2;
        let mut storage = vec![0u64; bytes.div_ceil(8)];
        let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFO>();
        // SAFETY: storage is aligned and sized for the header and full UTF-16 name.
        unsafe {
            (*info).Anonymous.Flags = 0x1 | 0x2; // REPLACE_IF_EXISTS | POSIX_SEMANTICS
            (*info).FileNameLength = (name.len() * 2) as u32;
            std::ptr::copy_nonoverlapping(name.as_ptr(), (*info).FileName.as_mut_ptr(), name.len());
        }
        // SAFETY: directory and the initialized, aligned input buffer remain live.
        let rename = || unsafe {
            SetFileInformationByHandle(
                directory.as_raw_handle(),
                FileRenameInfoEx,
                info.cast(),
                bytes as u32,
            )
        };
        assert_eq!(rename(), 0);
        drop(pin);
        assert_ne!(rename(), 0, "{}", io::Error::last_os_error());
        drop(directory);
        assert_eq!(
            std::fs::read(destination.join("nested/program.exe")).unwrap(),
            b"program"
        );
    }

    #[test]
    fn private_files_are_readable_after_reopen_but_hardlinks_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let directory = prepare_private_directory(&root.path().join("state")).unwrap();
        let path = directory.path().join("receipt");
        let pinned = open_private_file(&path, true, true).unwrap();
        let mut file = pinned.file();
        file.write_all(b"private receipt").unwrap();
        file.sync_all().unwrap();
        drop(pinned);
        let pinned = open_private_file(&path, false, false).unwrap();
        let mut text = String::new();
        pinned.file().read_to_string(&mut text).unwrap();
        assert_eq!(text, "private receipt");
        drop(pinned);
        std::fs::hard_link(&path, directory.path().join("alias")).unwrap();
        assert!(open_private_file(&path, true, false).is_err());
    }

    #[test]
    fn exclusive_staging_replaces_an_existing_private_record() {
        let root = tempfile::tempdir().unwrap();
        let directory = prepare_private_directory(&root.path().join("state")).unwrap();
        let record = directory.path().join("record");
        let staging = directory.path().join("staging");
        create_private_file(&record)
            .unwrap()
            .into_file()
            .write_all(b"old")
            .unwrap();
        let mut file = create_private_file(&staging).unwrap().into_file();
        assert!(create_private_file(&staging).is_err());
        file.write_all(b"new").unwrap();
        file.sync_all().unwrap();
        drop(file);
        replace_private_file(&staging, &record).unwrap();
        assert!(!staging.exists());
        let mut contents = String::new();
        open_private_file(&record, false, false)
            .unwrap()
            .file()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "new");
    }

    #[test]
    fn private_locks_share_one_object_and_release_with_the_owner() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("instance.lock");
        let first = open_private_lock_file(&path, true).unwrap();
        first.file().try_lock().unwrap();
        let second = open_private_lock_file(&path, false).unwrap();
        assert_eq!(first.identity().unwrap(), second.identity().unwrap());
        assert!(matches!(
            second.file().try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert!(std::fs::remove_file(&path).is_err());
        drop(first);
        second.file().try_lock().unwrap();
        assert!(open_private_lock_file(&root.path().join("instance.lock:alias"), true).is_err());
    }

    #[test]
    fn removal_pins_the_exact_file_and_rejects_replacement_and_hardlinks() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("private");
        drop(prepare_private_directory(&parent).unwrap());
        let path = parent.join("state");
        use std::io::Write;
        create_private_file(&path)
            .unwrap()
            .into_file()
            .write_all(b"state")
            .unwrap();
        let held = PrivateRemovalFile::open(&path).unwrap();
        assert!(std::fs::rename(&path, root.path().join("moved")).is_err());
        assert!(std::fs::write(&path, b"replacement").is_err());
        drop(held);
        assert_eq!(std::fs::read(&path).unwrap(), b"state");
        PrivateRemovalFile::open(&path).unwrap().remove().unwrap();
        assert!(!path.exists());
        create_private_file(&path)
            .unwrap()
            .into_file()
            .write_all(b"linked")
            .unwrap();
        std::fs::hard_link(&path, root.path().join("alias")).unwrap();
        assert!(PrivateRemovalFile::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"linked");
    }

    #[test]
    fn already_private_directories_support_multiple_independent_pins() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("private");
        let first = prepare_private_directory(&path).unwrap();
        let second = prepare_private_directory(&path).unwrap();
        assert_eq!(first.identity().unwrap(), second.identity().unwrap());
        let plain = PinnedPath::directory(&path).unwrap();
        assert_eq!(first.identity().unwrap(), plain.identity().unwrap());
        assert!(std::fs::rename(&path, root.path().join("moved")).is_err());
    }

    #[test]
    fn traversal_and_device_paths_are_rejected() {
        assert!(PinnedPath::directory(Path::new(r"C:\Users\..\Windows")).is_err());
        assert!(PinnedPath::directory(Path::new(r"\\server\share\folder")).is_err());
        assert!(
            PinnedPath::directory(Path::new(r"\\.\GLOBALROOT\Device\HarddiskVolume1")).is_err()
        );
    }
}
