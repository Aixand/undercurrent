// SPDX-License-Identifier: Apache-2.0

//! Atomically reloadable bounded in-memory detection-profile catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};

use undercurrent_application::{
    DetectionProfileCatalog, PortError, PortErrorKind, PortOperation, ProfileCatalogEntry,
    ProfileLintReport, ProfilePatternEngine, ProfileSourceKind, ProfileSummary,
    lint_detection_profile,
};
use undercurrent_domain::{DetectionProfile, ProfileName};

use crate::{RuntimeAdapterFailure, RuntimeLimitError};

/// Monotonic generation of one atomically activated profile catalog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProfileCatalogGeneration(u64);

impl ProfileCatalogGeneration {
    /// Returns the monotonic generation value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Lock-consistent bounded catalog pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetectionProfileCatalogPressure {
    /// Active definitions after user override resolution.
    pub profiles: usize,
    /// Inclusive configured active-profile limit.
    pub profile_limit: usize,
    /// Current atomic catalog generation.
    pub generation: ProfileCatalogGeneration,
}

/// Successful atomic profile-catalog activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileCatalogActivation {
    generation: ProfileCatalogGeneration,
    profiles: usize,
    overridden_builtins: usize,
    warnings: Vec<ProfileLintReport>,
}

impl ProfileCatalogActivation {
    /// Returns the newly active catalog generation.
    pub const fn generation(&self) -> ProfileCatalogGeneration {
        self.generation
    }

    /// Returns the number of active definitions.
    pub const fn profiles(&self) -> usize {
        self.profiles
    }

    /// Returns how many builtin names were replaced by user definitions.
    pub const fn overridden_builtins(&self) -> usize {
        self.overridden_builtins
    }

    /// Returns nonblocking lint reports for activated definitions.
    pub fn warnings(&self) -> &[ProfileLintReport] {
        &self.warnings
    }
}

/// Atomic profile-catalog activation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileCatalogActivationError {
    /// A source contains the same stable profile name more than once.
    DuplicateName {
        /// Duplicated stable name.
        profile: ProfileName,
        /// Source containing the duplicate.
        source: ProfileSourceKind,
    },
    /// Merged active definitions exceed configured capacity.
    CapacityExceeded {
        /// Attempted active definition count.
        attempted: usize,
        /// Configured inclusive limit.
        limit: usize,
    },
    /// At least one active definition has blocking lint errors.
    InvalidProfiles {
        /// Invalid reports in stable profile-name order.
        reports: Vec<ProfileLintReport>,
    },
    /// Catalog generation cannot advance without wrapping.
    GenerationExhausted,
}

impl Display for ProfileCatalogActivationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateName { profile, source } => {
                write!(formatter, "duplicate {source:?} profile name {profile}")
            }
            Self::CapacityExceeded { attempted, limit } => write!(
                formatter,
                "active profile count {attempted} exceeds configured limit {limit}"
            ),
            Self::InvalidProfiles { reports } => write!(
                formatter,
                "{} active profile definition(s) failed lint",
                reports.len()
            ),
            Self::GenerationExhausted => {
                formatter.write_str("profile catalog generation exhausted")
            }
        }
    }
}

impl Error for ProfileCatalogActivationError {}

/// Bounded, name-indexed, atomically reloadable profile catalog.
pub struct InMemoryDetectionProfileCatalog {
    max_profiles: usize,
    state: RwLock<ProfileCatalogState>,
}

impl InMemoryDetectionProfileCatalog {
    /// Constructs an empty profile catalog with a hard active-definition limit.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(max_profiles: usize) -> Result<Self, RuntimeLimitError> {
        if max_profiles == 0 {
            return Err(RuntimeLimitError::zero("max_detection_profiles"));
        }
        Ok(Self {
            max_profiles,
            state: RwLock::new(ProfileCatalogState::default()),
        })
    }

    /// Validates and atomically replaces the complete active catalog.
    ///
    /// Duplicate names within builtins or within user definitions are rejected.
    /// A user definition may override one builtin of the same name. Every
    /// resolved definition is linted before the write lock is acquired, and any
    /// failure leaves the previous generation unchanged. This function performs
    /// no filesystem or process IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed duplicate, capacity, lint, or generation failures.
    pub fn replace(
        &self,
        builtins: Vec<DetectionProfile>,
        user: Vec<DetectionProfile>,
        patterns: &dyn ProfilePatternEngine,
    ) -> Result<ProfileCatalogActivation, ProfileCatalogActivationError> {
        let mut active = BTreeMap::new();
        insert_unique(&mut active, builtins, ProfileSourceKind::BuiltIn)?;

        let builtin_names: BTreeSet<_> = active.keys().cloned().collect();
        let user_names = unique_names(&user, ProfileSourceKind::User)?;
        let overridden_builtins = user_names.intersection(&builtin_names).count();
        for profile in user {
            active.insert(
                profile.name().clone(),
                ProfileCatalogEntry::new(Arc::new(profile), ProfileSourceKind::User),
            );
        }

        if active.len() > self.max_profiles {
            return Err(ProfileCatalogActivationError::CapacityExceeded {
                attempted: active.len(),
                limit: self.max_profiles,
            });
        }

        let mut invalid = Vec::new();
        let mut warnings = Vec::new();
        for entry in active.values() {
            let report = lint_detection_profile(entry.profile(), patterns);
            if !report.is_valid() {
                invalid.push(report);
            } else if !report.issues().is_empty() {
                warnings.push(report);
            }
        }
        if !invalid.is_empty() {
            return Err(ProfileCatalogActivationError::InvalidProfiles { reports: invalid });
        }

        let mut state = self.write_state();
        let generation = ProfileCatalogGeneration(
            state
                .generation
                .0
                .checked_add(1)
                .ok_or(ProfileCatalogActivationError::GenerationExhausted)?,
        );
        state.entries = active;
        state.generation = generation;
        Ok(ProfileCatalogActivation {
            generation,
            profiles: state.entries.len(),
            overridden_builtins,
            warnings,
        })
    }

    /// Returns lock-consistent catalog pressure without cloning definitions.
    pub fn pressure(&self) -> DetectionProfileCatalogPressure {
        let state = self.read_state();
        DetectionProfileCatalogPressure {
            profiles: state.entries.len(),
            profile_limit: self.max_profiles,
            generation: state.generation,
        }
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, ProfileCatalogState> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, ProfileCatalogState> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn capacity_error() -> PortError {
        PortError::new(
            PortErrorKind::CapacityExceeded,
            PortOperation::ReadDetectionProfiles,
            None,
            RuntimeAdapterFailure("active profile catalog exceeds requested bound"),
        )
    }
}

impl DetectionProfileCatalog for InMemoryDetectionProfileCatalog {
    fn list(&self, limit: usize) -> Result<Vec<ProfileSummary>, PortError> {
        let state = self.read_state();
        if state.entries.len() > limit {
            return Err(Self::capacity_error());
        }
        Ok(state
            .entries
            .values()
            .map(ProfileSummary::from_entry)
            .collect())
    }

    fn get(&self, name: &ProfileName) -> Result<Option<ProfileCatalogEntry>, PortError> {
        Ok(self.read_state().entries.get(name).cloned())
    }

    fn all(&self, limit: usize) -> Result<Vec<ProfileCatalogEntry>, PortError> {
        let state = self.read_state();
        if state.entries.len() > limit {
            return Err(Self::capacity_error());
        }
        Ok(state.entries.values().cloned().collect())
    }
}

#[derive(Default)]
struct ProfileCatalogState {
    entries: BTreeMap<ProfileName, ProfileCatalogEntry>,
    generation: ProfileCatalogGeneration,
}

fn insert_unique(
    target: &mut BTreeMap<ProfileName, ProfileCatalogEntry>,
    profiles: Vec<DetectionProfile>,
    source: ProfileSourceKind,
) -> Result<(), ProfileCatalogActivationError> {
    for profile in profiles {
        let name = profile.name().clone();
        if target.contains_key(&name) {
            return Err(ProfileCatalogActivationError::DuplicateName {
                profile: name,
                source,
            });
        }
        target.insert(name, ProfileCatalogEntry::new(Arc::new(profile), source));
    }
    Ok(())
}

fn unique_names(
    profiles: &[DetectionProfile],
    source: ProfileSourceKind,
) -> Result<BTreeSet<ProfileName>, ProfileCatalogActivationError> {
    let mut names = BTreeSet::new();
    for profile in profiles {
        if !names.insert(profile.name().clone()) {
            return Err(ProfileCatalogActivationError::DuplicateName {
                profile: profile.name().clone(),
                source,
            });
        }
    }
    Ok(names)
}
