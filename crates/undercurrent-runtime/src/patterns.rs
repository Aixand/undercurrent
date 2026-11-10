// SPDX-License-Identifier: Apache-2.0

//! Bounded compiled regular-expression adapter for detection profiles.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use regex::{Regex, RegexBuilder};
use undercurrent_application::{
    ProfilePatternEngine, ProfilePatternError, ProfilePatternErrorKind,
};
use undercurrent_domain::PatternExpression;

use crate::RuntimeLimitError;

/// Hard cache, compiler, and match-input limits for the regex adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegexPatternEngineLimits {
    /// Maximum distinct compiled expressions retained.
    pub cached_patterns: usize,
    /// Approximate compiled regex size limit in bytes.
    pub compiled_size_bytes: usize,
    /// Determinized automaton size limit in bytes.
    pub determinize_size_bytes: usize,
    /// Maximum private text bytes examined by one match.
    pub match_text_bytes: usize,
}

impl RegexPatternEngineLimits {
    /// Validates every explicit hard bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first stable zero-valued field.
    pub fn validate(self) -> Result<Self, RuntimeLimitError> {
        for (field, value) in [
            ("regex_cached_patterns", self.cached_patterns),
            ("regex_compiled_size_bytes", self.compiled_size_bytes),
            ("regex_determinize_size_bytes", self.determinize_size_bytes),
            ("regex_match_text_bytes", self.match_text_bytes),
        ] {
            if value == 0 {
                return Err(RuntimeLimitError::zero(field));
            }
        }
        Ok(self)
    }
}

/// Payload-free compiled-pattern cache pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegexPatternEnginePressure {
    /// Currently retained compiled expressions.
    pub cached_patterns: usize,
    /// Inclusive retained expression maximum.
    pub cache_limit: usize,
    /// Successful cache lookups.
    pub cache_hits: u64,
    /// Expressions compiled because no cached entry existed.
    pub cache_misses: u64,
    /// Least-recently-used compiled expressions removed on admission.
    pub cache_evictions: u64,
}

/// Bounded thread-safe regex engine with a compiled-expression cache.
pub struct RegexPatternEngine {
    limits: RegexPatternEngineLimits,
    state: Mutex<RegexCache>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl Debug for RegexPatternEngine {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegexPatternEngine")
            .field("limits", &self.limits)
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}

struct RegexCache {
    access_sequence: u64,
    entries: HashMap<String, RegexCacheEntry>,
}

struct RegexCacheEntry {
    regex: Regex,
    last_access: u64,
}

impl RegexPatternEngine {
    /// Constructs an empty compiled-expression cache from validated limits.
    ///
    /// This function allocates no regex state, performs no IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed zero-limit failure.
    pub fn new(limits: RegexPatternEngineLimits) -> Result<Self, RuntimeLimitError> {
        Ok(Self {
            limits: limits.validate()?,
            state: Mutex::new(RegexCache {
                access_sequence: 0,
                entries: HashMap::new(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Returns payload-free cache occupancy and cumulative outcomes.
    ///
    /// This function retains no expression or sample text in its result,
    /// performs no external IO, and does not panic.
    pub fn pressure(&self) -> RegexPatternEnginePressure {
        RegexPatternEnginePressure {
            cached_patterns: self.lock_state().entries.len(),
            cache_limit: self.limits.cached_patterns,
            cache_hits: self.hits.load(Ordering::Acquire),
            cache_misses: self.misses.load(Ordering::Acquire),
            cache_evictions: self.evictions.load(Ordering::Acquire),
        }
    }

    fn compiled(&self, expression: &PatternExpression) -> Result<Regex, ProfilePatternError> {
        let key = expression.as_str();
        {
            let mut state = self.lock_state();
            if state.entries.contains_key(key) {
                let access = state.next_access();
                if let Some(entry) = state.entries.get_mut(key) {
                    entry.last_access = access;
                    increment_counter(&self.hits);
                    return Ok(entry.regex.clone());
                }
            }
        }

        increment_counter(&self.misses);
        let compiled = RegexBuilder::new(key)
            .size_limit(self.limits.compiled_size_bytes)
            .dfa_size_limit(self.limits.determinize_size_bytes)
            .build()
            .map_err(map_regex_error)?;

        let mut state = self.lock_state();
        if state.entries.contains_key(key) {
            let access = state.next_access();
            if let Some(entry) = state.entries.get_mut(key) {
                entry.last_access = access;
                return Ok(entry.regex.clone());
            }
        }
        if state.entries.len() >= self.limits.cached_patterns {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(expression, _)| expression.clone());
            if let Some(oldest) = oldest {
                state.entries.remove(&oldest);
                increment_counter(&self.evictions);
            }
        }
        let access = state.next_access();
        state.entries.insert(
            key.to_owned(),
            RegexCacheEntry {
                regex: compiled.clone(),
                last_access: access,
            },
        );
        Ok(compiled)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegexCache> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn increment_counter(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

impl ProfilePatternEngine for RegexPatternEngine {
    fn validate(&self, expression: &PatternExpression) -> Result<(), ProfilePatternError> {
        self.compiled(expression).map(|_| ())
    }

    fn is_match(
        &self,
        expression: &PatternExpression,
        text: &str,
    ) -> Result<bool, ProfilePatternError> {
        if text.len() > self.limits.match_text_bytes {
            return Err(ProfilePatternError::new(
                ProfilePatternErrorKind::ResourceLimit,
                None,
            ));
        }
        self.compiled(expression).map(|regex| regex.is_match(text))
    }
}

impl RegexCache {
    fn next_access(&mut self) -> u64 {
        if let Some(next) = self.access_sequence.checked_add(1) {
            self.access_sequence = next;
            return next;
        }
        let mut entries = self.entries.values_mut().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.last_access);
        for (index, entry) in entries.into_iter().enumerate() {
            entry.last_access = u64::try_from(index).unwrap_or(u64::MAX);
        }
        self.access_sequence = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);
        self.access_sequence
    }
}

fn map_regex_error(error: regex::Error) -> ProfilePatternError {
    let kind = match error {
        regex::Error::Syntax(_) => ProfilePatternErrorKind::InvalidSyntax,
        regex::Error::CompiledTooBig(_) => ProfilePatternErrorKind::ResourceLimit,
        _ => ProfilePatternErrorKind::InvalidSyntax,
    };
    ProfilePatternError::new(kind, None)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    fn expression(value: &str) -> PatternExpression {
        PatternExpression::new(value, 1_024).expect("pattern expression")
    }

    fn engine(cached_patterns: usize) -> RegexPatternEngine {
        RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 1_048_576,
            match_text_bytes: 4_096,
        })
        .expect("regex engine")
    }

    #[test]
    fn valid_patterns_are_compiled_once_and_reused() {
        let engine = engine(8);
        let expression = expression(r"(?m)^ready> $");

        assert!(engine.is_match(&expression, "output\nready> ").unwrap());
        assert!(!engine.is_match(&expression, "working").unwrap());

        let pressure = engine.pressure();
        assert_eq!(pressure.cached_patterns, 1);
        assert_eq!(pressure.cache_misses, 1);
        assert_eq!(pressure.cache_hits, 1);
    }

    #[test]
    fn syntax_and_input_limits_are_typed_and_payload_free() {
        let engine = engine(8);
        let invalid = expression("[");

        let syntax = engine.validate(&invalid).expect_err("syntax must fail");
        let input = engine
            .is_match(&expression("a"), &"x".repeat(4_097))
            .expect_err("oversized text must fail");

        assert_eq!(syntax.kind(), ProfilePatternErrorKind::InvalidSyntax);
        assert_eq!(input.kind(), ProfilePatternErrorKind::ResourceLimit);
        assert!(!format!("{syntax:?}").contains('['));
        assert!(!format!("{input:?}").contains(&"x".repeat(32)));
    }

    #[test]
    fn least_recently_used_entry_is_evicted_only_on_miss() {
        let engine = engine(2);
        let first = expression("first");
        let second = expression("second");
        let third = expression("third");

        engine.validate(&first).unwrap();
        engine.validate(&second).unwrap();
        engine.validate(&first).unwrap();
        engine.validate(&third).unwrap();
        engine.validate(&first).unwrap();
        engine.validate(&second).unwrap();

        let pressure = engine.pressure();
        assert_eq!(pressure.cached_patterns, 2);
        assert_eq!(pressure.cache_hits, 2);
        assert_eq!(pressure.cache_misses, 4);
        assert_eq!(pressure.cache_evictions, 2);
    }

    #[test]
    fn concurrent_compilation_retains_one_payload_free_entry() {
        let engine = Arc::new(engine(8));
        let barrier = Arc::new(Barrier::new(9));
        let mut workers = Vec::new();

        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                let private_sample = "private-ready-output";
                barrier.wait();
                engine
                    .is_match(&expression("ready"), private_sample)
                    .expect("matching must succeed")
            }));
        }
        barrier.wait();

        for worker in workers {
            assert!(worker.join().expect("worker must not panic"));
        }

        assert_eq!(engine.pressure().cached_patterns, 1);
        assert!(!format!("{engine:?}").contains("private-ready-output"));
    }

    #[test]
    fn zero_limits_are_rejected() {
        let error = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 0,
            compiled_size_bytes: 1,
            determinize_size_bytes: 1,
            match_text_bytes: 1,
        })
        .expect_err("zero cache must fail");

        assert_eq!(error.field(), "regex_cached_patterns");
    }
}
