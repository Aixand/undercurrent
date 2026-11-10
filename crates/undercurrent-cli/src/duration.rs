// SPDX-License-Identifier: Apache-2.0

//! Policy-driven duration parsing for CLI timeouts and execution policies.
//!
//! The parser is independent from CLI frameworks and product errors. Callers
//! choose the accepted units and grammar, then translate the typed error at
//! their own boundary.

#![deny(missing_docs)]

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

const NANOS_PER_MILLISECOND: u128 = 1_000_000;
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Canonical unit accepted by the shared duration parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DurationUnit {
    /// Milliseconds, written as `ms`.
    Milliseconds,
    /// Seconds, written as `s`.
    Seconds,
    /// Minutes, written as `m`.
    Minutes,
    /// Hours, written as `h`.
    Hours,
    /// Days, written as `d`.
    Days,
    /// Seven-day weeks, written as `w`.
    Weeks,
}

impl DurationUnit {
    const fn bit(self) -> u8 {
        match self {
            Self::Milliseconds => 1 << 0,
            Self::Seconds => 1 << 1,
            Self::Minutes => 1 << 2,
            Self::Hours => 1 << 3,
            Self::Days => 1 << 4,
            Self::Weeks => 1 << 5,
        }
    }

    const fn nanoseconds(self) -> u128 {
        match self {
            Self::Milliseconds => NANOS_PER_MILLISECOND,
            Self::Seconds => NANOS_PER_SECOND,
            Self::Minutes => 60 * NANOS_PER_SECOND,
            Self::Hours => 60 * 60 * NANOS_PER_SECOND,
            Self::Days => 24 * 60 * 60 * NANOS_PER_SECOND,
            Self::Weeks => 7 * 24 * 60 * 60 * NANOS_PER_SECOND,
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Milliseconds => "ms",
            Self::Seconds => "s",
            Self::Minutes => "m",
            Self::Hours => "h",
            Self::Days => "d",
            Self::Weeks => "w",
        }
    }
}

impl Display for DurationUnit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.suffix())
    }
}

/// Set of canonical duration units accepted by a parser policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurationUnits(u8);

impl DurationUnits {
    /// No accepted duration units.
    pub const NONE: Self = Self(0);
    /// Milliseconds only.
    pub const MILLISECONDS: Self = Self(DurationUnit::Milliseconds.bit());
    /// Seconds only.
    pub const SECONDS: Self = Self(DurationUnit::Seconds.bit());
    /// Minutes only.
    pub const MINUTES: Self = Self(DurationUnit::Minutes.bit());
    /// Hours only.
    pub const HOURS: Self = Self(DurationUnit::Hours.bit());
    /// Days only.
    pub const DAYS: Self = Self(DurationUnit::Days.bit());
    /// Weeks only.
    pub const WEEKS: Self = Self(DurationUnit::Weeks.bit());
    /// Common sub-day units: `ms`, `s`, `m`, and `h`.
    pub const SUB_DAY: Self =
        Self(Self::MILLISECONDS.0 | Self::SECONDS.0 | Self::MINUTES.0 | Self::HOURS.0);
    /// Every canonical unit supported by the parser.
    pub const ALL: Self = Self(Self::SUB_DAY.0 | Self::DAYS.0 | Self::WEEKS.0);

    /// Returns a set containing the units in either input set.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns true when the supplied unit belongs to this set.
    pub const fn contains(self, unit: DurationUnit) -> bool {
        self.0 & unit.bit() != 0
    }
}

/// Configurable grammar and bounds for human-readable duration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationParser {
    units: DurationUnits,
    default_unit: Option<DurationUnit>,
    compound: bool,
    decimal: bool,
    case_insensitive: bool,
    allow_zero: bool,
    maximum: Option<Duration>,
}

impl DurationParser {
    /// Creates a strict parser that requires one integer value with an explicit
    /// canonical unit.
    ///
    /// Zero values, compound segments, decimals, and uppercase units are
    /// rejected until explicitly enabled.
    pub const fn new(units: DurationUnits) -> Self {
        Self {
            units,
            default_unit: None,
            compound: false,
            decimal: false,
            case_insensitive: false,
            allow_zero: false,
            maximum: None,
        }
    }

    /// Accepts a unitless single value using `unit`.
    pub const fn with_default_unit(mut self, unit: DurationUnit) -> Self {
        self.default_unit = Some(unit);
        self
    }

    /// Accepts adjacent segments such as `1h30m`.
    pub const fn allow_compound(mut self) -> Self {
        self.compound = true;
        self
    }

    /// Accepts exact decimal segments such as `1.5h` when representable to a
    /// nanosecond without rounding.
    pub const fn allow_decimal(mut self) -> Self {
        self.decimal = true;
        self
    }

    /// Accepts uppercase canonical unit suffixes.
    pub const fn case_insensitive(mut self) -> Self {
        self.case_insensitive = true;
        self
    }

    /// Accepts a total duration of zero.
    pub const fn allow_zero(mut self) -> Self {
        self.allow_zero = true;
        self
    }

    /// Applies an inclusive maximum duration.
    pub const fn with_maximum(mut self, maximum: Duration) -> Self {
        self.maximum = Some(maximum);
        self
    }

    /// Parses a duration using this policy.
    ///
    /// Outer ASCII whitespace is ignored. Whitespace between segments is
    /// rejected so the representation remains deterministic.
    ///
    /// # Errors
    ///
    /// Returns [`DurationParseError`] for malformed numbers, missing or
    /// unsupported units, disallowed grammar features, precision loss,
    /// overflow, zero values, or values above the configured maximum.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn parse(self, input: &str) -> Result<Duration, DurationParseError> {
        let value = input.trim();
        if value.is_empty() {
            return Err(DurationParseError::new(
                input,
                DurationParseErrorReason::Empty,
            ));
        }

        let mut index = 0;
        let mut segments = 0;
        let mut total_nanos = 0_u128;
        while index < value.len() {
            if segments > 0 && !self.compound {
                return Err(DurationParseError::new(
                    input,
                    DurationParseErrorReason::MultipleSegments,
                ));
            }
            let number_start = index;
            while value.as_bytes().get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
            if index == number_start {
                return Err(DurationParseError::new(
                    input,
                    DurationParseErrorReason::MissingNumber { index },
                ));
            }

            let integer = value[number_start..index]
                .parse::<u128>()
                .map_err(|_| DurationParseError::new(input, DurationParseErrorReason::Overflow))?;
            let mut fraction = None;
            if value.as_bytes().get(index) == Some(&b'.') {
                if !self.decimal {
                    return Err(DurationParseError::new(
                        input,
                        DurationParseErrorReason::DecimalNotAllowed,
                    ));
                }
                index += 1;
                let fraction_start = index;
                while value.as_bytes().get(index).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                }
                if index == fraction_start {
                    return Err(DurationParseError::new(
                        input,
                        DurationParseErrorReason::MissingFraction { index },
                    ));
                }
                let digits = &value[fraction_start..index];
                let numerator = digits.parse::<u128>().map_err(|_| {
                    DurationParseError::new(input, DurationParseErrorReason::Overflow)
                })?;
                let denominator = 10_u128.checked_pow(digits.len() as u32).ok_or_else(|| {
                    DurationParseError::new(input, DurationParseErrorReason::Overflow)
                })?;
                fraction = Some((numerator, denominator));
            }

            let unit_start = index;
            while value
                .as_bytes()
                .get(index)
                .is_some_and(u8::is_ascii_alphabetic)
            {
                index += 1;
            }
            let unit = if unit_start == index {
                if segments == 0 && index == value.len() {
                    self.default_unit.ok_or_else(|| {
                        DurationParseError::new(
                            input,
                            DurationParseErrorReason::MissingUnit { index },
                        )
                    })?
                } else {
                    return Err(DurationParseError::new(
                        input,
                        DurationParseErrorReason::MissingUnit { index },
                    ));
                }
            } else {
                parse_unit(&value[unit_start..index], self.case_insensitive).ok_or_else(|| {
                    DurationParseError::new(
                        input,
                        DurationParseErrorReason::UnsupportedUnit {
                            unit: value[unit_start..index].to_owned(),
                        },
                    )
                })?
            };
            if !self.units.contains(unit) {
                return Err(DurationParseError::new(
                    input,
                    DurationParseErrorReason::UnitNotAllowed { unit },
                ));
            }

            let factor = unit.nanoseconds();
            let mut segment_nanos = integer.checked_mul(factor).ok_or_else(|| {
                DurationParseError::new(input, DurationParseErrorReason::Overflow)
            })?;
            if let Some((numerator, denominator)) = fraction {
                let scaled = numerator.checked_mul(factor).ok_or_else(|| {
                    DurationParseError::new(input, DurationParseErrorReason::Overflow)
                })?;
                if scaled % denominator != 0 {
                    return Err(DurationParseError::new(
                        input,
                        DurationParseErrorReason::PrecisionLoss,
                    ));
                }
                segment_nanos =
                    segment_nanos
                        .checked_add(scaled / denominator)
                        .ok_or_else(|| {
                            DurationParseError::new(input, DurationParseErrorReason::Overflow)
                        })?;
            }
            total_nanos = total_nanos.checked_add(segment_nanos).ok_or_else(|| {
                DurationParseError::new(input, DurationParseErrorReason::Overflow)
            })?;
            segments += 1;
        }

        let seconds = u64::try_from(total_nanos / NANOS_PER_SECOND)
            .map_err(|_| DurationParseError::new(input, DurationParseErrorReason::Overflow))?;
        let nanoseconds = (total_nanos % NANOS_PER_SECOND) as u32;
        let duration = Duration::new(seconds, nanoseconds);
        if duration.is_zero() && !self.allow_zero {
            return Err(DurationParseError::new(
                input,
                DurationParseErrorReason::Zero,
            ));
        }
        if let Some(maximum) = self.maximum
            && duration > maximum
        {
            return Err(DurationParseError::new(
                input,
                DurationParseErrorReason::ExceedsMaximum { maximum },
            ));
        }
        Ok(duration)
    }
}

fn parse_unit(value: &str, case_insensitive: bool) -> Option<DurationUnit> {
    let normalized;
    let value = if case_insensitive {
        normalized = value.to_ascii_lowercase();
        normalized.as_str()
    } else {
        value
    };
    match value {
        "ms" => Some(DurationUnit::Milliseconds),
        "s" => Some(DurationUnit::Seconds),
        "m" => Some(DurationUnit::Minutes),
        "h" => Some(DurationUnit::Hours),
        "d" => Some(DurationUnit::Days),
        "w" => Some(DurationUnit::Weeks),
        _ => None,
    }
}

/// Stable reason a duration value was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurationParseErrorReason {
    /// The value was empty after trimming outer whitespace.
    Empty,
    /// A segment did not begin with an unsigned integer.
    MissingNumber {
        /// Byte index where the number was expected.
        index: usize,
    },
    /// A decimal point was not followed by digits.
    MissingFraction {
        /// Byte index where fractional digits were expected.
        index: usize,
    },
    /// A required unit suffix was absent.
    MissingUnit {
        /// Byte index where the unit was expected.
        index: usize,
    },
    /// The unit suffix is not part of the canonical grammar.
    UnsupportedUnit {
        /// Rejected suffix.
        unit: String,
    },
    /// The unit is valid but excluded by the parser policy.
    UnitNotAllowed {
        /// Rejected canonical unit.
        unit: DurationUnit,
    },
    /// More than one segment was supplied to a single-segment parser.
    MultipleSegments,
    /// A decimal was supplied to an integer-only parser.
    DecimalNotAllowed,
    /// A decimal could not be represented exactly to nanosecond precision.
    PrecisionLoss,
    /// Numeric parsing or duration arithmetic overflowed.
    Overflow,
    /// The parsed duration was zero and the policy rejects zero.
    Zero,
    /// The parsed duration exceeded the inclusive policy maximum.
    ExceedsMaximum {
        /// Inclusive configured maximum.
        maximum: Duration,
    },
}

/// Checked duration parsing failure with the rejected value and stable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurationParseError {
    input: String,
    reason: DurationParseErrorReason,
}

impl DurationParseError {
    fn new(input: impl Into<String>, reason: DurationParseErrorReason) -> Self {
        Self {
            input: input.into(),
            reason,
        }
    }

    /// Returns the rejected duration value.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns the stable rejection reason.
    pub const fn reason(&self) -> &DurationParseErrorReason {
        &self.reason
    }
}

impl Display for DurationParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            DurationParseErrorReason::Empty => formatter.write_str("duration must not be empty"),
            DurationParseErrorReason::MissingNumber { index } => {
                write!(
                    formatter,
                    "duration segment at byte {index} is missing a number"
                )
            }
            DurationParseErrorReason::MissingFraction { index } => write!(
                formatter,
                "duration decimal at byte {index} is missing fractional digits"
            ),
            DurationParseErrorReason::MissingUnit { index } => {
                write!(
                    formatter,
                    "duration segment at byte {index} is missing a unit"
                )
            }
            DurationParseErrorReason::UnsupportedUnit { unit } => {
                write!(formatter, "unsupported duration unit {unit:?}")
            }
            DurationParseErrorReason::UnitNotAllowed { unit } => {
                write!(formatter, "duration unit {unit} is not allowed here")
            }
            DurationParseErrorReason::MultipleSegments => {
                formatter.write_str("compound durations are not allowed here")
            }
            DurationParseErrorReason::DecimalNotAllowed => {
                formatter.write_str("decimal durations are not allowed here")
            }
            DurationParseErrorReason::PrecisionLoss => formatter
                .write_str("duration cannot be represented exactly to nanosecond precision"),
            DurationParseErrorReason::Overflow => {
                formatter.write_str("duration is too large to represent")
            }
            DurationParseErrorReason::Zero => {
                formatter.write_str("duration must be greater than zero")
            }
            DurationParseErrorReason::ExceedsMaximum { maximum } => write!(
                formatter,
                "duration exceeds maximum of {}ms",
                maximum.as_millis()
            ),
        }
    }
}

impl Error for DurationParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_parser_accepts_selected_integer_units() {
        let parser = DurationParser::new(DurationUnits::SUB_DAY);

        assert_eq!(parser.parse("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parser.parse("2m").unwrap(), Duration::from_secs(120));
        assert!(matches!(
            parser.parse("1d").unwrap_err().reason(),
            DurationParseErrorReason::UnitNotAllowed {
                unit: DurationUnit::Days
            }
        ));
    }

    #[test]
    fn configured_parser_accepts_default_compound_and_case_insensitive_units() {
        let default_seconds =
            DurationParser::new(DurationUnits::ALL).with_default_unit(DurationUnit::Seconds);
        let compound = DurationParser::new(DurationUnits::ALL)
            .allow_compound()
            .case_insensitive();

        assert_eq!(
            default_seconds.parse("90").unwrap(),
            Duration::from_secs(90)
        );
        assert_eq!(compound.parse("1H30m").unwrap(), Duration::from_secs(5_400));
    }

    #[test]
    fn decimal_parser_uses_exact_nanosecond_arithmetic() {
        let parser = DurationParser::new(DurationUnits::SECONDS).allow_decimal();

        assert_eq!(parser.parse("1.25s").unwrap(), Duration::from_millis(1_250));
        assert!(matches!(
            parser.parse("0.0000000001s").unwrap_err().reason(),
            DurationParseErrorReason::PrecisionLoss
        ));
    }

    #[test]
    fn parser_enforces_zero_maximum_and_overflow() {
        let parser =
            DurationParser::new(DurationUnits::SECONDS).with_maximum(Duration::from_secs(60));

        assert!(matches!(
            parser.parse("0s").unwrap_err().reason(),
            DurationParseErrorReason::Zero
        ));
        assert!(matches!(
            parser.parse("61s").unwrap_err().reason(),
            DurationParseErrorReason::ExceedsMaximum { .. }
        ));
        assert!(matches!(
            parser
                .parse("999999999999999999999999999999999999999s")
                .unwrap_err()
                .reason(),
            DurationParseErrorReason::Overflow
        ));
    }

    #[test]
    fn parser_rejects_malformed_and_disallowed_grammar() {
        let parser = DurationParser::new(DurationUnits::ALL);

        for value in ["", "s", "1", "1.5s", "1h30m", "1 seconds", "-1s"] {
            assert!(parser.parse(value).is_err(), "{value:?} must fail");
        }
    }
}
