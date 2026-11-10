// SPDX-License-Identifier: Apache-2.0

/// Runtime capabilities available on a compilation target.
///
/// These flags identify implemented adapters, not target-native qualification.
/// Consult the platform tests before claiming support for a release artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimePlatformSupport {
    pub pseudo_terminal: bool,
    pub hosted_session_process: bool,
    /// Whether crash-safe bounded one-shot commands are supported by UCHS.
    pub supervised_session_commands: bool,
    /// Unix-style foreground process inspection; Windows uses managed job evidence.
    pub foreground_process_inspection: bool,
}

impl RuntimePlatformSupport {
    pub const fn current() -> Self {
        Self {
            pseudo_terminal: cfg!(any(unix, windows)),
            hosted_session_process: cfg!(any(unix, windows)),
            supervised_session_commands: cfg!(any(
                target_os = "linux",
                target_os = "macos",
                windows
            )),
            foreground_process_inspection: cfg!(any(target_os = "linux", target_os = "macos")),
        }
    }
}

pub const CURRENT_RUNTIME_PLATFORM_SUPPORT: RuntimePlatformSupport =
    RuntimePlatformSupport::current();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn support_declaration_matches_compile_time_adapters() {
        assert_eq!(
            CURRENT_RUNTIME_PLATFORM_SUPPORT.hosted_session_process,
            cfg!(any(unix, windows))
        );
        assert_eq!(
            CURRENT_RUNTIME_PLATFORM_SUPPORT.foreground_process_inspection,
            cfg!(any(target_os = "linux", target_os = "macos"))
        );
        assert_eq!(
            CURRENT_RUNTIME_PLATFORM_SUPPORT.pseudo_terminal,
            cfg!(any(unix, windows))
        );
    }
}
