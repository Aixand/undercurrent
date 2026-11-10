// SPDX-License-Identifier: Apache-2.0

//! Environment-based session context with optional daemon name resolution.

use std::io::Write;

use serde::Serialize;
use undercurrent_protocol::{
    GetSessionDetailRequest, ProductOperation, ProductRequest, ProductSuccess,
};

use crate::{
    CliError, CliInvocationContext, CliPresenter, CommonExitCode, ContextArguments,
    NativeProductClientFactory, OutputFormat, Verbosity,
};

#[derive(Serialize)]
struct SessionContext {
    inside: bool,
    session_id: Option<String>,
    session_name: Option<String>,
    name_lookup_error: Option<String>,
}

pub(crate) fn execute_context<W: Write>(
    arguments: &ContextArguments,
    context: &CliInvocationContext,
    output: &mut W,
) -> Result<CommonExitCode, CliError> {
    let session_id = std::env::var("UC_SESSION").ok().filter(|id| !id.is_empty());
    render_context(arguments, context, output, session_id, |id| {
        let client = NativeProductClientFactory::from_context(context).connect()?;
        let result = client.request(
            ProductRequest::GetSessionDetail(GetSessionDetailRequest {
                session_id: id.to_owned(),
                projection: Default::default(),
            }),
            context.common().timeout.get(),
        );
        client.close();
        match result? {
            ProductSuccess::GetSessionDetail(detail) => Ok(detail.summary.name),
            other => Err(crate::executor::operation_mismatch(
                ProductOperation::GetSessionDetail,
                other,
            )),
        }
    })
}

fn render_context<W: Write>(
    arguments: &ContextArguments,
    context: &CliInvocationContext,
    output: &mut W,
    session_id: Option<String>,
    resolve: impl FnOnce(&str) -> Result<String, CliError>,
) -> Result<CommonExitCode, CliError> {
    let inside = session_id.is_some();
    let mut report = SessionContext {
        inside,
        session_id,
        session_name: None,
        name_lookup_error: None,
    };
    let quiet = context.common().verbosity == Verbosity::Quiet
        && context.common().output == OutputFormat::Text;
    if let Some(id) = &report.session_id
        && !quiet
    {
        match resolve(id) {
            Ok(name) => report.session_name = Some(name),
            Err(error) if arguments.name => return Err(error),
            Err(error) => report.name_lookup_error = Some(error.to_string()),
        }
    }
    CliPresenter::new(context.common()).write_finite(output, &report, |_| {
        if arguments.name {
            return report.session_name.clone().unwrap_or_default();
        }
        match &report.session_id {
            None => "Not inside a UC session.".to_owned(),
            Some(id) => format!(
                "Session: {}\nID:      {}{}",
                safe_text(report.session_name.as_deref().unwrap_or("unavailable")),
                safe_text(id),
                report
                    .name_lookup_error
                    .as_ref()
                    .map(|error| format!("\nName lookup failed: {error}"))
                    .unwrap_or_default()
            ),
        }
    })?;
    Ok(if inside {
        CommonExitCode::Success
    } else {
        CommonExitCode::Failure
    })
}

fn safe_text(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GlobalCliOptions;

    fn context(json: bool, quiet: bool) -> CliInvocationContext {
        CliInvocationContext::from_options(GlobalCliOptions {
            json,
            quiet,
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn outside_and_quiet_context_never_connect() {
        for quiet in [false, true] {
            let mut output = Vec::new();
            let result = render_context(
                &ContextArguments { name: false },
                &context(false, quiet),
                &mut output,
                None,
                |_| panic!("outside context must not connect"),
            )
            .unwrap();
            assert_eq!(result, CommonExitCode::Failure);
            assert_eq!(output.is_empty(), quiet);
        }
        let mut output = Vec::new();
        assert_eq!(
            render_context(
                &ContextArguments { name: false },
                &context(false, true),
                &mut output,
                Some("id".into()),
                |_| panic!("quiet context must not connect")
            )
            .unwrap(),
            CommonExitCode::Success
        );
        assert!(output.is_empty());
    }

    #[test]
    fn json_resolves_by_id_and_quiet_does_not_suppress_json() {
        let mut output = Vec::new();
        render_context(
            &ContextArguments { name: false },
            &context(true, true),
            &mut output,
            Some("session-id".into()),
            |id| {
                assert_eq!(id, "session-id");
                Ok("backend".into())
            },
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(value["inside"], true);
        assert_eq!(value["session_id"], "session-id");
        assert_eq!(value["session_name"], "backend");
        assert!(value["name_lookup_error"].is_null());
    }

    #[test]
    fn lookup_failure_retains_context_but_name_only_fails() {
        for name in [false, true] {
            let mut output = Vec::new();
            let result = render_context(
                &ContextArguments { name },
                &context(false, false),
                &mut output,
                Some("session-id".into()),
                |_| {
                    Err(CliError::SessionNotFound {
                        name: "missing".into(),
                    })
                },
            );
            if name {
                assert!(result.is_err());
                assert!(output.is_empty());
            } else {
                assert_eq!(result.unwrap(), CommonExitCode::Success);
                let text = String::from_utf8(output).unwrap();
                assert!(text.contains("session-id"));
                assert!(text.contains("Name lookup failed"));
            }
        }
    }

    #[test]
    fn name_only_is_raw_and_human_output_escapes_terminal_controls() {
        for name in [false, true] {
            let mut output = Vec::new();
            render_context(
                &ContextArguments { name },
                &context(false, false),
                &mut output,
                Some("session-id".into()),
                |_| Ok("backend\x1b[2J".into()),
            )
            .unwrap();
            let text = String::from_utf8(output).unwrap();
            assert_eq!(text.contains('\x1b'), name);
            if name {
                assert_eq!(text.trim_end(), "backend\x1b[2J");
            }
        }
    }
}
