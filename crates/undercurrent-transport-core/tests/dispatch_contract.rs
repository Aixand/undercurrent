// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_transport_core::ServerDispatchError;

#[test]
fn dispatch_error_preserves_source_and_redacts_private_detail() {
    let error = ServerDispatchError::new(PrivateFailure);

    assert!(error.source().is_some());
    assert!(!error.to_string().contains("PRIVATE"));
    assert!(!format!("{error:?}").contains("PRIVATE"));
}

#[derive(Debug)]
struct PrivateFailure;

impl Display for PrivateFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PRIVATE payload")
    }
}

impl Error for PrivateFailure {}
