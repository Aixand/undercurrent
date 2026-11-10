// SPDX-License-Identifier: Apache-2.0
//! Shared local setup for the SDK examples; never starts a daemon.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use undercurrent_sdk::{
    DeterministicClientRecoveryJitter, LocalClientConfig, LocalClientConnector, ProductClient,
    StandaloneClientProfile,
};

pub type ExampleResult<T> = Result<T, Box<dyn Error>>;

pub fn with_client<T>(
    endpoint: PathBuf,
    action: impl FnOnce(&ProductClient) -> ExampleResult<T>,
) -> ExampleResult<T> {
    let seed = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64
        ^ u64::from(std::process::id());
    let profile = StandaloneClientProfile::standard(format!("sdk-example-{seed:x}"))?;
    let config = LocalClientConfig::new(endpoint, profile.hello().clone(), profile.local())?;
    let client = ProductClient::connect(
        Arc::new(LocalClientConnector::new(config)),
        profile.product(),
        Arc::new(DeterministicClientRecoveryJitter::new(seed)),
    )?;
    let result = action(&client);
    client.close();
    result
}
