// SPDX-License-Identifier: Apache-2.0
//! One lazily connected SDK client; no implicit daemon startup or mutation replay.
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use undercurrent_sdk::*;

pub struct Connection {
    pub home: PathBuf,
    pub endpoint: PathBuf,
    request_bytes: u32,
    client: Mutex<Option<ProductClient>>,
}

impl Connection {
    pub fn new(home: PathBuf, request_bytes: u32) -> Self {
        Self {
            // Windows pipe identity hashes the exact native endpoint spelling.
            endpoint: home.join("run").join("control.sock"),
            home,
            request_bytes,
            client: Mutex::new(None),
        }
    }

    pub fn get(&self) -> Result<ProductClient, Value> {
        let mut slot = self
            .client
            .lock()
            .map_err(|_| failure("internal", "Connection lock unavailable"))?;
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }
        let instance = uuid::Uuid::now_v7();
        let profile = StandaloneClientProfile::with_request_limit(
            format!("mcp-{instance}"),
            self.request_bytes,
        )
        .map_err(|_| failure("configuration", "Invalid SDK profile"))?;
        let base = profile.local();
        let limits = LocalClientLimits::new(
            base.handshake_frame_bytes(),
            Duration::from_secs(3),
            Duration::from_secs(3),
            base.maximum_message_bytes(),
            base.outbound_queue_messages(),
            base.inbound_queue_messages(),
        )
        .map_err(|_| failure("configuration", "Invalid connection limits"))?;
        let config = LocalClientConfig::new(self.endpoint.clone(), profile.hello().clone(), limits)
            .map_err(|_| failure("configuration", "Invalid local endpoint"))?;
        let initial = StandaloneClientProfile::single_connection_attempt(format!("mcp-{instance}"))
            .map_err(|_| failure("configuration", "Invalid connection policy"))?;
        let client = ProductClient::connect_with_initial_policy(
            Arc::new(LocalClientConnector::new(config)),
            profile.product(),
            initial.product().recovery_policy(),
            Arc::new(DeterministicClientRecoveryJitter::new(
                instance.as_u128() as u64
            )),
        )
        .map_err(|error| failure("unavailable", &error.to_string()))?;
        *slot = Some(client.clone());
        Ok(client)
    }

    pub fn close(&self) {
        if let Ok(mut slot) = self.client.lock()
            && let Some(client) = slot.take()
        {
            client.close();
        }
    }
}

pub fn failure(category: &str, message: &str) -> Value {
    json!({"category": category, "message": message, "effect_certainty": null})
}

pub fn sdk_error(error: ProductClientError) -> Value {
    let certainty = match &error {
        ProductClientError::Server(value) => value.effect_certainty,
        _ => None,
    };
    json!({"category":error.category().as_str(), "message":error.to_string(),
        "product_category":error.product_error_category(), "disposition":error.product_error_disposition(),
        "effect_certainty":certainty})
}

#[cfg(test)]
mod tests {
    use super::*;
    use undercurrent_config::{PathEnvironment, UndercurrentPaths};

    #[test]
    fn endpoint_spelling_matches_shared_native_paths() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let paths = UndercurrentPaths::resolve(&PathEnvironment {
            undercurrent_home: Some(home.clone()),
            ..Default::default()
        })
        .unwrap();
        let connection = Connection::new(home, 1_048_576);
        // Path equality normalizes separators; pipe names require OsStr equality.
        assert_eq!(
            connection.endpoint.as_os_str(),
            paths.run_dir().join("control.sock").as_os_str()
        );
    }
}
