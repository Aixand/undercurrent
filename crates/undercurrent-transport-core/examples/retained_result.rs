// SPDX-License-Identifier: Apache-2.0

//! Run with `cargo run --locked -p aixand-undercurrent-transport-core --example retained_result`.
//! Exercises only in-memory cache state; the bytes are not a product wire request.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use undercurrent_transport_core::{
    BoundedRequestIdempotencyStore, RequestIdempotencyCompletion, RequestIdempotencyLimits,
    RequestIdempotencyReservation, RequestIdempotencyScope,
};
use undercurrent_wire::{ClientId, IdempotencyKey};

fn main() -> Result<(), Box<dyn Error>> {
    let limits = RequestIdempotencyLimits::new(4, 1_024, 128, 128)?;
    let store = BoundedRequestIdempotencyStore::new(limits);
    let scope = RequestIdempotencyScope::new(
        ClientId::new("example-client")?,
        IdempotencyKey::new("example-mutation")?,
    );
    let request: Arc<[u8]> = Arc::from(&b"opaque sample mutation"[..]);
    let response: Arc<[u8]> = Arc::from(&b"opaque canonical result"[..]);

    assert_eq!(
        store.reserve(scope.clone(), request.clone())?,
        RequestIdempotencyReservation::Reserved
    );
    // A duplicate while execution is unresolved does not own another execution.
    assert_eq!(
        store.reserve(scope.clone(), request.clone())?,
        RequestIdempotencyReservation::Pending
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    assert_eq!(
        store.complete(&scope, response.clone(), deadline)?,
        RequestIdempotencyCompletion::Completed
    );

    // Simulated lost response: retry the same bytes without repeating an effect.
    let RequestIdempotencyReservation::Completed { response: replayed } =
        store.reserve(scope.clone(), request)?
    else {
        return Err("completed result was not retained".into());
    };
    assert_eq!(replayed, response);
    assert_eq!(
        store.reserve(scope, Arc::from(&b"different mutation"[..]))?,
        RequestIdempotencyReservation::PayloadConflict
    );

    // Explicit time supplied to the sweep makes this deterministic without sleep.
    assert_eq!(store.expire_before(deadline).records, 1);
    assert_eq!(store.pressure().retained_bytes, 0);
    println!("reserved -> pending -> completed -> replayed -> expired");
    Ok(())
}
