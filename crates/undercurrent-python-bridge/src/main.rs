//! Private JSON-lines subprocess adapter for the async Python package.
//!
//! One blocking reader dispatches finite requests to bounded workers and owns
//! watch/event subscriptions. All workers share one public [`ProductClient`];
//! the SDK owns authenticated local IPC, connection recovery, and stream resume.
//! The daemon remains responsible for authorization, durable state, and process
//! effects. This binary neither launches a daemon nor embeds a runtime.
//!
//! Stdout is exclusively protocol output. Stream acknowledgments release the
//! next bridge item; they do not acknowledge application processing or durable
//! effects. Python call cancellation has no command here; SDK request deadlines
//! and explicit subscription removal have separate cancellation semantics.
//!
//! See the crate README and `docs/reference/stdio-protocol.md` for the private envelope,
//! limits, normalization rules, and failure behavior. Use the Python package or
//! public Rust SDK for integrations rather than depending on this envelope.
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, Read},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
use undercurrent_protocol::*;
use undercurrent_sdk::*;

fn request_limit(args: impl IntoIterator<Item = String>) -> Result<u32, &'static str> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        None => Ok(DEFAULT_REQUEST_BYTES),
        Some("--request-bytes") => {
            let value = args
                .next()
                .ok_or("missing request byte limit")?
                .parse::<u32>()
                .map_err(|_| "invalid request byte limit")?;
            if args.next().is_some() || value == 0 || value > u32::MAX - 1838 {
                return Err("invalid request byte limit");
            }
            Ok(value)
        }
        _ => Err("unsupported bridge argument"),
    }
}

fn maximum_line_bytes(request_bytes: u32) -> u64 {
    (u64::from(request_bytes) + 65_536).max(4 * 1024 * 1024)
}
mod output;
type Output = Arc<output::EnvelopeOutput<io::Stdout>>;

/// Serializes and flushes one complete envelope under the shared stdout lock.
///
/// A write failure exits immediately with status zero; it does not prove that
/// pending daemon operations completed or run the normal EOF cleanup path.
fn emit(out: &Output, value: Value) {
    emit_envelope(out, value, false);
}

fn emit_envelope(out: &Output, value: Value, terminal: bool) {
    if out.emit(&value, terminal).is_err() {
        std::process::exit(0);
    }
}

fn recovery_jitter(instance: uuid::Uuid) -> DeterministicClientRecoveryJitter {
    // UUIDv7's low 64 bits carry per-instance random material. Reuse that
    // non-secret identity entropy instead of synchronizing every bridge at seed 1.
    DeterministicClientRecoveryJitter::new(instance.as_u128() as u64)
}

/// Private stdio envelope, distinct from the product DTO carried in `body`.
#[derive(Deserialize)]
struct Command {
    id: String,
    op: String,
    #[serde(default)]
    body: Value,
    #[serde(default = "default_timeout")]
    timeout: f64,
}
fn default_timeout() -> f64 {
    30.0
}

/// Keeps presentation category and available product disposition/certainty.
///
/// This projection omits the product error's retry hints and resource details;
/// absence of certainty must never be interpreted as proof of absent effects.
fn error_value(error: &ProductClientError) -> Value {
    let certainty = match error {
        ProductClientError::Server(error) => error.effect_certainty,
        _ => None,
    };
    json!({"message":error.to_string(), "category":format!("{:?}", error.category()),
        "product_category":error.product_error_category(),
        "effect_certainty":certainty,
        "disposition":error.product_error_disposition()})
}

/// Reader-owned subscription entry, retained until unsubscribe or normal EOF.
///
/// Worker exit alone does not remove the entry or release its capacity slot.
struct Active {
    subscription: Arc<ProductSubscription>,
    ack: mpsc::SyncSender<()>,
    worker: thread::JoinHandle<()>,
}
/// Wakes SDK receive and acknowledgment waits before joining the stream worker.
///
/// Cancellation closes the logical interest without stopping its session.
/// Peers normally remain active; failure to queue the SDK close command can
/// close the shared physical connection. See [close pressure](../../../docs/sdk/rust/streams-and-recovery.md#close-pressure-can-affect-peers).
fn detach(active: Active) {
    // Logical cancellation normally leaves peers active, but a full SDK close-command
    // queue can close their shared physical connection; see
    // [close pressure](../../../docs/sdk/rust/streams-and-recovery.md#close-pressure-can-affect-peers).
    active.subscription.cancel();
    // SDK cancellation cannot wake the separate wait for Python's ACK. Release
    // that wait too, before joining, or an unconsumed item can stall unsubscribe.
    drop(active.ack);
    let _ = active.worker.join();
}

/// Supplies standalone launch binding and current metadata for convenience mutations.
///
/// Start, input, stop, resize, and rename preserve only the caller's idempotency key
/// inside mutation metadata: attempts, supplied contracts, and compatible
/// unknown mutation fields are replaced. Other request variants pass through.
fn prepare(mut request: ProductRequest) -> ProductRequest {
    let operation = request.operation();
    let mutation = match &mut request {
        ProductRequest::StartSession(request) => {
            // Resolve the public standalone environment binding at the Rust boundary.
            if request.environment.binding.is_none()
                && let Some(cwd) = &request.working_directory
            {
                request.environment.binding = Some(ExecutionEnvironmentBindingDto {
                    working_directory_root: cwd.clone(),
                    binding_digest_sha256: standalone_environment_binding_digest_sha256(cwd),
                });
            }
            Some(&mut request.mutation)
        }
        ProductRequest::SendTerminalInput(request) => Some(&mut request.mutation),
        ProductRequest::StopSession(request) => Some(&mut request.mutation),
        ProductRequest::ResizeTerminal(request) => Some(&mut request.mutation),
        ProductRequest::RenameSession(request) => Some(&mut request.mutation),
        _ => None,
    };
    if let Some(mutation) = mutation {
        // Python convenience calls leave contract versions to this boundary;
        // the daemon rejects mutations without current metadata. See
        // [normalization](../docs/reference/stdio-protocol.md#mutation-normalization).
        let key = mutation.idempotency_key.take();
        *mutation = MutationContext::current(operation);
        mutation.idempotency_key = key;
    }
    request
}

/// Connects before announcing readiness, then owns command dispatch and cleanup.
///
/// Subscription activation and detach run on the reader thread. Finite request
/// results can arrive out of input order. Normal EOF detaches streams, closes
/// the shared client, and joins retained request workers; early errors leave
/// that explicit cleanup path and are reported by `main` as a fatal envelope.
fn run(out: &Output) -> Result<(), Box<dyn std::error::Error>> {
    let request_bytes = request_limit(std::env::args().skip(1))?;
    let max_line = maximum_line_bytes(request_bytes);
    let home = std::env::var_os("UNDERCURRENT_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let variable = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
            PathBuf::from(std::env::var_os(variable).unwrap_or_default())
                .join(".aixand")
                .join("undercurrent")
        });
    let instance = uuid::Uuid::now_v7();
    let profile =
        StandaloneClientProfile::with_request_limit(format!("python-{instance}"), request_bytes)?;
    let config = LocalClientConfig::new(
        home.join("run").join("control.sock"),
        profile.hello().clone(),
        profile.local(),
    )?;
    let client = ProductClient::connect(
        Arc::new(LocalClientConnector::new(config)),
        profile.product(),
        Arc::new(recovery_jitter(instance)),
    )?;
    emit(
        out,
        json!({"type":"ready", "protocol":env!("CARGO_PKG_VERSION")}),
    );
    let mut active: BTreeMap<String, Active> = BTreeMap::new();
    let count = Arc::new(AtomicUsize::new(0));
    let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    loop {
        let mut line = String::new();
        // Bound allocation before searching for a newline. The extra byte lets
        // the size guard reject an oversized line instead of parsing its prefix.
        let size = (&mut reader).take(max_line + 1).read_line(&mut line)?;
        if size == 0 {
            break;
        }
        if size as u64 > max_line {
            return Err("Python bridge input exceeds limit".into());
        }
        let command: Command = serde_json::from_str(&line)?;
        if !command.timeout.is_finite() || command.timeout <= 0.0 || command.timeout > 300.0 {
            emit(
                out,
                json!({"type":"error", "id":command.id,
                "error":{"category":"InvalidArgument", "message":"timeout must be in (0, 300]"}}),
            );
            continue;
        }
        let deadline = Duration::from_secs_f64(command.timeout);
        match command.op.as_str() {
            "request" => {
                let request: ProductRequest = match serde_json::from_value(command.body) {
                    Ok(request) => prepare(request),
                    Err(_) => {
                        emit(
                            out,
                            json!({"type":"error", "id":command.id,
                            "error":{"category":"InvalidArgument", "message":"Invalid product request"}}),
                        );
                        continue;
                    }
                };
                // Only this reader admits requests; workers only decrement. The
                // separate capacity check and increment rely on that ownership.
                // See [concurrency](../docs/internals/operation-flow.md#finite-requests-and-concurrency).
                if count.load(Ordering::Acquire) >= 16 {
                    emit(
                        out,
                        json!({"type":"error", "id":command.id,
                        "error":{"category":"Overloaded", "message":"Too many pending requests"}}),
                    );
                    continue;
                }
                workers.retain(|worker| !worker.is_finished());
                count.fetch_add(1, Ordering::AcqRel);
                let (client, out, count) = (client.clone(), out.clone(), count.clone());
                workers.push(thread::spawn(move || {
                    match client.request(request, deadline) {
                        Ok(result) => emit(
                            &out,
                            json!({"type":"result", "id":command.id, "result":result}),
                        ),
                        Err(error) => emit(
                            &out,
                            json!({"type":"error", "id":command.id, "error":error_value(&error)}),
                        ),
                    }
                    // Retain the slot through emission so blocked stdout also
                    // bounds the number of request workers waiting to write.
                    count.fetch_sub(1, Ordering::AcqRel);
                }));
            }
            "subscribe" => {
                let spec: ProductStreamSpec = match serde_json::from_value(command.body) {
                    Ok(
                        spec @ (ProductStreamSpec::SessionWatch(_)
                        | ProductStreamSpec::SessionEvents(_)),
                    ) => spec,
                    _ => {
                        emit(
                            out,
                            json!({"type":"error", "id":command.id,
                            "error":{"category":"InvalidArgument", "message":"Expected session watch or events specification"}}),
                        );
                        continue;
                    }
                };
                if active.len() >= 64 || active.contains_key(&command.id) {
                    emit(
                        out,
                        json!({"type":"error", "id":command.id,
                        "error":{"category":"Overloaded", "message":"Subscription capacity reached"}}),
                    );
                    continue;
                }
                let opened = match client.open_subscription(spec, deadline) {
                    Ok(opened) => opened,
                    Err(error) => {
                        emit(
                            out,
                            json!({"type":"error", "id":command.id, "error":error_value(&error)}),
                        );
                        continue;
                    }
                };
                let subscription = Arc::new(opened.subscription);
                let (ack, received) = mpsc::sync_channel(1);
                let (stream, output, id) = (subscription.clone(), out.clone(), command.id.clone());
                // Python awaits this result before exposing its iterator. Flush
                // it before spawning so even buffered SDK items follow activation.
                // See [activation](../docs/internals/operation-flow.md#activation-why-a-slow-open-stalls-the-reader).
                emit(
                    out,
                    json!({"type":"result", "id":command.id, "result":null}),
                );
                let worker = thread::spawn(move || {
                    loop {
                        let item = match stream.recv() {
                            Ok(item) => item,
                            Err(error) => {
                                emit(
                                    &output,
                                    json!({"type":"stream_error", "id":id, "error":error_value(&error)}),
                                );
                                break;
                            }
                        };
                        let value = match item {
                            ProductSubscriptionItem::Json { item, .. } => {
                                json!({"type":"item", "id":id, "item":item})
                            }
                            ProductSubscriptionItem::Gap(gap) => json!({"type":"item", "id":id,
                            "item":{"item":"gap", "value":{"reason":gap.reason.as_str(), "recovery":gap.recovery.as_str()}}}),
                            ProductSubscriptionItem::Terminal { .. } => break,
                        };
                        emit(&output, value);
                        // One unacknowledged item per stream bounds Python-side buffering.
                        // Python ACKs after dequeue, before yielding to user code;
                        // this is flow control, not a durable consumer checkpoint.
                        // See [delivery](../docs/internals/operation-flow.md#delivery-two-waits-must-be-released-on-detach).
                        if received.recv().is_err() {
                            break;
                        }
                    }
                });
                active.insert(
                    command.id,
                    Active {
                        subscription,
                        ack,
                        worker,
                    },
                );
            }
            "ack" => {
                if let Some(stream) = active.get(&command.id) {
                    // A full ACK slot must not block the sole reader: it still
                    // needs to accept unsubscribe to release a stalled stream.
                    let _ = stream.ack.try_send(());
                }
            }
            "unsubscribe" => {
                if let Some(stream) = active.remove(&command.id) {
                    detach(stream);
                }
            }
            _ => return Err("Unknown Python bridge command".into()),
        }
    }
    for (_, stream) in active {
        detach(stream);
    }
    // Closing wakes pending SDK calls before the joins; EOF is not a promise
    // to finish every submitted request. See
    // [EOF cleanup](../docs/internals/recovery.md#normal-eof-and-exceptional-exits).
    client.close();
    for worker in workers {
        let _ = worker.join();
    }
    Ok(())
}

/// Reports ordinary startup/reader errors as fatal and exits with status one.
fn main() {
    let out = Arc::new(output::EnvelopeOutput::new(io::stdout()));
    if let Err(error) = run(&out) {
        emit_envelope(
            &out,
            json!({"type":"fatal",
            "error":{"category":"Unavailable", "message":error.to_string()}}),
            true,
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_supplies_current_contract_and_preserves_request_identity() {
        let request = ProductRequest::RenameSession(undercurrent_protocol::RenameSessionRequest {
            session_id: "one".to_owned(),
            name: "codex-app-server-work-01".to_owned(),
            mutation: MutationContext {
                idempotency_key: Some("rename-one".to_owned()),
                ..MutationContext::default()
            },
        });
        let ProductRequest::RenameSession(prepared) = prepare(request) else {
            panic!("rename changed operation");
        };
        assert_eq!(prepared.session_id, "one");
        assert_eq!(
            prepared.mutation.idempotency_key.as_deref(),
            Some("rename-one")
        );
        assert_eq!(
            prepared.mutation.contract,
            MutationContext::current(undercurrent_protocol::ProductOperation::RenameSession)
                .contract
        );
    }

    #[test]
    fn bridge_request_option_controls_sdk_and_line_bounds() {
        assert_eq!(request_limit(Vec::new()).unwrap(), DEFAULT_REQUEST_BYTES);
        let parse = |args: &[&str]| request_limit(args.iter().map(|value| (*value).to_owned()));
        assert_eq!(parse(&["--request-bytes", "67108864"]).unwrap(), 67_108_864);
        assert_eq!(maximum_line_bytes(67_108_864), 67_174_400);
        assert_eq!(maximum_line_bytes(1024), 4 * 1024 * 1024);
        for args in [
            vec!["--request-bytes"],
            vec!["--request-bytes", "0"],
            vec!["--request-bytes", "4294967295"],
            vec!["--unknown"],
            vec!["--request-bytes", "12", "unexpected"],
        ] {
            assert!(parse(&args).is_err());
        }
    }

    #[test]
    fn bridge_client_identities_diversify_recovery_samples() {
        let samples = |instance| {
            let jitter = recovery_jitter(instance);
            (1..=16)
                .map(|attempt| jitter.sample_basis_points(attempt, 1_000))
                .collect::<Vec<_>>()
        };
        let first = uuid::Uuid::from_u128(1);
        let second = uuid::Uuid::from_u128(2);
        assert_eq!(samples(first), samples(first));
        assert_ne!(samples(first), samples(second));
        assert!(samples(second).into_iter().all(|sample| sample <= 1_000));
    }
}
