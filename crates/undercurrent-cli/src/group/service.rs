// SPDX-License-Identifier: Apache-2.0

//! Group metadata and shell-creation intent over the blocking public SDK.
//!
//! A pending request and unique creation label are published before a start is
//! sent. Success attaches the returned stable ID to the registry; response loss
//! retains intent for label-based reconciliation, without resending the start.
//! Registry publication and daemon effects are separate commits, so a registry
//! failure must never be interpreted as rollback of an accepted process.

use super::{
    error, io_error,
    store::{Group, MAX_MEMBERS, Member, Pending, Restore, Store},
};
use crate::{
    CliError, CliExecutionLimits, CliProductStreamPort, NativeCliArguments, NativeCliCommand,
    ResolvedCliConfig, StartPreparationContext, exact_name_lookup, expect_list,
    prepare_start_request,
};
use clap::Parser;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use undercurrent_protocol::{
    MutationContext, ProductOperation, ProductRequest, SessionSummaryDto, TerminalSizeDto,
};

pub(super) struct Service<'a, P> {
    pub scrollback: undercurrent_config::ScrollbackConfig,
    pub shortcuts: undercurrent_config::ShortcutPreferences,
    pub client: &'a P,
    pub store: Store,
    pub timeout: Duration,
    pub limits: CliExecutionLimits,
    pub launch_labels: BTreeMap<String, String>,
    pub launch_metadata: BTreeMap<String, String>,
}
impl<'a, P: CliProductStreamPort> Service<'a, P> {
    pub fn new(
        client: &'a P,
        config: &ResolvedCliConfig,
        limits: CliExecutionLimits,
        timeout: Duration,
    ) -> Result<Self, CliError> {
        Ok(Self {
            scrollback: config.document().terminal.scrollback,
            shortcuts: config.document().shortcuts.clone(),
            client,
            store: Store::new(
                &config.product_home().join("state"),
                config.connection().endpoint(),
            )?,
            limits,
            timeout,
            launch_labels: BTreeMap::new(),
            launch_metadata: BTreeMap::new(),
        })
    }

    pub fn create_group(
        &self,
        name: &str,
        cwd: Option<&Path>,
        shell: Option<&str>,
        persistent: bool,
    ) -> Result<Group, CliError> {
        super::store::validate_name(name)?;
        let here = std::env::current_dir()
            .map_err(|e| io_error("could not resolve group directory", e))?;
        let cwd = cwd.map_or(here.clone(), |p| {
            if p.is_absolute() {
                p.to_owned()
            } else {
                here.join(p)
            }
        });
        let cwd = std::fs::canonicalize(cwd)
            .map_err(|e| io_error("group directory does not exist", e))?;
        if !cwd.is_dir() {
            return Err(error("group directory must be a directory"));
        }
        let group = Group {
            id: uuid::Uuid::now_v7().to_string(),
            name: name.to_owned(),
            cwd,
            shell: shell
                .map(str::to_owned)
                .unwrap_or_else(crate::preparation::native_default_shell),
            members: vec![],
            selected: None,
            next_index: 1,
            pending: None,
            persistent,
            deleting: false,
        };
        self.store.update(|groups| {
            if groups.contains_key(name) {
                return Err(error("group already exists; use uc group attach GROUP"));
            }
            groups.insert(name.to_owned(), group.clone());
            Ok(())
        })?;
        Ok(group)
    }

    pub fn add(&self, group: &str, name: &str) -> Result<(), CliError> {
        let summary = expect_list(self.client.request(
            ProductRequest::ListSessions(exact_name_lookup(name)),
            self.timeout,
        )?)?
        .items
        .into_iter()
        .next()
        .ok_or_else(|| error("session not found"))?;
        self.store.update(|groups| {
            if groups
                .values()
                .any(|g| g.members.iter().any(|m| m.id == summary.id))
            {
                return Err(error("session already belongs to a group"));
            }
            let group = groups
                .get_mut(group)
                .ok_or_else(|| error("group not found"))?;
            if group.deleting {
                return Err(error(
                    "group deletion pending; resume uc group delete --stop",
                ));
            }
            group.members.push(Member {
                id: summary.id.clone(),
                name: summary.name.clone(),
                restore: None,
            });
            if group.selected.is_none() {
                group.selected = Some(summary.id.clone())
            }
            Ok(())
        })
    }

    pub fn remove(&self, group: &str, name: &str) -> Result<(), CliError> {
        // A retained name/ID also permits removal after the daemon session is gone.
        let existing = self.store.group(group)?;
        let retained = existing
            .members
            .iter()
            .find(|m| m.id == name || m.name == name)
            .map(|m| m.id.clone());
        let pending = existing
            .pending
            .as_ref()
            .is_some_and(|p| p.request.name == name);
        let resolved = if retained.is_none() && !pending {
            expect_list(self.client.request(
                ProductRequest::ListSessions(exact_name_lookup(name)),
                self.timeout,
            )?)?
            .items
            .into_iter()
            .next()
            .map(|s| s.id)
        } else {
            retained
        };
        self.store.update(|groups| {
            let group = groups
                .get_mut(group)
                .ok_or_else(|| error("group not found"))?;
            if group.deleting {
                return Err(error(
                    "group deletion pending; resume uc group delete --stop",
                ));
            }
            let forget_pending = group.pending.as_ref().is_some_and(|p| {
                p.request.name == name || (p.replaces.is_some() && p.replaces == resolved)
            });
            if forget_pending {
                let pending = group.pending.take().expect("pending checked");
                if let Some(prior) = pending.replaces {
                    group.members.retain(|m| m.id != prior);
                    if group
                        .selected
                        .as_ref()
                        .is_some_and(|key| !group.members.iter().any(|m| m.key() == key))
                    {
                        group.selected = group.members.first().map(|m| m.key().to_owned());
                    }
                }
                return Ok(());
            }
            let before = group.members.len();
            group.members.retain(|m| Some(&m.id) != resolved.as_ref());
            if before == group.members.len() {
                return Err(error("session is not a group member"));
            }
            if group
                .selected
                .as_ref()
                .is_some_and(|key| !group.members.iter().any(|m| m.key() == key))
            {
                group.selected = group.members.first().map(|m| m.key().to_owned())
            }
            Ok(())
        })
    }

    pub fn select(&self, name: &str, id: &str) -> Result<(), CliError> {
        self.store.update(|groups| {
            let group = groups
                .get_mut(name)
                .ok_or_else(|| error("group not found"))?;
            if group.deleting {
                return Err(error(
                    "group deletion pending; resume uc group delete --stop",
                ));
            }
            if !group.members.iter().any(|m| m.id == id) {
                return Err(error("group member was removed"));
            }
            group.selected = group
                .members
                .iter()
                .find(|m| m.id == id)
                .map(|m| m.key().to_owned());
            Ok(())
        })
    }

    pub fn prepare_create(&self, name: &str, size: TerminalSizeDto) -> Result<Pending, CliError> {
        self.prepare_named_create(name, size, None)
    }

    pub fn prepare_named_create(
        &self,
        name: &str,
        size: TerminalSizeDto,
        requested_name: Option<&str>,
    ) -> Result<Pending, CliError> {
        let saved = self.store.group(name)?;
        if saved.deleting {
            return Err(error(
                "group deletion pending; resume uc group delete --stop",
            ));
        }
        if saved.pending.is_some() {
            return Err(error(
                "a creation is pending; reopen to reconcile it, or explicitly remove its pending name",
            ));
        }
        let mut index = saved.next_index;
        let mut available = false;
        for _ in 0..100 {
            let candidate = requested_name
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{name}-shell-{index}"));
            let page = expect_list(self.client.request(
                ProductRequest::ListSessions(exact_name_lookup(&candidate)),
                self.timeout,
            )?)?;
            if page.items.is_empty() {
                available = true;
                break;
            }
            if requested_name.is_some() {
                return Err(error("session name already exists"));
            }
            index = index
                .checked_add(1)
                .ok_or_else(|| error("group naming index exhausted"))?;
        }
        if !available {
            return Err(error(
                "group shell names are occupied; choose another group",
            ));
        }
        self.store.update(|groups| {
            let group = groups
                .get_mut(name)
                .ok_or_else(|| error("group not found"))?;
            if group.deleting {
                return Err(error(
                    "group deletion pending; resume uc group delete --stop",
                ));
            }
            if group.pending.is_some() || group.next_index != saved.next_index {
                return Err(error(
                    "group creation changed concurrently; retry explicitly",
                ));
            }
            if group.members.len() >= MAX_MEMBERS {
                return Err(error("group member capacity exceeded"));
            }
            let token = uuid::Uuid::now_v7().to_string();
            let session_name = requested_name
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{}-shell-{index}", group.name));
            let args = NativeCliArguments::try_parse_from([
                "uc",
                "start",
                &session_name,
                "--interactive",
                "--shell",
                &group.shell,
                "--env",
                "TERM=xterm-256color",
            ])
            .map_err(|_| error("could not prepare group shell arguments"))?;
            let NativeCliCommand::Start(mut args) = args.command else {
                return Err(error("invalid shell command routing"));
            };
            args.columns = size.columns;
            args.rows = size.rows;
            args.label = self
                .launch_labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            args.metadata = self
                .launch_metadata
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            let context =
                StartPreparationContext::new(group.cwd.clone(), Some(group.shell.clone()))?;
            let mut request =
                prepare_start_request(&args, &context, mutation(ProductOperation::StartSession))?;
            request
                .labels
                .insert("uc.group.creation".to_owned(), token.clone());
            let restore = if group.persistent {
                Some(Restore {
                    slot: uuid::Uuid::now_v7().to_string(),
                    shell: group.shell.clone(),
                    cwd: group.cwd.clone(),
                    token: token.clone(),
                    open: true,
                    labels: self.launch_labels.clone(),
                    metadata: self.launch_metadata.clone(),
                })
            } else {
                None
            };
            if let Some(restore) = &restore {
                self.prepare_persistent_shell(restore, &mut request)?;
            }
            let pending = Pending {
                token,
                request,
                restore,
                replaces: None,
            };
            group.next_index = index
                .checked_add(1)
                .ok_or_else(|| error("group naming index exhausted"))?;
            group.pending = Some(pending.clone());
            Ok(pending)
        })
    }

    pub fn finish_create(
        &self,
        name: &str,
        token: &str,
        summary: &SessionSummaryDto,
    ) -> Result<(), CliError> {
        self.store.update(|groups| {
            if groups
                .iter()
                .any(|(owner, g)| owner != name && g.members.iter().any(|m| m.id == summary.id))
            {
                return Err(error(
                    "created session already belongs to another group; pending intent retained",
                ));
            }
            let group = groups
                .get_mut(name)
                .ok_or_else(|| error("group not found"))?;
            if group.pending.as_ref().is_none_or(|p| p.token != token) {
                return Err(error(
                    "pending creation changed; accepted process remains available in uc ls",
                ));
            }
            let pending = group.pending.as_ref().expect("matching pending checked");
            let restore = pending.restore.clone();
            if let Some(prior) = &pending.replaces {
                let member = group
                    .members
                    .iter_mut()
                    .find(|m| &m.id == prior)
                    .ok_or_else(|| error("restoration slot was removed"))?;
                if group.selected.as_ref() == Some(prior) {
                    group.selected = restore
                        .as_ref()
                        .map(|r| r.slot.clone())
                        .or_else(|| Some(summary.id.clone()));
                }
                member.id = summary.id.clone();
                member.restore = restore;
            } else if !group.members.iter().any(|m| m.id == summary.id) {
                group.members.push(Member {
                    id: summary.id.clone(),
                    name: summary.name.clone(),
                    restore,
                });
                group.selected = group.members.last().map(|m| m.key().to_owned());
            }
            group.pending = None;
            Ok(())
        })
    }

    // Recover membership using the saved creation label without another start.
    // An empty query cannot prove the original request had no effect, so it must
    // leave pending intent intact. Two rows suffice to detect ambiguous evidence.
    // See [creation recovery](../../docs/internals/groups-and-recovery.md#publish-intent-before-starting-a-shell).
    pub fn reconcile(&self, name: &str) -> Result<(), CliError> {
        let group = self.store.group(name)?;
        let Some(pending) = group.pending else {
            return Ok(());
        };
        let mut query = exact_name_lookup(&pending.request.name);
        query.filter.name = None;
        query.page_size = 2;
        // Reconcile with the exact label persisted before sending the start request.
        let label = if pending.request.labels.contains_key("uc.group.creation") {
            "uc.group.creation"
        } else {
            "uc.tui.creation"
        };
        query
            .filter
            .labels
            .insert(label.to_owned(), pending.token.clone());
        let page = expect_list(
            self.client
                .request(ProductRequest::ListSessions(query), self.timeout)?,
        )?;
        if page.items.len() > 1 || page.next_cursor.is_some() {
            return Err(error(
                "creation evidence is ambiguous; pending intent retained",
            ));
        }
        if let Some(summary) = page.items.first() {
            self.finish_create(name, &pending.token, summary)?
        }
        Ok(())
    }

    pub fn reject_create(&self, name: &str, token: &str) -> Result<(), CliError> {
        self.store.update(|groups| {
            let group = groups
                .get_mut(name)
                .ok_or_else(|| error("group not found"))?;
            if group.deleting {
                return Err(error(
                    "group deletion pending; resume uc group delete --stop",
                ));
            }
            if group.pending.as_ref().is_some_and(|p| p.token == token) {
                group.pending = None
            }
            Ok(())
        })
    }
}

pub(super) fn mutation(operation: ProductOperation) -> MutationContext {
    let mut value = MutationContext::current(operation);
    value.idempotency_key = Some(format!("uc-group-{}", uuid::Uuid::now_v7()));
    value
}

#[cfg(test)]
mod tests {
    fn executable_shell() -> String {
        #[cfg(windows)]
        {
            std::env::var("COMSPEC").expect("native command interpreter")
        }
        #[cfg(unix)]
        {
            "/bin/bash".to_owned()
        }
    }
    use super::*;
    use crate::{CliProductRequestPort, CliProductSubscription, OpenedCliSubscription};
    use std::sync::Mutex;
    use undercurrent_protocol::{ProductStreamSpec, ProductSuccess, SessionPageDto};
    use undercurrent_sdk::{ProductClientError, ProductSubscriptionItem};

    struct EmptyClient {
        requests: Mutex<Vec<ProductRequest>>,
    }
    impl CliProductRequestPort for EmptyClient {
        fn request(
            &self,
            request: ProductRequest,
            _timeout: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.requests.lock().unwrap().push(request);
            Ok(ProductSuccess::ListSessions(SessionPageDto {
                items: vec![],
                next_cursor: None,
                snapshot_revision: 0,
                total: None,
            }))
        }
    }
    struct NoStream;
    impl CliProductSubscription for NoStream {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            panic!("no stream requested")
        }
        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<ProductSubscriptionItem, ProductClientError> {
            panic!("no stream requested")
        }
        fn cancel(&self) {}
        fn close(self) {}
    }
    impl CliProductStreamPort for EmptyClient {
        type Subscription = NoStream;
        fn open_product_subscription(
            &self,
            _spec: ProductStreamSpec,
            _timeout: Duration,
        ) -> Result<OpenedCliSubscription<NoStream>, ProductClientError> {
            panic!("no stream requested")
        }
    }
    fn limits() -> CliExecutionLimits {
        CliExecutionLimits::new(crate::CliExecutionLimitValues {
            catalog_page_size: 100,
            bulk_selection_sessions: 1000,
            bulk_parallel_requests: 4,
            terminal_columns: 160,
            terminal_rows: 80,
            terminal_input_items: 16,
            terminal_input_bytes: 131072,
            terminal_output_page_bytes: 65536,
        })
        .unwrap()
    }
    #[test]
    fn pending_creation_survives_reopen_and_never_blindly_retries() {
        let root = tempfile::tempdir().unwrap();
        let client = EmptyClient {
            requests: Mutex::new(vec![]),
        };
        let service = Service {
            scrollback: Default::default(),
            shortcuts: Default::default(),
            launch_labels: BTreeMap::new(),
            launch_metadata: BTreeMap::new(),
            client: &client,
            store: Store::new(root.path(), Path::new("/test.sock")).unwrap(),
            timeout: Duration::from_secs(1),
            limits: limits(),
        };
        service
            .create_group("backend", Some(root.path()), Some("/bin/sh"), false)
            .unwrap();
        let pending = service
            .prepare_create(
                "backend",
                TerminalSizeDto {
                    columns: 80,
                    rows: 23,
                },
            )
            .unwrap();
        assert!(pending.request.name == "backend-shell-1");
        let reopened = Store::new(root.path(), Path::new("/test.sock"))
            .unwrap()
            .group("backend")
            .unwrap()
            .pending
            .unwrap();
        assert!(reopened.request == pending.request);
        assert!(
            service
                .prepare_create(
                    "backend",
                    TerminalSizeDto {
                        columns: 80,
                        rows: 23
                    }
                )
                .is_err()
        );
        service.reconcile("backend").unwrap();
        assert!(service.store.group("backend").unwrap().pending.is_some());
        assert!(
            client
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| matches!(r, ProductRequest::ListSessions(_)))
        );
        service.remove("backend", "backend-shell-1").unwrap();
        let second = service
            .prepare_create(
                "backend",
                TerminalSizeDto {
                    columns: 80,
                    rows: 23,
                },
            )
            .unwrap();
        assert!(second.request.name == "backend-shell-2");
        assert_ne!(second.token, pending.token);
        assert_ne!(
            second.request.mutation.idempotency_key,
            pending.request.mutation.idempotency_key
        );
    }
    #[test]
    fn persistent_creation_is_registered_before_spawn_and_deletion_never_forgets_unknown_work() {
        let root = tempfile::tempdir().unwrap();
        let client = EmptyClient {
            requests: Mutex::new(vec![]),
        };
        let service = Service {
            scrollback: Default::default(),
            shortcuts: Default::default(),
            launch_labels: BTreeMap::new(),
            launch_metadata: BTreeMap::new(),
            client: &client,
            store: Store::new(root.path(), Path::new("/test.sock")).unwrap(),
            timeout: Duration::from_secs(1),
            limits: limits(),
        };
        service
            .create_group(
                "persistent",
                Some(root.path()),
                Some(&executable_shell()),
                true,
            )
            .unwrap();
        let pending = service
            .prepare_create(
                "persistent",
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        let recipe = pending.restore.as_ref().unwrap();
        assert_eq!(
            pending.request.labels.get("uc.group.slot"),
            Some(&recipe.slot)
        );
        assert!(
            !pending
                .request
                .environment
                .variables
                .keys()
                .any(|k| k.starts_with("UC_GROUP_"))
        );
        assert_eq!(
            Path::new(&pending.request.command.program),
            std::fs::canonicalize(executable_shell()).unwrap()
        );
        // The durable request must serialize under the credential-aware contract.
        serde_json::to_vec(&pending.request).unwrap();
        assert!(service.delete("persistent", true, false).is_err());
        assert!(service.store.group("persistent").unwrap().deleting);
        assert!(
            service
                .prepare_create(
                    "persistent",
                    TerminalSizeDto {
                        columns: 80,
                        rows: 24
                    }
                )
                .is_err()
        );
        assert!(service.delete("persistent", false, false).is_err());
    }

    #[test]
    fn ordinary_group_delete_preserves_processes_and_stale_members_can_be_deleted_with_stop() {
        let root = tempfile::tempdir().unwrap();
        let client = EmptyClient {
            requests: Mutex::new(vec![]),
        };
        let service = Service {
            scrollback: Default::default(),
            shortcuts: Default::default(),
            launch_labels: BTreeMap::new(),
            launch_metadata: BTreeMap::new(),
            client: &client,
            store: Store::new(root.path(), Path::new("/test.sock")).unwrap(),
            timeout: Duration::from_secs(1),
            limits: limits(),
        };
        for stop in [false, true] {
            service
                .create_group("ordinary", Some(root.path()), Some("/bin/sh"), false)
                .unwrap();
            service
                .store
                .update(|groups| {
                    groups.get_mut("ordinary").unwrap().members.push(Member {
                        id: "gone".into(),
                        name: "old".into(),
                        restore: None,
                    });
                    Ok(())
                })
                .unwrap();
            service.delete("ordinary", stop, false).unwrap();
            assert!(service.store.group("ordinary").is_err());
        }
        assert!(
            client
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| matches!(r, ProductRequest::ListSessions(_)))
        );
    }
    #[test]
    fn forgetting_an_uncertain_replacement_also_forgets_its_restoration_slot() {
        let root = tempfile::tempdir().unwrap();
        let client = EmptyClient {
            requests: Mutex::new(vec![]),
        };
        let service = Service {
            scrollback: Default::default(),
            shortcuts: Default::default(),
            launch_labels: BTreeMap::new(),
            launch_metadata: BTreeMap::new(),
            client: &client,
            store: Store::new(root.path(), Path::new("/test.sock")).unwrap(),
            timeout: Duration::from_secs(1),
            limits: limits(),
        };
        service
            .create_group(
                "pending",
                Some(root.path()),
                Some(&executable_shell()),
                true,
            )
            .unwrap();
        let pending = service
            .prepare_create(
                "pending",
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
            )
            .unwrap();
        service
            .store
            .update(|groups| {
                let group = groups.get_mut("pending").unwrap();
                group.members.push(Member {
                    id: "old-session".into(),
                    name: "terminal".into(),
                    restore: pending.restore.clone(),
                });
                group.selected = Some(group.members[0].key().to_owned());
                group.pending.as_mut().unwrap().replaces = Some("old-session".into());
                Ok(())
            })
            .unwrap();
        service.remove("pending", &pending.request.name).unwrap();
        let group = service.store.group("pending").unwrap();
        assert!(group.pending.is_none());
        assert!(group.members.is_empty());
        assert!(group.selected.is_none());
    }
}
