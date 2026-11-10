// SPDX-License-Identifier: Apache-2.0

//! Non-interactive access to the same group registry and lifecycle service as `uc group`.

use super::{error, service::Service, store::Member};
use crate::{CliError, CliExecutionLimits, CliProductStreamPort, ResolvedCliConfig};
use serde::Serialize;
use std::{collections::BTreeMap, path::Path, time::Duration};
use undercurrent_protocol::{
    EffectCertaintyDto, ProductRequest, ProductSuccess, SessionSummaryDto, TerminalSizeDto,
};
use undercurrent_sdk::ProductClientError;

/// A saved member, including slots whose daemon session is no longer present.
#[derive(Serialize)]
pub struct GroupMember {
    /// Stable restoration slot, or the session ID for an adopted session.
    pub key: String,
    /// Most recently associated daemon session ID.
    pub session_id: String,
    /// Display name retained across shell restoration.
    pub name: String,
    /// Whether this member has a saved shell recipe in a persistent group.
    pub restorable: bool,
}

/// Public group metadata without private creation receipts or launch environments.
#[derive(Serialize)]
pub struct GroupRecord {
    /// Stable registry identity.
    pub id: String,
    /// Unique group name within this endpoint.
    pub name: String,
    /// Default directory for new shells.
    pub cwd: String,
    /// Default shell for new terminals.
    pub shell: String,
    /// Whether terminals created in this group retain restoration recipes.
    pub persistent: bool,
    /// Whether a CLI-requested group shutdown is unresolved.
    pub deleting: bool,
    /// Name of a shell whose creation outcome still needs reconciliation.
    pub pending_name: Option<String>,
    /// Ordered saved membership, independent of daemon session retention.
    pub members: Vec<GroupMember>,
}

impl From<super::store::Group> for GroupRecord {
    fn from(group: super::store::Group) -> Self {
        Self {
            id: group.id,
            name: group.name,
            cwd: group.cwd.to_string_lossy().into_owned(),
            shell: group.shell,
            persistent: group.persistent,
            deleting: group.deleting,
            pending_name: group.pending.map(|p| p.request.name),
            members: group
                .members
                .into_iter()
                .map(|member| GroupMember {
                    key: member.key().to_owned(),
                    restorable: group.persistent && member.restore.is_some(),
                    session_id: member.id,
                    name: member.name,
                })
                .collect(),
        }
    }
}

/// Headless group operations over an existing authenticated product client.
/// Reads never restore shells or claim the interactive group-view lock.
pub struct GroupClient<'a, P: CliProductStreamPort> {
    service: Service<'a, P>,
}

impl<'a, P: CliProductStreamPort> GroupClient<'a, P> {
    /// Start a custom launch as an attachment-only member, retaining uncertain intent.
    /// Unlike the shell-recipe API, this preserves the caller's entire start request.
    pub(crate) fn start_prepared(
        &self,
        group: &str,
        mut request: undercurrent_protocol::StartSessionRequest,
    ) -> Result<SessionSummaryDto, CliError> {
        self.service.reconcile(group)?;
        let token = uuid::Uuid::now_v7().to_string();
        request
            .labels
            .insert("uc.group.creation".into(), token.clone());
        let pending = super::store::Pending {
            token: token.clone(),
            request,
            restore: None,
            replaces: None,
        };
        self.service.store.update(|groups| {
            let saved = groups
                .get_mut(group)
                .ok_or_else(|| error("group not found"))?;
            if saved.deleting || saved.pending.is_some() {
                return Err(error(
                    "resolve the group's pending operation before starting another session",
                ));
            }
            if saved.members.len() >= super::store::MAX_MEMBERS {
                return Err(error("group member capacity exceeded"));
            }
            saved.pending = Some(pending.clone());
            Ok(())
        })?;
        match self.service.client.request(
            ProductRequest::StartSession(pending.request),
            self.service.timeout,
        ) {
            Ok(ProductSuccess::StartSession(result)) => {
                self.service.finish_create(group, &token, &result.session)?;
                Ok(result.session)
            }
            Err(e) => {
                if matches!(&e, ProductClientError::Server(e) if e.effect_certainty == Some(EffectCertaintyDto::KnownAbsent))
                {
                    self.service.reject_create(group, &token)?;
                }
                Err(e.into())
            }
            _ => Err(error(
                "unexpected creation response; pending intent retained",
            )),
        }
    }

    /// Keep a saved shell's future recipe consistent after a successful daemon patch.
    pub(crate) fn remember_label_patch(
        &self,
        session_id: &str,
        set: &BTreeMap<String, String>,
        remove: &[String],
    ) -> Result<(), CliError> {
        self.service.store.update(|groups| {
            for group in groups.values_mut() {
                if let Some(restore) = group
                    .members
                    .iter_mut()
                    .find(|member| member.id == session_id)
                    .and_then(|member| member.restore.as_mut())
                {
                    for key in remove {
                        restore.labels.remove(key);
                    }
                    restore.labels.extend(set.clone());
                }
            }
            Ok(())
        })
    }

    /// Uses the same resolved home, endpoint namespace, locks, and limits as the CLI.
    pub fn new(
        client: &'a P,
        config: &ResolvedCliConfig,
        timeout: Duration,
    ) -> Result<Self, CliError> {
        let limits = CliExecutionLimits::from_daemon(config).map_err(|e| error(e.to_string()))?;
        Ok(Self {
            service: Service::new(client, config, limits, timeout)?,
        })
    }

    /// Applies caller context to newly created shells and their saved recipes.
    /// Existing sessions and pending creation requests retain their context.
    pub fn with_launch_context(
        mut self,
        labels: BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, CliError> {
        if labels
            .keys()
            .chain(metadata.keys())
            .any(|key| key.starts_with("uc."))
        {
            return Err(error(
                "the uc.* attribute namespace belongs to Undercurrent",
            ));
        }
        self.service.launch_labels = labels;
        self.service.launch_metadata = metadata;
        Ok(self)
    }

    /// Reads membership without mutating restoration state or process lifecycle.
    pub fn list(&self) -> Result<Vec<GroupRecord>, CliError> {
        Ok(self
            .service
            .store
            .groups()?
            .into_iter()
            .map(GroupRecord::from)
            .collect())
    }

    /// Creates an empty group without opening a terminal or starting a process.
    pub fn create(
        &self,
        name: &str,
        cwd: &Path,
        shell: &str,
        persistent: bool,
    ) -> Result<(), CliError> {
        self.service
            .create_group(name, Some(cwd), Some(shell), persistent)?;
        Ok(())
    }

    /// Forgets a group and its restoration intent while leaving processes running.
    pub fn delete(&self, name: &str) -> Result<(), CliError> {
        self.service.delete(name, false, false)
    }

    /// Atomically assigns, moves, or removes membership with an expected source.
    /// A move preserves its restoration slot. Moving a saved shell into a
    /// non-persistent group is rejected instead of silently dropping that recipe.
    pub fn assign(
        &self,
        session_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<(), CliError> {
        let summary = if from.is_none() {
            Some(
                self.service
                    .summary(session_id)?
                    .ok_or_else(|| error("session not found"))?,
            )
        } else {
            None
        };
        self.service.store.update(|groups| {
            let owner = groups
                .iter()
                .find(|(_, g)| g.members.iter().any(|m| m.id == session_id))
                .map(|(name, _)| name.clone());
            if owner.as_deref() != from {
                return Err(error(
                    "group membership changed; refresh before trying again",
                ));
            }
            if from == to {
                return Ok(());
            }
            let member = if let Some(owner) = &owner {
                let group = &groups[owner];
                if group.deleting || group.pending.is_some() {
                    return Err(error(
                        "resolve the group's pending operation before changing membership",
                    ));
                }
                group
                    .members
                    .iter()
                    .find(|m| m.id == session_id)
                    .expect("owner checked")
                    .clone()
            } else {
                let summary = summary.as_ref().expect("unassigned session resolved");
                Member {
                    id: summary.id.clone(),
                    name: summary.name.clone(),
                    restore: None,
                }
            };
            if let Some(destination) = to {
                let target = groups
                    .get(destination)
                    .ok_or_else(|| error("destination group not found"))?;
                if target.deleting || target.pending.is_some() {
                    return Err(error("destination group has a pending operation"));
                }
                if member.restore.is_some() && !target.persistent {
                    return Err(error(
                        "this saved shell requires a persistent destination group",
                    ));
                }
            }
            if let Some(owner) = &owner {
                let group = groups.get_mut(owner).expect("owner checked");
                group.members.retain(|m| m.id != session_id);
                if group.selected.as_deref() == Some(member.key()) {
                    group.selected = group.members.first().map(|m| m.key().to_owned());
                }
            }
            if let Some(destination) = to {
                let target = groups.get_mut(destination).expect("destination checked");
                if target.selected.is_none() {
                    target.selected = Some(member.key().to_owned());
                }
                target.members.push(member);
            }
            Ok(())
        })
    }

    /// Starts one named shell with the group's defaults and durable creation receipt.
    /// An uncertain response retains the pending intent and is never retried.
    pub fn start(
        &self,
        group: &str,
        name: &str,
        size: TerminalSizeDto,
    ) -> Result<SessionSummaryDto, CliError> {
        self.service.reconcile(group)?;
        let pending = self.service.prepare_named_create(group, size, Some(name))?;
        match self.service.client.request(
            ProductRequest::StartSession(pending.request),
            self.service.timeout,
        ) {
            Ok(ProductSuccess::StartSession(result)) => {
                self.service
                    .finish_create(group, &pending.token, &result.session)?;
                Ok(result.session)
            }
            Err(e) => {
                if matches!(&e, ProductClientError::Server(e) if e.effect_certainty == Some(EffectCertaintyDto::KnownAbsent))
                {
                    self.service.reject_create(group, &pending.token)?;
                }
                Err(e.into())
            }
            _ => Err(error(
                "unexpected creation response; pending intent retained",
            )),
        }
    }

    /// Reconciles a prior start receipt without creating another shell.
    pub fn reconcile(&self, group: &str) -> Result<(), CliError> {
        self.service.reconcile(group)
    }

    /// Explicitly recreates one saved shell; returns any lifecycle or recovery notices.
    pub fn restore(
        &self,
        group: &str,
        session_id: &str,
        size: TerminalSizeDto,
    ) -> Result<Vec<String>, CliError> {
        self.service.reconcile(group)?;
        self.service.restore(group, Some(session_id), size)
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::{Restore, Store};
    use super::*;
    use crate::{
        CliExecutionLimitValues, CliProductRequestPort, CliProductSubscription,
        OpenedCliSubscription,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use undercurrent_protocol::{ProductStreamSpec, SessionPageDto};
    use undercurrent_sdk::ProductSubscriptionItem;

    struct Client(AtomicUsize);
    impl CliProductRequestPort for Client {
        fn request(
            &self,
            _: ProductRequest,
            _: Duration,
        ) -> Result<ProductSuccess, ProductClientError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(ProductSuccess::ListSessions(SessionPageDto {
                items: vec![],
                next_cursor: None,
                snapshot_revision: 0,
                total: None,
            }))
        }
    }
    struct NoSubscription;
    impl CliProductSubscription for NoSubscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            panic!("no subscriptions")
        }
        fn recv_timeout(&self, _: Duration) -> Result<ProductSubscriptionItem, ProductClientError> {
            panic!("no subscriptions")
        }
        fn cancel(&self) {}
        fn close(self) {}
    }
    impl CliProductStreamPort for Client {
        type Subscription = NoSubscription;
        fn open_product_subscription(
            &self,
            _: ProductStreamSpec,
            _: Duration,
        ) -> Result<OpenedCliSubscription<NoSubscription>, ProductClientError> {
            panic!("no subscriptions")
        }
    }
    fn client<'a>(port: &'a Client, root: &Path) -> GroupClient<'a, Client> {
        GroupClient {
            service: Service {
                client: port,
                store: Store::new(root, Path::new("/test.sock")).unwrap(),
                timeout: Duration::from_secs(1),
                scrollback: Default::default(),
                shortcuts: Default::default(),
                launch_labels: BTreeMap::new(),
                launch_metadata: BTreeMap::new(),
                limits: CliExecutionLimits::new(CliExecutionLimitValues {
                    catalog_page_size: 100,
                    bulk_selection_sessions: 1000,
                    bulk_parallel_requests: 4,
                    terminal_columns: 500,
                    terminal_rows: 300,
                    terminal_input_items: 16,
                    terminal_input_bytes: 131072,
                    terminal_output_page_bytes: 65536,
                })
                .unwrap(),
            },
        }
    }
    fn seed(api: &GroupClient<'_, Client>, root: &Path) {
        for (name, persistent) in [("source", true), ("target", true), ("temporary", false)] {
            api.create(name, root, "/bin/sh", persistent).unwrap();
        }
        api.service
            .store
            .update(|groups| {
                let source = groups.get_mut("source").unwrap();
                source.members.push(Member {
                    id: "session-a".into(),
                    name: "shell".into(),
                    restore: Some(Restore {
                        slot: "01950000-0000-7000-8000-000000000001".into(),
                        shell: "/bin/sh".into(),
                        cwd: root.to_owned(),
                        token: "01950000-0000-7000-8000-000000000002".into(),
                        open: true,
                        labels: BTreeMap::new(),
                        metadata: BTreeMap::new(),
                    }),
                });
                source.selected = Some("01950000-0000-7000-8000-000000000001".into());
                Ok(())
            })
            .unwrap();
    }
    #[test]
    fn custom_start_retains_complete_pending_request_and_does_not_retry_unknown_creation() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        api.create("custom", root.path(), "/bin/sh", false).unwrap();
        use clap::Parser;
        let parsed = crate::NativeCliArguments::try_parse_from([
            "uc",
            "start",
            "custom-worker",
            "--label",
            "task=ui",
            "--",
            "tool",
            "--flag",
            "two words",
        ])
        .unwrap();
        let crate::NativeCliCommand::Start(args) = parsed.command else {
            panic!("start")
        };
        let context = crate::StartPreparationContext::new(root.path(), None).unwrap();
        let request = crate::prepare_start_request(
            &args,
            &context,
            super::super::service::mutation(undercurrent_protocol::ProductOperation::StartSession),
        )
        .unwrap();
        // This fake returns an unexpected response: success cannot be established.
        assert!(api.start_prepared("custom", request.clone()).is_err());
        let pending = api.service.store.group("custom").unwrap().pending.unwrap();
        assert_eq!(
            pending.request.command.arguments,
            vec!["--flag", "two words"]
        );
        assert_eq!(pending.request.labels["task"], "ui");
        assert_eq!(pending.request.labels["uc.group.creation"], pending.token);
        assert!(pending.restore.is_none());
        assert_eq!(port.0.load(Ordering::Relaxed), 1);
        // Reopen only queries the receipt; absence is not permission to start again.
        let reopened = client(&port, root.path());
        assert!(reopened.start_prepared("custom", request).is_err());
        assert_eq!(port.0.load(Ordering::Relaxed), 2);
        assert_eq!(
            reopened
                .service
                .store
                .group("custom")
                .unwrap()
                .pending
                .unwrap()
                .token,
            pending.token
        );
    }

    #[test]
    fn label_patch_updates_saved_recipe_without_changing_its_identity() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        seed(&api, root.path());
        api.remember_label_patch(
            "session-a",
            &BTreeMap::from([
                ("team".into(), "old".into()),
                ("keep".into(), "value".into()),
                ("remove".into(), "value".into()),
            ]),
            &[],
        )
        .unwrap();
        api.remember_label_patch(
            "session-a",
            &BTreeMap::from([("team".into(), "new".into())]),
            &["remove".into()],
        )
        .unwrap();
        let member = api.service.store.group("source").unwrap().members.remove(0);
        assert_eq!(member.id, "session-a");
        let recipe = member.restore.unwrap();
        assert_eq!(recipe.slot, "01950000-0000-7000-8000-000000000001");
        assert_eq!(
            recipe.labels,
            BTreeMap::from([
                ("team".into(), "new".into()),
                ("keep".into(), "value".into())
            ])
        );
        assert_eq!(port.0.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn saved_shell_context_survives_reopening_with_different_launch_defaults() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path())
            .with_launch_context(
                BTreeMap::from([
                    ("app".into(), "example-app".into()),
                    ("session.kind".into(), "shell".into()),
                ]),
                BTreeMap::from([("inputs.path".into(), "private-reference".into())]),
            )
            .unwrap();
        #[cfg(unix)]
        let shell = "/bin/sh".to_owned();
        #[cfg(windows)]
        let shell = undercurrent_platform_windows::windows_directory()
            .unwrap()
            .join("System32/cmd.exe")
            .to_string_lossy()
            .into_owned();
        api.create("saved", root.path(), &shell, true).unwrap();
        let pending = api
            .service
            .prepare_named_create(
                "saved",
                TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
                Some("shell"),
            )
            .unwrap();
        assert_eq!(pending.request.labels["session.kind"], "shell");
        let reopened = client(&port, root.path())
            .with_launch_context(
                BTreeMap::from([("app".into(), "another".into())]),
                BTreeMap::new(),
            )
            .unwrap();
        let saved = reopened
            .service
            .store
            .group("saved")
            .unwrap()
            .pending
            .unwrap();
        let mut request = saved.request;
        request.labels.clear();
        request.metadata.clear();
        reopened
            .service
            .prepare_persistent_shell(&saved.restore.unwrap(), &mut request)
            .unwrap();
        assert_eq!(request.labels["app"], "example-app");
        assert_eq!(request.metadata["inputs.path"], "private-reference");
        assert!(
            !serde_json::to_string(&reopened.list().unwrap())
                .unwrap()
                .contains("private-reference")
        );
    }

    #[test]
    fn browsing_and_empty_creation_never_start_or_restore_shells() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        seed(&api, root.path());
        let groups = api.list().unwrap();
        assert_eq!(groups.len(), 3);
        assert!(
            groups
                .iter()
                .find(|g| g.name == "target")
                .unwrap()
                .members
                .is_empty()
        );
        assert_eq!(port.0.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn atomic_move_preserves_slot_recipe_order_and_single_membership() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        seed(&api, root.path());
        api.assign("session-a", Some("source"), Some("target"))
            .unwrap();
        let reopened = client(&port, root.path());
        let source = reopened.service.store.group("source").unwrap();
        let target = reopened.service.store.group("target").unwrap();
        assert!(source.members.is_empty());
        assert!(source.selected.is_none());
        assert_eq!(target.members.len(), 1);
        assert_eq!(
            target.selected.as_deref(),
            Some("01950000-0000-7000-8000-000000000001")
        );
        let restore = target.members[0].restore.as_ref().unwrap();
        assert_eq!(restore.slot, "01950000-0000-7000-8000-000000000001");
        assert_eq!(restore.token, "01950000-0000-7000-8000-000000000002");
        assert_eq!(restore.cwd, root.path());
        assert_eq!(port.0.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn rejected_moves_preserve_original_membership_and_restore_data() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        seed(&api, root.path());
        for (source, target) in [
            ("wrong", "target"),
            ("source", "missing"),
            ("source", "temporary"),
        ] {
            assert!(api.assign("session-a", Some(source), Some(target)).is_err());
        }
        let saved = api.service.store.group("source").unwrap();
        assert_eq!(
            saved.members[0].key(),
            "01950000-0000-7000-8000-000000000001"
        );
        assert!(
            api.service
                .store
                .group("target")
                .unwrap()
                .members
                .is_empty()
        );
    }
    #[test]
    fn remove_and_delete_only_forget_metadata() {
        let root = tempfile::tempdir().unwrap();
        let port = Client(AtomicUsize::new(0));
        let api = client(&port, root.path());
        seed(&api, root.path());
        api.assign("session-a", Some("source"), None).unwrap();
        assert!(
            api.service
                .store
                .group("source")
                .unwrap()
                .members
                .is_empty()
        );
        api.delete("source").unwrap();
        assert_eq!(api.list().unwrap().len(), 2);
        assert_eq!(port.0.load(Ordering::Relaxed), 0);
    }
}
