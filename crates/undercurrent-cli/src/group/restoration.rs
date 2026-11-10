// SPDX-License-Identifier: Apache-2.0

//! Evidence-based restoration of saved shell slots and resumable group deletion.
//!
//! Restoration distinguishes retained failure from intentional closure, and
//! consults removal observations when the original session is gone. It creates
//! a new process from a recipe, not a continuation of shell memory. Deletion
//! publishes its intent before stopping members so reconnecting views cannot
//! restore shells while termination remains unresolved.

use super::{
    error, io_error,
    service::{Service, mutation},
    store::{Pending, Restore},
};
use crate::{
    CliError, CliProductStreamPort, NativeCliArguments, NativeCliCommand, StartPreparationContext,
    exact_name_lookup, expect_list, prepare_start_request,
};
use clap::Parser;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use undercurrent_protocol::{
    LifecycleStateDto, ListSessionsRequest, ProductOperation, ProductRequest, ProductSuccess,
    RestartStatusDto, SessionFilterDto, SessionSummaryDto, StopSessionRequest, TerminalSizeDto,
};

impl<P: CliProductStreamPort> Service<'_, P> {
    pub(super) fn prepare_persistent_shell(
        &self,
        restore: &Restore,
        request: &mut undercurrent_protocol::StartSessionRequest,
    ) -> Result<(), CliError> {
        let shell = std::path::Path::new(&restore.shell);
        let candidates = if shell.components().count() > 1 {
            vec![shell.to_path_buf()]
        } else {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .map(|p| p.join(shell))
                .collect()
        };
        let executable_shell = candidates
            .into_iter()
            .find(|path| {
                #[cfg(unix)]
                {
                    path.metadata()
                        .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
                }
                #[cfg(windows)]
                {
                    path.is_file()
                }
            })
            .ok_or_else(|| error("saved shell executable unavailable"))?;
        let executable_shell = std::fs::canonicalize(executable_shell)
            .map_err(|e| io_error("saved shell executable unavailable", e))?;
        request.command.program = executable_shell.to_string_lossy().into_owned();
        request.labels.extend(restore.labels.clone());
        request.metadata.extend(restore.metadata.clone());
        request
            .labels
            .insert("uc.group.slot".into(), restore.slot.clone());
        Ok(())
    }

    pub(super) fn summary(&self, id: &str) -> Result<Option<SessionSummaryDto>, CliError> {
        let page = expect_list(self.client.request(
            ProductRequest::ListSessions(ListSessionsRequest {
                filter: SessionFilterDto {
                    ids: vec![id.to_owned()],
                    ..Default::default()
                },
                sort: Default::default(),
                projection: Default::default(),
                page_size: 1,
                cursor: None,
                include_total: false,
            }),
            self.timeout,
        )?)?;
        if page.next_cursor.is_some() {
            return Err(error("incomplete session lookup"));
        }
        Ok(page.items.into_iter().next())
    }

    fn removal_observation(
        &self,
        id: &str,
    ) -> Result<Option<undercurrent_protocol::SessionEventObservationDto>, CliError> {
        let result = self.client.request(
            ProductRequest::ReadSessionEvents(undercurrent_protocol::ReadSessionEventsRequest {
                tail: true,
                include_observation: true,
                session_ids: vec![id.to_owned()],
                kinds: vec![undercurrent_protocol::SessionEventKindDto::SessionRemoved],
                after_sequence: None,
                limit: 1,
            }),
            self.timeout,
        )?;
        let ProductSuccess::ReadSessionEvents(page) = result else {
            return Err(error("unexpected removal evidence response"));
        };
        Ok(page
            .items
            .last()
            .and_then(|event| event.observation.clone()))
    }

    pub(super) fn restore(
        &self,
        name: &str,
        explicit: Option<&str>,
        size: TerminalSizeDto,
    ) -> Result<Vec<String>, CliError> {
        let group = self.store.group(name)?;
        if group.deleting {
            return Ok(vec![
                "Deletion pending; resume uc group delete --stop".into(),
            ]);
        }
        if !group.persistent {
            if explicit.is_some() {
                return Err(error("group persistence is not enabled"));
            }
            return Ok(vec![]);
        }
        if group.pending.is_some() {
            return Ok(vec!["Creation outcome pending; reopen to reconcile".into()]);
        }
        if explicit.is_some_and(|target| {
            !group
                .members
                .iter()
                .any(|m| m.id == target || m.name == target)
        }) {
            return Err(error("terminal is not a group member"));
        }
        let mut notices = Vec::new();
        for member in group.members {
            if explicit.is_some_and(|target| target != member.id && target != member.name) {
                continue;
            }
            let Some(mut restore) = member.restore else {
                if explicit.is_some() {
                    notices.push(format!("{}: attachment-only terminal", member.name));
                }
                continue;
            };
            let current = self.summary(&member.id)?;
            if let Some(summary) = &current {
                if !restore.open && explicit.is_none() {
                    continue;
                }
                if matches!(summary.restart_status, RestartStatusDto::Scheduled { .. })
                    || matches!(
                        summary.lifecycle,
                        LifecycleStateDto::Running | LifecycleStateDto::Starting
                    )
                {
                    continue;
                }
                if matches!(summary.lifecycle, LifecycleStateDto::Stopping) {
                    notices.push(format!("{}: stop outcome unresolved", member.name));
                    continue;
                }
                if explicit.is_none()
                    && (matches!(
                        summary.lifecycle,
                        LifecycleStateDto::Stopped | LifecycleStateDto::Exited { .. }
                    ) || matches!(
                        summary.restart_status,
                        RestartStatusDto::NotScheduled {
                            reason: undercurrent_protocol::NoRestartReasonDto::ExplicitStop
                        }
                    ))
                {
                    self.store.update(|groups| {
                        if let Some(r) = groups
                            .get_mut(name)
                            .and_then(|g| g.members.iter_mut().find(|m| m.id == member.id))
                            .and_then(|m| m.restore.as_mut())
                        {
                            r.open = false;
                        }
                        Ok(())
                    })?;
                    continue;
                }
            } else {
                let removed = self.removal_observation(&member.id)?;
                let failed_cleanup = removed
                    .as_ref()
                    .is_some_and(|o| cleaned_failure_can_restore(o.lifecycle, o.restart_status));
                if explicit.is_none() && !failed_cleanup {
                    if !restore.open {
                        continue;
                    }
                    if removed.is_some() {
                        self.store.update(|groups| {
                            if let Some(r) = groups
                                .get_mut(name)
                                .and_then(|g| g.members.iter_mut().find(|m| m.id == member.id))
                                .and_then(|m| m.restore.as_mut())
                            {
                                r.open = false;
                            }
                            Ok(())
                        })?;
                    } else {
                        notices.push(format!(
                            "{}: session missing; use uc group restore {} {}",
                            member.name, name, member.name
                        ));
                    }
                    continue;
                }
                // Cleaning a failed session does not close its saved terminal slot.
                // This also repairs slots marked closed by older cleanup classification.
                if let Some(directory) = removed.and_then(|o| o.restoration_directory) {
                    restore.cwd = directory.into();
                }
            }
            if current.is_some() {
                let detail = self.client.request(
                    ProductRequest::GetSessionDetail(
                        undercurrent_protocol::GetSessionDetailRequest {
                            session_id: member.id.clone(),
                            projection: Default::default(),
                        },
                    ),
                    self.timeout,
                )?;
                let ProductSuccess::GetSessionDetail(detail) = detail else {
                    return Err(error("unexpected restoration directory response"));
                };
                if let Some(directory) = detail.restoration_directory {
                    restore.cwd = directory.into();
                }
            }
            if !restore.cwd.is_dir() {
                notices.push(format!("{}: saved directory unavailable", member.name));
                continue;
            }
            let mut name_query = exact_name_lookup(&member.name);
            name_query.projection = undercurrent_protocol::SessionProjectionDto::WithLabels;
            let occupied = expect_list(
                self.client
                    .request(ProductRequest::ListSessions(name_query), self.timeout)?,
            )?;
            if occupied.items.iter().any(|s| {
                s.id != member.id
                    && s.labels.as_ref().and_then(|l| l.get("uc.group.slot")) != Some(&restore.slot)
            }) {
                notices.push(format!("{}: name occupied by another session", member.name));
                continue;
            }
            // A retained terminal session owns its name. Replacement uses a fresh generated
            // launch name while the slot and displayed terminal name remain stable.
            let launch_name = if !occupied.items.is_empty() {
                format!(
                    "{}-r{}",
                    member.name,
                    &uuid::Uuid::now_v7().simple().to_string()[20..]
                )
            } else {
                member.name.clone()
            };
            restore.token = uuid::Uuid::now_v7().to_string();
            restore.open = true;
            let args = NativeCliArguments::try_parse_from([
                "uc",
                "start",
                &launch_name,
                "--interactive",
                "--shell",
                &restore.shell,
            ])
            .map_err(|_| error("could not prepare restored shell"))?;
            let NativeCliCommand::Start(mut args) = args.command else {
                unreachable!()
            };
            args.columns = size.columns;
            args.rows = size.rows;
            let context =
                StartPreparationContext::new(restore.cwd.clone(), Some(restore.shell.clone()))?;
            let mut request =
                prepare_start_request(&args, &context, mutation(ProductOperation::StartSession))?;
            request
                .labels
                .insert("uc.group.creation".into(), restore.token.clone());
            if let Err(e) = self.prepare_persistent_shell(&restore, &mut request) {
                notices.push(format!("{}: {}", member.name, e));
                continue;
            }
            let pending = Pending {
                token: restore.token.clone(),
                request,
                restore: Some(restore),
                replaces: Some(member.id.clone()),
            };
            self.store.update(|groups| {
                let saved = groups
                    .get_mut(name)
                    .ok_or_else(|| error("group not found"))?;
                if saved.deleting
                    || saved.pending.is_some()
                    || !saved.members.iter().any(|m| m.id == member.id)
                {
                    return Err(error("group changed during restoration"));
                }
                saved.pending = Some(pending.clone());
                Ok(())
            })?;
            match self
                .client
                .request(ProductRequest::StartSession(pending.request), self.timeout)
            {
                Ok(ProductSuccess::StartSession(summary)) => {
                    self.finish_create(name, &pending.token, &summary.session)?
                }
                Err(undercurrent_sdk::ProductClientError::Server(e))
                    if e.effect_certainty
                        == Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent) =>
                {
                    self.reject_create(name, &pending.token)?;
                    notices.push(format!("{}: shell creation rejected", member.name));
                }
                _ => {
                    notices.push(format!(
                        "{}: creation outcome pending; reopen to reconcile",
                        member.name
                    ));
                    break;
                }
            }
        }
        Ok(notices)
    }

    pub(super) fn delete(&self, name: &str, stop: bool, force: bool) -> Result<(), CliError> {
        let saved = self.store.group(name)?;
        let _lock = self.store.group_lock(&saved.id)?;
        if !stop {
            if saved.deleting {
                return Err(error("deletion pending; resume uc group delete --stop"));
            }
            return self.store.update(|groups| {
                groups.remove(name);
                Ok(())
            });
        }
        self.store.update(|groups| {
            groups
                .get_mut(name)
                .ok_or_else(|| error("group not found"))?
                .deleting = true;
            Ok(())
        })?;
        self.reconcile(name)?;
        let saved = self.store.group(name)?;
        if saved.pending.is_some() {
            return Err(error(
                "creation outcome unresolved; deletion retained with restoration disabled",
            ));
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        let mut unresolved = std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::channel();
            for _ in 0..saved
                .members
                .len()
                .min(self.limits.bulk_parallel_requests())
            {
                let sender = sender.clone();
                let members = &saved.members;
                let next = &next;
                scope.spawn(move || {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(member) = members.get(index) else {
                            break;
                        };
                        let result = (|| -> Result<(), CliError> {
                            if self.summary(&member.id)?.as_ref().is_none_or(terminal) {
                                return Ok(());
                            }
                            let stopped = self.client.request(
                                ProductRequest::StopSession(StopSessionRequest {
                                    mutation: mutation(ProductOperation::StopSession),
                                    session_id: member.id.clone(),
                                    expected_revision: None,
                                    force,
                                    graceful_only: false,
                                }),
                                self.timeout,
                            );
                            if self.summary(&member.id)?.as_ref().is_none_or(terminal) {
                                return Ok(());
                            }
                            Err(error(format!(
                                "termination not confirmed: {}",
                                stopped
                                    .err()
                                    .map(|e| e.to_string())
                                    .unwrap_or_else(|| "process remains active".into())
                            )))
                        })();
                        if let Err(e) = result {
                            let _ = sender.send((
                                index,
                                format!(
                                    "{} ({}): {}",
                                    member.name.escape_default(),
                                    member.id.escape_default(),
                                    e
                                ),
                            ));
                        }
                    }
                });
            }
            drop(sender);
            receiver.into_iter().collect::<Vec<_>>()
        });
        unresolved.sort_by_key(|(index, _)| *index);
        let unresolved = unresolved
            .into_iter()
            .map(|(_, error)| error)
            .collect::<Vec<_>>();
        if !unresolved.is_empty() {
            return Err(error(format!(
                "deletion pending; restoration disabled; unresolved sessions: {}",
                unresolved.join(", ")
            )));
        }
        self.store.update(|groups| {
            groups.remove(name);
            Ok(())
        })
    }
}

fn terminal(summary: &SessionSummaryDto) -> bool {
    !matches!(summary.restart_status, RestartStatusDto::Scheduled { .. })
        && matches!(
            summary.lifecycle,
            LifecycleStateDto::Stopped
                | LifecycleStateDto::Exited { .. }
                | LifecycleStateDto::Failed
        )
}

fn cleaned_failure_can_restore(
    lifecycle: LifecycleStateDto,
    restart: Option<RestartStatusDto>,
) -> bool {
    lifecycle == LifecycleStateDto::Failed
        && restart.is_some_and(|status| {
            !matches!(
                status,
                RestartStatusDto::Scheduled { .. }
                    | RestartStatusDto::NotScheduled {
                        reason: undercurrent_protocol::NoRestartReasonDto::ExplicitStop
                    }
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use undercurrent_protocol::NoRestartReasonDto;

    #[test]
    fn cleanup_preserves_failed_slots_but_not_explicit_closure_or_unknown_intent() {
        let disabled = Some(RestartStatusDto::NotScheduled {
            reason: NoRestartReasonDto::PolicyDisabled,
        });
        assert!(cleaned_failure_can_restore(
            LifecycleStateDto::Failed,
            disabled
        ));
        assert!(cleaned_failure_can_restore(
            LifecycleStateDto::Failed,
            Some(RestartStatusDto::Idle)
        ));
        assert!(!cleaned_failure_can_restore(
            LifecycleStateDto::Failed,
            None
        ));
        assert!(!cleaned_failure_can_restore(
            LifecycleStateDto::Failed,
            Some(RestartStatusDto::NotScheduled {
                reason: NoRestartReasonDto::ExplicitStop
            })
        ));
        assert!(!cleaned_failure_can_restore(
            LifecycleStateDto::Failed,
            Some(RestartStatusDto::Scheduled {
                attempt: 1,
                not_before_millis: 42
            })
        ));
        for lifecycle in [
            LifecycleStateDto::Stopped,
            LifecycleStateDto::Exited { code: Some(0) },
            LifecycleStateDto::Exited { code: Some(1) },
            LifecycleStateDto::Running,
            LifecycleStateDto::Stopping,
        ] {
            assert!(!cleaned_failure_can_restore(lifecycle, disabled));
        }
    }
}
