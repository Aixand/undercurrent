// SPDX-License-Identifier: Apache-2.0

//! Payload-free commit notifications for durable agent-event subscriptions.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use undercurrent_application::SubscriptionWake;
use undercurrent_domain::OperationAttemptId;

type AttemptSubscriptions = BTreeMap<u64, Weak<dyn SubscriptionWake>>;
type SubscriptionRegistry = BTreeMap<OperationAttemptId, AttemptSubscriptions>;
type ReplyKey = (OperationAttemptId, String);
type ReplySubscriptions = BTreeMap<u64, Weak<AgentReplyNotificationState>>;
type ReplySubscriptionRegistry = BTreeMap<ReplyKey, ReplySubscriptions>;

/// Process-local wake registry layered over the durable agent-event queue.
///
/// The registry carries no event payload or semantic state. A subscriber always
/// rereads the canonical repository after waking, so notification loss or
/// coalescing cannot fabricate an event.
#[derive(Default)]
pub(crate) struct AgentEventNotificationHub {
    next_id: AtomicU64,
    subscriptions: Mutex<SubscriptionRegistry>,
    reply_subscriptions: Mutex<ReplySubscriptionRegistry>,
}

impl AgentEventNotificationHub {
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        attempt_id: OperationAttemptId,
        wake: Arc<dyn SubscriptionWake>,
    ) -> AgentEventNotificationSubscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subscriptions
            .entry(attempt_id.clone())
            .or_default()
            .insert(id, Arc::downgrade(&wake));
        AgentEventNotificationSubscription {
            hub: Arc::downgrade(self),
            attempt_id,
            id,
            _wake: wake,
        }
    }

    pub(crate) fn notify(&self, attempt_id: &OperationAttemptId) {
        let wakes = {
            let mut subscriptions = self
                .subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(attempt_subscriptions) = subscriptions.get_mut(attempt_id) else {
                return;
            };
            let mut wakes = Vec::with_capacity(attempt_subscriptions.len());
            attempt_subscriptions.retain(|_, candidate| {
                candidate.upgrade().is_some_and(|wake| {
                    wakes.push(wake);
                    true
                })
            });
            if attempt_subscriptions.is_empty() {
                subscriptions.remove(attempt_id);
            }
            wakes
        };
        // SubscriptionWake is adapter code. Release the registry lock before
        // calling it so callback-driven subscription cleanup can take that lock.
        for wake in wakes {
            wake.wake();
        }
    }

    pub(crate) fn subscribe_reply(
        self: &Arc<Self>,
        attempt_id: OperationAttemptId,
        event_id: String,
    ) -> AgentReplyNotificationSubscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let state = Arc::new(AgentReplyNotificationState::default());
        let key = (attempt_id, event_id);
        let mut subscriptions = self
            .reply_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subscriptions
            .entry(key.clone())
            .or_default()
            .insert(id, Arc::downgrade(&state));
        AgentReplyNotificationSubscription {
            hub: Arc::downgrade(self),
            key,
            id,
            state,
        }
    }

    pub(crate) fn notify_reply(&self, attempt_id: &OperationAttemptId, event_id: &str) {
        let key = (attempt_id.clone(), event_id.to_owned());
        let notifications = {
            let mut subscriptions = self
                .reply_subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(reply_subscriptions) = subscriptions.get_mut(&key) else {
                return;
            };
            let mut notifications = Vec::with_capacity(reply_subscriptions.len());
            reply_subscriptions.retain(|_, candidate| {
                candidate.upgrade().is_some_and(|notification| {
                    notifications.push(notification);
                    true
                })
            });
            if reply_subscriptions.is_empty() {
                subscriptions.remove(&key);
            }
            notifications
        };
        for notification in notifications {
            notification.notify();
        }
    }

    fn unsubscribe(&self, attempt_id: &OperationAttemptId, id: u64) {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(attempt_subscriptions) = subscriptions.get_mut(attempt_id) else {
            return;
        };
        attempt_subscriptions.remove(&id);
        if attempt_subscriptions.is_empty() {
            subscriptions.remove(attempt_id);
        }
    }

    fn unsubscribe_reply(&self, key: &ReplyKey, id: u64) {
        let mut subscriptions = self
            .reply_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(reply_subscriptions) = subscriptions.get_mut(key) else {
            return;
        };
        reply_subscriptions.remove(&id);
        if reply_subscriptions.is_empty() {
            subscriptions.remove(key);
        }
    }
}

/// Registration lifetime for one stream-pump wake.
pub(crate) struct AgentEventNotificationSubscription {
    hub: Weak<AgentEventNotificationHub>,
    attempt_id: OperationAttemptId,
    id: u64,
    _wake: Arc<dyn SubscriptionWake>,
}

impl Drop for AgentEventNotificationSubscription {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.unsubscribe(&self.attempt_id, self.id);
        }
    }
}

#[derive(Default)]
struct AgentReplyNotificationState {
    generation: Mutex<u64>,
    changed: Condvar,
}

impl AgentReplyNotificationState {
    fn generation(&self) -> u64 {
        *self
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait_for_change(&self, observed: u64, timeout: Duration) -> u64 {
        let generation = self
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (generation, _) = self
            .changed
            .wait_timeout_while(generation, timeout, |current| *current == observed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation
    }

    fn notify(&self) {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        self.changed.notify_all();
    }
}

/// Payload-free wake registration for one exact retained agent-event reply.
pub(crate) struct AgentReplyNotificationSubscription {
    hub: Weak<AgentEventNotificationHub>,
    key: ReplyKey,
    id: u64,
    state: Arc<AgentReplyNotificationState>,
}

impl AgentReplyNotificationSubscription {
    pub(crate) fn generation(&self) -> u64 {
        self.state.generation()
    }

    pub(crate) fn wait_for_change(&self, observed: u64, timeout: Duration) -> u64 {
        self.state.wait_for_change(observed, timeout)
    }
}

impl Drop for AgentReplyNotificationSubscription {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.unsubscribe_reply(&self.key, self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingWake(AtomicUsize);

    impl SubscriptionWake for CountingWake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }

    #[test]
    fn durable_attempt_notifications_wake_only_live_exact_subscribers() {
        let hub = Arc::new(AgentEventNotificationHub::default());
        let first_attempt = OperationAttemptId::new("attempt:first").unwrap();
        let second_attempt = OperationAttemptId::new("attempt:second").unwrap();
        let first = Arc::new(CountingWake(AtomicUsize::new(0)));
        let second = Arc::new(CountingWake(AtomicUsize::new(0)));
        let first_wake: Arc<dyn SubscriptionWake> = first.clone();
        let second_wake: Arc<dyn SubscriptionWake> = second.clone();
        let first_subscription = hub.subscribe(first_attempt.clone(), first_wake);
        let _second_subscription = hub.subscribe(second_attempt.clone(), second_wake);

        hub.notify(&first_attempt);
        assert_eq!(first.0.load(Ordering::Acquire), 1);
        assert_eq!(second.0.load(Ordering::Acquire), 0);

        drop(first_subscription);
        hub.notify(&first_attempt);
        hub.notify(&second_attempt);
        assert_eq!(first.0.load(Ordering::Acquire), 1);
        assert_eq!(second.0.load(Ordering::Acquire), 1);
    }

    #[test]
    fn subscription_handle_retains_its_payload_free_wake() {
        struct SharedCountingWake(Arc<AtomicUsize>);

        impl SubscriptionWake for SharedCountingWake {
            fn wake(&self) {
                self.0.fetch_add(1, Ordering::Release);
            }
        }

        let hub = Arc::new(AgentEventNotificationHub::default());
        let attempt = OperationAttemptId::new("attempt:retained-wake").unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let wake = Arc::new(SharedCountingWake(observed.clone()));
        let wake_trait: Arc<dyn SubscriptionWake> = wake.clone();
        let subscription = hub.subscribe(attempt.clone(), wake_trait);
        drop(wake);

        hub.notify(&attempt);
        assert_eq!(observed.load(Ordering::Acquire), 1);

        drop(subscription);
        hub.notify(&attempt);
        assert_eq!(observed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn durable_reply_notifications_wake_only_the_exact_event_waiter() {
        let hub = Arc::new(AgentEventNotificationHub::default());
        let attempt = OperationAttemptId::new("attempt:first").unwrap();
        let first = hub.subscribe_reply(attempt.clone(), "event:first".into());
        let second = hub.subscribe_reply(attempt.clone(), "event:second".into());
        let first_generation = first.generation();
        let second_generation = second.generation();

        hub.notify_reply(&attempt, "event:first");

        assert_ne!(
            first.wait_for_change(first_generation, Duration::ZERO),
            first_generation
        );
        assert_eq!(
            second.wait_for_change(second_generation, Duration::ZERO),
            second_generation
        );
    }
}
