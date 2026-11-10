// SPDX-License-Identifier: Apache-2.0

use undercurrent_domain::{
    DeliveryId, InputDeliveryPolicy, InputItem, InputSequence, InputTextMode, Key, ModifiableKey,
    MouseButton, MouseEventKind, PayloadDigest, ProcessInstanceId, SessionId,
};

pub(super) fn digest(
    session: &SessionId,
    process: &ProcessInstanceId,
    delivery: Option<&DeliveryId>,
    input: &InputSequence,
    policy: InputDeliveryPolicy,
) -> PayloadDigest {
    // Explicit tags and lengths preserve item boundaries and types. Debug input
    // is redacted, and equal encoded byte counts do not imply equal effects.
    // Only the digest leaves this function; private input is not logged/stored.
    let mut bytes = b"undercurrent:write-input:v0.1.0-beta".to_vec();
    field(&mut bytes, session.as_str().as_bytes());
    field(&mut bytes, process.as_str().as_bytes());
    bytes.push(u8::from(delivery.is_some()));
    if let Some(delivery) = delivery {
        field(&mut bytes, delivery.as_str().as_bytes());
    }
    bytes.push(match policy.text_mode() {
        InputTextMode::Raw => 0,
        InputTextMode::Paste => 1,
    });
    bytes.push(u8::from(policy.trailing_submit_delay().is_some()));
    if let Some(delay) = policy.trailing_submit_delay() {
        bytes.extend_from_slice(&delay.as_secs().to_be_bytes());
        bytes.extend_from_slice(&delay.subsec_nanos().to_be_bytes());
    }
    append_input(&mut bytes, input);
    let digest = PayloadDigest::of_bytes(&bytes);
    bytes.fill(0);
    digest
}

pub(super) fn semantic_digest(
    input: &InputSequence,
    policy: undercurrent_domain::InputObservationPolicy,
) -> PayloadDigest {
    let mut bytes = b"undercurrent:semantic-input-delivery:v0.1.0-beta".to_vec();
    bytes.push(match policy {
        undercurrent_domain::InputObservationPolicy::ForceActive => 0,
        undercurrent_domain::InputObservationPolicy::Preserve => 1,
    });
    append_input(&mut bytes, input);
    let digest = PayloadDigest::of_bytes(&bytes);
    bytes.fill(0);
    digest
}

fn append_input(bytes: &mut Vec<u8>, input: &InputSequence) {
    bytes.extend_from_slice(&(input.items().len() as u64).to_be_bytes());
    for item in input.items() {
        match item {
            InputItem::AutoText(value) => {
                bytes.push(0);
                field(bytes, value.as_bytes());
            }
            InputItem::Text(value) => {
                bytes.push(1);
                field(bytes, value.as_bytes());
            }
            InputItem::Bytes(value) => {
                bytes.push(2);
                field(bytes, value);
            }
            InputItem::Paste(value) => {
                bytes.push(3);
                field(bytes, value.as_bytes());
            }
            InputItem::Key(value) => {
                bytes.push(4);
                key(bytes, value);
            }
            InputItem::Mouse(value) => {
                bytes.push(5);
                bytes.extend_from_slice(&value.column().to_be_bytes());
                bytes.extend_from_slice(&value.row().to_be_bytes());
                bytes.push(match value.button() {
                    MouseButton::Left => 0,
                    MouseButton::Middle => 1,
                    MouseButton::Right => 2,
                    MouseButton::ScrollUp => 3,
                    MouseButton::ScrollDown => 4,
                });
                bytes.push(match value.kind() {
                    MouseEventKind::Press => 0,
                    MouseEventKind::Release => 1,
                    MouseEventKind::Move => 2,
                });
            }
        }
    }
}

fn field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn key(bytes: &mut Vec<u8>, value: &Key) {
    let tag = match value {
        Key::Enter => 0,
        Key::Tab => 1,
        Key::BackTab => 2,
        Key::Escape => 3,
        Key::Backspace => 4,
        Key::Delete => 5,
        Key::Space => 6,
        Key::Up => 7,
        Key::Down => 8,
        Key::Left => 9,
        Key::Right => 10,
        Key::Home => 11,
        Key::End => 12,
        Key::PageUp => 13,
        Key::PageDown => 14,
        Key::Function(number) => {
            bytes.extend_from_slice(&[15, number.number()]);
            return;
        }
        Key::Control(character) => {
            bytes.extend_from_slice(&[16, character.character() as u8]);
            return;
        }
        Key::Modified { base, modifiers } => {
            bytes.push(17);
            match base {
                ModifiableKey::Up => bytes.push(0),
                ModifiableKey::Down => bytes.push(1),
                ModifiableKey::Left => bytes.push(2),
                ModifiableKey::Right => bytes.push(3),
                ModifiableKey::Home => bytes.push(4),
                ModifiableKey::End => bytes.push(5),
                ModifiableKey::PageUp => bytes.push(6),
                ModifiableKey::PageDown => bytes.push(7),
                ModifiableKey::Function(number) => bytes.extend_from_slice(&[8, number.number()]),
            }
            bytes.extend_from_slice(&[
                u8::from(modifiers.shift),
                u8::from(modifiers.control),
                u8::from(modifiers.alt),
            ]);
            return;
        }
    };
    bytes.push(tag);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn content_types_boundaries_and_effective_policy_change_digest() {
        let session = SessionId::new("session").unwrap();
        let process = ProcessInstanceId::new("process").unwrap();
        let immediate = InputDeliveryPolicy::immediate();
        let inputs = [
            vec![InputItem::Text("ab".into())],
            vec![InputItem::Text("ac".into())],
            vec![InputItem::Text("a".into()), InputItem::Text("b".into())],
            vec![InputItem::Bytes(b"ab".to_vec())],
            vec![InputItem::AutoText("ab".into())],
            vec![InputItem::Paste("ab".into())],
            vec![InputItem::Key(Key::Enter)],
            vec![InputItem::Key(Key::Tab)],
        ];
        let mut digests = std::collections::BTreeSet::new();
        for items in inputs {
            let input = InputSequence::new(items, 8, 1024).unwrap();
            assert!(digests.insert(digest(&session, &process, None, &input, immediate)));
        }
        let input = InputSequence::new(vec![InputItem::AutoText("ab".into())], 8, 1024).unwrap();
        for policy in [
            immediate.with_text_mode(InputTextMode::Paste),
            InputDeliveryPolicy::paced_trailing_submit(
                Duration::from_millis(10),
                Duration::from_secs(1),
            )
            .unwrap(),
        ] {
            assert!(digests.insert(digest(&session, &process, None, &input, policy)));
        }
    }
}
