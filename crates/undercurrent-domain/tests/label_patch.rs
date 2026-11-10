// SPDX-License-Identifier: Apache-2.0

use undercurrent_domain::{LabelPatch, Labels, MAX_LABELS};

#[test]
fn label_patch_preserves_unmentioned_keys_and_removal_is_idempotent() {
    let original = Labels::new([
        ("app".into(), "example-app".into()),
        ("workflow.run_id".into(), "run-1".into()),
    ])
    .unwrap();
    let patch = LabelPatch::new(
        Labels::new([("aixand.my_terminal".into(), "true".into())]).unwrap(),
        [],
    )
    .unwrap();
    let marked = patch.apply_to(&original).unwrap();
    assert_eq!(marked.get("app"), Some("example-app"));
    assert_eq!(marked.get("workflow.run_id"), Some("run-1"));
    assert_eq!(marked.get("aixand.my_terminal"), Some("true"));
    assert_eq!(patch.apply_to(&marked).unwrap(), marked);
    let remove = LabelPatch::new(Labels::default(), ["aixand.my_terminal".into()]).unwrap();
    assert_eq!(remove.apply_to(&marked).unwrap(), original);
    assert_eq!(remove.apply_to(&original).unwrap(), original);
}

#[test]
fn label_patch_validates_removal_keys_and_rejects_conflicting_intent() {
    let set = Labels::new([("app".into(), "example-app".into())]).unwrap();
    assert!(LabelPatch::new(set, ["app".into()]).is_err());
    for key in ["".to_owned(), "x".repeat(65), "bad\nkey".to_owned()] {
        assert!(LabelPatch::new(Labels::default(), [key]).is_err());
    }
    assert!(
        LabelPatch::new(
            Labels::default(),
            (0..=MAX_LABELS).map(|n| format!("key-{n}"))
        )
        .is_err()
    );
}

#[test]
fn full_label_maps_can_exchange_keys_but_cannot_overflow() {
    let original =
        Labels::new((0..MAX_LABELS).map(|n| (format!("key-{n}"), "value".into()))).unwrap();
    let set = Labels::new([("new".into(), "value".into())]).unwrap();
    assert!(
        LabelPatch::new(set.clone(), [])
            .unwrap()
            .apply_to(&original)
            .is_err()
    );
    let changed = LabelPatch::new(set, ["key-0".into()])
        .unwrap()
        .apply_to(&original)
        .unwrap();
    assert_eq!(changed.len(), MAX_LABELS);
    assert_eq!(changed.get("key-0"), None);
    assert_eq!(changed.get("new"), Some("value"));
    assert_eq!(original.get("key-0"), Some("value"));
}
