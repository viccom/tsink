use std::collections::BTreeSet;

use super::super::SeriesRegistry;
use crate::{DataPoint, Label, Row, TsinkError};

#[test]
fn resolves_same_series_to_same_id_regardless_of_label_order() {
    let registry = SeriesRegistry::new();

    let r1 = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "a"), Label::new("region", "use1")],
        )
        .unwrap();
    let r2 = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("region", "use1"), Label::new("host", "a")],
        )
        .unwrap();

    assert_eq!(r1.series_id, r2.series_id);
    assert!(r1.created);
    assert!(!r2.created);
    assert_eq!(registry.series_count(), 1);
}

#[test]
fn interns_metric_and_label_dictionaries_once() {
    let registry = SeriesRegistry::new();

    for _ in 0..100 {
        registry
            .resolve_or_insert(
                "http_requests_total",
                &[
                    Label::new("method", "GET"),
                    Label::new("status", "200"),
                    Label::new("route", "/users"),
                ],
            )
            .unwrap();
    }

    assert_eq!(registry.series_count(), 1);
    assert_eq!(registry.metric_dictionary_len(), 1);
    assert_eq!(registry.label_name_dictionary_len(), 3);
    assert_eq!(registry.label_value_dictionary_len(), 3);
}

#[test]
fn resolve_rows_returns_series_ids_for_batch_inserts() {
    let registry = SeriesRegistry::new();
    let rows = vec![
        Row::with_labels(
            "latency",
            vec![Label::new("host", "a"), Label::new("path", "/")],
            DataPoint::new(1, 1.0),
        ),
        Row::with_labels(
            "latency",
            vec![Label::new("path", "/"), Label::new("host", "a")],
            DataPoint::new(2, 2.0),
        ),
        Row::with_labels(
            "latency",
            vec![Label::new("host", "b"), Label::new("path", "/")],
            DataPoint::new(3, 3.0),
        ),
    ];

    let resolutions = registry.resolve_rows(&rows).unwrap();

    assert_eq!(resolutions.len(), 3);
    assert_eq!(resolutions[0].series_id, resolutions[1].series_id);
    assert_ne!(resolutions[0].series_id, resolutions[2].series_id);
    assert_eq!(registry.series_count(), 2);
}

#[test]
fn identity_cache_serves_repeat_resolves_without_dictionary_walks() {
    let registry = SeriesRegistry::new();
    let labels = [Label::new("host", "a"), Label::new("region", "use1")];
    let created = registry.resolve_or_insert("cpu", &labels).unwrap();
    assert!(created.created);

    // First resolve populates the identity cache via one dictionary walk.
    let first = registry.resolve_existing("cpu", &labels).unwrap();
    assert_eq!(first.series_id, created.series_id);
    let walks_after_cold = registry.resolve_existing_dictionary_walks();

    // Repeat resolves — sorted and unsorted label order alike — must be
    // served by the identity cache: no further dictionary walks, identical
    // resolutions.
    for _ in 0..3 {
        let repeat = registry.resolve_existing("cpu", &labels).unwrap();
        assert_eq!(repeat.series_id, created.series_id);
        assert_eq!(repeat.metric_id, created.metric_id);
        assert_eq!(repeat.label_pairs, created.label_pairs);
        assert!(!repeat.created);
    }
    let flipped = [Label::new("region", "use1"), Label::new("host", "a")];
    let repeat = registry.resolve_existing("cpu", &flipped).unwrap();
    assert_eq!(repeat.series_id, created.series_id);

    assert_eq!(
        registry.resolve_existing_dictionary_walks(),
        walks_after_cold,
        "repeat resolves must not walk the dictionaries again"
    );
}

#[test]
fn identity_cache_never_serves_series_pruned_from_registry() {
    let registry = SeriesRegistry::new();
    let a = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();
    let b = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();

    // Warm the identity cache for both series.
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "a")])
        .is_some());
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "b")])
        .is_some());

    registry.retain_series_ids(&BTreeSet::from([a.series_id]));

    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "a")])
        .is_some());
    assert_eq!(
        registry
            .resolve_existing("cpu", &[Label::new("host", "b")])
            .map(|resolution| resolution.series_id),
        None,
        "series pruned from the registry must not be served from the identity cache"
    );
    assert_eq!(registry.series_count(), 1);
    assert_ne!(a.series_id, b.series_id);
}

#[test]
fn identity_cache_does_not_surface_rolled_back_created_series() {
    let registry = SeriesRegistry::new();
    let labels = [Label::new("host", "rolled-back")];
    let created = registry.resolve_or_insert("cpu", &labels).unwrap();
    assert!(created.created);

    // A neighboring commit warms the cache for an unrelated series; the
    // rolled-back series must never be resolvable afterwards.
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "kept")])
        .unwrap();
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "kept")])
        .is_some());

    registry.rollback_created_series(std::slice::from_ref(&created));

    assert!(
        registry.resolve_existing("cpu", &labels).is_none(),
        "series created by a rolled-back batch must not surface via the identity cache"
    );
}

#[test]
fn resolve_existing_and_series_ids_for_metric_work() {
    let registry = SeriesRegistry::new();
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    let a = registry.resolve_or_insert("cpu", &labels_a).unwrap();
    let b = registry.resolve_or_insert("cpu", &labels_b).unwrap();

    let resolved = registry.resolve_existing("cpu", &labels_a).unwrap();
    assert_eq!(resolved.series_id, a.series_id);

    let ids = registry.series_ids_for_metric("cpu");
    assert_eq!(ids, vec![a.series_id, b.series_id]);
}

#[test]
fn register_series_with_id_restores_series_identity() {
    let registry = SeriesRegistry::new();
    let labels = vec![Label::new("region", "use1"), Label::new("host", "a")];

    let registered = registry
        .register_series_with_id(42, "cpu", &labels)
        .unwrap();
    assert_eq!(registered.series_id, 42);
    assert!(registered.created);

    let resolved = registry
        .resolve_existing(
            "cpu",
            &[Label::new("host", "a"), Label::new("region", "use1")],
        )
        .unwrap();
    assert_eq!(resolved.series_id, 42);

    let fresh = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    assert!(fresh.series_id > 42);
}

#[test]
fn decode_series_key_recovers_metric_and_labels() {
    let registry = SeriesRegistry::new();
    let resolution = registry
        .resolve_or_insert(
            "memory_usage",
            &[Label::new("host", "a"), Label::new("service", "api")],
        )
        .unwrap();

    let decoded = registry.decode_series_key(resolution.series_id).unwrap();
    assert_eq!(decoded.metric, "memory_usage");
    assert_eq!(
        decoded.labels,
        vec![Label::new("host", "a"), Label::new("service", "api")]
    );
}

#[test]
fn retain_series_ids_preserves_series_id_high_watermark() {
    let registry = SeriesRegistry::new();
    registry
        .register_series_with_id(42, "historical", &[Label::new("host", "a")])
        .unwrap();

    registry.retain_series_ids(&BTreeSet::new());

    let next = registry
        .resolve_or_insert("replacement", &[Label::new("host", "b")])
        .unwrap();
    assert!(next.series_id > 42);
}

#[test]
fn rejects_duplicate_label_names() {
    let registry = SeriesRegistry::new();

    let err = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a"), Label::new("host", "b")])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::InvalidLabel(message) if message.contains("duplicate label 'host'")
    ));

    let err = registry
        .register_series_with_id(
            42,
            "cpu",
            &[Label::new("region", "use1"), Label::new("region", "use2")],
        )
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::InvalidLabel(message) if message.contains("duplicate label 'region'")
    ));

    assert_eq!(registry.series_count(), 0);
}

#[test]
fn rejects_invalid_input() {
    let registry = SeriesRegistry::new();

    let metric_err = registry.resolve_or_insert("", &[]).unwrap_err();
    assert!(matches!(metric_err, TsinkError::MetricRequired));

    let label_err = registry
        .resolve_or_insert("m", &[Label::new("", "x")])
        .unwrap_err();
    assert!(matches!(label_err, TsinkError::InvalidLabel(_)));

    let oversized = Label::new("k", "x".repeat(crate::label::MAX_LABEL_VALUE_LEN + 1));
    let oversized_err = registry.resolve_or_insert("m", &[oversized]).unwrap_err();
    assert!(matches!(oversized_err, TsinkError::InvalidLabel(_)));
}
