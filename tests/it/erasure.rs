//! Article 17 erasure against real PostgreSQL.
//!
//! The lake keeps every reading; what erasure destroys is the ability to
//! attribute them to a person. These tests pin that the destruction is real,
//! irreversible, and provable — the three conditions regulators attach to
//! accepting anything short of physically deleting the rows.

use meterstore::{SubjectRef, SubjectRegistry};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use time::macros::datetime;

/// A 32-byte suppression key. Test-only: a real deployment loads one from its
/// secret manager, and losing it silently disables suppression.
const SECRET: &[u8] = b"test-suppression-key-32-bytes!!!";

async fn pool() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.expect("postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let pool = PgPool::connect(&format!(
        "postgresql://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .expect("connect");
    (pool, container)
}

async fn registry() -> (SubjectRegistry, testcontainers::ContainerAsync<Postgres>) {
    let (pool, container) = pool().await;
    let registry = SubjectRegistry::new(pool);
    registry.create_tables().await.expect("tables");
    (registry, container)
}

/// A registry that enforces a suppression list.
async fn suppressing() -> (SubjectRegistry, testcontainers::ContainerAsync<Postgres>) {
    let (pool, container) = pool().await;
    let registry = SubjectRegistry::with_erasure_secret(pool, SECRET).expect("secret");
    registry.create_tables().await.expect("tables");
    (registry, container)
}

#[tokio::test]
async fn a_reference_resolves_until_it_is_erased() {
    let (r, _c) = registry().await;

    let subject = r.register("customer-4821").await.unwrap();
    assert_eq!(
        r.resolve(&subject).await.unwrap().as_deref(),
        Some("customer-4821")
    );

    r.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    assert_eq!(
        r.resolve(&subject).await.unwrap(),
        None,
        "the link must be gone, not merely flagged"
    );
}

#[tokio::test]
async fn erasure_is_irreversible() {
    // No recovery path is the condition regulators attach: a soft delete that an
    // administrator could undo would not be erasure at all.
    let (r, _c) = registry().await;
    let subject = r.register("customer-4821").await.unwrap();

    r.erase(
        &subject,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    // The natural identifier is unreachable from either direction.
    assert_eq!(r.resolve(&subject).await.unwrap(), None);
    assert_eq!(r.lookup("customer-4821").await.unwrap(), None);
}

#[tokio::test]
async fn erasure_is_auditable_without_retaining_the_subject() {
    let (r, _c) = registry().await;
    let subject = r.register("customer-4821").await.unwrap();

    r.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    assert!(r.is_erased(&subject).await.unwrap());

    let trail = r.erasures(10).await.unwrap();
    assert_eq!(trail.len(), 1);
    assert_eq!(trail[0].reason, "DSAR-2026-0042");
    assert_eq!(trail[0].actor, "privacy-team");

    // The trail proves an erasure happened without recording whom it concerned —
    // otherwise it would preserve the very link being destroyed.
    let rendered = format!("{:?}", trail[0]);
    assert!(
        !rendered.contains("customer-4821"),
        "the audit trail must not retain the natural identifier"
    );
}

#[tokio::test]
async fn erasing_one_subject_leaves_every_other_intact() {
    let (r, _c) = registry().await;
    let a = r.register("customer-a").await.unwrap();
    let b = r.register("customer-b").await.unwrap();

    r.erase(
        &a,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    assert_eq!(r.resolve(&a).await.unwrap(), None);
    assert_eq!(
        r.resolve(&b).await.unwrap().as_deref(),
        Some("customer-b"),
        "erasure must be per subject, not per file or per table"
    );
}

#[tokio::test]
async fn registering_the_same_subject_twice_returns_one_reference() {
    // An ingest path may register per batch; that must not accumulate references
    // for one subject, or erasure would have to find them all.
    let (r, _c) = registry().await;

    let first = r.register("customer-4821").await.unwrap();
    let second = r.register("customer-4821").await.unwrap();
    assert_eq!(first, second);
}

#[tokio::test]
async fn a_reference_reveals_nothing_about_its_subject() {
    // A derived reference — a hash of a meter serial, say — would survive
    // erasure as a re-identification path, because anyone holding the serial
    // could recompute it.
    let (r, _c) = registry().await;
    let subject = r.register("customer-4821").await.unwrap();

    assert!(!subject.as_str().contains("4821"));
    assert!(!subject.as_str().contains("customer"));
}

#[tokio::test]
async fn erasing_an_unknown_reference_is_recorded_rather_than_ignored() {
    // A repeated request must stay auditable: answering "already done" silently
    // leaves nothing to show a regulator.
    let (r, _c) = registry().await;
    let unknown = SubjectRef::new("sub_deadbeef").unwrap();

    r.erase(
        &unknown,
        "DSAR-9",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    assert!(r.is_erased(&unknown).await.unwrap());
}

#[tokio::test]
async fn erasure_requires_a_reason() {
    let (r, _c) = registry().await;
    let subject = r.register("customer-4821").await.unwrap();

    assert!(
        r.erase(
            &subject,
            "  ",
            "privacy-team",
            datetime!(2026-07-28 09:00 UTC)
        )
        .await
        .is_err(),
        "an unexplained erasure cannot be defended later"
    );
}

#[tokio::test]
async fn without_a_suppression_key_a_replay_silently_resurrects_a_subject() {
    // Pinning the limitation rather than the feature. Erasure deletes the
    // mapping, so nothing is left to recognise the identifier by — a pipeline
    // replaying old messages registers it again and gets a working reference.
    // This is why `with_erasure_secret` exists, and why the plain constructor
    // documents that replay defeats it.
    let (r, _c) = registry().await;

    let first = r.register("customer-4821").await.unwrap();
    r.erase(
        &first,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let second = r.register("customer-4821").await.unwrap();
    assert_ne!(first, second, "a fresh reference, not the erased one");
    assert_eq!(
        r.resolve(&second).await.unwrap().as_deref(),
        Some("customer-4821"),
        "the link is rebuilt — the outcome a suppression key prevents"
    );
}

#[tokio::test]
async fn a_suppression_key_makes_erasure_stick() {
    let (r, _c) = suppressing().await;

    let subject = r.register("customer-4821").await.unwrap();
    r.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let replayed = r.register("customer-4821").await;
    assert!(
        replayed.is_err(),
        "a replayed message must not re-link an erased subject"
    );
    assert!(r.is_suppressed("customer-4821").await.unwrap());
    // Everyone else is unaffected — suppression is per identifier, not a mode.
    assert!(r.register("customer-9999").await.is_ok());
}

#[tokio::test]
async fn the_suppression_list_does_not_retain_the_identifier() {
    // The tombstone outlives the mapping, so anything reversible in it would
    // survive the erasure it documents.
    let (pool, _c) = pool().await;
    let r = SubjectRegistry::with_erasure_secret(pool.clone(), SECRET).expect("secret");
    r.create_tables().await.expect("tables");

    let subject = r.register("customer-4821").await.unwrap();
    r.erase(
        &subject,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let stored: Vec<u8> = sqlx::query_scalar("SELECT natural_id_hmac FROM meterstore_erasures")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(stored.len(), 32, "a SHA-256 tag, not the identifier");
    assert!(
        !String::from_utf8_lossy(&stored).contains("customer-4821"),
        "the identifier must not appear in the tombstone"
    );
}

#[tokio::test]
async fn a_mistaken_erasure_can_be_lifted_without_restoring_the_old_link() {
    // Without this, one erasure against the wrong subject locks a real customer
    // out of the system permanently.
    let (r, _c) = suppressing().await;

    let original = r.register("customer-4821").await.unwrap();
    r.erase(
        &original,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();
    assert!(r.register("customer-4821").await.is_err());

    assert!(
        r.lift_suppression(
            "customer-4821",
            "erased in error, ticket OPS-77",
            "privacy-team"
        )
        .await
        .unwrap()
    );

    let fresh = r.register("customer-4821").await.unwrap();
    assert_ne!(
        fresh, original,
        "a new reference — lifting must not re-attach the erased history"
    );
    assert!(
        r.resolve(&original).await.unwrap().is_none(),
        "the original reference stays dead"
    );
    // The audit trail survives, so the whole sequence remains reviewable.
    assert_eq!(r.erasures(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_short_suppression_key_is_refused() {
    let (pool, _c) = pool().await;
    assert!(
        SubjectRegistry::with_erasure_secret(pool, b"too-short").is_err(),
        "a brute-forceable key would leak the identifiers it exists to forget"
    );
}

#[tokio::test]
async fn lifting_requires_a_configured_key_and_a_reason() {
    let (r, _c) = suppressing().await;
    assert!(
        r.lift_suppression("customer-1", "  ", "actor")
            .await
            .is_err()
    );

    let (plain, _c2) = registry().await;
    assert!(
        plain
            .lift_suppression("customer-1", "reason", "actor")
            .await
            .is_err(),
        "there is nothing to lift without a suppression list"
    );
}

#[tokio::test]
async fn a_registry_debug_does_not_print_its_key() {
    let (r, _c) = suppressing().await;
    let rendered = format!("{r:?}");
    assert!(!rendered.contains("test-suppression-key"));
    assert!(rendered.contains("suppression"));
}

#[tokio::test]
async fn erasure_can_be_enclosed_in_a_caller_s_transaction() {
    // An Article 17 request reaches an application's own tables too, and those
    // must succeed or fail *with* the mapping. Two transactions cannot give that,
    // and the failure mode is the worst kind: a subject reported as erased whose
    // derived rows survived.
    let (pool, _container) = pool().await;
    let registry = SubjectRegistry::new(pool.clone());
    registry.create_tables().await.expect("tables");

    sqlx::query("CREATE TABLE downstream (subject_ref TEXT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("a stand-in for the caller's own tables");

    let subject = registry.register("DE-METER-1").await.expect("register");
    sqlx::query("INSERT INTO downstream (subject_ref) VALUES ($1)")
        .bind(subject.as_str())
        .execute(&pool)
        .await
        .expect("derived row");

    // Roll back: neither the mapping nor the derived row may be gone.
    let mut tx = pool.begin().await.expect("begin");
    registry
        .erase_in(
            &mut tx,
            &subject,
            "rehearsal",
            "dpo",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .expect("erase step");
    sqlx::query("DELETE FROM downstream WHERE subject_ref = $1")
        .bind(subject.as_str())
        .execute(&mut *tx)
        .await
        .expect("cascade");
    tx.rollback().await.expect("rollback");

    assert!(
        registry.resolve(&subject).await.expect("resolve").is_some(),
        "a rolled-back erasure must leave the mapping intact"
    );
    let survivors: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downstream WHERE subject_ref = $1")
            .bind(subject.as_str())
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(
        survivors, 1,
        "and the caller's cascade must roll back with it"
    );

    // Commit: both go together.
    let mut tx = pool.begin().await.expect("begin");
    registry
        .erase_in(
            &mut tx,
            &subject,
            "Art. 17 request",
            "dpo",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .expect("erase step");
    sqlx::query("DELETE FROM downstream WHERE subject_ref = $1")
        .bind(subject.as_str())
        .execute(&mut *tx)
        .await
        .expect("cascade");
    tx.commit().await.expect("commit");

    assert!(
        registry.resolve(&subject).await.expect("resolve").is_none(),
        "the mapping is gone"
    );
    let survivors: i64 =
        sqlx::query_scalar("SELECT count(*) FROM downstream WHERE subject_ref = $1")
            .bind(subject.as_str())
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(survivors, 0, "and so are the rows that referenced it");
}
