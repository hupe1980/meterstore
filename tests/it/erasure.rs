//! Article 17 erasure against real PostgreSQL.
//!
//! The lake keeps every reading; what erasure destroys is the ability to
//! attribute them to a person. These tests pin that the destruction is real,
//! irreversible, and provable — the three conditions regulators attach to
//! accepting anything short of physically deleting the rows.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test.
#![cfg(feature = "testkit")]

use metering::interval::Sparte;
use meterstore::{ErasureQuery, ErasureRecord, ErasureTrigger, SubjectRef, SubjectRegistry};
use sqlx::PgPool;
use time::OffsetDateTime;
use time::macros::datetime;

/// An instant in the retention epoch these tests register against.
///
/// A reference belongs to one collection year, so every registration names the
/// year it is for — here, 2026.
const IN_2026: OffsetDateTime = datetime!(2026-06-01 00:00 UTC);

/// A 32-byte suppression key. Test-only: a real deployment loads one from its
/// secret manager, and losing it silently disables suppression.
const SECRET: &[u8] = b"test-suppression-key-32-bytes!!!";

async fn pool() -> PgPool {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    PgPool::connect(&url).await.expect("connect")
}

async fn registry() -> SubjectRegistry {
    let pool = pool().await;
    let registry = SubjectRegistry::new(pool);
    registry.create_tables().await.expect("tables");
    registry
}

/// A registry that enforces a suppression list.
async fn suppressing() -> SubjectRegistry {
    let pool = pool().await;
    let registry = SubjectRegistry::with_erasure_secret(pool, SECRET).expect("secret");
    registry.create_tables().await.expect("tables");
    registry
}

#[tokio::test]
async fn a_reference_resolves_until_it_is_erased() {
    let r = registry().await;

    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
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
    let r = registry().await;
    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();

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
    assert_eq!(
        r.lookup("customer-4821", IN_2026, Sparte::Strom)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn erasure_is_auditable_without_retaining_the_subject() {
    let r = registry().await;
    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();

    r.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    assert!(r.is_erased(&subject).await.unwrap());

    let trail = r.erasures(&ErasureQuery::new().limit(10)).await.unwrap();
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
    let r = registry().await;
    let a = r
        .register("customer-a", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    let b = r
        .register("customer-b", IN_2026, Sparte::Strom)
        .await
        .unwrap();

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
    let r = registry().await;

    let first = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    let second = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    assert_eq!(first, second);
}

#[tokio::test]
async fn a_reference_reveals_nothing_about_its_subject() {
    // A derived reference — a hash of a meter serial, say — would survive
    // erasure as a re-identification path, because anyone holding the serial
    // could recompute it.
    let r = registry().await;
    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();

    assert!(!subject.as_str().contains("4821"));
    assert!(!subject.as_str().contains("customer"));
}

#[tokio::test]
async fn erasing_an_unknown_reference_is_recorded_rather_than_ignored() {
    // A repeated request must stay auditable: answering "already done" silently
    // leaves nothing to show a regulator.
    let r = registry().await;
    let unknown = SubjectRef::new("s2026_deadbeefdeadbeefdeadbeefdeadbeef").unwrap();

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
    let r = registry().await;
    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();

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
    let r = registry().await;

    let first = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.erase(
        &first,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let second = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    assert_ne!(first, second, "a fresh reference, not the erased one");
    assert_eq!(
        r.resolve(&second).await.unwrap().as_deref(),
        Some("customer-4821"),
        "the link is rebuilt — the outcome a suppression key prevents"
    );
}

#[tokio::test]
async fn a_suppression_key_makes_erasure_stick() {
    let r = suppressing().await;

    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let replayed = r.register("customer-4821", IN_2026, Sparte::Strom).await;
    assert!(
        replayed.is_err(),
        "a replayed message must not re-link an erased subject"
    );
    assert!(r.is_suppressed("customer-4821").await.unwrap());
    // Everyone else is unaffected — suppression is per identifier, not a mode.
    assert!(
        r.register("customer-9999", IN_2026, Sparte::Strom)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn the_suppression_list_does_not_retain_the_identifier() {
    // The tombstone outlives the mapping, so anything reversible in it would
    // survive the erasure it documents.
    let pool = pool().await;
    let r = SubjectRegistry::with_erasure_secret(pool.clone(), SECRET).expect("secret");
    r.create_tables().await.expect("tables");

    let subject = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
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
    let r = suppressing().await;

    let original = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.erase(
        &original,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();
    assert!(
        r.register("customer-4821", IN_2026, Sparte::Strom)
            .await
            .is_err()
    );

    assert!(
        r.lift_suppression(
            "customer-4821",
            "erased in error, ticket OPS-77",
            "privacy-team",
            datetime!(2026-07-02 09:00 UTC),
        )
        .await
        .unwrap()
    );

    let fresh = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    assert_ne!(
        fresh, original,
        "a new reference — lifting must not re-attach the erased history"
    );
    assert!(
        r.resolve(&original).await.unwrap().is_none(),
        "the original reference stays dead"
    );
    // The audit trail survives, so the whole sequence remains reviewable.
    assert_eq!(
        r.erasures(&ErasureQuery::new().limit(10))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_short_suppression_key_is_refused() {
    let pool = pool().await;
    assert!(
        SubjectRegistry::with_erasure_secret(pool, b"too-short").is_err(),
        "a brute-forceable key would leak the identifiers it exists to forget"
    );
}

#[tokio::test]
async fn lifting_requires_a_configured_key_and_a_reason() {
    let r = suppressing().await;
    assert!(
        r.lift_suppression("customer-1", "  ", "actor", IN_2026)
            .await
            .is_err()
    );

    let plain = registry().await;
    assert!(
        plain
            .lift_suppression("customer-1", "reason", "actor", IN_2026)
            .await
            .is_err(),
        "there is nothing to lift without a suppression list"
    );
}

#[tokio::test]
async fn a_registry_debug_does_not_print_its_key() {
    let r = suppressing().await;
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
    let pool = pool().await;
    let registry = SubjectRegistry::new(pool.clone());
    registry.create_tables().await.expect("tables");

    sqlx::query("CREATE TABLE downstream (subject_ref TEXT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("a stand-in for the caller's own tables");

    let subject = registry
        .register("DE-METER-1", IN_2026, Sparte::Strom)
        .await
        .expect("register");
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

#[tokio::test]
async fn a_gas_reference_is_minted_for_the_year_its_readings_are_balanced_in() {
    // The bug this parameter closes. `2026-01-01T00:00Z` is 01:00 on New Year's
    // Day in Berlin — 2026 on the wall clock, and still Gastag 2025-12-31, which
    // is the day the reading is settled and invoiced on.
    //
    // Mint from the local calendar year while the write path checks against the
    // balancing year, and for those six hours the only reference the public API
    // can produce for a gas reading is one the write refuses. Both sides read
    // `retention_epoch`, so they cannot drift apart.
    let r = registry().await;
    let at = datetime!(2026-01-01 0:00 UTC);

    let gas = r.register("customer-4821", at, Sparte::Gas).await.unwrap();
    let power = r
        .register("customer-4821", at, Sparte::Strom)
        .await
        .unwrap();

    assert_eq!(gas.epoch().unwrap(), 2025);
    assert_eq!(power.epoch().unwrap(), 2026);
    assert_ne!(gas, power, "two epochs, so two references");

    // And each is what its own commodity's lookup finds.
    assert_eq!(
        r.lookup("customer-4821", at, Sparte::Gas).await.unwrap(),
        Some(gas)
    );
    assert_eq!(
        r.lookup("customer-4821", at, Sparte::Strom).await.unwrap(),
        Some(power)
    );
}

#[tokio::test]
async fn every_epoch_of_an_identifier_can_be_enumerated() {
    // Without this a consumer has to guess a window and probe it year by year —
    // and a guess one year short reports a subject as fully erased while a
    // mapping survives.
    let r = registry().await;

    for year in [2022, 2024, 2026] {
        r.register_in_epoch("customer-4821", year).await.unwrap();
    }
    r.register_in_epoch("someone-else", 2023).await.unwrap();

    assert_eq!(
        r.epochs("customer-4821").await.unwrap(),
        vec![2022, 2024, 2026]
    );
    assert_eq!(r.epochs("never-seen").await.unwrap(), Vec::<i32>::new());

    let registrations = r.registrations("customer-4821").await.unwrap();
    assert_eq!(registrations.len(), 3);
    assert_eq!(registrations[0].epoch, 2022);
    assert_eq!(
        r.references("customer-4821").await.unwrap(),
        registrations
            .iter()
            .map(|reg| reg.subject.clone())
            .collect::<Vec<_>>()
    );

    // Erasing removes the epoch from the live picture rather than blanking it.
    r.erase(
        &registrations[1].subject,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();
    assert!(
        r.epochs("customer-4821").await.unwrap().is_empty(),
        "erasing by reference reaches every epoch of the person"
    );
}

#[tokio::test]
async fn erase_all_names_the_identifier_a_request_names() {
    // An Article 17 request arrives with a customer number, never with the
    // opaque token the lake stores.
    let r = registry().await;

    for year in [2022, 2024, 2026] {
        r.register_in_epoch("customer-4821", year).await.unwrap();
    }
    let survivor = r.register_in_epoch("customer-9999", 2026).await.unwrap();

    let erased = r
        .erase_all(
            "customer-4821",
            "DSAR-2026-0042",
            "privacy-team",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .unwrap();

    assert_eq!(erased.len(), 3, "one record per epoch: {erased:?}");
    assert_eq!(
        erased.iter().map(ErasureRecord::epoch).collect::<Vec<_>>(),
        vec![Some(2022), Some(2024), Some(2026)],
        "oldest first"
    );
    assert!(r.epochs("customer-4821").await.unwrap().is_empty());
    assert!(
        r.resolve(&survivor).await.unwrap().is_some(),
        "erasure is per identifier, not a mode"
    );
}

#[tokio::test]
async fn erase_all_suppresses_an_identifier_that_was_never_registered() {
    // A request may arrive before the ingest does. There is no linkage to
    // destroy, and the useful half of the request is the other one: do not
    // start.
    let r = suppressing().await;

    let recorded = r
        .erase_all(
            "customer-not-yet-here",
            "DSAR-2026-0100",
            "privacy-team",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .unwrap();

    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0].subject.is_none(),
        "there was no reference to name: {recorded:?}"
    );
    assert!(r.is_suppressed("customer-not-yet-here").await.unwrap());
    assert!(
        r.register("customer-not-yet-here", IN_2026, Sparte::Strom)
            .await
            .is_err(),
        "the replay this exists to refuse"
    );

    // Idempotent: a repeated request must not grow the trail without bound.
    r.erase_all(
        "customer-not-yet-here",
        "DSAR-2026-0100",
        "privacy-team",
        datetime!(2026-07-29 09:00 UTC),
    )
    .await
    .unwrap();
    assert_eq!(
        r.erasures(&ErasureQuery::new().limit(10))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn erase_all_without_a_key_cannot_suppress_and_says_so_by_recording_nothing() {
    // The limitation `SubjectRegistry::new` documents, at the point it bites:
    // with no key there is nothing to recognise the identifier by later, so
    // there is nothing honest to record.
    let r = registry().await;

    let recorded = r
        .erase_all(
            "customer-not-yet-here",
            "DSAR-1",
            "privacy-team",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .unwrap();

    assert!(recorded.is_empty(), "{recorded:?}");
    assert!(
        r.erasures(&ErasureQuery::new().limit(10))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn resolvable_answers_for_a_whole_batch_in_one_round_trip() {
    // The write path's check. It must not hand back the identifiers behind a
    // batch — it only needs to know which references are still usable.
    let r = registry().await;

    let live = r
        .register("customer-a", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    let dead = r
        .register("customer-b", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.erase(
        &dead,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();

    let asked = vec![
        live.as_str().to_string(),
        dead.as_str().to_string(),
        "s2026_deadbeefdeadbeefdeadbeefdeadbeef".to_string(),
    ];
    let found = r.resolvable(&asked).await.unwrap();

    assert!(found.contains(live.as_str()));
    assert!(!found.contains(dead.as_str()));
    assert!(!found.contains("s2026_deadbeefdeadbeefdeadbeefdeadbeef"));
    assert!(r.resolvable(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn erase_all_can_be_enclosed_in_a_caller_s_transaction() {
    // The pairing `erase_in` has, for the entry point a request actually names.
    let pool = pool().await;
    let registry = SubjectRegistry::new(pool.clone());
    registry.create_tables().await.expect("tables");

    let subject = registry
        .register("DE-METER-1", IN_2026, Sparte::Strom)
        .await
        .expect("register");

    let mut tx = pool.begin().await.expect("begin");
    registry
        .erase_all_in(
            &mut tx,
            "DE-METER-1",
            "rehearsal",
            "dpo",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .expect("erase step");
    tx.rollback().await.expect("rollback");

    assert!(
        registry.resolve(&subject).await.expect("resolve").is_some(),
        "a rolled-back erasure must leave the mapping intact"
    );

    let mut tx = pool.begin().await.expect("begin");
    registry
        .erase_all_in(
            &mut tx,
            "DE-METER-1",
            "Art. 17 request",
            "dpo",
            datetime!(2026-07-28 09:00 UTC),
        )
        .await
        .expect("erase step");
    tx.commit().await.expect("commit");

    assert!(registry.resolve(&subject).await.expect("resolve").is_none());
}

#[tokio::test]
async fn the_trail_says_which_duty_each_erasure_discharged() {
    // § 60 Abs. 6 and Article 17 are different duties with different legal
    // bases, and a regulator asks about them separately. `reason` is
    // caller-supplied free text, so without a trigger column the trail cannot
    // answer either question: a sweep that stopped running is invisible behind
    // the requests that kept arriving.
    let r = registry().await;

    let requested = r
        .register("customer-a", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.register_in_epoch("customer-b", 2019).await.unwrap();

    r.erase(
        &requested,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-28 09:00 UTC),
    )
    .await
    .unwrap();
    r.expire_epochs_before(
        datetime!(2023-01-01 00:00 UTC),
        "§ 60 Abs. 6 MsbG",
        "retention-job",
        datetime!(2026-07-29 03:00 UTC),
    )
    .await
    .unwrap();

    let all = r.erasures(&ErasureQuery::new()).await.unwrap();
    assert_eq!(all.len(), 2);

    let requests = r
        .erasures(&ErasureQuery::new().trigger(ErasureTrigger::Request))
        .await
        .unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].subject.as_ref(), Some(&requested));
    assert_eq!(requests[0].trigger, ErasureTrigger::Request);

    let sweeps = r
        .erasures(&ErasureQuery::new().trigger(ErasureTrigger::Retention))
        .await
        .unwrap();
    assert_eq!(sweeps.len(), 1);
    assert_eq!(sweeps[0].epoch(), Some(2019));
    assert_eq!(sweeps[0].trigger, ErasureTrigger::Retention);
}

#[tokio::test]
async fn the_trail_can_be_read_for_a_period() {
    // What an auditor asks for: a quarter, not "the most recent fifty".
    let r = registry().await;

    for (id, at) in [
        ("customer-a", datetime!(2026-06-30 23:00 UTC)),
        ("customer-b", datetime!(2026-07-01 00:00 UTC)),
        ("customer-c", datetime!(2026-09-30 23:59 UTC)),
        ("customer-d", datetime!(2026-10-01 00:00 UTC)),
    ] {
        let subject = r.register(id, IN_2026, Sparte::Strom).await.unwrap();
        r.erase(&subject, "DSAR", "privacy-team", at).await.unwrap();
    }

    let q3 = r
        .erasures(
            &ErasureQuery::new()
                .since(datetime!(2026-07-01 00:00 UTC))
                .until(datetime!(2026-10-01 00:00 UTC)),
        )
        .await
        .unwrap();

    assert_eq!(q3.len(), 2, "half-open, so both boundaries land outside");
    assert_eq!(q3[0].erased_at, datetime!(2026-09-30 23:59 UTC));
    assert_eq!(q3[1].erased_at, datetime!(2026-07-01 00:00 UTC));

    // A period stated backwards selects nothing, which reads as "nothing was
    // erased" — the one answer an audit query must not give by accident.
    assert!(
        r.erasures(
            &ErasureQuery::new()
                .since(datetime!(2026-10-01 00:00 UTC))
                .until(datetime!(2026-07-01 00:00 UTC))
        )
        .await
        .is_err()
    );
    assert!(r.erasures(&ErasureQuery::new().limit(0)).await.is_err());
}

#[tokio::test]
async fn lifting_a_suppression_is_itself_recorded() {
    // Lifting reverses a compliance decision. Left to a log line, a reviewer
    // would see an erasure and then a live registration for the same person,
    // with nothing in between to say who authorised it.
    let r = suppressing().await;

    let original = r
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    r.erase(
        &original,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    assert!(
        r.erasures(&ErasureQuery::new()).await.unwrap()[0]
            .lifted
            .is_none()
    );

    assert!(
        r.lift_suppression(
            "customer-4821",
            "erased in error, ticket OPS-77",
            "dpo",
            datetime!(2026-07-02 11:30 UTC),
        )
        .await
        .unwrap()
    );

    let trail = r.erasures(&ErasureQuery::new()).await.unwrap();
    assert_eq!(trail.len(), 1, "the erasure row survives the lift");
    let lift = trail[0].lifted.as_ref().expect("the lift is on the record");
    assert_eq!(lift.at, datetime!(2026-07-02 11:30 UTC));
    assert_eq!(lift.actor, "dpo");
    assert_eq!(lift.reason, "erased in error, ticket OPS-77");

    // And it still holds no natural identifier — the lift is audited, not the
    // subject.
    assert!(!format!("{:?}", trail[0]).contains("customer-4821"));

    // A second lift finds nothing left to clear.
    assert!(
        !r.lift_suppression(
            "customer-4821",
            "again",
            "dpo",
            datetime!(2026-07-03 09:00 UTC)
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn a_retired_key_still_recognises_the_erasures_it_recorded() {
    // Rotation is additive because a tombstone cannot be re-keyed: it is
    // `HMAC(key, identifier)` and the identifier was destroyed in the same
    // transaction that wrote it. Keeping the old key in the ring is the whole of
    // what makes an erasure survive a rotation.
    const OLD_KEY: &[u8] = b"retired-suppression-key-32-bytes";
    const NEW_KEY: &[u8] = b"current-suppression-key-32-bytes";

    let pool = pool().await;
    let old = SubjectRegistry::with_erasure_secret(pool.clone(), OLD_KEY).expect("secret");
    old.create_tables().await.expect("tables");

    let subject = old
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    old.erase(
        &subject,
        "DSAR-2026-0042",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    // Rotated: the new key writes, the old one is kept for reading.
    let rotated =
        SubjectRegistry::with_erasure_keys(pool.clone(), &[NEW_KEY, OLD_KEY]).expect("ring");
    assert_eq!(rotated.erasure_key_count(), 2);
    assert!(rotated.is_suppressed("customer-4821").await.unwrap());
    assert!(
        rotated
            .register("customer-4821", IN_2026, Sparte::Strom)
            .await
            .is_err(),
        "an erasure recorded under the retired key must still refuse the replay"
    );

    // A subject erased *after* the rotation is tombstoned under the new key, so
    // the ring covers both generations at once.
    let later = rotated
        .register("customer-9999", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    rotated
        .erase(
            &later,
            "DSAR-2",
            "privacy-team",
            datetime!(2026-08-01 09:00 UTC),
        )
        .await
        .unwrap();
    assert!(rotated.is_suppressed("customer-9999").await.unwrap());

    // And lifting reaches a tombstone written under the retired key, so an
    // identifier is genuinely liftable rather than half-lifted.
    assert!(
        rotated
            .lift_suppression(
                "customer-4821",
                "OPS-77",
                "dpo",
                datetime!(2026-08-02 09:00 UTC)
            )
            .await
            .unwrap()
    );
    assert!(!rotated.is_suppressed("customer-4821").await.unwrap());
}

#[tokio::test]
async fn dropping_the_old_key_on_rotation_silently_re_opens_registration() {
    // Pinning the hazard rather than the feature, because it is the reason the
    // ring exists and the reason a retired key is kept rather than destroyed.
    // Nothing can report this: the tombstone is still there and simply stops
    // being recognised.
    const OLD_KEY: &[u8] = b"retired-suppression-key-32-bytes";
    const NEW_KEY: &[u8] = b"current-suppression-key-32-bytes";

    let pool = pool().await;
    let old = SubjectRegistry::with_erasure_secret(pool.clone(), OLD_KEY).expect("secret");
    old.create_tables().await.expect("tables");

    let subject = old
        .register("customer-4821", IN_2026, Sparte::Strom)
        .await
        .unwrap();
    old.erase(
        &subject,
        "DSAR-1",
        "privacy-team",
        datetime!(2026-07-01 09:00 UTC),
    )
    .await
    .unwrap();

    let replaced = SubjectRegistry::with_erasure_secret(pool, NEW_KEY).expect("secret");
    assert!(!replaced.is_suppressed("customer-4821").await.unwrap());
    assert!(
        replaced
            .register("customer-4821", IN_2026, Sparte::Strom)
            .await
            .is_ok(),
        "the outcome a key ring prevents"
    );
}

#[tokio::test]
async fn the_mapping_row_cannot_disagree_with_the_reference_about_its_epoch() {
    // The epoch is written twice — as a column the sweep selects on, and inside
    // the reference the write path parses. A row where they disagreed would be
    // swept on one year while refusing readings from the other, and both halves
    // would look well formed.
    let pool = pool().await;
    let r = SubjectRegistry::new(pool.clone());
    r.create_tables().await.expect("tables");

    let refused = sqlx::query(
        "INSERT INTO meterstore_subject_map (subject_ref, natural_id, epoch) \
         VALUES ($1, $2, $3)",
    )
    .bind("s2026_0123456789abcdef0123456789abcdef")
    .bind("customer-4821")
    .bind(2022)
    .execute(&pool)
    .await;

    assert!(
        refused.is_err(),
        "a 2026 reference must not be stored against epoch 2022"
    );

    // And the agreeing row goes in, so the constraint has not swallowed the
    // ordinary case.
    sqlx::query(
        "INSERT INTO meterstore_subject_map (subject_ref, natural_id, epoch) \
         VALUES ($1, $2, $3)",
    )
    .bind("s2022_0123456789abcdef0123456789abcdef")
    .bind("customer-4821")
    .bind(2022)
    .execute(&pool)
    .await
    .expect("an agreeing row");
}

#[tokio::test]
async fn a_registry_carrying_an_earlier_schema_says_so_at_setup() {
    // `CREATE TABLE IF NOT EXISTS` is silent against a database holding an
    // earlier shape of these tables: it creates nothing, reports success, and
    // the divergence surfaces as `column "trigger" does not exist` at the first
    // erasure — a compliance operation, from an error that says nothing about
    // what to do.
    let pool = pool().await;
    sqlx::query(
        "CREATE TABLE meterstore_erasures (
             subject_ref TEXT PRIMARY KEY,
             erased_at   TIMESTAMPTZ NOT NULL,
             reason      TEXT NOT NULL,
             actor       TEXT NOT NULL
         )",
    )
    .execute(&pool)
    .await
    .expect("a table of the shape an earlier version created");

    let err = SubjectRegistry::new(pool)
        .create_tables()
        .await
        .expect_err("the drift must be reported at setup")
        .to_string();

    assert!(err.contains("meterstore_erasures"), "{err}");
    assert!(err.contains("trigger"), "{err}");
    // And it says what to do about it, which is the whole point of catching it
    // here rather than at the first request.
    assert!(err.contains("drop"), "{err}");
}
