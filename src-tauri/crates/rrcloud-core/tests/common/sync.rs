//! Shared test support for the journal-engine suites (publisher, reader,
//! manifest): consumer test doubles, a counting/fault-injecting [`S3Api`]
//! wrapper, entry builders, and db scaffolding.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use rrcloud_core::clock::DeviceId;
use rrcloud_core::journal::{encode_segment, JournalEntry, Kind, Op, JOURNAL_VERSION};
use rrcloud_core::keys::{classify_key, journal_segment_key, sidecar_key, KeyClass, RelKey};
use rrcloud_core::reader::{ConsumerError, JournalConsumer};
use rrcloud_core::s3::{
    ByteRange, GetObjectOutput, HeadObjectOutput, ListObjectsV2Output, ListObjectsV2Request,
    PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Error,
};
use rrcloud_core::state::{DeletedRecord, ItemRecord, ItemState, StateTxn, SyncDb};

// Canonical lowercase UUIDv4 device ids for multi-device scenarios.
pub const DEV_A: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
pub const DEV_B: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";
pub const DEV_C: &str = "c0ffee00-0000-4000-8000-000000000001";
pub const DEV_X: &str = "deadbeef-0000-4000-9000-000000000002";
pub const DEV_Y: &str = "fee1f00d-0000-4000-a000-000000000003";

pub fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("valid device id")
}

pub fn rel(s: &str) -> RelKey {
    RelKey::new(s).expect("valid relkey")
}

/// A scratch state db minted for `device`; the TempDir keeps it alive.
pub fn open_db(device: &DeviceId) -> (tempfile::TempDir, PathBuf, SyncDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.redb");
    let db = SyncDb::open(&path, Some(device.clone())).expect("open state db");
    (dir, path, db)
}

// ---------------------------------------------------------------------------
// Journal entry builders
// ---------------------------------------------------------------------------

/// A complete v1 entry with `seq` 0 (the staging form: seqs are stamped at
/// publication / by [`stamped`]).
pub fn entry(device: &DeviceId, op: Op, kind: Kind, key: String) -> JournalEntry {
    JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts: 1_769_900_000,
        device: device.clone(),
        op,
        kind,
        key,
        vv: [(device.clone(), 1u32)].into_iter().collect(),
        size: Some(64),
        blake3: None,
        sem_hash: None,
        rating: None,
        color_label: None,
        content_id: None,
        w: None,
        h: None,
        mtime: Some(1_769_899_000),
        from_key: None,
    }
}

/// `count` sidecar `put` entries for distinct relkeys under `tag/`.
pub fn sidecar_entries(device: &DeviceId, tag: &str, count: usize) -> Vec<JournalEntry> {
    (0..count)
        .map(|i| {
            entry(
                device,
                Op::Put,
                Kind::Sidecar,
                sidecar_key(&rel(&format!("{tag}/img-{i:04}.NEF"))),
            )
        })
        .collect()
}

/// Stamps `entries` with contiguous seqs starting at `first_seq`.
pub fn stamped(mut entries: Vec<JournalEntry>, first_seq: u64) -> Vec<JournalEntry> {
    for (i, e) in entries.iter_mut().enumerate() {
        e.seq = first_seq + i as u64;
    }
    entries
}

/// Encodes `entries` and PUTs them as `device`'s segment at `first_seq`
/// through the raw client (crafting foreign devices' prefixes without a
/// publisher). Returns the segment's bucket key.
pub async fn put_raw_segment(
    client: &S3Client,
    bucket: &str,
    device: &DeviceId,
    first_seq: u64,
    entries: &[JournalEntry],
) -> String {
    let bytes = encode_segment(entries).expect("encode segment");
    let key = journal_segment_key(device, first_seq);
    client
        .put_object(
            bucket,
            &key,
            Bytes::from(bytes),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put raw segment");
    key
}

// ---------------------------------------------------------------------------
// Consumer test doubles
// ---------------------------------------------------------------------------

/// What [`RecordingConsumer`] records per applied entry.
pub type Applied = (DeviceId, u64, String, Op);

/// The unit's consumer test double: records every applied entry, and can
/// inject a failure or a panic at one `(device, seq)` — after performing a
/// durable probe mutation, so the atomicity tests can check that the
/// probe rolled back with the applied mark.
#[derive(Default)]
pub struct RecordingConsumer {
    /// `(device, seq, key, op)` per applied entry, in application order.
    pub transcript: Vec<Applied>,
    /// Return `Err` when applying this `(device, seq)`.
    pub fail_on: Option<(DeviceId, u64)>,
    /// Panic when applying this `(device, seq)`.
    pub panic_on: Option<(DeviceId, u64)>,
    /// When set, every apply first writes a probe item record at relkey
    /// `probe-<first 8 of device>-<seq>` through the transaction.
    pub probe_items: bool,
}

/// The relkey the probe mutation writes for `(device, seq)`.
pub fn probe_relkey(device: &DeviceId, seq: u64) -> RelKey {
    rel(&format!("probe-{}-{seq}", &device.as_str()[..8]))
}

fn probe_record() -> ItemRecord {
    ItemRecord {
        kind: Kind::Sidecar,
        state: ItemState::Dirty,
        size: 1,
        mtime_unix_ns: 0,
        blake3: None,
        sem_hash: None,
        vv: Default::default(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
    }
}

impl JournalConsumer for RecordingConsumer {
    fn apply(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), ConsumerError> {
        if self.probe_items {
            txn.replay_put_item(&probe_relkey(&entry.device, entry.seq), &probe_record())?;
        }
        if self.panic_on.as_ref() == Some(&(entry.device.clone(), entry.seq)) {
            panic!(
                "injected consumer panic at ({}, {})",
                entry.device, entry.seq
            );
        }
        if self.fail_on.as_ref() == Some(&(entry.device.clone(), entry.seq)) {
            return Err(format!(
                "injected consumer failure at ({}, {})",
                entry.device, entry.seq
            )
            .into());
        }
        self.transcript
            .push((entry.device.clone(), entry.seq, entry.key.clone(), entry.op));
        Ok(())
    }
}

/// A minimal state-building consumer for the §2.3 equivalence proof: maps
/// `put` entries to wholesale item records and `del` entries to
/// delete + deleted-set rows, identically whether the entry came off the
/// journal or was synthesized from a manifest row — so "merge == replay"
/// is checkable as state equality.
pub struct ReplayConsumer;

/// The relkey an entry's bucket key addresses (originals/sidecars/xmp).
pub fn entry_relkey(key: &str) -> Result<RelKey, ConsumerError> {
    match classify_key(key) {
        KeyClass::Original { relkey }
        | KeyClass::Sidecar { relkey, .. }
        | KeyClass::Xmp { relkey } => Ok(relkey),
        other => Err(format!("unclassifiable entry key {key:?}: {other:?}").into()),
    }
}

impl JournalConsumer for ReplayConsumer {
    fn apply(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), ConsumerError> {
        let relkey = entry_relkey(&entry.key)?;
        match entry.op {
            Op::Put => {
                let record = ItemRecord {
                    kind: entry.kind,
                    state: ItemState::Synced,
                    size: entry.size.unwrap_or(0),
                    mtime_unix_ns: entry.mtime.unwrap_or(0).saturating_mul(1_000_000_000),
                    blake3: entry.blake3.clone(),
                    sem_hash: entry.sem_hash.clone(),
                    vv: entry.vv.clone(),
                    content_id: entry.content_id.clone(),
                    w: entry.w,
                    h: entry.h,
                    pinned: false,
                    last_access_unix: 0,
                    verified_remote: false,
                    attested: false,
                    base_unknown: false,
                };
                txn.replay_put_item(&relkey, &record)?;
            }
            Op::Del => {
                txn.delete_item(&relkey)?;
                txn.record_deleted(
                    &relkey,
                    &DeletedRecord {
                        vv: entry.vv.clone(),
                        server_ts: entry.ts,
                    },
                )?;
            }
            Op::Move | Op::Attest => {}
        }
        Ok(())
    }
}

/// Applies `entries` to `db` through [`ReplayConsumer`] directly (no
/// journal involved) — how a test builds "the state these entries
/// describe" for equivalence assertions.
pub fn apply_entries_locally(db: &SyncDb, entries: &[JournalEntry]) {
    let mut consumer = ReplayConsumer;
    for e in entries {
        db.with_txn_err::<_, ConsumerError>(|t| consumer.apply(t, e))
            .expect("local apply");
    }
}

// ---------------------------------------------------------------------------
// Counting / fault-injecting S3 wrapper
// ---------------------------------------------------------------------------

/// A thin [`S3Api`] wrapper around the real client: counts LIST calls
/// (pinning the §2.2 steady-state polling cost), records every GET key
/// and every **attempted** PUT key (pinning publish ordering), and can
/// make selected PUTs fail — either without reaching the backend
/// (`fail_puts`) or after the bytes landed (`put_then_fail`, modeling a
/// crash/connection loss between the backend's commit and our bookkeeping)
/// — or selected GETs fail without reaching the backend (`fail_gets`,
/// modeling a persistently unreadable object).
pub struct FakeS3 {
    pub inner: S3Client,
    pub lists: AtomicU32,
    pub gets: Mutex<Vec<String>>,
    pub put_attempts: Mutex<Vec<String>>,
    pub fail_puts: HashSet<String>,
    pub put_then_fail: HashSet<String>,
    pub fail_gets: HashSet<String>,
}

impl FakeS3 {
    pub fn new(inner: S3Client) -> Self {
        FakeS3 {
            inner,
            lists: AtomicU32::new(0),
            gets: Mutex::new(Vec::new()),
            put_attempts: Mutex::new(Vec::new()),
            fail_puts: HashSet::new(),
            put_then_fail: HashSet::new(),
            fail_gets: HashSet::new(),
        }
    }

    pub fn list_count(&self) -> u32 {
        self.lists.load(Ordering::SeqCst)
    }

    pub fn attempted_puts(&self) -> Vec<String> {
        self.put_attempts.lock().expect("lock").clone()
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.gets.lock().expect("lock").clone()
    }
}

impl S3Api for FakeS3 {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        self.put_attempts
            .lock()
            .expect("lock")
            .push(key.to_string());
        if self.put_then_fail.contains(key) {
            self.inner.put_object(bucket, key, body, opts).await?;
            return Err(S3Error::InvalidRequest(format!(
                "injected post-landing PUT failure for {key}"
            )));
        }
        if self.fail_puts.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected PUT failure for {key}"
            )));
        }
        self.inner.put_object(bucket, key, body, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        self.gets.lock().expect("lock").push(key.to_string());
        if self.fail_gets.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected GET failure for {key}"
            )));
        }
        self.inner.get_object(bucket, key, range).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        self.inner.head_object(bucket, key).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_objects_v2(bucket, request).await
    }
}
