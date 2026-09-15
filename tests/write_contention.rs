//! Cross-process write contention, crash recovery and snapshot/restore.
//!
//! Every test uses its own store under the OS temp dir and drives the real
//! binary (`--stress-append`, `--snapshot`, `--restore`), so what is measured
//! is exactly what production processes do: N independent SQLite
//! connections on one file, JSON files beside it.

use mcp_memory_rs::config::MemoryConfig;
use mcp_memory_rs::store::{atomic_json, snapshot, Store};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const WRITERS: u32 = 8;
const APPENDS_PER_WRITER: u32 = 40;
const CATEGORY: &str = "stress_log";

fn test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "mcp-memory-contention-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

/// Write a config.toml for an isolated store and return (config path, config).
fn isolated_config(root: &Path, extra_storage: &str) -> (PathBuf, MemoryConfig) {
    let base = root.join("store");
    let cfg_path = root.join("config.toml");
    fs::write(
        &cfg_path,
        format!(
            "[server]\nmode='offline'\n\n[storage]\nbase_dir='{}'\nmax_versions=400\n{extra_storage}\n[acl]\ndevice_name='stress'\n",
            base.display()
        ),
    )
    .unwrap();
    // Parse in-process instead of touching the environment: tests run on
    // parallel threads and env vars are process-global.
    let cfg: MemoryConfig = toml::from_str(&fs::read_to_string(&cfg_path).unwrap()).unwrap();
    (cfg_path, cfg)
}

fn bin(cfg_path: &Path, home: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_mcp-memory-rs"));
    c.env_clear()
        .env("HOME", home)
        .env("MCP_MEMORY_CONFIG", cfg_path)
        .env("MCP_MEMORY_REQUIRE_CONFIG", "1")
        .env("MCP_MEMORY_MODE", "offline")
        .env("MCP_DEVICE", "stress")
        .stdin(Stdio::null());
    c
}

#[test]
fn eight_processes_times_forty_appends_lose_nothing() {
    let root = test_root("multiproc");
    let (cfg_path, cfg) = isolated_config(&root, "lock_deadline_ms=30000\n");

    let started = Instant::now();
    let children: Vec<_> = (0..WRITERS)
        .map(|i| {
            bin(&cfg_path, &root)
                .args([
                    "--stress-append",
                    CATEGORY,
                    &APPENDS_PER_WRITER.to_string(),
                    &format!("p{i}"),
                    "1000",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut failures = Vec::new();
    for child in children {
        let out = child.wait_with_output().unwrap();
        if !out.status.success() {
            failures.push(format!(
                "status={:?} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
    }
    let elapsed = started.elapsed();
    assert!(failures.is_empty(), "writer failures: {failures:#?}");

    // Verify from a fresh connection, like a reader would.
    let store = Store::new(&cfg).unwrap();
    let value = store.read(CATEGORY).unwrap();
    let entries = value["entries"].as_array().expect("log entries");
    let expected = (WRITERS * APPENDS_PER_WRITER) as usize;
    assert_eq!(entries.len(), expected, "every append must be present");

    let mut seen = BTreeSet::new();
    let mut last_id: Option<u64> = None;
    for e in entries {
        let id = e["id"].as_u64().unwrap();
        assert!(
            last_id.is_none_or(|p| id > p),
            "ids must be strictly increasing: {id} after {last_id:?}"
        );
        last_id = Some(id);
        let key = (
            e["data"]["tag"].as_str().unwrap().to_string(),
            e["data"]["seq"].as_u64().unwrap(),
        );
        assert!(seen.insert(key.clone()), "duplicate entry {key:?}");
    }
    assert_eq!(seen.len(), expected, "320 distinct (tag, seq) pairs");
    for i in 0..WRITERS {
        for seq in 0..APPENDS_PER_WRITER {
            assert!(
                seen.contains(&(format!("p{i}"), seq as u64)),
                "missing p{i}/{seq}"
            );
        }
    }

    // SQLite side agrees: one version per append, hash matches the file.
    let versions = store.history(CATEGORY, 1000).unwrap();
    assert_eq!(
        versions.len(),
        expected,
        "one version row per committed append"
    );
    let meta = store.get_meta(CATEGORY).unwrap().unwrap();
    let on_disk =
        fs::read_to_string(cfg.categories_dir().join(format!("{CATEGORY}.json"))).unwrap();
    assert_eq!(meta.content_hash, sha256_hex(&on_disk));
    assert!(store
        .dirty_entries()
        .unwrap()
        .iter()
        .any(|d| d.category_name == CATEGORY));
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());

    eprintln!(
        "Contention stress: {WRITERS}x{APPENDS_PER_WRITER} appends in {:.1}s ({:.1} ms/append wall, sqlite {})",
        elapsed.as_secs_f64(),
        elapsed.as_millis() as f64 / expected as f64,
        Store::sqlite_version()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn deadline_is_honoured_and_reported_when_a_writer_hogs_the_lock() {
    let root = test_root("deadline");
    let (_cfg_path, cfg) =
        isolated_config(&root, "lock_deadline_ms=400\nsqlite_busy_timeout_ms=20\n");
    let mut writer = Store::new(&cfg).unwrap();
    writer
        .write("cat", &serde_json::json!({"v": 1}), None, "t", None)
        .unwrap();

    // Hog the write lock from a raw connection (simulates a slow sibling).
    let hog = rusqlite::Connection::open(cfg.db_path()).unwrap();
    hog.execute_batch("BEGIN IMMEDIATE").unwrap();

    let started = Instant::now();
    let err = writer
        .write("cat", &serde_json::json!({"v": 2}), None, "t", None)
        .unwrap_err();
    let waited = started.elapsed();
    match err {
        mcp_memory_rs::error::MemoryError::Busy { waited_ms, .. } => {
            assert!(waited_ms >= 350, "waited_ms={waited_ms}");
        }
        other => panic!("expected Busy, got {other:?}"),
    }
    assert!(
        waited.as_millis() < 3_000,
        "must give up near the deadline: {waited:?}"
    );
    // Nothing leaked: no staged file, content unchanged, readers fine.
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());
    assert_eq!(writer.read("cat").unwrap()["v"], 1);

    hog.execute_batch("ROLLBACK").unwrap();
    writer
        .write("cat", &serde_json::json!({"v": 3}), None, "t", None)
        .unwrap();
    assert_eq!(writer.read("cat").unwrap()["v"], 3);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn crash_after_commit_before_promote_is_completed_on_next_open() {
    let root = test_root("crash-promote");
    let (_p, cfg) = isolated_config(&root, "");
    let mut store = Store::new(&cfg).unwrap();
    store
        .write("c", &serde_json::json!({"gen": 1}), None, "t", None)
        .unwrap();

    // Simulate: SQLite committed hash(gen=2) but the process died before the
    // rename. Build that state by hand: write the staged file and update the
    // category row exactly as the store would have.
    let new_content = serde_json::to_string_pretty(&serde_json::json!({"gen": 2})).unwrap();
    let staged = atomic_json::stage(&cfg.categories_dir(), "c", &new_content).unwrap();
    let conn = rusqlite::Connection::open(cfg.db_path()).unwrap();
    conn.execute(
        "UPDATE category SET content_hash = ?1 WHERE name = 'c'",
        [sha256_hex(&new_content)],
    )
    .unwrap();
    drop(store);
    std::mem::forget(staged); // do NOT promote: that is the crash

    let reopened = Store::new(&cfg).unwrap();
    assert_eq!(
        reopened.read("c").unwrap()["gen"],
        2,
        "interrupted commit completed"
    );
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn crash_before_commit_discards_the_staged_file() {
    let root = test_root("crash-discard");
    let (_p, cfg) = isolated_config(&root, "");
    let mut store = Store::new(&cfg).unwrap();
    store
        .write("c", &serde_json::json!({"gen": 1}), None, "t", None)
        .unwrap();
    let staged = atomic_json::stage(&cfg.categories_dir(), "c", "{\"gen\": 99}").unwrap();
    drop(store);
    std::mem::forget(staged); // SQLite never committed gen=99

    let reopened = Store::new(&cfg).unwrap();
    assert_eq!(
        reopened.read("c").unwrap()["gen"],
        1,
        "aborted write discarded"
    );
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn interrupted_delete_is_settled_both_ways() {
    let root = test_root("crash-delete");
    let (_p, cfg) = isolated_config(&root, "");
    let mut store = Store::new(&cfg).unwrap();
    store
        .write("keep", &serde_json::json!({"k": 1}), None, "t", None)
        .unwrap();
    store
        .write("gone", &serde_json::json!({"g": 1}), None, "t", None)
        .unwrap();
    // "keep": parked but the DELETE never committed → must come back.
    let _p1 = atomic_json::stage_delete(&cfg.categories_dir(), "keep")
        .unwrap()
        .unwrap();
    // "gone": rows deleted and committed, file still parked → must finish.
    let _p2 = atomic_json::stage_delete(&cfg.categories_dir(), "gone")
        .unwrap()
        .unwrap();
    let conn = rusqlite::Connection::open(cfg.db_path()).unwrap();
    conn.execute("DELETE FROM search_index WHERE category_name = 'gone'", [])
        .unwrap();
    conn.execute(
        "DELETE FROM category_version WHERE category_name = 'gone'",
        [],
    )
    .unwrap();
    conn.execute("DELETE FROM category WHERE name = 'gone'", [])
        .unwrap();
    drop(store);
    std::mem::forget(_p1);
    std::mem::forget(_p2);

    let reopened = Store::new(&cfg).unwrap();
    assert_eq!(reopened.read("keep").unwrap()["k"], 1);
    assert!(matches!(
        reopened.read("gone"),
        Err(mcp_memory_rs::error::MemoryError::NotFound(_))
    ));
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());
    let backups: Vec<_> = fs::read_dir(cfg.backups_dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("gone.") && n.ends_with(".deleted.json"))
        .collect();
    assert_eq!(backups.len(), 1, "deleted content preserved in backups");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn snapshot_with_uncheckpointed_wal_restores_identically() {
    let root = test_root("snapshot");
    let (cfg_path, cfg) = isolated_config(&root, "");
    let mut store = Store::new(&cfg).unwrap();
    // Keep the WAL un-checkpointed so the snapshot must read through it.
    let raw = rusqlite::Connection::open(cfg.db_path()).unwrap();
    raw.execute_batch("PRAGMA wal_autocheckpoint=100000")
        .unwrap();
    for i in 0..20 {
        store
            .write(
                &format!("cat{i}"),
                &serde_json::json!({"i": i, "pad": "x".repeat(2000)}),
                None,
                "t",
                None,
            )
            .unwrap();
    }
    store
        .append("journal", serde_json::json!({"e": 1}), None, "t")
        .unwrap();
    assert!(
        fs::metadata(format!("{}-wal", cfg.db_path().display()))
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "WAL must hold data for this test to mean anything"
    );
    let before_meta: Vec<_> = store.list().unwrap();
    let before_versions = store.history("cat3", 100).unwrap().len();
    let before_dirty = store.dirty_entries().unwrap().len();

    let snap = root.join("snap");
    let out = bin(&cfg_path, &root)
        .args(["--snapshot", snap.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest = snapshot::read_manifest(&snap).unwrap();
    assert_eq!(manifest.categories, 21);
    assert!(
        !snap.join("memory.db-wal").exists(),
        "VACUUM INTO yields a self-contained db"
    );

    // Mutate after the snapshot, then restore and expect the snapshot state.
    store.delete("cat3", "t").unwrap();
    store
        .write(
            "cat0",
            &serde_json::json!({"i": "changed"}),
            None,
            "t",
            None,
        )
        .unwrap();
    drop(store);
    drop(raw);

    let refused = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!refused.status.success(), "restore must require --stopped");

    let ok = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );

    let restored = Store::new(&cfg).unwrap();
    let after_meta = restored.list().unwrap();
    assert_eq!(after_meta.len(), before_meta.len());
    for m in &before_meta {
        let a = after_meta
            .iter()
            .find(|x| x.name == m.name)
            .expect("category restored");
        assert_eq!(a.content_hash, m.content_hash, "hash of {}", m.name);
        let file =
            fs::read_to_string(cfg.categories_dir().join(format!("{}.json", m.name))).unwrap();
        assert_eq!(
            sha256_hex(&file),
            m.content_hash,
            "file of {} matches db",
            m.name
        );
    }
    assert_eq!(restored.read("cat0").unwrap()["i"], 0);
    assert_eq!(
        restored.history("cat3", 100).unwrap().len(),
        before_versions
    );
    assert_eq!(restored.dirty_entries().unwrap().len(), before_dirty);
    assert_eq!(
        restored.read("journal").unwrap()["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // Previous state was parked, not destroyed.
    let parked: Vec<_> = fs::read_dir(cfg.resolved_base_dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("restore-parked-"))
        .collect();
    assert_eq!(parked.len(), 1);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn restore_refuses_a_tampered_snapshot_without_touching_the_live_store() {
    let root = test_root("tamper");
    let (cfg_path, cfg) = isolated_config(&root, "");
    let mut store = Store::new(&cfg).unwrap();
    store
        .write("a", &serde_json::json!({"a": 1}), None, "t", None)
        .unwrap();
    let snap = root.join("snap");
    store
        .snapshot(
            &snap,
            snapshot::Options {
                with_backups: false,
            },
        )
        .unwrap();
    snapshot::verify(&snap).unwrap();
    // Corrupt one category file inside the snapshot.
    fs::write(snap.join("categories/a.json"), "{\"a\": 2}").unwrap();
    assert!(matches!(
        snapshot::verify(&snap),
        Err(mcp_memory_rs::error::MemoryError::Inconsistent(_))
    ));
    store
        .write("a", &serde_json::json!({"a": 3}), None, "t", None)
        .unwrap();
    drop(store);
    let out = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(!out.status.success(), "tampered snapshot must be refused");
    // Live store untouched: no parking dir, content still a=3.
    let parked = fs::read_dir(cfg.resolved_base_dir())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("restore-parked-")
        })
        .count();
    assert_eq!(parked, 0);
    assert_eq!(Store::new(&cfg).unwrap().read("a").unwrap()["a"], 3);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn concurrent_readers_only_ever_see_committed_content_and_touch_nothing() {
    // ABA guard: writers alternate A / B / A (A re-written with the SAME bytes
    // as an earlier commit) while readers hammer read(). Every read must
    // return exactly one of the committed contents, readers must never
    // rename/delete a file, and the end state must be consistent.
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    let root = test_root("readers");
    let (_p, cfg) = isolated_config(&root, "lock_deadline_ms=30000\n");
    let mut seed = Store::new(&cfg).unwrap();
    let a = serde_json::json!({"v": "A", "pad": "x".repeat(4000)});
    let b = serde_json::json!({"v": "B", "pad": "y".repeat(4000)});
    seed.write("c", &a, None, "t", None).unwrap();
    drop(seed);

    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(2 + 4));
    let mut handles = Vec::new();
    for w in 0..2 {
        let cfg = cfg.clone();
        let (a, b) = (a.clone(), b.clone());
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut st = Store::new(&cfg).unwrap();
            barrier.wait();
            for i in 0..60 {
                let v = if (i + w) % 2 == 0 { &a } else { &b };
                st.write("c", v, None, "w", None).unwrap();
            }
            0usize
        }));
    }
    for _ in 0..4 {
        let cfg = cfg.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let st = Store::new(&cfg).unwrap();
            barrier.wait();
            let mut reads = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let (v, meta) = st.read_with_meta("c").unwrap();
                let tag = v["v"].as_str().unwrap();
                assert!(tag == "A" || tag == "B", "torn or foreign content: {tag}");
                // Content and metadata must be one coherent pair.
                let bytes = serde_json::to_string_pretty(&v).unwrap();
                assert_eq!(
                    sha256_hex(&bytes),
                    meta.content_hash,
                    "value/meta incoherent"
                );
                reads += 1;
            }
            reads
        }));
    }
    let writers: Vec<_> = handles.drain(..2).collect();
    for h in writers {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let reads: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(reads > 50, "readers actually ran ({reads} reads)");

    let st = Store::new(&cfg).unwrap();
    let meta = st.get_meta("c").unwrap().unwrap();
    let file = fs::read_to_string(cfg.categories_dir().join("c.json")).unwrap();
    assert_eq!(
        sha256_hex(&file),
        meta.content_hash,
        "final file == committed hash"
    );
    assert!(atomic_json::pending_names(&cfg.categories_dir())
        .unwrap()
        .is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn read_with_meta_pairs_content_with_its_own_hash_even_mid_commit() {
    // Deterministic shape of the race: SQLite already committed hash(B) and
    // B sits in a staged file, while <name>.json still holds A (the owner is
    // between COMMIT and rename). A reader must get (B, hash B), never
    // (A, hash B).
    let root = test_root("coherent");
    let (_p, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    let a = serde_json::json!({"v": "A"});
    let b = serde_json::json!({"v": "B"});
    st.write("c", &a, None, "t", None).unwrap();
    let b_text = serde_json::to_string_pretty(&b).unwrap();
    let staged = atomic_json::stage(&cfg.categories_dir(), "c", &b_text).unwrap();
    let raw = rusqlite::Connection::open(cfg.db_path()).unwrap();
    raw.execute(
        "UPDATE category SET content_hash = ?1 WHERE name = 'c'",
        [sha256_hex(&b_text)],
    )
    .unwrap();
    let (v, meta) = st.read_with_meta("c").unwrap();
    assert_eq!(v["v"], "B");
    assert_eq!(meta.content_hash, sha256_hex(&b_text));
    // and the reader touched nothing: the stage is still there, final still A
    assert!(staged.staged_path().exists());
    assert_eq!(
        fs::read_to_string(cfg.categories_dir().join("c.json")).unwrap(),
        serde_json::to_string_pretty(&a).unwrap()
    );
    // A CAS with that hash now updates B → C, never resurrects A.
    std::mem::forget(staged);
    let _ = st
        .write(
            "c",
            &serde_json::json!({"v": "C"}),
            None,
            "t",
            Some(&meta.content_hash),
        )
        .unwrap();
    assert_eq!(st.read("c").unwrap()["v"], "C");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn restore_refuses_a_snapshot_whose_backup_was_altered() {
    let root = test_root("tamper-backup");
    let (cfg_path, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    st.write("h", &serde_json::json!({"gen": 1}), None, "t", None)
        .unwrap();
    st.write("h", &serde_json::json!({"gen": 2}), None, "t", None)
        .unwrap();
    drop(st);
    let snap = root.join("snap");
    let out = bin(&cfg_path, &root)
        .args(["--snapshot", snap.to_str().unwrap(), "--with-backups"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let m = snapshot::verify(&snap).unwrap();
    assert!(m.backup_files >= 1 && m.backups.len() == m.backup_files);
    // Alter one backup keeping the same count.
    let victim = snap.join("backups").join(m.backups.keys().next().unwrap());
    fs::write(&victim, "{\"gen\": 999}").unwrap();
    assert!(matches!(
        snapshot::verify(&snap),
        Err(mcp_memory_rs::error::MemoryError::Inconsistent(_))
    ));
    let out = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(!out.status.success(), "altered backup must be refused");
    assert_eq!(
        Store::new(&cfg).unwrap().read("h").unwrap()["gen"],
        2,
        "live untouched"
    );
    // --verify-store on the live store: clean.
    let out = bin(&cfg_path, &root)
        .args(["--verify-store"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn restore_never_touches_live_when_the_snapshot_backups_are_broken() {
    let root = test_root("tamper-backup-dir");
    let (cfg_path, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    st.write("h", &serde_json::json!({"gen": 1}), None, "t", None)
        .unwrap();
    st.write("h", &serde_json::json!({"gen": 2}), None, "t", None)
        .unwrap();
    drop(st);
    let snap = root.join("snap");
    let out = bin(&cfg_path, &root)
        .args(["--snapshot", snap.to_str().unwrap(), "--with-backups"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let m = snapshot::verify(&snap).unwrap();
    let victim = snap.join("backups").join(m.backups.keys().next().unwrap());
    // (i) a backup replaced by a DIRECTORY, same count
    fs::remove_file(&victim).unwrap();
    fs::create_dir(&victim).unwrap();
    assert!(snapshot::verify(&snap).is_err());
    let out = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    // (ii) backups/ missing entirely while the manifest says included
    fs::remove_dir_all(snap.join("backups")).unwrap();
    assert!(snapshot::verify(&snap).is_err());
    let out = bin(&cfg_path, &root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    // live untouched in every case: no parking, no staging, content intact
    let leftovers: Vec<_> = fs::read_dir(cfg.resolved_base_dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("restore-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    let live = Store::new(&cfg).unwrap();
    assert_eq!(live.read("h").unwrap()["gen"], 2);
    assert!(Store::verify_store(&cfg).unwrap().is_clean());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn verify_store_is_read_only_and_reports_pending() {
    // A DELETE-mode store (as left by 0.3.1) with an uncommitted staged file:
    // --verify-store must change nothing (journal mode, files, stage) and
    // must NOT call the store clean.
    let root = test_root("verify-ro");
    let (cfg_path, cfg) = isolated_config(&root, "journal_mode='delete'\n");
    let mut st = Store::new(&cfg).unwrap();
    st.write("c", &serde_json::json!({"v": 1}), None, "t", None)
        .unwrap();
    drop(st);
    let _stage = atomic_json::stage(&cfg.categories_dir(), "c", "{\"v\": 99}").unwrap();
    let snapshot_dir = |cfg: &MemoryConfig| -> Vec<(String, Vec<u8>)> {
        let mut v = Vec::new();
        for e in fs::read_dir(cfg.categories_dir()).unwrap() {
            let e = e.unwrap();
            v.push((
                e.file_name().to_string_lossy().to_string(),
                fs::read(e.path()).unwrap(),
            ));
        }
        v.push(("memory.db".into(), fs::read(cfg.db_path()).unwrap()));
        v.sort();
        v
    };
    let before = snapshot_dir(&cfg);
    let report = Store::verify_store(&cfg).unwrap();
    assert_eq!(report.journal_mode.to_ascii_lowercase(), "delete");
    assert_eq!(report.pending, vec!["c".to_string()]);
    assert!(!report.is_clean(), "pending files must not count as clean");
    let out = bin(&cfg_path, &root)
        .args(["--verify-store"])
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "CLI must exit non-zero on a non-clean store"
    );
    assert_eq!(
        before,
        snapshot_dir(&cfg),
        "verify-store changed bytes on disk"
    );
    assert!(!PathBuf::from(format!("{}-wal", cfg.db_path().display())).exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn adopt_json_plan_then_apply_repairs_a_legacy_json_first_divergence() {
    use mcp_memory_rs::store::recovery;
    // Old protocol: JSON rewritten, SQL failed → file newer than the row.
    let root = test_root("adopt");
    let (cfg_path, cfg) = isolated_config(&root, "journal_mode='delete'\n");
    let mut st = Store::new(&cfg).unwrap();
    st.append("jl", serde_json::json!({"e": 1}), None, "t")
        .unwrap();
    st.append("jl", serde_json::json!({"e": 2}), None, "t")
        .unwrap(); // makes a backup of v1
    let before = st.get_meta("jl").unwrap().unwrap();
    let versions_before = st.history("jl", 100).unwrap().len();
    drop(st);
    // Out-of-band newer content (what a failed append left behind), plus a
    // memory category whose DB row is stale.
    let mut v: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(cfg.categories_dir().join("jl.json")).unwrap())
            .unwrap();
    v["entries"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"id": 2, "ts": "2026-09-15T07:05:13Z", "data": {"e": 3}}));
    let newer = serde_json::to_string_pretty(&v).unwrap();
    fs::write(cfg.categories_dir().join("jl.json"), &newer).unwrap();
    let rep = Store::verify_store(&cfg).unwrap();
    assert_eq!(rep.mismatches, vec!["jl".to_string()]);
    // The new reader cannot serve it (no file carries the committed hash).
    let st = Store::new(&cfg).unwrap(); // NB: this opens with WAL on this isolated copy
    assert!(matches!(
        st.read("jl"),
        Err(mcp_memory_rs::error::MemoryError::Inconsistent(_))
    ));
    drop(st);

    // --plan is read-only and describes the divergence semantically.
    let plan_path = root.join("plan.json");
    let out = bin(&cfg_path, &root)
        .args(["--adopt-json", "--plan", plan_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let plan: recovery::Plan =
        serde_json::from_str(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    assert_eq!(plan.entries.len(), 1);
    let e = &plan.entries[0];
    assert_eq!(e.name, "jl");
    assert_eq!(e.kind, "log");
    assert_eq!(e.db_hash.as_deref(), Some(before.content_hash.as_str()));
    assert_eq!(e.file_hash, sha256_hex(&newer));
    assert!(
        e.db_version_backup.is_none(),
        "v2 was never backed up (it was the newest)"
    );
    assert!(e.delta.is_none());
    // With a backup of the database version present, the delta is semantic.
    let db_version_text = {
        let mut v2 = v.clone();
        v2["entries"].as_array_mut().unwrap().pop();
        serde_json::to_string_pretty(&v2).unwrap()
    };
    assert_eq!(
        sha256_hex(&db_version_text),
        before.content_hash,
        "reconstructed db version"
    );
    fs::write(
        cfg.backups_dir().join("jl.2026-09-15T00-00-00.000.json"),
        &db_version_text,
    )
    .unwrap();
    let plan2 = recovery::plan(&cfg).unwrap();
    match &plan2.entries[0].delta {
        Some(recovery::Delta::Log {
            added_ids,
            missing_ids,
            duplicate_ids,
            changed_common_ids,
            entries_db,
            entries_file,
            file_log_valid,
        }) => {
            assert_eq!(added_ids, &vec![2]);
            assert!(missing_ids.is_empty() && duplicate_ids.is_empty());
            assert!(changed_common_ids.is_empty(), "common ids untouched");
            assert_eq!((*entries_db, *entries_file), (2, 3));
            assert!(file_log_valid);
        }
        other => panic!("expected Log delta, got {other:?}"),
    }

    // Precondition failure: a plan describing another file hash is refused.
    let mut wrong = plan.clone();
    wrong.entries[0].file_hash = sha256_hex("not this");
    let wrong_path = root.join("wrong.json");
    fs::write(&wrong_path, serde_json::to_string(&wrong).unwrap()).unwrap();
    let out = bin(&cfg_path, &root)
        .args(["--adopt-json", "--apply", wrong_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success(), "stale plan must be refused");
    assert_eq!(
        Store::verify_store(&cfg).unwrap().mismatches,
        vec!["jl".to_string()]
    );

    let out = bin(&cfg_path, &root)
        .args(["--adopt-json", "--apply", plan_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rep = Store::verify_store(&cfg).unwrap();
    assert!(rep.is_clean(), "{rep:?}");
    let st = Store::new(&cfg).unwrap();
    let (val, meta) = st.read_with_meta("jl").unwrap();
    assert_eq!(
        val["entries"].as_array().unwrap().len(),
        3,
        "file content adopted"
    );
    assert_eq!(meta.content_hash, sha256_hex(&newer));
    assert_eq!(meta.updated_by, "recovery");
    let hist = st.history("jl", 100).unwrap();
    assert_eq!(hist.len(), versions_before + 1);
    assert!(hist.iter().any(|h| h
        .reason
        .as_deref()
        .unwrap_or("")
        .starts_with("recovery: adopted")));
    assert!(st
        .dirty_entries()
        .unwrap()
        .iter()
        .any(|d| d.category_name == "jl"));
    let adopted_copies = fs::read_dir(cfg.backups_dir())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".adopted.json")
        })
        .count();
    assert_eq!(adopted_copies, 1);
    // Rerun of the SAME plan: recognized as already adopted, +0 versions.
    drop(st);
    let out = bin(&cfg_path, &root)
        .args(["--adopt-json", "--apply", plan_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("AlreadyConsistent"));
    let mut st = Store::new(&cfg).unwrap();
    assert_eq!(
        st.history("jl", 100).unwrap().len(),
        versions_before + 1,
        "+0 versions on rerun"
    );
    // and the log keeps working (ids continue)
    st.append("jl", serde_json::json!({"e": 4}), None, "t")
        .unwrap();
    let v = st.read("jl").unwrap();
    assert_eq!(v["entries"].as_array().unwrap().len(), 4);
    assert_eq!(v["entries"][3]["id"], 3);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn sync_import_keeps_physical_hash_and_remembers_the_remote_one() {
    use mcp_memory_rs::sync::{apply_sync_envelope, SyncTransferEntry, SyncTransferEnvelope};
    let root = test_root("sync-hash");
    let (_p, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    // A peer that serialized the same value differently (compact) and hashed THAT.
    let data = serde_json::json!({"b": 1, "a": {"z": [1, 2, 3], "y": "x"}});
    let peer_bytes = serde_json::to_string(&data).unwrap(); // compact ≠ our pretty
    let peer_hash = sha256_hex(&peer_bytes);
    let envelope = SyncTransferEnvelope {
        source_device: "peer".into(),
        exported_at: "2026-09-15T10:00:00Z".into(),
        categories: vec![SyncTransferEntry {
            name: "shared".into(),
            data: data.clone(),
            content_hash: peer_hash.clone(),
            updated_at: "2026-09-15T10:00:00Z".into(),
            updated_by: "peer".into(),
            reason: None,
        }],
        deleted: vec![],
    };
    let r1 = apply_sync_envelope(&mut st, &envelope, "last_write_wins", "me").unwrap();
    assert_eq!(r1.pulled, vec!["shared".to_string()]);
    // Invariant: the row's hash is the hash of OUR bytes on disk.
    let meta = st.get_meta("shared").unwrap().unwrap();
    let on_disk = fs::read_to_string(cfg.categories_dir().join("shared.json")).unwrap();
    assert_eq!(meta.content_hash, sha256_hex(&on_disk));
    assert_ne!(meta.content_hash, peer_hash, "peer formatted differently");
    assert_eq!(
        st.last_remote_hash("shared").unwrap().as_deref(),
        Some(peer_hash.as_str())
    );
    assert!(Store::verify_store(&cfg).unwrap().is_clean());
    let versions = st.history("shared", 10).unwrap().len();
    // Second sync of the very same remote version: nothing happens, no loop.
    let r2 = apply_sync_envelope(&mut st, &envelope, "last_write_wins", "me").unwrap();
    assert!(r2.pulled.is_empty() && r2.conflicts.is_empty(), "{r2:?}");
    assert_eq!(st.history("shared", 10).unwrap().len(), versions);
    // A genuinely newer remote version is still taken.
    let mut newer = envelope.clone();
    newer.categories[0].data = serde_json::json!({"b": 2});
    newer.categories[0].content_hash = sha256_hex("whatever the peer hashed");
    newer.categories[0].updated_at = "2026-09-15T11:00:00Z".into();
    let r3 = apply_sync_envelope(&mut st, &newer, "last_write_wins", "me").unwrap();
    assert_eq!(r3.pulled, vec!["shared".to_string()]);
    assert_eq!(st.read("shared").unwrap()["b"], 2);
    assert!(Store::verify_store(&cfg).unwrap().is_clean());
    // Alias must not survive a local write: import A, write B locally, then
    // the peer proposes A again with a NEWER timestamp → RemoteWins re-imports
    // A (the alias is void because local bytes are B), LocalWins keeps B.
    let mut a_again = envelope.clone();
    a_again.categories[0].updated_at = "2099-01-01T00:00:00Z".into(); // A, newer than any local write
    st.write(
        "shared",
        &serde_json::json!({"local": "B"}),
        None,
        "me",
        None,
    )
    .unwrap();
    assert!(!st
        .holds_remote_version(
            "shared",
            &st.get_meta("shared").unwrap().unwrap(),
            &peer_hash,
            "peer"
        )
        .unwrap());
    let r_lw = apply_sync_envelope(&mut st, &a_again, "local_wins", "me").unwrap();
    assert!(r_lw.pulled.is_empty());
    assert_eq!(r_lw.conflicts.len(), 1);
    assert_eq!(st.read("shared").unwrap()["local"], "B");
    let r_rw = apply_sync_envelope(&mut st, &a_again, "last_write_wins", "me").unwrap();
    assert_eq!(
        r_rw.pulled,
        vec!["shared".to_string()],
        "newer remote A re-imported"
    );
    assert_eq!(st.read("shared").unwrap()["b"], 1);
    assert!(Store::verify_store(&cfg).unwrap().is_clean());
    // Same device check: an alias from another device does not count.
    let mut other_dev = a_again.clone();
    other_dev.categories[0].updated_by = "other".into();
    other_dev.categories[0].updated_at = "2099-01-01T00:30:00Z".into();
    let r_od = apply_sync_envelope(&mut st, &other_dev, "last_write_wins", "me").unwrap();
    assert_eq!(
        r_od.pulled,
        vec!["shared".to_string()],
        "alias is per device"
    );
    // Delete drops the alias.
    st.delete("shared", "me").unwrap();
    assert!(st.last_remote_hash("shared").unwrap().is_none());
    let r_new = apply_sync_envelope(&mut st, &envelope, "last_write_wins", "me").unwrap();
    assert_eq!(r_new.pulled, vec!["shared".to_string()]);
    // Crash between COMMIT and rename on the sync path: the staged file's
    // hash is the committed (physical) one, so recovery promotes it.
    let staged_text = serde_json::to_string_pretty(&serde_json::json!({"b": 3})).unwrap();
    let staged = atomic_json::stage(&cfg.categories_dir(), "shared", &staged_text).unwrap();
    let raw = rusqlite::Connection::open(cfg.db_path()).unwrap();
    raw.execute(
        "UPDATE category SET content_hash = ?1 WHERE name = 'shared'",
        [sha256_hex(&staged_text)],
    )
    .unwrap();
    drop(st);
    std::mem::forget(staged);
    let reopened = Store::new(&cfg).unwrap();
    assert_eq!(
        reopened.read("shared").unwrap()["b"],
        3,
        "committed sync write recovered"
    );
    assert!(Store::verify_store(&cfg).unwrap().is_clean());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn interrupted_restore_marker_makes_the_store_refuse_to_open() {
    let root = test_root("marker");
    let (_p, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    st.write("c", &serde_json::json!({"v": 1}), None, "t", None)
        .unwrap();
    drop(st);
    let marker = cfg.resolved_base_dir().join(snapshot::RESTORE_MARKER);
    fs::write(&marker, "simulated crash mid-swap").unwrap();
    assert!(matches!(
        Store::new(&cfg),
        Err(mcp_memory_rs::error::MemoryError::Inconsistent(_))
    ));
    let report = Store::verify_store(&cfg).unwrap();
    assert!(report.interrupted_restore.is_some() && !report.is_clean());
    fs::remove_file(&marker).unwrap();
    assert_eq!(Store::new(&cfg).unwrap().read("c").unwrap()["v"], 1);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn merges_from_eight_processes_keep_every_key() {
    let root = test_root("merge");
    let (cfg_path, cfg) = isolated_config(&root, "lock_deadline_ms=30000\n");
    let children: Vec<_> = (0..WRITERS)
        .map(|i| {
            bin(&cfg_path, &root)
                .args(["--stress-merge", "shared", "40", &format!("p{i}")])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for child in children {
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let st = Store::new(&cfg).unwrap();
    let v = st.read("shared").unwrap();
    let map = v.as_object().unwrap();
    assert_eq!(
        map.len(),
        (WRITERS * 40) as usize,
        "no patch lost across processes"
    );
    for i in 0..WRITERS {
        for k in 0..40 {
            assert_eq!(map[&format!("p{i}_{k}")], serde_json::json!(k));
        }
    }
    // A write on a log is refused inside the lock, whatever the caller did before.
    let mut st = st;
    st.append("jlog", serde_json::json!({"e": 1}), None, "t")
        .unwrap();
    assert!(matches!(
        st.write("jlog", &serde_json::json!({"x": 1}), None, "t", None),
        Err(mcp_memory_rs::error::MemoryError::KindMismatch(_))
    ));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn snapshot_with_backups_restores_history_payloads_into_a_fresh_store() {
    let root = test_root("history");
    let (cfg_path, cfg) = isolated_config(&root, "");
    let mut st = Store::new(&cfg).unwrap();
    st.write("h", &serde_json::json!({"gen": 1}), None, "t", None)
        .unwrap();
    st.write("h", &serde_json::json!({"gen": 2}), None, "t", None)
        .unwrap();
    st.write("h", &serde_json::json!({"gen": 3}), None, "t", None)
        .unwrap();
    let versions = st.history("h", 10).unwrap();
    assert_eq!(versions.len(), 3);
    let oldest = versions.iter().min_by_key(|v| v.id).unwrap().clone();
    assert_eq!(st.read_version(oldest.id).unwrap().unwrap()["gen"], 1);
    drop(st);

    let snap = root.join("snap");
    let out = bin(&cfg_path, &root)
        .args(["--snapshot", snap.to_str().unwrap(), "--with-backups"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let m = snapshot::verify(&snap).unwrap();
    assert!(m.includes_backups && m.backup_files >= 2, "{m:?}");

    // Brand-new destination store: no residual backups can fake the result.
    let dest_root = test_root("history-dest");
    let (dest_cfg_path, dest_cfg) = isolated_config(&dest_root, "");
    Store::new(&dest_cfg).unwrap(); // create an empty store, then release it
    let out = bin(&dest_cfg_path, &dest_root)
        .args(["--restore", snap.to_str().unwrap(), "--stopped"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let restored = Store::new(&dest_cfg).unwrap();
    assert_eq!(restored.read("h").unwrap()["gen"], 3);
    assert_eq!(restored.history("h", 10).unwrap().len(), 3);
    assert_eq!(
        restored.read_version(oldest.id).unwrap().unwrap()["gen"],
        1,
        "historical payload recovered from the snapshot's backups"
    );
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(dest_root);
}

fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
