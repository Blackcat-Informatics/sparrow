//! temporary probe: query latency during a large archive prepare
use sparrow_core::{ArchivePolicy, Config, RdfGraphLayers, RdfReadLimits, Store};
const DAY: u64 = 24 * 60 * 60 * 1000;
#[test]
fn probe319() {
    let n: usize = std::env::var("N").unwrap().parse().unwrap();
    let live = std::env::var("LIVE").is_ok();
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config {
        archive: Some(ArchivePolicy::default()),
        disk_budget: 8 * 1024 * 1024 * 1024,
        max_records: 200_000,
        ..sparrow_test_support::storage_config!(Config)
    };
    let store = Store::open(dir.path(), cfg).unwrap();
    let mut ids = Vec::new();
    let mut k = 0;
    let mut left = n;
    while left > 0 {
        let c = left.min(1000);
        let mut body = String::new();
        for i in 0..c {
            body.push_str(&format!("line number {:06}\n", k * 1000 + i));
        }
        let r = store
            .ingest(&format!("seg-{k}"), "source", "log", body.as_bytes(), 1)
            .unwrap();
        if ids.len() < 3 {
            ids.push(r.record_ids[0].clone());
        }
        k += 1;
        left -= c;
    }
    let done = std::sync::atomic::AtomicBool::new(false);
    let t = std::time::Instant::now();
    std::thread::scope(|sc| {
        let ing = live.then(|| {
            sc.spawn(|| {
                let mut k = 0u64;
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    store
                        .ingest(&format!("live-{k}"), "source", "log", b"x\n", 100 * DAY)
                        .unwrap();
                    k += 1;
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                k
            })
        });
        let q = sc.spawn(|| {
            let mut lat = Vec::new();
            let mut refused: std::collections::BTreeMap<String, u64> = Default::default();
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let s = std::time::Instant::now();
                let r = store.with_rdf_record_layers(
                    &ids,
                    RdfGraphLayers::ALL,
                    2,
                    RdfReadLimits::default(),
                    |_| (),
                );
                let e = s.elapsed().as_micros();
                match r {
                    Ok(()) => lat.push(e),
                    Err(err) => *refused.entry(err.code.to_string()).or_default() += 1,
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            (lat, refused)
        });
        let seg = store.archive_prepare(1 + 30 * DAY).unwrap().expect("due");
        eprintln!("PROBE records={} prepare_ms={}", seg.records, t.elapsed().as_millis());
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        let (mut lat, refused) = q.join().unwrap();
        lat.sort();
        let p = |f: f64| lat.get(((lat.len() as f64 - 1.0) * f) as usize).copied().unwrap_or(0);
        eprintln!(
            "PROBE live={live} served={} refused={refused:?} p50_us={} p99_us={} max_us={}",
            lat.len(), p(0.5), p(0.99), lat.last().copied().unwrap_or(0)
        );
        if let Some(h) = ing {
            eprintln!("PROBE ingests={}", h.join().unwrap());
        }
    });
}
