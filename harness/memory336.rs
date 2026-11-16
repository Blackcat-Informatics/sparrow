//! Uncommitted measurement harness (issue 336). MODE=ingest|archive|count.
//! Env: STORE, MODE, OUT, SHAPE=log|json, RECORDS, KEYS, DAYS, SEG (records per segment), TAG.
use sparrow_core::{ArchivePolicy, Config, Store};
use std::time::Instant;

const DAY: u64 = 86_400_000;

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("env {k}"))
}

fn xs(i: u64) -> impl FnMut() -> u64 {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}

fn log_line(i: u64) -> String {
    let mut r = xs(i);
    let units = ["sshd.service", "systemd-logind.service", "katamari-manager.service", "kernel", "dbus-broker.service"];
    let msg = format!(
        "request {:016x} from 10.{}.{}.{} completed in {} ms status {} bytes {}",
        r(), r() % 255, r() % 255, r() % 255, r() % 5000, [200, 204, 404, 500][(r() % 4) as usize], r() % 100_000
    );
    format!(
        "{{\"__REALTIME_TIMESTAMP\":\"{}\",\"_HOSTNAME\":\"katamari\",\"_SYSTEMD_UNIT\":\"{}\",\"PRIORITY\":\"{}\",\"_PID\":\"{}\",\"_BOOT_ID\":\"{:032x}\",\"MESSAGE\":\"{}\"}}\n",
        1_700_000_000_000_000u64 + i * 997, units[(r() % 5) as usize], r() % 8, r() % 40_000, r(), msg
    )
}

fn json_line(i: u64, keys: u64) -> String {
    let mut r = xs(i);
    let mut s = String::from("{");
    for k in 0..keys {
        if k > 0 {
            s.push(',');
        }
        match k % 10 {
            0 => s.push_str(&format!("\"k{k}\":{{\"a\":{},\"b\":\"{:08x}\"}}", r() % 1000, r() as u32)),
            1 | 4 | 7 => s.push_str(&format!("\"k{k}\":{}", r() % 1_000_000)),
            _ => s.push_str(&format!("\"k{k}\":\"v{:012x}\"", r() & 0xFFFF_FFFF_FFFF)),
        }
    }
    s.push_str("}\n");
    s
}

fn status() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let f = |k: &str| {
        s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse().ok()).unwrap_or(0)
    };
    (f("VmRSS:"), f("VmHWM:"))
}

fn cgroup_dir() -> String {
    let cg = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let path = cg.trim().rsplit(':').next().unwrap_or("").to_owned();
    format!("/sys/fs/cgroup{path}")
}

fn cgroup_report(tag: &str) {
    let d = cgroup_dir();
    eprintln!("M336 cgroup_dir={d}");
    for f in ["memory.peak", "memory.current", "memory.high", "memory.max", "memory.events"] {
        let v = std::fs::read_to_string(format!("{d}/{f}")).unwrap_or_else(|e| format!("ERR {e}"));
        eprintln!("M336 {tag} {f} {}", v.trim().replace('\n', " "));
    }
    let st = std::fs::read_to_string(format!("{d}/memory.stat")).unwrap_or_default();
    let pick: Vec<_> = st.lines().filter(|l| ["anon ", "file ", "file_mapped ", "slab ", "sock ", "shmem ", "kernel "].iter().any(|p| l.starts_with(p))).collect();
    eprintln!("M336 {tag} memory.stat {}", pick.join(" | "));
}

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg").unwrap_or_default().split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

fn walk(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p).map(|d| d.flatten().map(|e| {
        let m = e.metadata().unwrap();
        if m.is_dir() { walk(&e.path()) } else { m.len() }
    }).sum()).unwrap_or(0)
}

#[test]
fn memory336() {
    let mode = env("MODE");
    let store_dir = std::path::PathBuf::from(env("STORE"));
    eprintln!("M336 start mode={mode} loadavg={} status={:?}", loadavg(), status());
    if mode == "count" {
        // Reads written segment files back and counts their rows; separate process so it
        // does not touch the archive peak.
        let out = std::path::PathBuf::from(env("OUT"));
        let mut names: Vec<_> = std::fs::read_dir(&out).unwrap().flatten().map(|e| e.path()).collect();
        names.sort();
        for p in names {
            let bytes = std::fs::read(&p).unwrap();
            let g = purrdf::gts::reader::read(&bytes, false, None);
            eprintln!("M336 count file={} bytes={} quads={} terms={} blobs={} reifiers={} annotations={} diag={}", p.file_name().unwrap().to_string_lossy(), bytes.len(), g.quads.len(), g.terms.len(), g.blobs.len(), g.reifiers.len(), g.annotations.len(), g.diagnostics.len());
        }
        return;
    }
    let seg: u64 = std::env::var("SEG").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let cfg = Config {
        archive: Some(ArchivePolicy { max_segment_records: seg, ..ArchivePolicy::default() }),
        disk_budget: 8 * 1024 * 1024 * 1024,
        max_records: 400_000,
        ..sparrow_test_support::storage_config!(Config)
    };
    std::fs::create_dir_all(&store_dir).unwrap();
    let days: u64 = std::env::var("DAYS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let store = Store::open(&store_dir, cfg).unwrap();
    eprintln!("M336 opened store seg={seg} status={:?}", status());
    if mode == "ingest" {
        let records: u64 = env("RECORDS").parse().unwrap();
        let shape = env("SHAPE");
        let keys: u64 = std::env::var("KEYS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        let per_artifact: u64 = std::env::var("PER_ARTIFACT").ok().and_then(|v| v.parse().ok()).unwrap_or(500);
        let artifacts = records.div_ceil(per_artifact);
        let t = Instant::now();
        let (mut made, mut payload) = (0u64, 0u64);
        for a in 0..artifacts {
            let n = per_artifact.min(records - made);
            let body: String = (made..made + n).map(|i| if shape == "json" { json_line(i, keys) } else { log_line(i) }).collect();
            payload += body.len() as u64;
            let at = 1_000 + (a % days) * DAY;
            store.ingest(&format!("m336-{a}"), "katamari.stored", "jsonl", body.as_bytes(), at).unwrap();
            made += n;
            if a % 10 == 9 { eprintln!("M336 ingest progress records={made} ms={} status={:?}", t.elapsed().as_millis(), status()); }
        }
        store.flush().unwrap();
        let (rss, hwm) = status();
        eprintln!("M336 ingested records={made} payload_bytes={payload} ingest_ms={} VmRSS_kB={rss} VmHWM_kB={hwm}", t.elapsed().as_millis());
        let sb = walk(&store_dir);
        eprintln!("M336 store_dir_bytes={sb} per_record={}", sb / made.max(1));
        cgroup_report("ingest-end");
        eprintln!("M336 end loadavg={}", loadavg());
        return;
    }
    // mode == archive
    let out = std::path::PathBuf::from(env("OUT"));
    std::fs::create_dir_all(&out).unwrap();
    let now = 1_000 + (days + 30) * DAY;
    let max_segments: u64 = std::env::var("MAX_SEGMENTS").ok().and_then(|v| v.parse().ok()).unwrap_or(u64::MAX);
    let (mut n, mut recs, mut file_bytes) = (0u64, 0u64, 0u64);
    loop {
        if n >= max_segments { break; }
        let (r0, h0) = status();
        let p = Instant::now();
        let prepared = store.archive_prepare(now);
        let prep = p.elapsed().as_millis();
        let (r1, h1) = status();
        let segment = match prepared {
            Ok(Some(s)) => s,
            Ok(None) => break,
            Err(e) => {
                eprintln!("M336 prepare ERROR after_segments={n} code={} msg={} before(rss,hwm)=({r0},{h0}) after=({r1},{h1})", e.code, e.message);
                break;
            }
        };
        let w = Instant::now();
        let mut bytes = 0usize;
        for (name, b) in &segment.files {
            let path = out.join(name);
            std::fs::write(&path, b).unwrap();
            std::fs::File::open(&path).unwrap().sync_all().unwrap();
            bytes += b.len();
        }
        let write = w.elapsed().as_millis();
        let (r2, h2) = status();
        store.archive_commit(&segment.segment_id).unwrap();
        n += 1;
        recs += segment.records;
        file_bytes += bytes as u64;
        eprintln!("M336 segment={n} records={} artifacts={} bytes={bytes} prepare_ms={prep} write_ms={write} shortened={:?} before_rss={r0} before_hwm={h0} after_prepare_rss={r1} after_prepare_hwm={h1} after_write_rss={r2} after_write_hwm={h2} (kB) loadavg={}", segment.records, segment.artifacts, segment.shortened, loadavg());
    }
    let (rss, hwm) = status();
    eprintln!("M336 done segments={n} records={recs} file_bytes={file_bytes} halvings={} VmRSS_kB={rss} VmHWM_kB={hwm}", store.archive_halvings());
    cgroup_report("archive-end");
    eprintln!("M336 end loadavg={}", loadavg());
}
