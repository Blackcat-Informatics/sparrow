//! Uncommitted rehearsal harness (issue 326): ages a katamari-shaped store out through the real
//! prepare path and times each phase. Env: OUT (segment directory), RECORDS, DAYS, STORE.
use sparrow_core::{ArchivePolicy, Config, Store};
use std::io::Read;
use std::time::Instant;

const DAY: u64 = 86_400_000;

fn line(i: u64) -> String {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let mut r = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
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

fn cpu_stat() -> String {
    let cg = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let path = cg.trim().rsplit(':').next().unwrap_or("").to_owned();
    std::fs::read_to_string(format!("/sys/fs/cgroup{path}/cpu.stat")).unwrap_or_else(|e| format!("no cpu.stat: {e}"))
}

#[test]
fn rehearsal() {
    let out = std::path::PathBuf::from(std::env::var("OUT").unwrap());
    let store_dir = std::path::PathBuf::from(std::env::var("STORE").unwrap());
    let records: u64 = std::env::var("RECORDS").unwrap().parse().unwrap();
    let days: u64 = std::env::var("DAYS").unwrap().parse().unwrap();
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&store_dir).unwrap();
    let cfg = Config {
        archive: Some(ArchivePolicy::default()),
        disk_budget: 8 * 1024 * 1024 * 1024,
        max_records: 400_000,
        ..sparrow_test_support::storage_config!(Config)
    };
    let store = Store::open(&store_dir, cfg).unwrap();
    let t = Instant::now();
    let per_artifact = 500u64;
    let artifacts = records.div_ceil(per_artifact);
    let mut payload_bytes = 0u64;
    let mut made = 0u64;
    for a in 0..artifacts {
        let n = per_artifact.min(records - made);
        let body: String = (made..made + n).map(line).collect();
        payload_bytes += body.len() as u64;
        let at = 1_000 + (a % days) * DAY;
        store.ingest(&format!("rehearsal-{a}"), "katamari.stored", "jsonl", body.as_bytes(), at).unwrap();
        made += n;
    }
    eprintln!("REH ingested records={made} artifacts={artifacts} payload_bytes={payload_bytes} ingest_ms={}", t.elapsed().as_millis());
    let store_bytes: u64 = walk(&store_dir);
    eprintln!("REH store_dir_bytes={store_bytes} per_record={}", store_bytes / made.max(1));
    let now = 1_000 + (days + 30) * DAY;
    let (mut seg, mut recs, mut file_bytes, mut worst_prepare) = (0u64, 0u64, 0u64, 0u128);
    let (mut prep_total, mut write_total, mut read_total) = (0u128, 0u128, 0u128);
    let total = Instant::now();
    loop {
        let p = Instant::now();
        let Some(segment) = store.archive_prepare(now).unwrap() else { break };
        let prep = p.elapsed().as_millis();
        let w = Instant::now();
        for (name, bytes) in &segment.files {
            let path = out.join(name);
            std::fs::write(&path, bytes).unwrap();
            std::fs::File::open(&path).unwrap().sync_all().unwrap();
            file_bytes += bytes.len() as u64;
            assert!(file_bytes <= 2 * 1024 * 1024 * 1024, "NAS write bound exceeded");
        }
        let write = w.elapsed().as_millis();
        let r = Instant::now();
        for (name, bytes) in &segment.files {
            let mut back = Vec::new();
            std::fs::File::open(out.join(name)).unwrap().read_to_end(&mut back).unwrap();
            assert_eq!(&back, bytes);
        }
        let read = r.elapsed().as_millis();
        store.archive_commit(&segment.segment_id).unwrap();
        seg += 1;
        recs += segment.records;
        worst_prepare = worst_prepare.max(prep);
        prep_total += prep;
        write_total += write;
        read_total += read;
        let bytes: usize = segment.files.iter().map(|f| f.1.len()).sum();
        eprintln!("REH segment={seg} records={} bytes={bytes} prepare_ms={prep} write_ms={write} readback_ms={read} shortened={:?}", segment.records, segment.shortened);
    }
    eprintln!("REH done segments={seg} records={recs} file_bytes={file_bytes} per_record_file_bytes={} total_ms={}", file_bytes / recs.max(1), total.elapsed().as_millis());
    eprintln!("REH prepare_total_ms={prep_total} write_total_ms={write_total} readback_total_ms={read_total} worst_prepare_ms={worst_prepare} halvings={}", store.archive_halvings());
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for l in status.lines().filter(|l| l.starts_with("VmHWM") || l.starts_with("VmRSS")) {
        eprintln!("REH {l}");
    }
    eprintln!("REH cpu.stat {}", cpu_stat().replace('\n', " "));
}

fn walk(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p).map(|d| d.flatten().map(|e| {
        let m = e.metadata().unwrap();
        if m.is_dir() { walk(&e.path()) } else { m.len() }
    }).sum()).unwrap_or(0)
}
