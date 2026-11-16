p='crates/sparrow-archive/src/main.rs'
s=open(p).read()
s=s.replace('''//! `sparrow-archive list|verify DIR`: offline, read-only. One JSON line per segment on
//! stdout, diagnostics on stderr, and a nonzero exit on any failure.''','''//! `sparrow-archive list|verify DIR` and `extract DIR ARTIFACT_ID`: offline, read-only.
//! `list` and `verify` print one JSON line per segment on stdout; `extract` prints one
//! artifact's bytes, and only after they are verified. Diagnostics go to stderr, and any
//! failure exits nonzero.''')
s=s.replace('use sparrow_archive::{ArchiveError, Segment, Summary, list_segments};','use sparrow_archive::{ArchiveError, Segment, Summary, extract, list_segments};')
s=s.replace('const USAGE: &str = "usage: sparrow-archive list|verify DIR";','const USAGE: &str = "usage: sparrow-archive list|verify DIR | extract DIR ARTIFACT_ID";')
s=s.replace('''fn open(path: &Path) -> Result<(Segment, Summary), ArchiveError> {''','''/// The segment's artifacts, so a person can find an artifact id and extract it.
fn entries(segment: &Segment) -> String {
    let items: Vec<String> = segment
        .artifacts()
        .iter()
        .map(|artifact| {
            format!(
                "{{\\"id\\":{},\\"source\\":{},\\"received_at\\":{},\\"byte_length\\":{}}}",
                quote(&artifact.id),
                quote(&artifact.source),
                artifact.received_at,
                artifact.byte_length
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn open(path: &Path) -> Result<(Segment, Summary), ArchiveError> {''')
s=s.replace('''            ("list", Ok((_, summary))) => {
                emit(&format!("{{\\"path\\":{name},{}}}", counts(summary)))?;''','''            ("list", Ok((segment, summary))) => {
                emit(&format!(
                    "{{\\"path\\":{name},{},\\"entries\\":{}}}",
                    counts(summary),
                    entries(&segment)
                ))?;''')
s=s.replace('''fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [command, root] = args.as_slice() else {''','''/// Write one artifact's verified bytes to stdout. Nothing reaches stdout before the whole
/// artifact has been read from every segment and checked.
fn take(root: &Path, artifact_id: &str) -> ExitCode {
    let fail = |error: &dyn std::fmt::Display| {
        let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
        ExitCode::FAILURE
    };
    let bytes = match list_segments(root).and_then(|paths| extract(&paths, artifact_id)) {
        Ok(bytes) => bytes,
        Err(error) => return fail(&error),
    };
    let mut out = std::io::stdout().lock();
    match out.write_all(&bytes).and_then(|()| out.flush()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: stdout: {error}");
            ExitCode::from(2)
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [command, root, artifact_id] = args.as_slice()
        && command == "extract"
    {
        return take(Path::new(root), artifact_id);
    }
    let [command, root] = args.as_slice() else {''')
open(p,'w').write(s)
