import re,sys
p='crates/sparrow-core/src/storage.rs'
s=open(p).read()
a=s.index('    /// Restore one archived artifact under its original')
b=s.index('    /// Accept normalized typed events and their producer positions')
s=s[:a]+s[b:]
a=s.index('        let id = match origin.restore() {')
b=s.index('        let mut prepared = ingest::RecordBatch::new(&writer)?;',a)
s=s[:a]+'        let id = random_id("artifact")?;\n'+s[b:]
old='''                id: match origin.restore() {
                    Some(restore) => restore.record_ids.get(ordinal).cloned().ok_or_else(|| {
                        Error::new("INVALID_ARGUMENT", "restore names too few records")
                    })?,
                    None => random_id("record")?,
                },
'''
assert old in s
s=s.replace(old,'                id: random_id("record")?,\n')
open(p,'w').write(s)
p='crates/sparrow-core/src/storage/ingest.rs'
s=open(p).read()
s=s.replace('''    /// Evidence restored from an archive segment under its original identities.
    Restore(&'a Restore),
''','')
a=s.index('/// The original identities an archived artifact')
b=s.index("impl<'a> Origin<'a> {")
s=s[:a]+s[b:]
s=s.replace('''    pub(super) fn restore(self) -> Option<&'a Restore> {
        match self {
            Self::Restore(value) => Some(value),
            _ => None,
        }
    }
''','')
open(p,'w').write(s)
