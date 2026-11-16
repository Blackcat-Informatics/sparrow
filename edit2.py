p='crates/sparrow-archive/src/decode.rs'
s=open(p).read()
s=s.replace('''//! artifacts; replacing the format replaces this module only. Listing, naming and the
//! restore flow never see the format.''','''//! artifacts; replacing the format replaces this module only. Listing, naming and
//! extraction never see the format.''')
open(p,'w').write(s)
p='crates/sparrow-archive/src/extract.rs'
s=open(p).read()
a=s.index('    let digest = purrdf_hash')
b=s.index('    Ok(bytes)')
s=s[:a]+s[b:]
s=s.replace('''/// `byte_length` and its BLAKE3 equals the artifact's recorded `payload_id`. Nothing is
/// written anywhere.''','''/// `byte_length`. Each record's bytes were already checked by the decoder against the digest
/// the archive carries for them (the blob digest named by its link quad). Nothing is
/// written anywhere.''')
s=s.replace('''/// Every segment is decoded''','''/// Every segment is decoded''')
s=s.replace('''/// `byte_length`. Each''','''/// `byte_length`. Each''')
s=s.replace('''/// artifact whose records are not all present or whose bytes do not match its recorded
/// length and digest; the last two name''','''/// artifact whose records are not all present or whose bytes do not add up to its recorded
/// length; the last two name''')
s=s.replace('''and the result is returned only when its length equals the artifact's recorded''','''and the result is returned only when its length equals the artifact's recorded''')
open(p,'w').write(s)
