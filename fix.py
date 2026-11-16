p="/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/crates/sparrow-server/src/archive.rs"
s=open(p).read()
i=s.index("        archive_segment(&flight(), &store, &root, \"s\", &segment).unwrap();\n        assert_eq!(\n            fs::metadata(root.join(\"STORE_ID\"))")
j=s.index("archive_segment(",i)
s=s[:j]+'''store.ingest("r2", "source", "log", b"c\\nd\\n", 2).unwrap();
        let second = store.archive_prepare(LATER).unwrap().expect("a second segment");
        '''+s[j:].replace("&segment).unwrap();","&second).unwrap();",1)
open(p,"w").write(s)
