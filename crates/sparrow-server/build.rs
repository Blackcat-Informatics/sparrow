// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Compile Sparrow's versioned Cap'n Proto contract.

fn main() {
    println!("cargo:rerun-if-changed=schema/sparrow.capnp");
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/sparrow.capnp")
        .run()
        .expect(
            "Cap'n Proto schema compilation failed; install capnp and inspect schema/sparrow.capnp",
        );
}
