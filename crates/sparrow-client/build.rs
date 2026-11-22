// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Compile Sparrow's versioned Cap'n Proto contract.

fn main() {
    println!("cargo:rerun-if-changed=schema/sparrow.capnp");
    println!("cargo:rerun-if-changed=schema/spool.capnp");
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/sparrow.capnp")
        .file("schema/spool.capnp")
        .run()
        .expect(
            "Cap'n Proto schema compilation failed; install capnp and inspect schema/sparrow.capnp",
        );
}
