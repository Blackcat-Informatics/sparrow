// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Write the instance role row into a stopped store that has none.

use sparrow_core::InstanceRole;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [data_dir, role] = args.as_slice() else {
        return Err("usage: stamp DATA_DIR production|scratch (the daemon must be stopped)".into());
    };
    let role = InstanceRole::parse(role)?;
    let store_id = sparrow_core::stamp(data_dir, role)?;
    println!("store_id: {store_id}");
    println!("instance_role: {}", role.as_str());
    Ok(())
}
