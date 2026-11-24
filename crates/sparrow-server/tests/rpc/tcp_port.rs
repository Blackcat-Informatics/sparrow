// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Coordinate TCP port selection across independently started RPC test processes.

use std::{
    io,
    net::{Ipv4Addr, TcpListener},
    os::{
        linux::net::SocketAddrExt,
        unix::net::{SocketAddr, UnixDatagram},
    },
    sync::atomic::{AtomicU32, Ordering},
};

/// Skip the kernel's ephemeral range, including during the handoff to the daemon.
pub(super) fn candidates() -> impl Iterator<Item = u16> {
    static NEXT_PORT: AtomicU32 = AtomicU32::new(10240);
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let bounds = range
        .split_whitespace()
        .map(|value| value.parse::<u32>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(bounds.len(), 2);
    (0..128).map(move |_| {
        let mut port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if port >= bounds[0] {
            port += bounds[1] - bounds[0] + 1;
        }
        u16::try_from(port).expect("no non-ephemeral unprivileged test port is available")
    })
}

/// Return a TCP probe and a separate lease to retain through daemon startup and restarts.
pub(super) fn reserve(ports: impl IntoIterator<Item = u16>) -> (TcpListener, UnixDatagram) {
    for port in ports {
        match try_reserve(port) {
            Ok(reservation) => return reservation,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => continue,
            Err(error) => panic!("test TCP listener admission failed on port {port}: {error}"),
        }
    }
    panic!("test TCP listener range is occupied");
}

fn try_reserve(port: u16) -> io::Result<(TcpListener, UnixDatagram)> {
    // An abstract socket name is shared across test processes in this network namespace
    // and the kernel releases it when its last descriptor closes.
    let name = SocketAddr::from_abstract_name(format!("sparrow-rpc-tests-tcp-{port}"))?;
    let lease = UnixDatagram::bind_addr(&name)?;
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))?;
    Ok((listener, lease))
}

#[test]
fn tcp_test_ports_do_not_collide_across_processes() {
    use super::ProcessOutput as _;
    use std::process::Command;

    const LEASED_PORTS: &str = "SPARROW_RPC_TEST_LEASED_PORTS";
    if let Some(leased) = std::env::var_os(LEASED_PORTS) {
        let occupied = leased
            .to_str()
            .unwrap()
            .split(',')
            .map(|port| port.parse::<u16>().unwrap())
            .collect::<Vec<_>>();
        let reservations = (0..occupied.len())
            .map(|_| reserve(occupied.iter().copied().chain(candidates())))
            .collect::<Vec<_>>();
        for (listener, _) in &reservations {
            let port = listener.local_addr().unwrap().port();
            assert!(
                !occupied.contains(&port),
                "child reused leased TCP port {port}"
            );
        }
        return;
    }

    let mut leases = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..3 {
        let (listener, lease) = reserve(candidates());
        ports.push(listener.local_addr().unwrap().port().to_string());
        leases.push(lease);
        drop(listener);
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tcp_port::tcp_test_ports_do_not_collide_across_processes",
        ])
        .env(LEASED_PORTS, ports.join(","))
        .admitted_output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "child did not pass the port handoff check: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(leases);
}
