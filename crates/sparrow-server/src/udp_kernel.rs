// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Kernel evidence for one UDP syslog socket: the receive buffer it was given and the datagrams
//! the kernel dropped before the daemon could read them.
//!
//! The drop count is the `drops` column of `/proc/self/net/udp` (`udp6` for an IPv6 socket) in
//! the row whose `inode` is the socket's. The file is read only when asked for: on a status
//! request and when the reader leaves a wait. There is no timer. When the file cannot be read, or
//! has no row for the socket, the state says so with the reason and no count is reported.

use std::io;
use std::net::SocketAddr;
use std::os::fd::AsFd;
use std::path::PathBuf;

/// Receive buffer requested for every UDP syslog socket.
pub(crate) const REQUESTED_RCVBUF_BYTES: usize = 8 * 1024 * 1024;
const TABLE_V4: &str = "/proc/self/net/udp";
const TABLE_V6: &str = "/proc/self/net/udp6";
const RMEM_MAX: &str = "/proc/sys/net/core/rmem_max";

/// What was asked of the kernel and what it reports back. Linux stores twice the request (room
/// for bookkeeping) after clamping the request to `net.core.rmem_max`, so `granted` is that
/// stored size: it is not the payload capacity, and it may be below `requested`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Buffer {
    pub requested: usize,
    pub granted: usize,
    /// The error of the enlarge request, when the kernel refused it: the socket then keeps the
    /// default buffer, `granted` is that default, and the daemon still takes syslog.
    pub enlarge_error: Option<String>,
}

/// Ask for a receive buffer of `requested` bytes and read back what the kernel granted. The
/// kernel clamping the size is normal and only reported. A refused request does not stop the
/// daemon taking syslog: it is returned in `enlarge_error` for status and the journal. Failing
/// to read the size back means the socket itself is broken, and fails.
pub(crate) fn request_buffer(socket: impl AsFd, requested: usize) -> io::Result<Buffer> {
    request_buffer_with(socket, requested, |socket, size| {
        rustix::net::sockopt::set_socket_recv_buffer_size(socket, size)
    })
}

/// [`request_buffer`] with the setting call supplied, so a test can make the kernel refuse.
fn request_buffer_with(
    socket: impl AsFd,
    requested: usize,
    set: impl FnOnce(std::os::fd::BorrowedFd<'_>, usize) -> rustix::io::Result<()>,
) -> io::Result<Buffer> {
    let socket = socket.as_fd();
    let enlarge_error = set(socket, requested).err().map(|error| {
        format!("cannot set SO_RCVBUF to {requested} bytes on the UDP syslog socket: {error}")
    });
    let granted = rustix::net::sockopt::socket_recv_buffer_size(socket).map_err(|error| {
        io::Error::other(format!(
            "cannot read back SO_RCVBUF of the UDP syslog socket: {error}"
        ))
    })?;
    Ok(Buffer {
        requested,
        granted,
        enlarge_error,
    })
}

/// The sysctl `net.core.rmem_max` when the process can read it. A unit with `ProcSubset=pid`
/// hides `/proc/sys`, so `None` is expected there.
pub(crate) fn rmem_max() -> Option<u64> {
    std::fs::read_to_string(RMEM_MAX).ok()?.trim().parse().ok()
}

/// Why a `/proc/net/udp` table could not give a count.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TableError {
    /// The text has no header row ending in `drops`.
    NoHeader,
    /// A row (1-based line number) lacks the inode or drops column or holds a non-number.
    Malformed(usize),
}
impl std::fmt::Display for TableError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHeader => out.write_str("no header row ending in a drops column"),
            Self::Malformed(line) => write!(out, "line {line} is not a socket row"),
        }
    }
}

/// Column of `inode` and the least columns of a socket row; `drops` is the last.
const INODE_COLUMN: usize = 9;
const ROW_COLUMNS: usize = 13;

/// The `drops` of the row whose inode is `inode`: `Ok(None)` when no row has it. A row that is
/// not a socket row is an error, never a zero.
pub(crate) fn parse_drops(text: &str, inode: u64) -> Result<Option<u64>, TableError> {
    let mut lines = text.lines().enumerate();
    match lines.next() {
        Some((_, header))
            if header.split_whitespace().next() == Some("sl")
                && header.split_whitespace().next_back() == Some("drops") => {}
        _ => return Err(TableError::NoHeader),
    }
    let mut found = None;
    for (index, line) in lines {
        if line.trim().is_empty() {
            continue;
        }
        let malformed = || TableError::Malformed(index + 1);
        let columns: Vec<&str> = line.split_whitespace().collect();
        if columns.len() < ROW_COLUMNS {
            return Err(malformed());
        }
        let row_inode: u64 = columns[INODE_COLUMN].parse().map_err(|_| malformed())?;
        let drops: u64 = columns[columns.len() - 1]
            .parse()
            .map_err(|_| malformed())?;
        if row_inode == inode {
            found = Some(drops);
        }
    }
    Ok(found)
}

/// Whether the kernel drop count could be read at the last sample.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Counter {
    Available(u64),
    Unavailable(String),
}

/// The receive buffer and the drop counter of one socket.
#[derive(Clone, Debug)]
pub(crate) struct Kernel {
    pub buffer: Buffer,
    pub counter: Counter,
    /// Set once the socket has closed: the counter then holds its final value and is never read
    /// again, since the socket's row is gone from the table.
    pub finished: bool,
    table: PathBuf,
    inode: u64,
}
impl Kernel {
    /// Bind the evidence to `socket`, bound at `address`: its inode names its row. The first
    /// sample is taken here.
    pub(crate) fn new(socket: impl AsFd, address: SocketAddr, buffer: Buffer) -> io::Result<Self> {
        let inode = rustix::fs::fstat(socket.as_fd())
            .map_err(|error| {
                io::Error::other(format!("cannot stat the UDP syslog socket: {error}"))
            })?
            .st_ino;
        let table = if address.is_ipv6() {
            TABLE_V6
        } else {
            TABLE_V4
        };
        Ok(Self::at(PathBuf::from(table), inode, buffer))
    }

    /// The same for a table at another path.
    pub(crate) fn at(table: PathBuf, inode: u64, buffer: Buffer) -> Self {
        let mut kernel = Self {
            buffer,
            counter: Counter::Unavailable("not sampled".into()),
            finished: false,
            table,
            inode,
        };
        kernel.sample();
        kernel
    }

    /// Read the table now. A failure replaces any earlier count with its reason.
    pub(crate) fn sample(&mut self) {
        let counter = self.read();
        self.record(counter);
    }

    /// Keep a count read by [`Kernel::read`], unless the socket has closed and the count is final.
    pub(crate) fn record(&mut self, counter: Counter) {
        if !self.finished {
            self.counter = counter;
        }
    }

    /// Take the last sample and freeze the counter. Call while the socket is still open.
    pub(crate) fn finish(&mut self) {
        self.sample();
        self.finished = true;
    }

    /// Read the table, without touching the saved count, so a caller can do the file read
    /// outside any lock.
    pub(crate) fn read(&self) -> Counter {
        let table = self.table.display();
        match std::fs::read_to_string(&self.table) {
            Err(error) => Counter::Unavailable(format!("cannot read {table}: {error}")),
            Ok(text) => match parse_drops(&text, self.inode) {
                Err(error) => Counter::Unavailable(format!("cannot parse {table}: {error}")),
                Ok(None) => Counter::Unavailable(format!(
                    "{table} has no row for socket inode {}",
                    self.inode
                )),
                Ok(Some(drops)) => Counter::Available(drops),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows as Linux prints them: two sockets on `udp`, the second one with 17 drops.
    const V4: &str = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  101: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 40001 2 0000000000000000 0
  102: 0100007F:E4A1 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 40002 2 0000000000000000 17
";
    const HEADER: &str = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n";
    const ROW_777_5: &str = "  1: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 777 2 0000000000000000 5\n";
    const V6: &str = "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  201: 00000000000000000000000001000000:0202 00000000000000000000000000000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 50001 2 0000000000000000 9
";

    /// Value: the count must belong to our socket. Another socket's row (a different inode) must
    /// not be read as ours, and a socket the table does not list has no count, not zero.
    #[test]
    fn the_count_is_the_row_of_the_socket_inode_and_a_missing_row_is_no_count() {
        assert_eq!(parse_drops(V4, 40002), Ok(Some(17)));
        assert_eq!(parse_drops(V4, 40001), Ok(Some(0)));
        assert_eq!(parse_drops(V4, 99999), Ok(None));
    }

    /// Value: the IPv6 table has a different header and wider addresses; the same parser must
    /// read it, since a listener may be bound to an IPv6 loopback address.
    #[test]
    fn the_ipv6_table_gives_its_count() {
        assert_eq!(parse_drops(V6, 50001), Ok(Some(9)));
    }

    /// Value: a damaged table must be an error. Reading a short row or an empty text as zero
    /// would report "no loss" about a socket whose loss is unknown.
    #[test]
    fn a_damaged_table_is_an_error_never_a_zero() {
        let short = V4.replace(" 2 0000000000000000 17", "");
        assert_eq!(parse_drops(&short, 40002), Err(TableError::Malformed(3)));
        let word = V4.replace("40002", "forty");
        assert_eq!(parse_drops(&word, 40002), Err(TableError::Malformed(3)));
        assert_eq!(parse_drops("", 40002), Err(TableError::NoHeader));
        assert_eq!(parse_drops("garbage\n", 40002), Err(TableError::NoHeader));
    }

    /// Value: a table that cannot be read (a unit hiding it, a missing file) must say
    /// unavailable with the reason and carry no count, so status never invents one.
    #[test]
    fn an_unreadable_table_is_unavailable_with_its_reason() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = Kernel::at(
            dir.path().join("absent"),
            1,
            Buffer {
                requested: 1,
                granted: 1,
                enlarge_error: None,
            },
        );
        let Counter::Unavailable(reason) = &kernel.counter else {
            panic!("an unreadable table gave a count: {:?}", kernel.counter);
        };
        assert!(reason.contains("cannot read") && reason.contains("absent"));
    }

    /// Value: the buffer request reaches the kernel and the reported size is what getsockopt
    /// returns. The kernel stores twice the request (clamped to rmem_max, never below its
    /// floor), so the assertion is on the reported value: it is within the floor and double the
    /// request, and equals double the clamp when the sysctl can be read.
    #[test]
    fn the_granted_size_is_what_the_kernel_reports() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let buffer = request_buffer(&socket, REQUESTED_RCVBUF_BYTES).unwrap();
        assert_eq!(buffer.requested, REQUESTED_RCVBUF_BYTES);
        assert!(
            (4096..=2 * REQUESTED_RCVBUF_BYTES).contains(&buffer.granted),
            "granted {}",
            buffer.granted
        );
        if let Some(limit) = rmem_max() {
            let clamped = usize::try_from(limit).unwrap().min(REQUESTED_RCVBUF_BYTES);
            assert_eq!(buffer.granted, 2 * clamped);
        }
    }

    /// Value (#254): a refused enlarge request must not stop the daemon taking syslog. The
    /// refusal comes back as text beside the size the socket kept, and a failing read-back
    /// (a broken socket) is still an error. The kernel refusal is simulated through the seam.
    #[test]
    fn a_refused_enlarge_is_reported_and_the_socket_keeps_its_default_buffer() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let buffer = request_buffer_with(&socket, REQUESTED_RCVBUF_BYTES, |_, _| {
            Err(rustix::io::Errno::PERM)
        })
        .unwrap();
        assert_eq!(buffer.requested, REQUESTED_RCVBUF_BYTES);
        assert!(buffer.granted >= 4096 && buffer.granted < REQUESTED_RCVBUF_BYTES);
        let reason = buffer.enlarge_error.expect("the refusal is reported");
        assert!(
            reason.contains("SO_RCVBUF") && reason.contains("8388608"),
            "{reason}"
        );
    }

    /// Value (#254): the final count survives the socket closing. After `finish` the row may
    /// vanish from the table (the socket is closed) and the count must not turn into
    /// "unavailable".
    #[test]
    fn a_finished_counter_keeps_its_final_count_after_its_row_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("udp");
        std::fs::write(&table, format!("{HEADER}{ROW_777_5}")).unwrap();
        let buffer = Buffer {
            requested: 1,
            granted: 1,
            enlarge_error: None,
        };
        let mut kernel = Kernel::at(table.clone(), 777, buffer);
        kernel.finish();
        std::fs::write(&table, HEADER).unwrap();
        kernel.sample();
        assert_eq!(kernel.counter, Counter::Available(5));
        assert!(kernel.finished);
    }

    /// Value: the drop count is the kernel's own, for the right socket. A tiny buffer, a burst
    /// sent while nothing reads, then reading everything left: datagrams read plus the counted
    /// drops equal the datagrams sent. Loopback delivery and its drops happen inside `send`, so
    /// the test waits on nothing.
    #[test]
    fn counted_drops_plus_datagrams_read_equal_datagrams_sent() {
        const SENT: u64 = 200;
        let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = receiver.local_addr().unwrap();
        let buffer = request_buffer(&receiver, 1).unwrap();
        let mut kernel = Kernel::new(&receiver, address, buffer).unwrap();
        assert_eq!(kernel.counter, Counter::Available(0));
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for _ in 0..SENT {
            sender.send_to(&[b'x'; 1000], address).unwrap();
        }
        receiver.set_nonblocking(true).unwrap();
        let mut read = 0;
        let mut datagram = [0; 2048];
        while receiver.recv_from(&mut datagram).is_ok() {
            read += 1;
        }
        kernel.sample();
        let Counter::Available(drops) = kernel.counter else {
            panic!("the counter was not readable: {:?}", kernel.counter);
        };
        assert!(drops > 0, "a burst into a minimal buffer dropped nothing");
        // Delivery and its drop happen inside `send` on stock loopback, so nothing is in flight
        // and the sum is exact; a host that defers loopback work (RPS, softirq backlog) may still
        // hold a few datagrams, so the test allows a small bounded number and never more datagrams
        // than were sent.
        assert!(
            read + drops <= SENT,
            "read {read} + drops {drops} > sent {SENT}"
        );
        assert!(
            SENT - read - drops <= 8,
            "read {read} + drops {drops} of {SENT} sent"
        );
    }
}
