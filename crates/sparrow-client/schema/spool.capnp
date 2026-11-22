@0xa9bcc3513dfe918a;
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
using Sparrow = import "sparrow.capnp";

# The checkpoint is replaced atomically. Its cursor points immediately after the
# last certified event; its optional seal describes the exact subsequent request.
struct Header {
  format @0 :Text;
  source @1 :Text;
  instance @2 :Data;
  store @3 :Text;
  spanEpoch @4 :UInt32;
  confirmed @5 :UInt64;
  confirmedToken @6 :Data;
  nextBatch @7 :UInt64;
  cursor @8 :Position;
  seal @9 :Seal;
  artifact @10 :Text;
}
struct Position { segment @0 :UInt64; offset @1 :UInt64; }
struct Seal { lastSequence @0 :UInt64; deadline @1 :UInt64; }
struct Entry { event @0 :Sparrow.ProducerEvent; token @1 :Data; at @2 :UInt64; }
