// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One bounded single-message container for immutable producer retry intents.

use crate::Error;
use capnp::traits::Owned;
use std::io::Read;

pub(crate) trait EncodeIntent {
    type Wire: Owned;
    fn encode_intent(&self, out: <Self::Wire as Owned>::Builder<'_>) -> Result<(), Error>;
}

pub(crate) fn write<I: EncodeIntent>(output: impl std::io::Write, intent: &I) -> Result<(), Error> {
    let mut message = capnp::message::Builder::new_default();
    intent.encode_intent(message.init_root::<<I::Wire as Owned>::Builder<'_>>())?;
    capnp::serialize::write_message(output, &message)?;
    Ok(())
}

pub(crate) fn read<T: Owned, R>(
    mut input: impl Read,
    decode: impl for<'a> FnOnce(T::Reader<'a>) -> Result<R, Error>,
) -> Result<R, Error> {
    let mut bytes = Vec::new();
    input.by_ref().take(16_385).read_to_end(&mut bytes)?;
    if bytes.len() > 16_384 {
        return Err(Error::InvalidArgument("producer intent exceeds 16 KiB"));
    }
    let mut slice = bytes.as_slice();
    let message = capnp::serialize::read_message(
        &mut slice,
        capnp::message::ReaderOptions {
            traversal_limit_in_words: Some(2048),
            nesting_limit: 16,
        },
    )?;
    if !slice.is_empty() {
        return Err(Error::InvalidArgument("producer intent has trailing bytes"));
    }
    decode(message.get_root::<T::Reader<'_>>()?)
}
