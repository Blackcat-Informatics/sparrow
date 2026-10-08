// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Stable-ID pin renewal and one pin receipt renderer for both agent adapters.

use super::{Result, failure, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};
pub(super) mod read;
pub(crate) use read::Read;

pub(crate) struct Renewal {
    pub request: String,
    pub store: String,
    pub id: String,
    pub days: u32,
    pub expected_revision: Option<u64>,
}

impl Renewal {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        if !(4..=5).contains(&args.len()) {
            return Err(invalid(
                "usage: pin-renew REQUEST_ID STORE_ID PIN_ID DAYS [EXPECTED_REVISION]; integer days, decimal UInt64 revision",
            ));
        }
        Ok(Self {
            request: args[0].clone(),
            store: args[1].clone(),
            id: args[2].clone(),
            days: args[3]
                .parse()
                .map_err(|_| invalid("DAYS requires a UInt32 integer"))?,
            expected_revision: args.get(4).map(|value| revision(value)).transpose()?,
        })
    }

    pub(crate) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let mut call = client.pin_renew_request();
        let mut input = call.get().init_request();
        input.set_request_id(&self.request);
        input.set_store_id(&self.store);
        input.set_pin_id(&self.id);
        input.set_days(self.days);
        let mut revision = input.init_expected_revision();
        match self.expected_revision {
            Some(value) => revision.set_value(value),
            None => revision.set_absent(()),
        }
        let response = call.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => render(value?),
            wire::result::Error(error) => Err(failure(error?)),
        }
    }
}

pub(crate) fn revision(value: &str) -> Result<u64> {
    if value.is_empty() || !value.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(invalid(
            "expected_revision requires a decimal UInt64 string",
        ));
    }
    value
        .parse()
        .map_err(|_| invalid("expected_revision exceeds UInt64"))
}

pub(super) fn member_ids(
    list: capnp::struct_list::Reader<'_, wire::evidence::Owned>,
) -> Result<Value> {
    let members = list
        .iter()
        .map(|member| {
            let id = match member.which()? {
                wire::evidence::Artifact(id) | wire::evidence::Record(id) => {
                    id?.to_str()?.to_owned()
                }
            };
            Ok(Value::from(id))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::array(members))
}

pub(crate) fn render(value: wire::pin::Reader<'_>) -> Result<Value> {
    let count = value.get_members()?.len();
    let members = member_ids(value.get_members()?)?;
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("request_id", value.get_request_id()?.to_str()?)
        .with("pin_id", value.get_id()?.to_str()?)
        .with("name", value.get_name()?.to_str()?)
        .with("expires_at", instant(value.get_expires_at()?)?)
        .with("revision", value.get_revision().to_string())
        .with("direct_member_count", count.to_string())
        .with(
            "durability",
            match value.get_durability()? {
                wire::Durability::Buffered => "buffered",
                wire::Durability::Durable => "durable",
            },
        )
        .with("members", members)
        .into())
}
