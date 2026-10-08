// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One typed ephemeral-result mutation and receipt certificate.

use crate::{Discovery, Error, sparrow_capnp as wire};

/// Certified result lease retirement; evidence retention is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultReleaseReceipt {
    /// Original persistent store identity.
    pub store_id: String,
    /// Original captured result identity, not a page cursor.
    pub snapshot_id: String,
    /// True only when this call removed an active retained capture.
    pub released: bool,
}

/// Release once or confirm absence; a lost reply can be retried with the same ID.
pub async fn release_result(
    client: &wire::sparrow::Client,
    store_id: &str,
    snapshot_id: &str,
) -> Result<ResultReleaseReceipt, Error> {
    if store_id.is_empty()
        || store_id.len() > 128
        || snapshot_id.is_empty()
        || snapshot_id.len() > 128
        || snapshot_id.contains('/')
    {
        return Err(Error::InvalidArgument(
            "result release requires a bounded store and snapshot ID, not a page cursor",
        ));
    }
    Discovery::fetch(client).await?;
    let mut call = client.result_release_request();
    let mut query = call.get().init_query();
    query.set_schema_version(crate::SCHEMA_VERSION);
    query.set_store_id(store_id);
    query.set_snapshot_id(snapshot_id);
    let response = call.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        certify(value, store_id, snapshot_id)
    })
}

fn certify(
    value: wire::result_released::Reader<'_>,
    store: &str,
    snapshot: &str,
) -> Result<ResultReleaseReceipt, Error> {
    if value.get_store_id()?.to_str()? != store || value.get_snapshot_id()?.to_str()? != snapshot {
        return Err(Error::Protocol(
            "result release receipt changed its store or snapshot identity",
        ));
    }
    Ok(ResultReleaseReceipt {
        store_id: store.into(),
        snapshot_id: snapshot.into(),
        released: value.get_released(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_certificate_preserves_outcome_and_rejects_changed_identity() {
        for released in [false, true] {
            let mut message = capnp::message::Builder::new_default();
            let mut out = message.init_root::<wire::result_released::Builder<'_>>();
            out.set_store_id("store:expected");
            out.set_snapshot_id("record-read:expected");
            out.set_released(released);
            let reader = message
                .get_root_as_reader::<wire::result_released::Reader<'_>>()
                .unwrap();
            assert_eq!(
                certify(reader, "store:expected", "record-read:expected")
                    .unwrap()
                    .released,
                released
            );
            for (store, snapshot) in [
                ("store:other", "record-read:expected"),
                ("store:expected", "record-read:other"),
            ] {
                assert!(matches!(
                    certify(reader, store, snapshot),
                    Err(Error::Protocol(_))
                ));
            }
        }
    }
}
