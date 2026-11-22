// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::*;

#[test]
fn native_terms_keep_scoped_blanks_direction_and_triples_across_flat_transport() {
    let value = TermValue::Triple {
        s: TermBox::new(TermValue::Blank {
            label: "same-label".into(),
            scope: BlankScope(37),
        }),
        p: TermBox::new(TermValue::iri("urn:predicate")),
        o: TermBox::new(TermValue::Literal {
            lexical_form: "مرحبا\n\"\\".into(),
            datatype: "http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString".into(),
            language: Some("ar".into()),
            direction: Some(RdfTextDirection::Rtl),
        }),
    };
    let other = TermValue::Blank {
        label: "same-label".into(),
        scope: BlankScope(56),
    };
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<wire::sparql_data::Builder<'_>>();
    let ids = encode(&[&value, &other], |n| root.reborrow().init_terms(n)).unwrap();
    let decoded = decode(root.into_reader().get_terms().unwrap(), &ids, true).unwrap();
    assert_eq!(decoded, vec![value, other]);
}

#[test]
fn compact_dags_and_unreachable_terms_are_refused_before_expansion() {
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<wire::sparql_data::Builder<'_>>();
    let mut table = root.reborrow().init_terms(20);
    table.reborrow().get(0).set_iri("urn:x");
    for index in 1..20 {
        let mut node = table.reborrow().get(index).init_triple();
        node.set_subject(index - 1);
        node.set_predicate(index - 1);
        node.set_object(index - 1);
    }
    assert_eq!(
        decode(root.into_reader().get_terms().unwrap(), &[19], false)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<wire::sparql_data::Builder<'_>>();
    let mut table = root.reborrow().init_terms(2);
    table.reborrow().get(0).set_iri("urn:x");
    table.get(1).set_iri("urn:unreachable");
    assert_eq!(
        decode(root.into_reader().get_terms().unwrap(), &[0], false)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn binding_semantics_are_validated_by_the_native_projection_codec() {
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<wire::sparql_data::Builder<'_>>();
    let mut table = root.reborrow().init_terms(1);
    let mut literal = table.reborrow().get(0).init_literal();
    literal.set_lexical("x");
    literal.set_datatype("urn:custom");
    literal.init_language().set_value("ar");
    assert_eq!(
        decode(root.into_reader().get_terms().unwrap(), &[0], true)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn diagnostic_transport_preserves_native_machine_fields_and_full_u128_capacity() {
    use purrdf_lex::{
        allocation::{CapacityError, CapacitySpace, StorageError},
        diagnostic::{DiagnosticParameter, DiagnosticPresentation, DiagnosticValue},
    };
    let amount = u128::MAX - 37;
    let native = purrdf::RdfDiagnostic::from(StorageError::Capacity(CapacityError::new(
        CapacitySpace::LocalTermBytes,
        amount,
        u128::from(u64::MAX) + 1,
    )))
    .with_location(
        purrdf::RdfLocation::file("query.rq")
            .with_line(9007199254740993)
            .with_column(17)
            .with_gts_term(56),
    )
    .with_detail("detail\n\"")
    .with_presentation(
        DiagnosticPresentation::new(
            "capacity.test",
            "requested {count}",
            vec![DiagnosticParameter::new(
                "count",
                DiagnosticValue::Unsigned(u64::MAX),
            )],
        )
        .unwrap(),
    );
    let mut message = capnp::message::Builder::new_default();
    let out = message.init_root::<wire::native_diagnostic::Builder<'_>>();
    diagnostic::encode(out, &native).unwrap();
    let mut actual = diagnostic::render(
        message
            .get_root_as_reader::<wire::native_diagnostic::Reader<'_>>()
            .unwrap(),
    )
    .unwrap();
    if let purrdf_lex::json::Value::Object(ref mut fields) = actual {
        fields.remove("storage_failure");
    }
    assert_eq!(actual, native.to_json());
    assert_eq!(
        actual["capacity"]["attempted"].as_str(),
        Some(amount.to_string().as_str())
    );
}
