// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One typed transport adapter over native RDF values and native tree walks.

use crate::sparrow_capnp as wire;
use purrdf::{
    BlankScope, RdfTextDirection, TermValue,
    ir::{Nested, TermBox},
};
use sparrow_core::Error;
use std::ops::ControlFlow;

pub(crate) mod diagnostic;
#[cfg(test)]
mod tests;

pub(crate) const MAX_NODES: usize = 512;
pub(crate) const MAX_BYTES: usize = 256 * 1024;

use crate::rpc::invalid;
fn capacity() -> Error {
    Error::new(
        "RESOURCE_LIMIT",
        "RDF transport forest exceeds 512 expanded nodes or 256 KiB admission",
    )
}

fn language(value: wire::optional_text::Reader<'_>) -> Result<Option<&str>, Error> {
    match value.which().map_err(invalid)? {
        wire::optional_text::Absent(()) => Ok(None),
        wire::optional_text::Value(value) => {
            Ok(Some(value.map_err(invalid)?.to_str().map_err(invalid)?))
        }
    }
}

fn direction(value: wire::RdfDirection) -> Option<RdfTextDirection> {
    match value {
        wire::RdfDirection::Absent => None,
        wire::RdfDirection::Ltr => Some(RdfTextDirection::Ltr),
        wire::RdfDirection::Rtl => Some(RdfTextDirection::Rtl),
    }
}

fn leaf(value: wire::rdf_term_node::Reader<'_>) -> Result<TermValue, Error> {
    Ok(match value.which().map_err(invalid)? {
        wire::rdf_term_node::Iri(text) => {
            TermValue::Iri(text.map_err(invalid)?.to_str().map_err(invalid)?.to_owned())
        }
        wire::rdf_term_node::Blank(value) => {
            let value = value.map_err(invalid)?;
            TermValue::Blank {
                label: value
                    .get_label()
                    .map_err(invalid)?
                    .to_str()
                    .map_err(invalid)?
                    .to_owned(),
                scope: BlankScope(value.get_scope()),
            }
        }
        wire::rdf_term_node::Literal(value) => {
            let value = value.map_err(invalid)?;
            TermValue::Literal {
                lexical_form: value
                    .get_lexical()
                    .map_err(invalid)?
                    .to_str()
                    .map_err(invalid)?
                    .to_owned(),
                datatype: value
                    .get_datatype()
                    .map_err(invalid)?
                    .to_str()
                    .map_err(invalid)?
                    .to_owned(),
                language: language(value.get_language().map_err(invalid)?)?.map(str::to_owned),
                direction: direction(value.get_direction().map_err(invalid)?),
            }
        }
        wire::rdf_term_node::Triple(_) => {
            return Err(invalid("triple node reached the leaf adapter"));
        }
    })
}

fn children(value: wire::rdf_term_node::Reader<'_>) -> Result<Option<[u32; 3]>, Error> {
    if let wire::rdf_term_node::Triple(value) = value.which().map_err(invalid)? {
        let value = value.map_err(invalid)?;
        Ok(Some([
            value.get_subject(),
            value.get_predicate(),
            value.get_object(),
        ]))
    } else {
        Ok(None)
    }
}

/// Decode only referenced roots after pricing the expanded forest. Shared node
/// references cannot amplify a compact DAG into unbounded owned term trees.
pub(crate) fn decode(
    nodes: capnp::struct_list::Reader<'_, wire::rdf_term_node::Owned>,
    roots: &[u32],
    validate_bindings: bool,
) -> Result<Vec<TermValue>, Error> {
    if nodes.len() as usize > MAX_NODES || roots.len() > MAX_NODES {
        return Err(capacity());
    }
    let mut costs = Vec::<usize>::new();
    costs
        .try_reserve_exact(nodes.len() as usize)
        .map_err(|_| capacity())?;
    let mut raw = 0usize;
    for (index, node) in nodes.iter().enumerate() {
        let (text, links) = match node.which().map_err(invalid)? {
            wire::rdf_term_node::Iri(text) => (text.map_err(invalid)?.len(), None),
            wire::rdf_term_node::Blank(value) => (
                value.map_err(invalid)?.get_label().map_err(invalid)?.len(),
                None,
            ),
            wire::rdf_term_node::Literal(value) => {
                let value = value.map_err(invalid)?;
                value.get_direction().map_err(invalid)?;
                let lang = language(value.get_language().map_err(invalid)?)?.map_or(0, str::len);
                let text = value
                    .get_lexical()
                    .map_err(invalid)?
                    .len()
                    .checked_add(value.get_datatype().map_err(invalid)?.len())
                    .and_then(|n| n.checked_add(lang))
                    .ok_or_else(capacity)?;
                (text, None)
            }
            wire::rdf_term_node::Triple(_) => (0, children(node)?),
        };
        let own = sparrow_core::sparql_term_node_bytes(text).ok_or_else(capacity)?;
        raw = raw
            .checked_add(own)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or_else(capacity)?;
        let mut expanded = own;
        for link in links.into_iter().flatten() {
            if link as usize >= index {
                return Err(invalid("RDF term links must reference earlier nodes"));
            }
            expanded = expanded
                .checked_add(costs[link as usize])
                .filter(|n| *n <= MAX_BYTES)
                .ok_or_else(capacity)?;
        }
        costs.push(expanded);
    }
    let mut total = 0usize;
    for root in roots {
        let cost = costs
            .get(*root as usize)
            .ok_or_else(|| invalid("RDF root is outside the term table"))?;
        total = total
            .checked_add(*cost)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or_else(capacity)?;
    }
    let mut visited = [false; MAX_NODES];
    let mut values = Vec::new();
    values
        .try_reserve_exact(roots.len())
        .map_err(|_| capacity())?;
    for root in roots {
        let value = purrdf::ir::try_fold_nested(
            *root,
            &mut visited,
            |visited, id| {
                visited[id as usize] = true;
                let node = nodes.get(id);
                Ok(match children(node)? {
                    Some([s, p, o]) => Nested::Triple(s, p, o),
                    None => Nested::Leaf(leaf(node)?),
                })
            },
            |_, _, s, p, o| {
                Ok::<_, Error>(TermValue::Triple {
                    s: TermBox::new(s),
                    p: TermBox::new(p),
                    o: TermBox::new(o),
                })
            },
        )?;
        if validate_bindings {
            let limits =
                purrdf::projections::ProjectionLimits::new(1, MAX_BYTES, MAX_BYTES, MAX_BYTES, 16)
                    .map_err(invalid)?;
            purrdf::projections::ProjectionTerm::from_term_value(&value, limits)
                .map_err(invalid)?;
        }
        values.push(value);
    }
    if visited[..nodes.len() as usize]
        .iter()
        .any(|visited| !visited)
    {
        return Err(invalid("RDF forest contains unreachable nodes"));
    }
    Ok(values)
}

enum Node<'a> {
    Leaf(&'a TermValue),
    Triple([u32; 3]),
}

/// Encode native roots once, with page-local links and no recursive wire structs.
pub(crate) fn encode<'a>(
    roots: &[&TermValue],
    init: impl FnOnce(u32) -> capnp::struct_list::Builder<'a, wire::rdf_term_node::Owned>,
) -> Result<Vec<u32>, Error> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for root in roots {
        bytes = bytes
            .checked_add(sparrow_core::sparql_term_wire_bytes(root)?)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or_else(capacity)?;
        if let ControlFlow::Break(()) = root.visit_terms(|_| {
            count += 1;
            if count > MAX_NODES {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }) {
            return Err(capacity());
        }
    }
    let mut nodes = Vec::<Node<'_>>::new();
    nodes.try_reserve_exact(count).map_err(|_| capacity())?;
    let mut root_ids = Vec::new();
    root_ids
        .try_reserve_exact(roots.len())
        .map_err(|_| capacity())?;
    for root in roots {
        let id = purrdf::ir::try_fold_nested(
            *root,
            &mut nodes,
            |nodes, term| {
                Ok::<_, Error>(match term {
                    TermValue::Triple { s, p, o } => Nested::Triple(&**s, &**p, &**o),
                    _ => {
                        let id = nodes.len() as u32;
                        nodes.push(Node::Leaf(term));
                        Nested::Leaf(id)
                    }
                })
            },
            |nodes, _, s, p, o| {
                let id = nodes.len() as u32;
                nodes.push(Node::Triple([s, p, o]));
                Ok::<_, Error>(id)
            },
        )?;
        root_ids.push(id);
    }
    let mut table = init(nodes.len() as u32);
    for (index, node) in nodes.into_iter().enumerate() {
        let mut target = table.reborrow().get(index as u32);
        match node {
            Node::Leaf(TermValue::Iri(text)) => target.set_iri(text),
            Node::Leaf(TermValue::Blank { label, scope }) => {
                let mut blank = target.init_blank();
                blank.set_label(label);
                blank.set_scope(scope.ordinal());
            }
            Node::Leaf(TermValue::Literal {
                lexical_form,
                datatype,
                language,
                direction,
            }) => {
                let mut literal = target.init_literal();
                literal.set_lexical(lexical_form);
                literal.set_datatype(datatype);
                literal.set_direction(match direction {
                    None => wire::RdfDirection::Absent,
                    Some(RdfTextDirection::Ltr) => wire::RdfDirection::Ltr,
                    Some(RdfTextDirection::Rtl) => wire::RdfDirection::Rtl,
                });
                sparrow_client::encode_optional(
                    literal.init_language(),
                    language.as_deref(),
                    |mut out, v| out.set_value(v),
                    |mut out| out.set_absent(()),
                );
            }
            Node::Leaf(TermValue::Triple { .. }) => {
                return Err(invalid("native triple reached the leaf encoder"));
            }
            Node::Triple([s, p, o]) => {
                let mut triple = target.init_triple();
                triple.set_subject(s);
                triple.set_predicate(p);
                triple.set_object(o);
            }
        }
    }
    Ok(root_ids)
}
