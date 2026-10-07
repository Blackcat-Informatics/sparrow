// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! The census's JSON output: a value type and a pretty writer, nothing else.
//!
//! The census only ever writes JSON (the baseline, `--index`, `--dump-ledger`) and
//! never reads it, so this is a writer rather than a dependency. Object members are
//! held in a `BTreeMap`, so every document is emitted with its keys sorted at every
//! level: the published layout is a property of the type, not of a sort pass a
//! caller might forget.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// One JSON value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Json {
    /// `null`.
    Null,
    /// `true` or `false`.
    Bool(bool),
    /// A number, already in its JSON spelling (integers only).
    Number(String),
    /// A string.
    Str(String),
    /// An array.
    Array(Vec<Self>),
    /// An object, keys sorted.
    Object(BTreeMap<String, Self>),
}

/// A JSON object under construction.
#[derive(Clone, Debug, Default)]
pub(crate) struct Object(BTreeMap<String, Json>);

impl Object {
    /// An empty object.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The object with `key` set to `value`.
    pub(crate) fn with(mut self, key: &str, value: impl Into<Json>) -> Self {
        self.0.insert(key.to_owned(), value.into());
        self
    }

    /// Set `key` to `value`.
    pub(crate) fn insert(&mut self, key: String, value: impl Into<Json>) {
        self.0.insert(key, value.into());
    }
}

impl From<Object> for Json {
    fn from(object: Object) -> Self {
        Self::Object(object.0)
    }
}

impl From<bool> for Json {
    fn from(flag: bool) -> Self {
        Self::Bool(flag)
    }
}

impl From<usize> for Json {
    fn from(number: usize) -> Self {
        Self::Number(number.to_string())
    }
}

impl From<i64> for Json {
    fn from(number: i64) -> Self {
        Self::Number(number.to_string())
    }
}

impl From<&str> for Json {
    fn from(text: &str) -> Self {
        Self::Str(text.to_owned())
    }
}

impl From<String> for Json {
    fn from(text: String) -> Self {
        Self::Str(text)
    }
}

impl From<&String> for Json {
    fn from(text: &String) -> Self {
        Self::Str(text.clone())
    }
}

impl<T: Clone + Into<Self>> From<&[T]> for Json {
    fn from(items: &[T]) -> Self {
        items.iter().cloned().collect()
    }
}

impl<T: Into<Self>> From<Vec<T>> for Json {
    fn from(items: Vec<T>) -> Self {
        items.into_iter().collect()
    }
}

impl<T: Into<Self>> FromIterator<T> for Json {
    fn from_iter<I: IntoIterator<Item = T>>(items: I) -> Self {
        Self::Array(items.into_iter().map(Into::into).collect())
    }
}

impl Json {
    /// The value as pretty-printed JSON: two-space indentation, one member or
    /// element per line, empty containers on one line.
    pub(crate) fn pretty(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out
    }

    fn write(&self, out: &mut String, depth: usize) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
            Self::Number(number) => out.push_str(number),
            Self::Str(text) => write_string(out, text),
            Self::Array(items) if items.is_empty() => out.push_str("[]"),
            Self::Object(members) if members.is_empty() => out.push_str("{}"),
            Self::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    separate(out, index, depth + 1);
                    item.write(out, depth + 1);
                }
                close(out, depth, ']');
            }
            Self::Object(members) => {
                out.push('{');
                for (index, (key, value)) in members.iter().enumerate() {
                    separate(out, index, depth + 1);
                    write_string(out, key);
                    out.push_str(": ");
                    value.write(out, depth + 1);
                }
                close(out, depth, '}');
            }
        }
    }
}

/// The comma before every element but the first, then a new indented line.
fn separate(out: &mut String, index: usize, depth: usize) {
    if index > 0 {
        out.push(',');
    }
    out.push('\n');
    out.push_str(&"  ".repeat(depth));
}

/// A new line at `depth`, then the closing delimiter.
fn close(out: &mut String, depth: usize, delimiter: char) {
    out.push('\n');
    out.push_str(&"  ".repeat(depth));
    out.push(delimiter);
}

/// A JSON string literal: quotes, backslashes and control characters escaped.
fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if u32::from(control) < 0x20 => {
                // Writing into a `String` cannot fail.
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::{Json, Object};

    #[test]
    fn members_are_sorted_and_nested_values_are_indented() {
        let document = Json::from(
            Object::new()
                .with("b", vec!["x", "y"])
                .with("a", Object::new().with("n", 3_usize).with("t", true))
                .with("e", Vec::<Json>::new())
                .with("z", Json::Null),
        );
        assert_eq!(
            document.pretty(),
            "{\n  \"a\": {\n    \"n\": 3,\n    \"t\": true\n  },\n  \"b\": [\n    \"x\",\n    \"y\"\n  ],\n  \"e\": [],\n  \"z\": null\n}"
        );
    }

    #[test]
    fn strings_escape_quotes_backslashes_and_controls() {
        assert_eq!(
            Json::from("a\"b\\c\nd\u{1}").pretty(),
            "\"a\\\"b\\\\c\\nd\\u0001\""
        );
        assert_eq!(Json::from(-4_i64).pretty(), "-4");
    }
}
