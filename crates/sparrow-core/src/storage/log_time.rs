// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Explicit whole-record capture through PurRDF's bounded native pattern engine.

use super::{
    Error,
    source_time::{EventTime, SourceTimeProfile},
};
use purrdf_core::xsd_regex::xpath::{
    self, CompiledPattern, Limits, PatternCache, Profile, Resource,
};
use std::sync::{Arc, Mutex, OnceLock};

pub(super) const LAW: &str =
    "sparrow-log-time/1;whole-record;xpath-3.1-2017-03-21;utf8;native-capture";

/// One explicit capture supplying the source timestamp; no field/unit guessing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogTimeCapture {
    /// Native XPath 3.1 pattern, matched against the complete original log record.
    pub pattern: String,
    /// Native group number, including zero for the complete record.
    pub group: u32,
    /// Optional native `i` and `s` flags; multiline and quoting are excluded.
    pub flags: String,
}

fn limits() -> Limits {
    Limits::new()
        .with(Resource::PatternBytes, 8192)
        .with(Resource::CompileSteps, 100_000)
        .with(Resource::ProgramNodes, 16_384)
        .with(Resource::CompileSlots, 65_536)
        .with(Resource::MatchSteps, 2_000_000)
        .with(Resource::MatchStates, 4096)
        .with(Resource::MatchSlots, 65_536)
}

fn native_error(error: xpath::Error) -> Error {
    Error::new(
        if error.is_operational() {
            "RESOURCE_LIMIT"
        } else {
            "INVALID_ARGUMENT"
        },
        format!("native log timestamp pattern: {error}"),
    )
}

impl LogTimeCapture {
    /// Explicit interpretation law and finite native matcher controls.
    pub fn descriptor(&self) -> purrdf_lex::json::Value {
        use purrdf_lex::json::Object;
        Object::new()
            .with("kind", "log_capture")
            .with("law", LAW)
            .with("pattern", self.pattern.clone())
            .with("group", self.group)
            .with("flags", self.flags.clone())
            .with("regex_profile", Profile::Xpath31.name())
            .with(
                "compile_steps",
                limits().limit(Resource::CompileSteps).to_string(),
            )
            .with(
                "match_steps",
                limits().limit(Resource::MatchSteps).to_string(),
            )
            .into()
    }
    pub(super) fn validate_controls(&self) -> Result<(), Error> {
        if self.pattern.len() > 4096
            || !["", "i", "s", "is", "si"].contains(&self.flags.as_str())
            || self.group > 32
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "log time pattern admits at most 4096 bytes, group 0..32 and only i/s flags",
            ));
        }
        Ok(())
    }
    pub(super) fn compiled(&self) -> Result<Arc<CompiledPattern>, Error> {
        self.validate_controls()?;
        // The native noncapturing wrapper preserves caller group numbering.
        let pattern = format!("^(?:{})$", self.pattern);
        static CACHE: OnceLock<Mutex<PatternCache>> = OnceLock::new();
        let program = CACHE
            .get_or_init(|| Mutex::new(PatternCache::default()))
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "native log pattern cache poisoned"))?
            .compiled_shared(Profile::Xpath31, &pattern, &self.flags, limits())
            .map_err(native_error)?;
        if self.group as usize > program.capture_count() {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "log time group exceeds the native capture inventory",
            ));
        }
        Ok(program)
    }

    pub(super) fn capture(
        &self,
        profile: &SourceTimeProfile,
        bytes: Option<&[u8]>,
        start: u64,
        end: u64,
    ) -> Result<EventTime, Error> {
        let Some(bytes) = bytes else {
            return Ok(EventTime::unknown("log_input_unavailable", Some(profile)));
        };
        let Ok(text) = std::str::from_utf8(bytes) else {
            return Ok(EventTime::unknown("log_invalid_utf8", Some(profile)));
        };
        let program = self.compiled().map_err(|error| {
            if error.code == "INVALID_ARGUMENT" {
                Error::new(
                    "STORAGE_FAILURE",
                    format!("stored log time pattern is invalid: {error}"),
                )
            } else {
                error
            }
        })?;
        let Some(captures) = program.find(text, limits()).map_err(native_error)? else {
            return Ok(EventTime::unknown("log_no_match", Some(profile)));
        };
        if captures.get(0) != Some(0..bytes.len()) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "whole-record native pattern returned a partial match",
            ));
        }
        let Some(range) = captures.get(self.group as usize) else {
            return Ok(EventTime::unknown("log_capture_missing", Some(profile)));
        };
        let Some(millis) =
            super::source_time::interpret(&text[range.clone()], profile.representation)?
        else {
            return Ok(EventTime::unknown("invalid_instant", Some(profile)));
        };
        super::source_time::known(
            profile,
            millis,
            self.group as u64,
            start..end,
            range.start as u64..range.end as u64,
        )
    }
}
