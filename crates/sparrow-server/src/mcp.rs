// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded stdio MCP lifecycle and tool dispatch over the typed daemon client.

use crate::{AgentError, agent::tool};
use futures::{
    FutureExt, StreamExt,
    future::{AbortHandle, Abortable},
    stream::FuturesUnordered,
};
use purrdf_lex::json::{self, Object, Value, record::Record};
use sparrow_client::{Client, Endpoint};
use std::{collections::BTreeMap, io, time::Duration};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

const VERSION: &str = "2025-11-25";
const FRAME_LIMIT: usize = 128 * 1024;

/// Serve newline-delimited MCP on stdin/stdout without opening a database writer.
/// Call within a Tokio `LocalSet`; each tool uses the SDK's selected RPC endpoint.
pub async fn run_mcp(endpoint: &Endpoint) -> Result<(), Box<dyn std::error::Error>> {
    serve(
        endpoint,
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

async fn frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "MCP frame ended without newline",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let length = newline.map_or(available.len(), |index| index + 1);
        if frame.len() + length > FRAME_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP request exceeds 128 KiB",
            ));
        }
        frame.extend_from_slice(&available[..length]);
        reader.consume(length);
        if newline.is_some() {
            return Ok(Some(frame));
        }
    }
}

fn response(id: Value, result: Value) -> Value {
    Object::new()
        .with("jsonrpc", "2.0")
        .with("id", id)
        .with("result", result)
        .into()
}
fn protocol_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    Object::new()
        .with("jsonrpc", "2.0")
        .with("id", id)
        .with(
            "error",
            Object::new()
                .with("code", code)
                .with("message", message.into()),
        )
        .into()
}
fn tool_result(value: Value, error: bool) -> Value {
    Object::new()
        .with(
            "content",
            Value::array([Value::from(
                Object::new()
                    .with("type", "text")
                    .with("text", json::write_compact(&value)),
            )]),
        )
        .with("structuredContent", value)
        .with("isError", error)
        .into()
}

fn id_key(id: &Value) -> Option<String> {
    match id {
        Value::String(value) if value.len() <= 512 => Some(format!("s:{value}")),
        Value::Number(value) => value
            .lexeme()
            .parse::<i128>()
            .ok()
            .map(|value| format!("n:{value}")),
        _ => None,
    }
}

async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> io::Result<()> {
    let mut message = json::write_compact(value);
    if message.len() > 8 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP response exceeds 8 MiB",
        ));
    }
    message.push('\n');
    writer.write_all(message.as_bytes()).await?;
    writer.flush().await
}

async fn serve<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    endpoint: &Endpoint,
    mut reader: R,
    mut writer: W,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut initialized = false;
    let mut ready = false;
    let mut eof = false;
    let mut pending = BTreeMap::<String, AbortHandle>::new();
    let mut jobs = FuturesUnordered::new();
    // Read framing in a persistent future: select cancellation must not discard a partial line.
    let mut incoming = Box::pin(frame(&mut reader));
    loop {
        if eof && jobs.is_empty() {
            break;
        }
        tokio::select! {
            completed = jobs.next(), if !jobs.is_empty() => {
                if let Some((key, reply)) = completed {
                    pending.remove(&key);
                    if let Ok(reply) = reply { send(&mut writer, &reply).await?; }
                }
            }
            received = &mut incoming, if !eof => {
                let bytes = match received {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => { eof = true; continue; },
                    Err(error) => { send(&mut writer, &protocol_error(Value::Null, -32700, error.to_string())).await?; return Err(error.into()); }
                };
                drop(incoming); incoming = Box::pin(frame(&mut reader));
                let value = match json::read_slice(&bytes, json::Limits { max_depth: 64, max_values: 16384, max_string_bytes: FRAME_LIMIT, unique_members: true }) {
                    Ok(value) => value,
                    Err(error) => { send(&mut writer, &protocol_error(Value::Null, -32700, error.to_string())).await?; continue; }
                };
                let decode = || {
                    let mut record = Record::new(&value, "MCP request")?;
                    let version: String = record.required("jsonrpc")?;
                    let method: String = record.required("method")?;
                    let id = record.raw("id")?.cloned();
                    let params = record.raw("params")?.cloned().unwrap_or_else(|| Object::new().into());
                    record.deny_unknown()?;
                    Ok::<_, purrdf_lex::json::record::DecodeError>((version, method, id, params))
                };
                let (version, method, id, params) = match decode() {
                    Ok(request) => request,
                    Err(error) => { send(&mut writer, &protocol_error(Value::Null, -32600, error.to_string())).await?; continue; }
                };
                if version != "2.0" { send(&mut writer, &protocol_error(Value::Null, -32600, "jsonrpc requires 2.0")).await?; continue; }
                let Some(id) = id else {
                    if params.as_object().is_none() { continue; }
                    match method.as_str() {
                        "notifications/initialized" if initialized => ready = true,
                        "notifications/cancelled" => {
                            if let Some(key) = params.get("requestId").and_then(id_key) && let Some(handle) = pending.get(&key) { handle.abort(); }
                        }
                        _ => {},
                    }
                    continue;
                };
                let Some(key) = id_key(&id) else { send(&mut writer, &protocol_error(Value::Null, -32600, "request ID requires a bounded string or integer")).await?; continue; };
                if pending.contains_key(&key) { send(&mut writer, &protocol_error(id, -32600, "request ID is already in progress")).await?; continue; }
                if params.as_object().is_none() { send(&mut writer, &protocol_error(id, -32602, "params requires an object")).await?; continue; }
                match method.as_str() {
                    "ping" => send(&mut writer, &response(id, Object::new().into())).await?,
                    "initialize" => {
                        if initialized { send(&mut writer, &protocol_error(id, -32600, "session already initialized")).await?; continue; }
                        let valid = || {
                            let mut fields = Record::new(&params, "initialize params")?;
                            let _: String = fields.required("protocolVersion")?;
                            let capabilities = fields.raw("capabilities")?.ok_or_else(|| purrdf_lex::json::record::DecodeError::missing_field("capabilities"))?;
                            let _ = Record::new(capabilities, "client capabilities")?;
                            let info = fields.raw("clientInfo")?.ok_or_else(|| purrdf_lex::json::record::DecodeError::missing_field("clientInfo"))?;
                            let mut info = Record::new(info, "clientInfo")?;
                            let _: String = info.required("name")?; let _: String = info.required("version")?;
                            Ok::<_, purrdf_lex::json::record::DecodeError>(())
                        };
                        if let Err(error) = valid() { send(&mut writer, &protocol_error(id, -32602, error.to_string())).await?; continue; }
                        initialized = true;
                        send(&mut writer, &response(id, Object::new().with("protocolVersion", VERSION)
                            .with("capabilities", Object::new().with("tools", Object::new().with("listChanged", false)))
                            .with("serverInfo", Object::new().with("name", "sparrow").with("version", env!("CARGO_PKG_VERSION")))
                            .with("instructions", "Queries expose coverage and work limits. Times select server acceptance. Read original bytes to verify evidence. Pin mutations need stable request IDs; cancellation does not prove non-delivery.")
                            .into())).await?;
                    }
                    _ if !ready => send(&mut writer, &protocol_error(id, -32000, "initialize and send notifications/initialized first")).await?,
                    "tools/list" => {
                        let mut fields = Record::new(&params, "tools/list params")?;
                        let _ = fields.raw("_meta")?;
                        if let Err(error) = fields.deny_unknown() { send(&mut writer, &protocol_error(id, -32602, error.to_string())).await?; continue; }
                        send(&mut writer, &response(id, Object::new().with("tools", tool::definitions()).into())).await?;
                    }
                    "tools/call" => {
                        let decode = || {
                            let mut fields = Record::new(&params, "tools/call params")?;
                            let name: String = fields.required("name")?;
                            let arguments = fields.raw("arguments")?.cloned().unwrap_or_else(|| Object::new().into());
                            let _ = fields.raw("_meta")?; fields.deny_unknown()?;
                            Ok::<_, purrdf_lex::json::record::DecodeError>((name, arguments))
                        };
                        let (name, arguments) = match decode() { Ok(value) => value, Err(error) => { send(&mut writer, &protocol_error(id, -32602, error.to_string())).await?; continue; } };
                        if !tool::known(&name) { send(&mut writer, &protocol_error(id, -32602, format!("unknown tool: {name}"))).await?; continue; }
                        let operation = match tool::Operation::parse(&name, &arguments) {
                            Ok(operation) => operation,
                            Err(error) => { send(&mut writer, &response(id, tool_result(Object::new().with("error", error.json()).into(), true))).await?; continue; }
                        };
                        if pending.len() == 4 { send(&mut writer, &response(id, tool_result(Object::new().with("error", Object::new().with("code", "RESOURCE_LIMIT").with("message", "four MCP calls are already in progress").with("retryable", true)).into(), true))).await?; continue; }
                        let endpoint = endpoint.clone(); let result_key = key.clone(); let (handle, registration) = AbortHandle::new_pair(); pending.insert(key, handle);
                        jobs.push(async move {
                            let reply = Abortable::new(async move {
                            let result = tokio::time::timeout(Duration::from_secs(30), async {
                                let client = Client::connect_endpoint(&endpoint).await.map_err(|error| match error {
                                    sparrow_client::Error::Io(error) => AgentError::from(capnp::Error::from(error)),
                                    error => AgentError::from(error),
                                })?;
                                operation.execute(client.capability()).await
                            }).await.unwrap_or_else(|_| Err(AgentError { code: "RPC_TIMEOUT".into(), message: "tool exceeded 30 seconds; mutation delivery may be unknown; retry its stable request ID".into(), retryable: true }));
                            let output = match result { Ok(value) => { let failed = value.get("execution_status").and_then(Value::as_str) == Some("failed"); tool_result(value, failed) }, Err(error) => tool_result(Object::new().with("error", error.json()).into(), true) };
                            response(id, output)
                            }, registration).await;
                            (result_key, reply)
                        }.boxed_local());
                    }
                    _ => send(&mut writer, &protocol_error(id, -32601, format!("unknown method: {method}"))).await?,
                }
            }
        }
    }
    Ok(())
}
