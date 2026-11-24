// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Stateless Streamable HTTP transport for local log inspection.

use super::{
    CALL_LIMIT, FRAME_LIMIT, Message, RequestAction, ToolAccess, VERSION, call_limit, dispatch,
    encode, execute, id_key, protocol_error,
};
use futures::{StreamExt, stream::FuturesUnordered};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    HeaderMap, Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{self, HeaderName, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use purrdf_lex::json::{self, Value};
use sparrow_client::Endpoint;
use std::{convert::Infallible, io, net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore, time::timeout};

const CONNECTION_LIMIT: usize = 16;
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(45);

/// A loopback-only listener for Sparrow's read-only Streamable HTTP endpoint.
pub struct McpHttp {
    listener: TcpListener,
    address: SocketAddr,
}

impl McpHttp {
    /// Bind a numeric IPv4 or IPv6 loopback address. Port zero selects a free port.
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MCP HTTP requires a loopback address such as 127.0.0.1:8765 or [::1]:8765",
            ));
        }
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        Ok(Self { listener, address })
    }

    /// The numeric address clients can use to reach the listener.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Serve `/mcp` until the future is dropped or the listener fails.
    /// Call within a Tokio `LocalSet` for the typed daemon client.
    pub async fn serve(self, endpoint: &Endpoint) -> io::Result<()> {
        let authority = self.address.to_string();
        let localhost = format!("localhost:{}", self.address.port());
        let calls = Semaphore::new(CALL_LIMIT);
        let mut connections = FuturesUnordered::new();
        let mut http = http1::Builder::new();
        http.keep_alive(false)
            .timer(TokioTimer::new())
            .header_read_timeout(READ_TIMEOUT);

        loop {
            tokio::select! {
                _ = connections.next(), if !connections.is_empty() => {},
                accepted = self.listener.accept(), if connections.len() < CONNECTION_LIMIT => {
                    let (stream, _) = accepted?;
                    let service = service_fn(|request| async {
                        Ok::<_, Infallible>(
                            handle(request, &authority, &localhost, endpoint, &calls).await,
                        )
                    });
                    let connection = http.serve_connection(TokioIo::new(stream), service);
                    connections.push(async move {
                        match timeout(CONNECTION_TIMEOUT, connection).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => tracing::debug!(%error, "MCP HTTP connection ended"),
                            Err(_) => tracing::debug!("MCP HTTP connection exceeded its deadline"),
                        }
                    });
                }
            }
        }
    }
}

type Reply = Response<Full<Bytes>>;

fn singleton(headers: &HeaderMap, name: HeaderName) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(HeaderValue::to_str)
        .transpose()
        .map_err(|_| ())?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}

fn local_request(request: &Request<Incoming>, authority: &str, localhost: &str) -> bool {
    // Accept only origin-form targets and require a single local Host. A remote
    // origin that resolves its own hostname to loopback still carries that hostname.
    if request.uri().authority().is_some() {
        return false;
    }
    let Ok(Some(host)) = singleton(request.headers(), header::HOST) else {
        return false;
    };
    let default_port = authority.ends_with(":80");
    let host = without_default_port(host, default_port);
    if !host.eq_ignore_ascii_case(without_default_port(authority, default_port))
        && !host.eq_ignore_ascii_case(without_default_port(localhost, default_port))
    {
        return false;
    }
    match singleton(request.headers(), header::ORIGIN) {
        Ok(None) => true,
        Ok(Some(origin)) => origin.strip_prefix("http://").is_some_and(|origin_host| {
            without_default_port(origin_host, default_port).eq_ignore_ascii_case(host)
        }),
        Err(()) => false,
    }
}

fn without_default_port(authority: &str, default_port: bool) -> &str {
    if default_port {
        authority.strip_suffix(":80").unwrap_or(authority)
    } else {
        authority
    }
}

fn empty(status: StatusCode) -> Reply {
    let mut reply = Response::new(Full::default());
    *reply.status_mut() = status;
    reply
}

fn json_reply(status: StatusCode, value: &Value) -> Reply {
    let body = encode(value).unwrap_or_else(|error| {
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        json::write_compact(&protocol_error(id, -32000, error.to_string()))
    });
    let mut reply = Response::new(Full::new(Bytes::from(body)));
    *reply.status_mut() = status;
    reply.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    reply
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    reply
}

async fn handle(
    request: Request<Incoming>,
    authority: &str,
    localhost: &str,
    endpoint: &Endpoint,
    calls: &Semaphore,
) -> Reply {
    if !local_request(&request, authority, localhost) {
        return empty(StatusCode::FORBIDDEN);
    }
    if request.uri().path() != "/mcp" {
        return empty(StatusCode::NOT_FOUND);
    }
    if !matches!(
        singleton(
            request.headers(),
            HeaderName::from_static("mcp-protocol-version")
        ),
        Ok(None | Some(VERSION))
    ) {
        return empty(StatusCode::BAD_REQUEST);
    }
    if request.method() != Method::POST {
        let mut reply = empty(StatusCode::METHOD_NOT_ALLOWED);
        reply
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
        return reply;
    }
    if !matches!(
        singleton(request.headers(), header::CONTENT_TYPE),
        Ok(Some(value)) if value.split(';').next().is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
    ) {
        return empty(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let body = match timeout(
        READ_TIMEOUT,
        Limited::new(request.into_body(), FRAME_LIMIT).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(error)) if error.is::<LengthLimitError>() => {
            return empty(StatusCode::PAYLOAD_TOO_LARGE);
        }
        Ok(Err(_)) => return empty(StatusCode::BAD_REQUEST),
        Err(_) => return empty(StatusCode::REQUEST_TIMEOUT),
    };
    let Message { method, id, params } = match Message::decode(&body) {
        Ok(message) => message,
        Err(error) => return json_reply(StatusCode::BAD_REQUEST, &error),
    };
    let Some(id) = id else {
        return empty(if params.as_object().is_some() {
            StatusCode::ACCEPTED
        } else {
            StatusCode::BAD_REQUEST
        });
    };
    if id_key(&id).is_none() {
        return json_reply(
            StatusCode::BAD_REQUEST,
            &protocol_error(
                Value::Null,
                -32600,
                "request ID requires a bounded string or integer",
            ),
        );
    }
    let access = ToolAccess::HttpRead;
    let reply = match dispatch(&method, &params, &id, access) {
        RequestAction::Reply(reply) => reply,
        RequestAction::Call(operation) => match calls.try_acquire() {
            Ok(_permit) => execute(endpoint, *operation, id, access).await,
            Err(_) => call_limit(id),
        },
    };
    json_reply(StatusCode::OK, &reply)
}
