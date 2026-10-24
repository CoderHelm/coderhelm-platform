//! Remote MCP servers: connected by address over Streamable HTTP, signed in
//! to with OAuth 2.1 (or a static key, or nothing), their tools allowed one
//! by one by an admin.
//!
//! Follows the MCP specification, revision 2026-07-28, with the fallbacks it
//! describes for older servers:
//!
//! - [`http`]: HTTPS only to public addresses (checked after DNS and on
//!   every redirect hop), with timeouts and size caps.
//! - [`discovery`]: protected resource metadata (RFC 9728) and authorization
//!   server metadata (RFC 8414, OpenID Connect discovery).
//! - [`oauth`]: PKCE (S256), one-time `state`, Client ID Metadata Documents,
//!   dynamic registration (RFC 7591) or a pasted client, resource indicators
//!   (RFC 8707), refresh, revocation (RFC 7009), `iss` checks (RFC 9207).
//! - [`client`]: the MCP requests themselves, in either protocol era.
//! - [`tools`]: the allow list and its second-admin approval.
//! - [`store`]: the records, sealed where they hold secrets.
//! - [`runtime`]: everything above put together for the gateway and worker.
//!
//! A token Coderhelm gets for a server is sent to that server only, and is
//! never handed to a model or passed through to anything else.

pub mod client;
pub mod discovery;
pub mod http;
pub mod oauth;
pub mod runtime;
pub mod store;
pub mod tools;
