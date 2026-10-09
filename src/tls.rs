//! Transport security for the PostgreSQL connections this crate opens.
//!
//! `sqlx`'s own default is `sslmode=prefer`: try TLS, and fall back to
//! plaintext without a word when the server offers none. For a metering record
//! that is the wrong default, so every connection this crate opens from a URL —
//! the hot tier's pool and the SQL catalogue's — goes through [`require_tls`]:
//!
//! - **no `sslmode`** (in the URL or in `PGSSLMODE`) becomes `verify-full`:
//!   encrypted, and the server's certificate checked against its host name;
//! - **`verify-full`, `verify-ca`, `require`** are honoured — `require` is
//!   encrypted and not authenticated, which is a deployment's choice to make;
//! - **`disable`** is honoured as the explicit opt-out it is;
//! - **`prefer` and `allow`** are refused: each silently falls back to
//!   plaintext, which is what the default exists to remove;
//! - a **Unix-socket** URL is left alone, because it never crosses a network.
//!
//! A pool the caller builds — `PostgresHot::new`, a `SubjectRegistry`, a
//! `MeterStoreBuilder` — is the caller's, with whatever `sslmode` it chose.

use crate::error::{Error, Result};

/// The URL with an `sslmode` this crate will connect with, or the refusal.
///
/// `environment` is the value of `PGSSLMODE`, which libpq documents as the
/// operator's knob: an operator who set it chose it, so it counts as explicit.
pub(crate) fn require_tls(url: &str, environment: Option<&str>) -> Result<String> {
    if over_unix_socket(url) {
        return Ok(url.to_string());
    }
    let named = query_value(url, "sslmode");
    let mode = named.or(environment);
    match mode {
        None => Ok(with_param(url, "sslmode", "verify-full")),
        Some("verify-full" | "verify-ca" | "require" | "disable") => Ok(url.to_string()),
        Some(refused @ ("prefer" | "allow")) => Err(Error::config(format!(
            "sslmode={refused} falls back to a plaintext connection without a word when \
             the server offers no TLS, so this crate does not connect with it. Use \
             verify-full (the default: encrypted and the server's certificate checked; \
             add sslrootcert= for a private CA), require (encrypted, not authenticated) \
             or disable (plaintext, explicitly)"
        ))),
        Some(other) => Err(Error::config(format!(
            "sslmode={other} is not a PostgreSQL TLS mode; use verify-full, verify-ca, \
             require or disable"
        ))),
    }
}

/// Whether the URL reaches the server over a Unix-domain socket.
///
/// libpq's two spellings: an empty host with a `host=` parameter naming a
/// directory, or a percent-encoded path as the host.
fn over_unix_socket(url: &str) -> bool {
    query_value(url, "host").is_some_and(|h| h.starts_with('/'))
        || url
            .split_once("://")
            .map(|(_, rest)| rest.rsplit_once('@').map_or(rest, |(_, h)| h))
            .is_some_and(|host| host.starts_with("%2F") || host.starts_with("%2f"))
}

fn query_value<'a>(url: &'a str, key: &str) -> Option<&'a str> {
    let (_, query) = url.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then_some(v)
    })
}

fn with_param(url: &str, key: &str, value: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{key}={value}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgresql://edm:secret@db.internal:5432/meters";

    #[test]
    fn no_mode_means_encrypted_and_verified() {
        assert_eq!(
            require_tls(URL, None).unwrap(),
            format!("{URL}?sslmode=verify-full")
        );
        assert_eq!(
            require_tls(&format!("{URL}?application_name=edm"), None).unwrap(),
            format!("{URL}?application_name=edm&sslmode=verify-full")
        );
    }

    #[test]
    fn an_explicit_mode_is_honoured_unless_it_falls_back_silently() {
        for mode in ["verify-full", "verify-ca", "require", "disable"] {
            let url = format!("{URL}?sslmode={mode}");
            assert_eq!(require_tls(&url, None).unwrap(), url);
        }
        for mode in ["prefer", "allow"] {
            let err = require_tls(&format!("{URL}?sslmode={mode}"), None).unwrap_err();
            assert!(err.to_string().contains("plaintext"), "{err}");
        }
        assert!(require_tls(&format!("{URL}?sslmode=bogus"), None).is_err());
    }

    #[test]
    fn the_environment_counts_as_a_choice_and_the_url_overrides_it() {
        assert_eq!(require_tls(URL, Some("require")).unwrap(), URL);
        assert!(require_tls(URL, Some("prefer")).is_err());
        let url = format!("{URL}?sslmode=verify-full");
        assert_eq!(require_tls(&url, Some("prefer")).unwrap(), url);
    }

    #[test]
    fn a_unix_socket_never_crosses_a_network() {
        for url in [
            "postgresql:///meters?host=/var/run/postgresql",
            "postgresql://%2Fvar%2Frun%2Fpostgresql/meters",
        ] {
            assert_eq!(require_tls(url, None).unwrap(), url);
        }
    }
}
