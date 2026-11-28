// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::codec::*;
use super::dsn::*;
use super::pool::*;
use super::schema::*;
use super::tls::*;
use super::*;
use deadpool_postgres::PoolError;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, Error as RustlsError};
use std::path::Path;
use std::time::Duration;
use tokio_postgres::config::Host;
use tokio_postgres::error::SqlState;
#[test]
fn redact_postgres_url_masks_uri_and_dsn_passwords() {
    assert_eq!(
        redact_postgres_url("postgres://alice:s3cr3t@db.example.com:5432/jj?sslmode=require"),
        "postgres://alice:***@db.example.com:5432/jj?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url("postgresql://alice:p@ss:word@127.0.0.1:5432/jj"),
        "postgresql://alice:***@127.0.0.1:5432/jj"
    );
    // Unencoded `/`, `?`, `#`, and `=` in passwords must still be redacted before logging.
    assert_eq!(
        redact_postgres_url(
            "postgres://alice:my/pass?foo=bar#baz@db.example.com:5432/jj?sslmode=require"
        ),
        "postgres://alice:***@db.example.com:5432/jj?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:5432/sec?k=v#h@db.example.com:5432/jj"),
        "postgres://alice:***@db.example.com:5432/jj"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:p@ss/word@db.example.com:5432/jj"),
        "postgres://alice:***@db.example.com:5432/jj"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@[::1]:5432/jj?password=top=secret==&sslmode=disable"),
        "postgres://alice@[::1]:5432/jj?password=***&sslmode=disable"
    );
    // `tokio-postgres` `UrlParser::parse_credentials` consumes up to `@`, making
    // `localhost:5432/jj?application_name=dev@laptop` syntactically indistinguishable from
    // `alice:5432/TOPSECRET?k=v@db.example.com`; fail closed by masking the `:` segment.
    assert_eq!(
        redact_postgres_url("postgres://localhost:5432/jj?application_name=dev@laptop"),
        "postgres://localhost:***@laptop"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@localhost/jj"),
        "postgres://alice@localhost/jj"
    );
    // URIs with both a pre-query `:` and a query `@` fail closed across the `:`..`@` span
    // while still redacting `&password=` across the post-query tail.
    assert_eq!(
        redact_postgres_url(
            "postgres://alice:secret@db.example.com:5432/jj?application_name=team@corp/app&password=TOPSECRET"
        ),
        "postgres://alice:***@corp/app&password=***"
    );
    // Without a pre-query `@`, `tokio-postgres` consumes up to `@` as credentials; fail closed
    // while also masking `&password=` across the post-query tail.
    assert_eq!(
        redact_postgres_url(
            "postgres://db.example.com:5432/jj?application_name=team@corp/app&password=TOPSECRET"
        ),
        "postgres://db.example.com:***@corp/app&password=***"
    );
    assert_eq!(
        redact_postgres_url(
            "postgres://alice@db.example.com:5432/jj?%70assword=TOPSECRET&sslmode=require"
        ),
        "postgres://alice@db.example.com:5432/jj?%70assword=***&sslmode=require"
    );
    // Passwordless `user@host` with `?password=` and later query `@` must never skip query redaction.
    for (input, expected) in [
        (
            "postgres://alice@db.example.com?password=TOPSECRET&application_name=team@corp/app",
            "postgres://alice@db.example.com?password=***&application_name=team@corp/app",
        ),
        (
            "postgres://alice@db.example.com?password=TOPSECRET&application_name=team@corp:5432",
            "postgres://alice@db.example.com?password=***&application_name=team@corp:5432",
        ),
        (
            "postgres://alice@db.example.com?password=TOPSECRET&application_name=team@corp,b",
            "postgres://alice@db.example.com?password=***&application_name=team@corp,b",
        ),
        (
            "postgres://alice@db.example.com?password=TOPSECRET:123&application_name=team@corp/app",
            "postgres://alice@db.example.com?password=***:***@corp/app",
        ),
    ] {
        let out = redact_postgres_url(input);
        assert_eq!(out, expected);
        assert!(!out.contains("TOPSECRET"), "leaked secret in {out}");
    }
    // `?password=` values containing `@` (including paired with `?`, `=`, or `:`) must be
    // masked instead of exposing the post-`@` suffix.
    for (input, expected) in [
        (
            "postgres://alice:secret@db.example.com?password=my@TOPSECRET/foo",
            "postgres://alice:***",
        ),
        (
            "postgres://alice:secret@db.example.com?password=my@TOPSECRET:5432",
            "postgres://alice:***",
        ),
        (
            "postgres://alice:secret@db.example.com?password=my@TOPSECRET,xyz",
            "postgres://alice:***",
        ),
        (
            "postgres://alice:secret@db.example.com?password=my@TOPSECRET&sslmode=require",
            "postgres://alice:***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com:5432/jj?password=my?sec@TOPSECRET",
            "postgres://alice@db.example.com:***",
        ),
        (
            "postgres://alice@db.example.com:5432/jj?password=my?k=v@TOPSECRET",
            "postgres://alice@db.example.com:***",
        ),
        (
            "postgres://alice:pass@db.example.com/jj?password=my?sec@TOPSECRET",
            "postgres://alice:***",
        ),
        (
            "postgres://alice:pass@db.example.com/jj?password=my?sec@TOPSECRET&sslmode=require",
            "postgres://alice:***&sslmode=require",
        ),
        (
            "postgres://db.example.com/jj?foo=1&password=SEC1?x:SEC2@TOPSECRET",
            "postgres://db.example.com/jj?***&password=***:***",
        ),
        (
            "postgres://alice@db.example.com/jj?password=my?sec:1@TOPSECRET",
            "postgres://alice@db.example.com/jj?password=***:***",
        ),
        (
            "postgres://alice@db.example.com:5432/jj?sslpassword=my?sec@TOPSECRET&sslmode=require",
            "postgres://alice@db.example.com:***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com/jj?sslpassword=TOPSECRET&sslmode=require",
            "postgres://alice@db.example.com/jj?sslpassword=***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com/jj?password=my&TOPSECRET&aGVsbG8=&sslmode=require",
            "postgres://alice@db.example.com/jj?password=***&***&***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com:5432/jj?password=part1&part2@TOPSECRET&sslmode=require",
            "postgres://alice@db.example.com:***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com:5432/jj?password=part1&k=v@TOPSECRET&sslmode=require",
            "postgres://alice@db.example.com:***&sslmode=require",
        ),
        (
            "postgres://alice@db.example.com/jj?password=part1&part2:1@TOPSECRET",
            "postgres://alice@db.example.com/jj?password=***&***:***",
        ),
        (
            "postgres://alice:pass@db.example.com/jj?sslmode=require;password=my@TOPSECRET",
            "postgres://alice:***",
        ),
        (
            "postgres://alice:secret@db.example.com/jj?foo=1@password=TOPSECRET?sslmode=require",
            "postgres://alice:***@password=***?sslmode=require",
        ),
        (
            "postgres://alice:p@ss?a=1@password=TOPSECRET?sslmode=require",
            "postgres://alice:***@password=***?sslmode=require",
        ),
        (
            "postgres://db.example.com/jj?sslmode=require;password=TOPSECRET",
            "postgres://db.example.com/jj?sslmode=***",
        ),
        (
            "postgres://db.example.com/jj?sslmode=require?password=TOPSECRET",
            "postgres://db.example.com/jj?sslmode=***",
        ),
        (
            "postgres://db.example.com/jj?sslmode=require;%70assword=TOPSECRET",
            "postgres://db.example.com/jj?sslmode=***",
        ),
        (
            "postgres://db.example.com:5432/jj?password =my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:5432/jj?password%20=my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:5432/jj? password=my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:5432/jj?sslmode;password=my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:5432/jj?sslmode=require&;password=my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:5432/jj?password%3Dmy@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://alice:secret@db.example.com/jj?foo=1@password%3DTOPSECRET",
            "postgres://alice:***@***",
        ),
        (
            "postgres://db.example.com:5432/jj?password=my#1@TOPSECRET:5432",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:543200/jj?password=my@TOPSECRET",
            "postgres://db.example.com:***",
        ),
        (
            "postgres://db.example.com:543200/jj?application_name=team@corp&password=TOPSECRET",
            "postgres://db.example.com:***@corp&password=***",
        ),
    ] {
        let out = redact_postgres_url(input);
        assert_eq!(out, expected);
        assert!(
            !out.contains("TOPSECRET") && !out.contains("SEC1") && !out.contains("SEC2"),
            "leaked secret in {out}"
        );
    }
    // Special-character dbnames, IPv6 zone IDs, malformed ports/hosts, `[`-prefixed usernames,
    // email/IAM usernames containing `@`, `?` in usernames, and digit-prefixed or `@`-containing
    // passwords must always redact fail-closed.
    for input in [
        "postgres://alice:TOPSECRET@db.example.com:5432/proj#1",
        "postgres://alice:TOPSECRET@db.example.com:5432/app=prod",
        "postgres://alice:TOPSECRET@db.example.com:5432/a&b",
        "postgres://alice:TOPSECRET@[fe80::1%25eth0]:5432/jj",
        "postgres://alice:TOPSECRET@db.example.com:543200/jj",
        "postgres://alice:TOPSECRET@db.example.com:abc/jj",
        "postgres://alice:TOPSECRET@[::1:5432/jj",
        "postgres://alice:TOPSECRET@host1,/jj",
        "postgres://alice:TOPSECRET@db host/jj",
        "postgres://alice:TOPSECRET@",
        "postgres://alice:5432/TOPSECRET?k=v@db.example.com:5432/jj",
        "postgres://alice:5432?k=TOPSECRET@db.example.com:5432/jj",
        "postgres://alice:5432/TOPSECRET?k=v#h@db.example.com/jj",
        "postgres://alice:?k=TOPSECRET@db.example.com:5432/jj",
        "postgres://alice:5432/TOPSECRET?k=v@db.example.com",
        "postgres://alice:5432/TOPSECRET?password=foo@db.example.com:5432/jj",
        "postgres://alice:/TOPSECRET?password=foo@db.example.com:5432/jj",
        "postgres://alice:5432/TOPSECRET?k=v&password=foo@db.example.com:5432/jj?sslmode=require",
        "postgres://alice:5432/TOPSECRET?foo=bar@db.example.com:5432/proj&v=1",
        "postgres://alice:p@ss/TOPSECRET?a=b@db.example.com:5432/jj",
        "postgres://alice:p@ss:5432?a=TOPSECRET@db.example.com:5432/jj",
        "postgres://alice:p1@TOPSECRET?password=1@db.example.com:5432/jj",
        "postgres://alice:p1@TOPSECRET?foo=1@db.example.com:5432/jj&bar=2",
        "postgres://alice:TOPSECRET?a=1@db.example.com:5432/jj?password=my@QSECRET",
    ] {
        let out = redact_postgres_url(input);
        assert!(
            !out.contains("TOPSECRET")
                && !out.contains("QSECRET")
                && out.starts_with("postgres://alice:***"),
            "failed to redact {input:?} -> {out:?}"
        );
    }
    assert_eq!(
        redact_postgres_url("postgres://[alice]:TOPSECRET?foo=bar@db.example.com:5432/jj"),
        "postgres://[alice]:***@db.example.com:5432/jj"
    );
    // Email / Cloud SQL IAM usernames with unencoded `@` (and optional `?`) before `:password`.
    assert_eq!(
        redact_postgres_url("postgres://alice@proj.iam:TOPSECRET?foo=bar@db.example.com:5432/jj"),
        "postgres://alice@proj.iam:***@db.example.com:5432/jj"
    );
    assert_eq!(
        redact_postgres_url(
            "postgres://alice@proj.iam:TOPSECRET?foo=bar@db.example.com:5432/jj?sslmode=require"
        ),
        "postgres://alice@proj.iam:***@db.example.com:5432/jj?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url(
            "postgres://alice@proj.iam:TOPSECRET?password=1@db.example.com:5432/jj"
        ),
        "postgres://alice@proj.iam:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@proj?iam:TOPSECRET@db.example.com:5432/jj"),
        "postgres://alice@proj?***:***@db.example.com:5432/jj"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@proj?iam=1:TOPSECRET@db.example.com:5432/jj"),
        "postgres://alice@proj?***:***@db.example.com:5432/jj"
    );
    // Truncated `@`-less `user:password` URIs (including `?` suffixes and non-IPv6 brackets)
    // fail closed through EOF while valid host:port and bracketed IPv6 literals stay intact.
    assert_eq!(
        redact_postgres_url("postgres://alice:TOPSECRET"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:TOP?SECRET"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:TOP?k=SECRET"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:TOP/PASS?k=SECRET"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:TOP/SECRET?sslmode=require"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[alice]:TOPSECRET/jj"),
        "postgres://[alice]:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[alice:TOPSECRET]"),
        "postgres://[alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[alice:TOPSECRET]foo"),
        "postgres://[alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[::1]foo:TOPSECRET"),
        "postgres://[:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[::1]foo:TOPSECRET/jj"),
        "postgres://[:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:12345678"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:00012345"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:0000"),
        "postgres://alice:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:0"),
        "postgres://alice:***"
    );
    // Truncated `@`-username and `@`-password URIs redact the post-`@` authority fail-closed.
    assert_eq!(
        redact_postgres_url("postgres://alice@proj.iam:TOPSECRET"),
        "postgres://alice@proj.iam:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@proj.iam:00012345"),
        "postgres://alice@proj.iam:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@proj.iam:TOP/PASS?sslmode=require"),
        "postgres://alice@proj.iam:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice@proj.iam:TOP?SECRET"),
        "postgres://alice@proj.iam:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:p@ss:TOPSECRET"),
        "postgres://alice:***@ss:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:p?1@ss:TOPSECRET"),
        "postgres://alice:***@ss:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:p?1@ss:TOP/PASS?sslmode=require"),
        "postgres://alice:***@ss:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://[::1]:12345678"),
        "postgres://[::1]:***"
    );
    assert_eq!(
        redact_postgres_url("postgres://localhost:/mydb?sslmode=require"),
        "postgres://localhost:/mydb?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url("postgres://[::1]:5432/jj?sslmode=require"),
        "postgres://[::1]:5432/jj?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url("postgres://[fe80::1%25eth0]:5432/jj?sslmode=require"),
        "postgres://[fe80::1%25eth0]:5432/jj?sslmode=require"
    );
    assert_eq!(
        redact_postgres_url("postgres://host1:5432,host2:5433/jj?sslmode=require"),
        "postgres://host1:5432,host2:5433/jj?sslmode=require"
    );
    // Malformed libpq DSNs (bare tokens, Base64 `=`, unknown keys, doubled quotes,
    // unterminated quotes on unknown keys, empty values absorbing `password=`, mismatched
    // quotes spanning `password=`, and `sslpassword=`) fail closed while preserving valid
    // `options` and `dbname` values containing `=`.
    assert_eq!(
        redact_postgres_url("host=db.example.com options='-c search_path=public' dbname=app=prod"),
        "host=db.example.com options='-c search_path=public' dbname=app=prod"
    );
    assert_eq!(
        redact_postgres_url("host=db.example.com port= password=TOPSECRET dbname=jj"),
        "host=db.example.com port=*** dbname=jj"
    );
    assert_eq!(
        redact_postgres_url("host=db.example.com sslmode= password=TOPSECRET"),
        "host=db.example.com sslmode=***"
    );
    assert_eq!(
        redact_postgres_url(
            "host=db.example.com sslmode='require password=TOPSECRET sslrootcert='/etc/ca.pem'"
        ),
        "host=db.example.com sslmode=*** ***"
    );
    assert_eq!(
        redact_postgres_url(
            "host=db.example.com application_name='my app password=TOPSECRET dbname='jj'"
        ),
        "host=db.example.com application_name=*** ***"
    );
    assert_eq!(
        redact_postgres_url("host=db password=my TOPSECRET dbname=jj"),
        "host=db password=*** *** dbname=jj"
    );
    assert_eq!(
        redact_postgres_url("host=db password=my aGVsbG8= dbname=jj"),
        "host=db password=*** ***"
    );
    assert_eq!(
        redact_postgres_url("host=db password=my foo=TOPSECRET dbname=jj"),
        "host=db password=*** *** dbname=jj"
    );
    assert_eq!(
        redact_postgres_url("host=db password=my =TOPSECRET dbname=jj"),
        "host=db password=*** *** dbname=jj"
    );
    assert_eq!(
        redact_postgres_url("host=db password='my''TOP=SECRET' dbname=jj"),
        "host=db password=*** *** dbname=jj"
    );
    assert_eq!(
        redact_postgres_url("host=db password=my TOPSECRET='oops"),
        "host=db password=*** ***"
    );
    assert_eq!(
        redact_postgres_url("host=db sslpassword=TOPSECRET"),
        "host=db sslpassword=***"
    );
    // Unencoded `?` inside a username before `:password@host` (and `?password=` prefix safety).
    assert_eq!(
        redact_postgres_url("postgres://al?ice:TOPSECRET@db.example.com:5432/jj"),
        "postgres://al?***:***@db.example.com:5432/jj"
    );
    let q_pass_colon =
        redact_postgres_url("postgres://al?password=SECRET1:SECRET2@db.example.com:5432/jj");
    assert!(
        !q_pass_colon.contains("SECRET1") && !q_pass_colon.contains("SECRET2"),
        "leaked secret in {q_pass_colon}"
    );
    let host_pass_colon = redact_postgres_url(
        "postgres://db.example.com/jj?password=TOPSECRET:123&application_name=team@corp/app",
    );
    assert!(
        !host_pass_colon.contains("TOPSECRET"),
        "leaked secret in {host_pass_colon}"
    );
    // `#` is literal inside PostgreSQL URI query values (not a fragment delimiter).
    assert_eq!(
        redact_postgres_url(
            "postgres://db.example.com:5432/jj?password=my#TOPSECRET&sslmode=require"
        ),
        "postgres://db.example.com:5432/jj?password=***&sslmode=require"
    );
    assert_eq!(
        redact_postgres_url(
            "postgres://db.example.com:5432/jj?application_name=v1#rc1&password=TOPSECRET"
        ),
        "postgres://db.example.com:5432/jj?application_name=v1#rc1&password=***"
    );
    // Multi-host authorities and malformed passwords paired with query `@` parameters.
    assert_eq!(
        redact_postgres_url(
            "postgres://host1:5432,host2:5432/jj?application_name=team@corp&password=TOPSECRET"
        ),
        "postgres://host1:***@corp&password=***"
    );
    assert_eq!(
        redact_postgres_url(
            "postgres://alice:my/SECRET@db.example.com:5432/jj?application_name=team@corp&password=QUERYSECRET"
        ),
        "postgres://alice:***@corp&password=***"
    );
    assert_eq!(
        redact_postgres_url("postgres://alice:p@ss?a=b@db.example.com:5432/jj"),
        "postgres://alice:***@db.example.com:5432/jj"
    );
    let fail_closed_tail = redact_postgres_url(
        "postgres://alice:p@ss/word@db.example.com:5432/jj?application_name=team@corp",
    );
    assert!(
        !fail_closed_tail.contains("p@ss")
            && !fail_closed_tail.contains("word")
            && fail_closed_tail.starts_with("postgres://alice:***@"),
        "unexpected redaction: {fail_closed_tail}"
    );
    assert_eq!(
        redact_postgres_url("host=db\u{00A0}password=TOPSECRET sslmode=disable"),
        "host=db password=*** sslmode=disable"
    );
    // Unterminated single-quoted DSN value fails closed instead of leaking trailing `password=`.
    let unterminated = redact_postgres_url("application_name='my app password=TOPSECRET");
    assert!(!unterminated.contains("TOPSECRET"), "{unterminated}");
    assert_eq!(unterminated, "application_name=***");
    assert_eq!(
        redact_postgres_url("host=127.0.0.1 port=5432 user=jj password=my=secret=1 dbname=cc"),
        "host=127.0.0.1 port=5432 user=jj password=*** dbname=cc"
    );
    assert_eq!(
        redact_postgres_url("host=localhost password='my \\'quoted\\' pass' sslmode=disable"),
        "host=localhost password=*** sslmode=disable"
    );

    let cfg = PostgresConnectConfig {
        url: "postgres://alice:debug-secret/123?x=1@db.example.com:5432/jj".to_string(),
        ..Default::default()
    };
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains("debug-secret"), "Debug leaked secret: {dbg}");
    assert!(dbg.contains("alice:***@db.example.com:5432/jj"));
}

#[test]
fn parse_postgres_tls_options_handles_sslmode_and_strips_sslrootcert() {
    let parsed =
        parse_postgres_tls_options("postgres://u:p@127.0.0.1:5432/db?sslmode=disable").unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Disable);
    assert_eq!(parsed.ssl_root_cert, None);
    assert!(parsed.normalized_dsn.contains("sslmode=disable"));

    let parsed =
        parse_postgres_tls_options("postgres://u:p@db.internal:5432/db?sslmode=prefer").unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Prefer);
    assert!(parsed.normalized_dsn.contains("sslmode=prefer"));

    let parsed =
        parse_postgres_tls_options("postgres://u:p@db.internal:5432/db?sslmode=require").unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Require);
    assert!(parsed.normalized_dsn.contains("sslmode=require"));

    let parsed = parse_postgres_tls_options(
        "postgres://u:p@db.internal:5432/db?sslmode=verify-ca&sslrootcert=%2Fetc%2Fjj-cc%2Fca.crt",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::VerifyCa);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/jj-cc/ca.crt"));
    assert!(parsed.normalized_dsn.contains("sslmode=require"));
    assert!(!parsed.normalized_dsn.contains("sslrootcert"));
    assert!(tokio_postgres::Config::from_str(&parsed.normalized_dsn).is_ok());

    let parsed =
        parse_postgres_tls_options("postgresql://u:p@db.internal:5432/db?sslrootcert=/etc/ca.pem")
            .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Unspecified);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/ca.pem"));
    assert_eq!(
        parsed.normalized_dsn,
        "postgresql://u:p@db.internal:5432/db"
    );
    assert!(tokio_postgres::Config::from_str(&parsed.normalized_dsn).is_ok());

    let parsed = parse_postgres_tls_options(
        "host=db.internal user=jj password=secret sslmode=verify-full sslrootcert='/etc/pg ca/root.crt'",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::VerifyFull);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/pg ca/root.crt"));
    assert!(parsed.normalized_dsn.contains("sslmode=require"));
    assert!(!parsed.normalized_dsn.contains("sslrootcert"));
    assert!(tokio_postgres::Config::from_str(&parsed.normalized_dsn).is_ok());

    let err = parse_postgres_tls_options("postgres://u:supersecret@h/db?sslmode=bogus")
        .expect_err("unknown sslmode must fail");
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    assert!(
        !err.to_string().contains("supersecret"),
        "error leaked password: {err}"
    );

    let err = parse_postgres_tls_options("postgres://u:supersecret@h/db?sslrootcert=")
        .expect_err("empty sslrootcert must fail");
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    assert!(!err.to_string().contains("supersecret"));

    // Valid URIs with `@` and `/` inside query values (and percent-encoded query keys/values)
    // must match `tokio_postgres::Config::from_str` without dropping `sslmode` or `sslrootcert`.
    let parsed = parse_postgres_tls_options(
        "postgres://alice:secret@db.example.com:5432/jj?application_name=team@corp/app&sslmode=require",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Require);
    assert_eq!(
        tokio_postgres::Config::from_str(&parsed.normalized_dsn)
            .unwrap()
            .get_ssl_mode(),
        PgSslMode::Require
    );

    let parsed = parse_postgres_tls_options(
        "postgres://alice:secret@db.example.com:5432/jj?user=sa@proj.iam&sslrootcert=/etc/ca.pem&sslmode=verify-full",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::VerifyFull);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/ca.pem"));
    assert!(!parsed.normalized_dsn.contains("sslrootcert"));
    assert_eq!(
        tokio_postgres::Config::from_str(&parsed.normalized_dsn)
            .unwrap()
            .get_ssl_mode(),
        PgSslMode::Require
    );

    let parsed = parse_postgres_tls_options(
        "postgres://alice:secret@db.example.com:5432/jj?%73slmode=%72equire&%73slrootcert=%2Fetc%2Fca.pem",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Require);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/ca.pem"));
    assert_eq!(
        tokio_postgres::Config::from_str(&parsed.normalized_dsn)
            .unwrap()
            .get_ssl_mode(),
        PgSslMode::Require
    );

    // Unencoded `?` inside a password before `@` must not shadow the real query string.
    let parsed = parse_postgres_tls_options(
        "postgres://u:bad?sslmode=disable@db.internal:5432/db?sslmode=verify-full&sslrootcert=/etc/ca.pem",
    )
    .unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::VerifyFull);
    assert_eq!(parsed.ssl_root_cert.as_deref(), Some("/etc/ca.pem"));
    assert!(parsed.normalized_dsn.ends_with("/db?sslmode=require"));

    // Uppercase DSN `SSLMODE` is normalized to lowercase for `tokio_postgres::Config::from_str`.
    let parsed = parse_postgres_tls_options("host=db.internal SSLMODE=require").unwrap();
    assert_eq!(parsed.ssl_mode, ParsedSslMode::Require);
    assert_eq!(parsed.normalized_dsn, "host=db.internal sslmode=require");
    assert_eq!(
        tokio_postgres::Config::from_str(&parsed.normalized_dsn)
            .unwrap()
            .get_ssl_mode(),
        PgSslMode::Require
    );

    // Unterminated single-quoted DSN tokens must fail closed without leaking trailing passwords.
    for bad_dsn in [
        "host=db.example.com sslmode='require password=TOPSECRET",
        "host=db.example.com sslmode=verify-ca sslrootcert='/etc/ca.pem password=TOPSECRET",
    ] {
        let err = parse_postgres_tls_options(bad_dsn)
            .expect_err("unterminated single-quoted DSN parameter must fail closed");
        assert!(matches!(err, StoreError::InvalidArgument(_)));
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET"),
            "unterminated DSN error leaked secret for {bad_dsn:?}: {msg}"
        );
    }

    // Malformed URI smuggling `sslmode=TOPSECRET` inside an unencoded password must not echo it.
    let err = parse_postgres_tls_options(
        "postgres://alice:p@ss?sslmode=TOPSECRET@db.example.com:5432/jj",
    )
    .expect_err("invalid sslmode must fail");
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    let msg = err.to_string();
    assert!(
        !msg.contains("TOPSECRET") && !msg.contains("topsecret"),
        "sslmode error echoed secret: {msg}"
    );
}

#[test]
fn resolve_tls_policy_matches_libpq_modes_and_unix_sockets() {
    let tcp_hosts = vec![Host::Tcp("db.example.com".to_string())];
    let redacted = "postgres://u:***@db.example.com/jj";
    assert_eq!(
        resolve_tls_policy(&tcp_hosts, ParsedSslMode::Disable, None, None, redacted).unwrap(),
        ResolvedTlsPolicy::NoTls
    );
    assert_eq!(
        resolve_tls_policy(&tcp_hosts, ParsedSslMode::Unspecified, None, None, redacted).unwrap(),
        ResolvedTlsPolicy::EncryptOnly {
            pg_ssl_mode: PgSslMode::Prefer,
        }
    );
    assert_eq!(
        resolve_tls_policy(&tcp_hosts, ParsedSslMode::Require, None, None, redacted).unwrap(),
        ResolvedTlsPolicy::EncryptOnly {
            pg_ssl_mode: PgSslMode::Require,
        }
    );
    assert_eq!(
        resolve_tls_policy(
            &tcp_hosts,
            ParsedSslMode::Require,
            None,
            Some("/etc/ca.crt"),
            redacted,
        )
        .unwrap(),
        ResolvedTlsPolicy::VerifyCa {
            pg_ssl_mode: PgSslMode::Require,
            ca_source: CaCertSource::UrlPemFile {
                path: PathBuf::from("/etc/ca.crt"),
                redacted_url: redacted.to_string(),
            },
        }
    );
    // Matching libpq 16+, `sslrootcert=system` is only accepted with `sslmode=verify-full`.
    for mode in [
        ParsedSslMode::Disable,
        ParsedSslMode::Unspecified,
        ParsedSslMode::Prefer,
        ParsedSslMode::Require,
        ParsedSslMode::VerifyCa,
    ] {
        assert!(matches!(
            resolve_tls_policy(&tcp_hosts, mode, None, Some("system"), redacted),
            Err(StoreError::InvalidArgument(_))
        ));
    }
    assert_eq!(
        resolve_tls_policy(
            &tcp_hosts,
            ParsedSslMode::VerifyCa,
            Some(Path::new("/override/ca.pem")),
            Some("/url/ca.pem"),
            redacted,
        )
        .unwrap(),
        ResolvedTlsPolicy::VerifyCa {
            pg_ssl_mode: PgSslMode::Require,
            ca_source: CaCertSource::CliPemFile(PathBuf::from("/override/ca.pem")),
        }
    );
    assert_eq!(
        resolve_tls_policy(
            &tcp_hosts,
            ParsedSslMode::VerifyFull,
            None,
            Some("system"),
            redacted,
        )
        .unwrap(),
        ResolvedTlsPolicy::VerifyFull {
            ca_source: CaCertSource::SystemRoots,
        }
    );

    #[cfg(unix)]
    {
        let unix_hosts = vec![Host::Unix(PathBuf::from("/var/run/postgresql"))];
        for mode in [
            ParsedSslMode::Disable,
            ParsedSslMode::Unspecified,
            ParsedSslMode::Prefer,
        ] {
            assert_eq!(
                resolve_tls_policy(&unix_hosts, mode, None, None, redacted).unwrap(),
                ResolvedTlsPolicy::NoTls
            );
        }
        for mode in [
            ParsedSslMode::Require,
            ParsedSslMode::VerifyCa,
            ParsedSslMode::VerifyFull,
        ] {
            assert!(matches!(
                resolve_tls_policy(&unix_hosts, mode, None, None, redacted),
                Err(StoreError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            resolve_tls_policy(
                &unix_hosts,
                ParsedSslMode::Disable,
                None,
                Some("system"),
                redacted,
            ),
            Err(StoreError::InvalidArgument(_))
        ));

        // Mixed Unix + TCP host lists fail fast on explicit TLS-required modes while keeping
        // `disable` and opportunistic `unspecified`/`prefer` valid.
        let mixed_hosts = vec![
            Host::Unix(PathBuf::from("/var/run/postgresql")),
            Host::Tcp("db.example.com".to_string()),
        ];
        let mixed_hosts_rev = vec![
            Host::Tcp("db.example.com".to_string()),
            Host::Unix(PathBuf::from("/var/run/postgresql")),
        ];
        for hosts in [&mixed_hosts, &mixed_hosts_rev] {
            assert_eq!(
                resolve_tls_policy(hosts, ParsedSslMode::Disable, None, None, redacted).unwrap(),
                ResolvedTlsPolicy::NoTls
            );
            for mode in [ParsedSslMode::Unspecified, ParsedSslMode::Prefer] {
                assert_eq!(
                    resolve_tls_policy(hosts, mode, None, None, redacted).unwrap(),
                    ResolvedTlsPolicy::EncryptOnly {
                        pg_ssl_mode: PgSslMode::Prefer,
                    }
                );
            }
            for mode in [
                ParsedSslMode::Require,
                ParsedSslMode::VerifyCa,
                ParsedSslMode::VerifyFull,
            ] {
                assert!(
                    matches!(
                        resolve_tls_policy(hosts, mode, None, None, redacted),
                        Err(StoreError::InvalidArgument(_))
                    ),
                    "mixed Unix+TCP hosts with {mode:?} must fail fast"
                );
            }
        }
    }

    // URL-embedded `sslrootcert` (without CLI `--postgres-ca-cert`) preserves libpq `prefer`
    // semantics when `sslmode` is omitted or `prefer`.
    for mode in [ParsedSslMode::Unspecified, ParsedSslMode::Prefer] {
        let policy =
            resolve_tls_policy(&tcp_hosts, mode, None, Some("/etc/ca.crt"), redacted).unwrap();
        assert_eq!(
            policy,
            ResolvedTlsPolicy::VerifyCa {
                pg_ssl_mode: PgSslMode::Prefer,
                ca_source: CaCertSource::UrlPemFile {
                    path: PathBuf::from("/etc/ca.crt"),
                    redacted_url: redacted.to_string(),
                },
            }
        );
        assert_eq!(policy.pg_ssl_mode(), PgSslMode::Prefer);
    }
}

#[test]
fn resolve_tls_policy_fails_closed_when_cli_ca_cert_flag_is_set() {
    let tcp_hosts = vec![Host::Tcp("db.example.com".to_string())];
    let redacted = "postgres://u:***@db.example.com/jj";
    let cli_ca = Path::new("/etc/jj-cc/pg-ca.pem");
    let expected_source = CaCertSource::CliPemFile(cli_ca.to_path_buf());

    // Unspecified sslmode + --postgres-ca-cert enforces TLS (`PgSslMode::Require`) and full
    // hostname verification (`VerifyFull`), overriding any URL `sslrootcert`.
    for url_root_cert in [None, Some("/url/ca.pem"), Some("system")] {
        let policy = resolve_tls_policy(
            &tcp_hosts,
            ParsedSslMode::Unspecified,
            Some(cli_ca),
            url_root_cert,
            redacted,
        )
        .unwrap();
        assert_eq!(
            policy,
            ResolvedTlsPolicy::VerifyFull {
                ca_source: expected_source.clone(),
            }
        );
        assert_eq!(policy.pg_ssl_mode(), PgSslMode::Require);
    }

    // Explicit `verify-full` enforces TLS + hostname verification with the CLI CA.
    let verify_full = resolve_tls_policy(
        &tcp_hosts,
        ParsedSslMode::VerifyFull,
        Some(cli_ca),
        Some("system"),
        redacted,
    )
    .unwrap();
    assert_eq!(
        verify_full,
        ResolvedTlsPolicy::VerifyFull {
            ca_source: expected_source.clone(),
        }
    );
    assert_eq!(verify_full.pg_ssl_mode(), PgSslMode::Require);

    // Explicit `verify-ca` and `require` enforce TLS (`PgSslMode::Require`) and verify the
    // server certificate chain against the CLI CA without silently dropping it.
    for mode in [ParsedSslMode::VerifyCa, ParsedSslMode::Require] {
        let policy = resolve_tls_policy(
            &tcp_hosts,
            mode,
            Some(cli_ca),
            Some("/url/ca.pem"),
            redacted,
        )
        .unwrap();
        assert_eq!(
            policy,
            ResolvedTlsPolicy::VerifyCa {
                pg_ssl_mode: PgSslMode::Require,
                ca_source: expected_source.clone(),
            }
        );
        assert_eq!(policy.pg_ssl_mode(), PgSslMode::Require);
    }

    // Contradictory `disable` and `prefer` modes fail closed with InvalidArgument.
    let disable_err = resolve_tls_policy(
        &tcp_hosts,
        ParsedSslMode::Disable,
        Some(cli_ca),
        None,
        redacted,
    )
    .unwrap_err();
    assert!(matches!(disable_err, StoreError::InvalidArgument(_)));
    assert!(
        disable_err.to_string().contains("--postgres-ca-cert")
            && disable_err.to_string().contains("sslmode=disable"),
        "unexpected disable error: {disable_err}"
    );

    let prefer_err = resolve_tls_policy(
        &tcp_hosts,
        ParsedSslMode::Prefer,
        Some(cli_ca),
        None,
        redacted,
    )
    .unwrap_err();
    assert!(matches!(prefer_err, StoreError::InvalidArgument(_)));
    assert!(
        prefer_err.to_string().contains("--postgres-ca-cert")
            && prefer_err.to_string().contains("sslmode=prefer"),
        "unexpected prefer error: {prefer_err}"
    );

    // All-Unix and mixed Unix+TCP host lists fail closed across every sslmode when
    // `--postgres-ca-cert` is explicitly set.
    #[cfg(unix)]
    {
        let unix_hosts = vec![Host::Unix(PathBuf::from("/var/run/postgresql"))];
        let mixed_hosts = vec![
            Host::Tcp("db.example.com".to_string()),
            Host::Unix(PathBuf::from("/var/run/postgresql")),
        ];
        for hosts in [&unix_hosts, &mixed_hosts] {
            for mode in [
                ParsedSslMode::Unspecified,
                ParsedSslMode::Disable,
                ParsedSslMode::Prefer,
                ParsedSslMode::Require,
                ParsedSslMode::VerifyCa,
                ParsedSslMode::VerifyFull,
            ] {
                let err =
                    resolve_tls_policy(hosts, mode, Some(cli_ca), None, redacted).unwrap_err();
                assert!(
                    matches!(err, StoreError::InvalidArgument(_)),
                    "mode {mode:?} on Unix socket with --postgres-ca-cert must fail closed: {err:?}"
                );
                assert!(
                    err.to_string().contains("--postgres-ca-cert")
                        && err.to_string().contains("Unix-domain socket"),
                    "unexpected Unix socket error for {mode:?}: {err}"
                );
            }
        }
    }
}

#[test]
fn pool_timeouts_are_bounded_and_map_to_unavailable() {
    let timeouts = default_pool_timeouts();
    assert_eq!(timeouts.wait, Some(POOL_WAIT_TIMEOUT));
    assert_eq!(timeouts.create, Some(POOL_CREATE_TIMEOUT));
    assert_eq!(timeouts.recycle, Some(POOL_RECYCLE_TIMEOUT));

    for kind in [
        deadpool_postgres::TimeoutType::Wait,
        deadpool_postgres::TimeoutType::Create,
        deadpool_postgres::TimeoutType::Recycle,
    ] {
        let mapped = map_pool_err(PoolError::Timeout(kind));
        assert!(
            matches!(mapped, StoreError::Unavailable(_)),
            "expected PoolError::Timeout({kind:?}) -> Unavailable, got {mapped:?}"
        );
    }
}

#[test]
fn commit_watermark_encode_decode_round_trips_and_rejects_invalid() {
    for seq in [0i64, 1, 42, i64::MAX] {
        let encoded = encode_commit_watermark(seq);
        assert_eq!(encoded.len(), 8);
        assert_eq!(decode_commit_watermark(&encoded).unwrap(), seq);
    }
    assert!(matches!(
        decode_commit_watermark(&[]),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        decode_commit_watermark(&[0u8; 7]),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        decode_commit_watermark(&(-1i64).to_be_bytes()),
        Err(StoreError::InvalidArgument(_))
    ));
}

#[test]
fn retryable_sqlstate_detects_serialization_and_deadlock_only() {
    assert!(is_retryable_sqlstate(&SqlState::T_R_SERIALIZATION_FAILURE));
    assert!(is_retryable_sqlstate(&SqlState::T_R_DEADLOCK_DETECTED));
    assert!(!is_retryable_sqlstate(&SqlState::UNIQUE_VIOLATION));
    assert!(!is_retryable_sqlstate(&SqlState::FOREIGN_KEY_VIOLATION));
    assert!(!is_retryable_sqlstate(&SqlState::INVALID_PASSWORD));
}

#[test]
fn postgres_schema_ddl_has_required_tables_collation_cascade_and_commit_seq() {
    for needle in [
        "CREATE TABLE IF NOT EXISTS projects",
        "commit_seq BIGINT NOT NULL DEFAULT 0",
        "ADD COLUMN IF NOT EXISTS commit_seq BIGINT NOT NULL DEFAULT 0",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_name",
        "CREATE TABLE IF NOT EXISTS repositories",
        "CREATE TABLE IF NOT EXISTS op_heads",
        "CREATE TABLE IF NOT EXISTS commits",
        "seq BIGINT NOT NULL",
        "CREATE INDEX IF NOT EXISTS commits_by_change_id",
        "CREATE INDEX IF NOT EXISTS commits_by_seq",
        "SET commit_seq = GREATEST(p.commit_seq, c.max_seq)",
        "CREATE TABLE IF NOT EXISTS trees",
        "CREATE TABLE IF NOT EXISTS tree_entries",
        "name TEXT COLLATE \"C\" NOT NULL",
        "CREATE TABLE IF NOT EXISTS symlinks",
        "CREATE TABLE IF NOT EXISTS blobs",
        "CREATE TABLE IF NOT EXISTS project_blobs",
        "CREATE TABLE IF NOT EXISTS views",
        "CREATE TABLE IF NOT EXISTS operations",
        "CREATE TABLE IF NOT EXISTS workspaces",
        "\"user\" TEXT NOT NULL",
        "workspace_name TEXT COLLATE \"C\" NOT NULL",
        "PRIMARY KEY (repo_id, workspace_name)",
        "ON DELETE CASCADE",
    ] {
        assert!(
            POSTGRES_SCHEMA.contains(needle),
            "postgres_schema.sql missing expected DDL fragment: {needle}"
        );
    }
    assert!(
        !POSTGRES_SCHEMA.contains("user_email"),
        "postgres_schema.sql must not reference legacy user_email column"
    );
}

#[tokio::test]
async fn connect_rejects_empty_url_and_zero_max_conns() {
    let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
        url: "   ".to_string(),
        ..Default::default()
    })
    .await
    .err()
    .expect("empty url must fail");
    assert!(matches!(err, StoreError::InvalidArgument(_)));

    let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
        url: "postgres://u:secret@127.0.0.1:5432/db?sslmode=disable".to_string(),
        max_conns: 0,
        ..Default::default()
    })
    .await
    .err()
    .expect("zero max_conns must fail");
    assert!(matches!(err, StoreError::InvalidArgument(_)));

    // Fail closed before dialing when `sslrootcert=system` is paired with non-verify-full.
    let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
        url: "postgres://u:supersecret@127.0.0.1:5432/db?sslmode=require&sslrootcert=system"
            .to_string(),
        ..Default::default()
    })
    .await
    .err()
    .expect("sslrootcert=system with sslmode=require must fail closed");
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    assert!(!err.to_string().contains("supersecret"));

    // Malformed DSNs and password-smuggling URIs must fail closed before dialing and never
    // echo secret bytes via TLS, CA-file, or `tokio-postgres` option-name errors.
    for bad_url in [
        "host=db.example.com sslmode='require password=TOPSECRET",
        "host=db.example.com sslmode=verify-ca sslrootcert='/etc/ca.pem password=TOPSECRET",
        "host=db.example.com port= password=TOPSECRET dbname=jj",
        "host=db.example.com sslmode= password=TOPSECRET",
        "host=db.example.com sslrootcert= password=TOPSECRET",
        "host=db.example.com sslmode='require password=TOPSECRET sslrootcert='/etc/ca.pem'",
        "host=db.example.com application_name='my app password=TOPSECRET dbname='jj'",
        "host=db.example.com password=my TOPSECRET dbname=jj",
        "host=db.example.com password=my aGVsbG8= dbname=jj",
        "host=db.example.com password=my foo=TOPSECRET dbname=jj",
        "host=db.example.com password=my =TOPSECRET dbname=jj",
        "host=db.example.com password='my''TOP=SECRET' dbname=jj",
        "host=db.example.com password=my TOPSECRET='oops",
        "host=db.example.com sslpassword=TOPSECRET",
        "postgres://alice:TOPSECRET",
        "postgres://alice:12345678",
        "postgres://alice@proj.iam:TOPSECRET",
        "postgres://alice@proj.iam:TOP/PASS?sslmode=require",
        "postgres://alice@proj.iam:TOP?SECRET",
        "postgres://alice:p@ss:TOPSECRET",
        "postgres://alice:p?1@ss:TOPSECRET",
        "postgres://alice:p?1@ss:TOP/PASS?sslmode=require",
        "postgres://[::1]:12345678",
        "postgres://[::1]foo:TOPSECRET",
        "postgres://[::1]foo:TOPSECRET/jj",
        "postgres://alice:TOP?SECRET",
        "postgres://alice:TOP?k=TOPSECRET",
        "postgres://alice:TOP/PASS?k=TOPSECRET",
        "postgres://[alice:TOPSECRET]foo",
        "postgres://db.example.com/jj?sslmode=require;password=TOPSECRET",
        "postgres://db.example.com/jj?sslmode=require?password=TOPSECRET",
        "postgres://db.example.com/jj?sslmode=require;%70assword=TOPSECRET",
        "postgres://alice@db.example.com/jj?password=my&TOPSECRET",
        "postgres://alice@db.example.com/jj?password=my&aGVsbG8=",
        "postgres://alice@db.example.com/jj?sslpassword=TOPSECRET",
        "postgres://alice@db.example.com:5432/jj?password=part1&part2@TOPSECRET&sslmode=require",
        "postgres://alice@db.example.com:5432/jj?password=part1&k=v@TOPSECRET&sslmode=require",
        "postgres://alice@db.example.com/jj?password=part1&part2:1@TOPSECRET",
        "postgres://alice:secret@db.example.com/jj?foo=1@password=TOPSECRET?sslmode=require",
        "postgres://alice:p@ss?a=1@password=TOPSECRET?sslmode=require",
        "postgres://alice:secret@db.example.com/jj?foo=1@password%3DTOPSECRET",
        "postgres://alice@proj.iam:TOPSECRET?foo=bar@db.example.com:5432/jj",
        "postgres://alice@proj?iam:TOPSECRET@db.example.com:5432/jj",
        "postgres://alice:p@ss?sslmode=TOPSECRET@db.example.com:5432/jj",
        "postgres://alice:p@ss?sslrootcert=TOPSECRET&application_name=x@db.example.com:5432/jj",
        "postgres://alice:p@ss?TOPSECRET=1@db.example.com:5432/jj",
    ] {
        let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
            url: bad_url.to_string(),
            ..Default::default()
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("expected InvalidArgument for {bad_url:?}"));
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "expected InvalidArgument for {bad_url:?}, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET")
                && !msg.contains("topsecret")
                && !msg.contains("aGVsbG8=")
                && !msg.contains("TOP=SECRET")
                && !msg.contains("12345678")
                && !msg.contains("SECRET"),
            "connect() error leaked secret for {bad_url:?}: {msg}"
        );
    }

    // `tokio-postgres` parses these malformed URIs as a non-routable host (splitting at `@` or
    // accepting `[...]` as a host literal) and fails at dial time; verify every returned error
    // still masks `TOPSECRET`.
    for dial_fail_url in [
        "postgres://[alice:TOPSECRET]",
        "postgres://alice:00012345",
        "postgres://alice@proj.iam:00012345",
        "postgres://db.example.com:5432/jj?password =my@TOPSECRET",
        "postgres://db.example.com:5432/jj?password%20=my@TOPSECRET",
        "postgres://db.example.com:5432/jj? password=my@TOPSECRET",
        "postgres://db.example.com:5432/jj?sslmode;password=my@TOPSECRET",
        "postgres://db.example.com:5432/jj?sslmode=require&;password=my@TOPSECRET",
        "postgres://db.example.com:5432/jj?password%3Dmy@TOPSECRET",
    ] {
        let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
            url: dial_fail_url.to_string(),
            ..Default::default()
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("expected connect failure for {dial_fail_url:?}"));
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET") && !msg.contains("topsecret") && !msg.contains("00012345"),
            "connect() error leaked secret for {dial_fail_url:?}: {msg}"
        );
    }
}

#[tokio::test]
async fn connect_with_cli_ca_cert_fails_closed_on_contradictory_modes_and_plaintext_downgrade() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    // Public self-signed test CA certificate (no private key).
    const TEST_CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBmzCCAUGgAwIBAgIUIy0ZlRPAHq+b7l5bOv7v2fKcfZkwCgYIKoZIzj0EAwIw\n\
IjEgMB4GA1UEAwwXamotY29tbWl0LWNsb3VkLXRlc3QtY2EwIBcNMjYxMTI3MTkz\n\
MjA2WhgPMjEyNjExMDMxOTMyMDZaMCIxIDAeBgNVBAMMF2pqLWNvbW1pdC1jbG91\n\
ZC10ZXN0LWNhMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEnv4YRR/jNty2+QNz\n\
JWqWTgskMP6xINVQidiYlny7FddpzmApMXWlynY0o084cLAwTjwxUhUTq51q7CAI\n\
vSFsbaNTMFEwHQYDVR0OBBYEFEiXCYbPOSgN/TeTdtXMXHUC1GftMB8GA1UdIwQY\n\
MBaAFEiXCYbPOSgN/TeTdtXMXHUC1GftMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI\n\
zj0EAwIDSAAwRQIhAKKb3QrZHXKt9tdKXELc2ESuS6P8YsmwDEyv8EjL5NiqAiAv\n\
KmRXZ3BBBzL15PrE6TZgYTht9uk9HeMu9tAlQxjLrw==\n\
-----END CERTIFICATE-----\n";

    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("test-ca.pem");
    std::fs::write(&ca_path, TEST_CA_PEM).unwrap();

    // Contradictory sslmodes fail closed before dialing and never echo credentials.
    for bad_url in [
        "postgres://alice:TOPSECRET@127.0.0.1:5432/jj?sslmode=disable",
        "postgres://alice:TOPSECRET@127.0.0.1:5432/jj?sslmode=prefer",
    ] {
        let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
            url: bad_url.to_string(),
            ca_cert: Some(ca_path.clone()),
            ..Default::default()
        })
        .await
        .err()
        .unwrap_or_else(|| panic!("expected InvalidArgument for {bad_url}"));
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "expected InvalidArgument for {bad_url}, got {err:?}"
        );
        assert!(!err.to_string().contains("TOPSECRET"));
    }

    #[cfg(unix)]
    {
        for bad_unix_dsn in [
            "host=/var/run/postgresql user=alice password=TOPSECRET dbname=jj",
            "host=/var/run/postgresql,127.0.0.1 user=alice password=TOPSECRET dbname=jj",
        ] {
            let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
                url: bad_unix_dsn.to_string(),
                ca_cert: Some(ca_path.clone()),
                ..Default::default()
            })
            .await
            .err()
            .expect("Unix socket with --postgres-ca-cert must fail closed");
            assert!(matches!(err, StoreError::InvalidArgument(_)));
            assert!(!err.to_string().contains("TOPSECRET"));
        }

        let mixed_tls_err = PostgresMetadataStore::connect(&PostgresConnectConfig {
            url: "host=/var/run/postgresql,127.0.0.1 user=alice password=TOPSECRET dbname=jj sslmode=require"
                .to_string(),
            ..Default::default()
        })
        .await
        .err()
        .expect("mixed Unix+TCP DSN with sslmode=require must fail closed");
        assert!(matches!(mixed_tls_err, StoreError::InvalidArgument(_)));
        assert!(!mixed_tls_err.to_string().contains("TOPSECRET"));
    }

    // Wire-level downgrade test: a server replying `'N'` to PostgreSQL `SSLRequest` must cause
    // `connect()` with `ca_cert` and omitted `sslmode` to abort immediately without sending a
    // plaintext `StartupMessage`.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut ssl_req = [0u8; 8];
        stream.read_exact(&mut ssl_req).await.unwrap();
        assert_eq!(
            ssl_req,
            [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f],
            "client must initiate PostgreSQL SSLRequest"
        );
        stream.write_all(b"N").await.unwrap();
        stream.flush().await.unwrap();
        let mut extra = [0u8; 16];
        let n = stream.read(&mut extra).await.unwrap_or(0);
        assert_eq!(
            n, 0,
            "client with --postgres-ca-cert must not send plaintext StartupMessage after SSL refusal"
        );
    });

    let err = PostgresMetadataStore::connect(&PostgresConnectConfig {
        url: format!("postgres://alice:TOPSECRET@{addr}/jj"),
        ca_cert: Some(ca_path),
        ..Default::default()
    })
    .await
    .err()
    .expect("plaintext downgrade must be rejected when --postgres-ca-cert is set");
    assert!(
        !err.to_string().contains("TOPSECRET"),
        "downgrade error leaked password: {err}"
    );
    mock_server.await.unwrap();
}

#[test]
fn direct_ssl_negotiation_sets_postgresql_alpn_on_rustls_client_configs() {
    const TEST_CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBmzCCAUGgAwIBAgIUIy0ZlRPAHq+b7l5bOv7v2fKcfZkwCgYIKoZIzj0EAwIw\n\
IjEgMB4GA1UEAwwXamotY29tbWl0LWNsb3VkLXRlc3QtY2EwIBcNMjYxMTI3MTkz\n\
MjA2WhgPMjEyNjExMDMxOTMyMDZaMCIxIDAeBgNVBAMMF2pqLWNvbW1pdC1jbG91\n\
ZC10ZXN0LWNhMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEnv4YRR/jNty2+QNz\n\
JWqWTgskMP6xINVQidiYlny7FddpzmApMXWlynY0o084cLAwTjwxUhUTq51q7CAI\n\
vSFsbaNTMFEwHQYDVR0OBBYEFEiXCYbPOSgN/TeTdtXMXHUC1GftMB8GA1UdIwQY\n\
MBaAFEiXCYbPOSgN/TeTdtXMXHUC1GftMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI\n\
zj0EAwIDSAAwRQIhAKKb3QrZHXKt9tdKXELc2ESuS6P8YsmwDEyv8EjL5NiqAiAv\n\
KmRXZ3BBBzL15PrE6TZgYTht9uk9HeMu9tAlQxjLrw==\n\
-----END CERTIFICATE-----\n";

    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("alpn-ca.pem");
    std::fs::write(&ca_path, TEST_CA_PEM).unwrap();
    let ca_source = CaCertSource::CliPemFile(ca_path);

    // Verify URI and DSN parsing preserve `sslnegotiation=direct` vs default `postgres`.
    for (url, expected_direct) in [
        (
            "postgres://alice@db.example.com/jj?sslmode=require&sslnegotiation=direct",
            true,
        ),
        (
            "host=db.example.com user=alice dbname=jj sslmode=verify-full sslnegotiation=direct",
            true,
        ),
        ("postgres://alice@db.example.com/jj?sslmode=require", false),
        (
            "postgres://alice@db.example.com/jj?sslmode=require&sslnegotiation=postgres",
            false,
        ),
    ] {
        let parsed = parse_postgres_tls_options(url).unwrap();
        let pg_cfg = tokio_postgres::Config::from_str(&parsed.normalized_dsn).unwrap();
        let direct = pg_cfg.get_ssl_negotiation() == PgSslNegotiation::Direct;
        assert_eq!(direct, expected_direct, "url={url}");
    }

    let expected_alpn = vec![b"postgresql".to_vec()];
    assert_eq!(
        build_encrypt_only_rustls_config(true)
            .unwrap()
            .alpn_protocols,
        expected_alpn
    );
    assert!(
        build_encrypt_only_rustls_config(false)
            .unwrap()
            .alpn_protocols
            .is_empty()
    );

    for verify_hostname in [false, true] {
        assert_eq!(
            build_verified_rustls_config(&ca_source, verify_hostname, true)
                .unwrap()
                .alpn_protocols,
            expected_alpn
        );
        assert!(
            build_verified_rustls_config(&ca_source, verify_hostname, false)
                .unwrap()
                .alpn_protocols
                .is_empty()
        );
    }
}

// Public ECDSA P-256 test certificates (valid 2026-01-01 to 2048-01-01, no private keys).
const VERIFY_CA_TEST_ROOT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBjjCCATSgAwIBAgICEAEwCgYIKoZIzj0EAwIwLjEsMCoGA1UEAwwjamotY29t\n\
bWl0LWNsb3VkLXZlcmlmeS1jYS10ZXN0LXJvb3QwHhcNMjYwMTAxMDAwMDAwWhcN\n\
NDgwMTAxMDAwMDAwWjAuMSwwKgYDVQQDDCNqai1jb21taXQtY2xvdWQtdmVyaWZ5\n\
LWNhLXRlc3Qtcm9vdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABN3fqfDQ6Ckq\n\
5/B183dfPgoJ5DbNPZW6S7gK4UV+/xUknfLwwqlhvNgkGcxdtoyGPmXE1TT4Dz+/\n\
JLEvXKDX2pejQjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0G\n\
A1UdDgQWBBTQ0FvMJgZ82kWIoo8y3Cj4KC2meDAKBggqhkjOPQQDAgNIADBFAiEA\n\
rRiQcqOlpPBf3aNuIgz1NA9ibE45O42adSjiGjnG00ACIFrXD8lFrDCBnZspPVn6\n\
C4lFUGFDm+6x+3Tm7qVhVx/Y\n\
-----END CERTIFICATE-----\n";

// Leaf certificate for DNS:db.example.com signed by VERIFY_CA_TEST_ROOT_PEM (no private key).
const VERIFY_CA_TEST_LEAF_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIByDCCAW+gAwIBAgICEAIwCgYIKoZIzj0EAwIwLjEsMCoGA1UEAwwjamotY29t\n\
bWl0LWNsb3VkLXZlcmlmeS1jYS10ZXN0LXJvb3QwHhcNMjYwMTAxMDAwMDAwWhcN\n\
NDgwMTAxMDAwMDAwWjAZMRcwFQYDVQQDDA5kYi5leGFtcGxlLmNvbTBZMBMGByqG\n\
SM49AgEGCCqGSM49AwEHA0IABNqI4aCIlFyaLlAkHJJ8Ha7vHorwcgi3FgJTjRlb\n\
/8LU/JspKG87bhgBXE5bYD0DzN/8tlaKNNt0uTV9Dly4OGGjgZEwgY4wDAYDVR0T\n\
AQH/BAIwADAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwGQYD\n\
VR0RBBIwEIIOZGIuZXhhbXBsZS5jb20wHQYDVR0OBBYEFF0qC+aoGH8I0B/pHBRX\n\
QSM7hwuSMB8GA1UdIwQYMBaAFNDQW8wmBnzaRYiijzLcKPgoLaZ4MAoGCCqGSM49\n\
BAMCA0cAMEQCICt3rKQgRgBGy41axE2pQPq9VnrIjb1yH4fAs3uLA0ASAiA6k2Ub\n\
dfoJQTgMfgERXBmhPuH8CG62VPS0AMi/ob6SwA==\n\
-----END CERTIFICATE-----\n";

// Independent public ECDSA P-256 test CA that did not sign VERIFY_CA_TEST_LEAF_PEM.
const VERIFY_CA_TEST_UNTRUSTED_ROOT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIBjjCCATSgAwIBAgICIAEwCgYIKoZIzj0EAwIwLjEsMCoGA1UEAwwjamotY29t\n\
bWl0LWNsb3VkLXVudHJ1c3RlZC10ZXN0LXJvb3QwHhcNMjYwMTAxMDAwMDAwWhcN\n\
NDgwMTAxMDAwMDAwWjAuMSwwKgYDVQQDDCNqai1jb21taXQtY2xvdWQtdW50cnVz\n\
dGVkLXRlc3Qtcm9vdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABNv4mQCE0cpa\n\
/D42UPMqTZE/FFyqGAD88v9RRxzTG1FoS1f9R893c32COusqjsSr/EacaWS9ND9g\n\
/HsWqom1oGijQjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0G\n\
A1UdDgQWBBSIJCcPVah42ArfOTlswAtzeUI9VzAKBggqhkjOPQQDAgNIADBFAiEA\n\
uv5RvU40QBkfBFdHQKuC05ybO1LUWqbymo0Dm7XAS1kCICd4+/KcgOOI/ysSH2pL\n\
rMCWT6KtB4RhLxmNyrxT0tWZ\n\
-----END CERTIFICATE-----\n";

#[test]
fn load_root_cert_store_handles_missing_empty_corrupt_and_redacts_url_paths() {
    let dir = tempfile::tempdir().unwrap();
    let redacted_url = "postgres://alice:***@db.example.com:5432/jj";

    // Missing file: CliPemFile reports the path; UrlPemFile reports only `redacted_url`.
    let missing_path = dir.path().join("TOPSECRET-missing.pem");
    let cli_err =
        load_root_cert_store(&CaCertSource::CliPemFile(missing_path.clone())).unwrap_err();
    assert!(matches!(cli_err, StoreError::InvalidArgument(_)));
    assert!(cli_err.to_string().contains("TOPSECRET-missing.pem"));

    let url_err = load_root_cert_store(&CaCertSource::UrlPemFile {
        path: missing_path,
        redacted_url: redacted_url.to_string(),
    })
    .unwrap_err();
    assert!(matches!(url_err, StoreError::InvalidArgument(_)));
    let url_msg = url_err.to_string();
    assert!(url_msg.contains(redacted_url), "unexpected msg: {url_msg}");
    assert!(
        !url_msg.contains("TOPSECRET"),
        "UrlPemFile error leaked raw sslrootcert path: {url_msg}"
    );

    // Empty file -> InvalidArgument with no X.509 certs (and no path leak for UrlPemFile).
    let empty_path = dir.path().join("TOPSECRET-empty.pem");
    std::fs::write(&empty_path, b"").unwrap();
    let cli_empty =
        load_root_cert_store(&CaCertSource::CliPemFile(empty_path.clone())).unwrap_err();
    assert!(matches!(cli_empty, StoreError::InvalidArgument(_)));
    assert!(cli_empty.to_string().contains("no valid X.509"));

    let url_empty = load_root_cert_store(&CaCertSource::UrlPemFile {
        path: empty_path,
        redacted_url: redacted_url.to_string(),
    })
    .unwrap_err();
    assert!(matches!(url_empty, StoreError::InvalidArgument(_)));
    assert!(!url_empty.to_string().contains("TOPSECRET"));

    // Corrupt PEM -> InvalidArgument parse error (and no path leak for UrlPemFile).
    let corrupt_path = dir.path().join("TOPSECRET-corrupt.pem");
    std::fs::write(
        &corrupt_path,
        "-----BEGIN CERTIFICATE-----\n!!!not-valid-base64!!!\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    let cli_corrupt =
        load_root_cert_store(&CaCertSource::CliPemFile(corrupt_path.clone())).unwrap_err();
    assert!(matches!(cli_corrupt, StoreError::InvalidArgument(_)));
    assert!(cli_corrupt.to_string().contains("failed to parse PEM"));

    let url_corrupt = load_root_cert_store(&CaCertSource::UrlPemFile {
        path: corrupt_path,
        redacted_url: redacted_url.to_string(),
    })
    .unwrap_err();
    assert!(matches!(url_corrupt, StoreError::InvalidArgument(_)));
    assert!(!url_corrupt.to_string().contains("TOPSECRET"));

    // Valid PEM.
    let valid_path = dir.path().join("valid-ca.pem");
    std::fs::write(&valid_path, VERIFY_CA_TEST_ROOT_PEM).unwrap();
    let roots = load_root_cert_store(&CaCertSource::CliPemFile(valid_path)).unwrap();
    assert_eq!(roots.len(), 1);
}

#[test]
fn verify_ca_server_verifier_enforces_ca_and_validity_while_skipping_hostname() {
    ensure_rustls_crypto_provider();
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let untrusted_ca_path = dir.path().join("untrusted-ca.pem");
    std::fs::write(&ca_path, VERIFY_CA_TEST_ROOT_PEM).unwrap();
    std::fs::write(&untrusted_ca_path, VERIFY_CA_TEST_UNTRUSTED_ROOT_PEM).unwrap();

    let trusted_roots = Arc::new(load_root_cert_store(&CaCertSource::CliPemFile(ca_path)).unwrap());
    let untrusted_roots =
        Arc::new(load_root_cert_store(&CaCertSource::CliPemFile(untrusted_ca_path)).unwrap());
    let leaf = CertificateDer::from_pem_slice(VERIFY_CA_TEST_LEAF_PEM.as_bytes()).unwrap();

    let webpki = WebPkiServerVerifier::builder_with_provider(trusted_roots, Arc::clone(&provider))
        .build()
        .unwrap();
    let verify_ca = VerifyCaServerVerifier {
        inner: Arc::clone(&webpki),
    };

    let matching_name = ServerName::try_from("db.example.com").unwrap();
    let mismatched_name = ServerName::try_from("other.example.com").unwrap();
    // Fixed timestamps inside (2027-01-15), after (2050-01-01), and before (2025-01-01)
    // the 2026-01-01..2048-01-01 certificate validity window.
    let valid_time = UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000));
    let expired_time = UnixTime::since_unix_epoch(Duration::from_secs(2_524_608_000));
    let not_yet_valid_time = UnixTime::since_unix_epoch(Duration::from_secs(1_735_689_600));

    // Matching SAN + trusted CA + valid time passes both verify-full and verify-ca.
    webpki
        .verify_server_cert(&leaf, &[], &matching_name, &[], valid_time)
        .expect("matching SAN should pass WebPkiServerVerifier");
    verify_ca
        .verify_server_cert(&leaf, &[], &matching_name, &[], valid_time)
        .expect("matching SAN should pass VerifyCaServerVerifier");

    // Mismatched SAN fails WebPkiServerVerifier (`verify-full`) but passes `verify-ca`.
    let full_mismatch = webpki
        .verify_server_cert(&leaf, &[], &mismatched_name, &[], valid_time)
        .unwrap_err();
    assert!(
        matches!(
            full_mismatch,
            RustlsError::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            )
        ),
        "expected hostname mismatch error from WebPkiServerVerifier, got {full_mismatch:?}"
    );
    verify_ca
        .verify_server_cert(&leaf, &[], &mismatched_name, &[], valid_time)
        .expect("VerifyCaServerVerifier must accept CA-signed cert with mismatched SAN");

    // Untrusted CA is rejected even with a matching SAN.
    let untrusted_webpki = WebPkiServerVerifier::builder_with_provider(untrusted_roots, provider)
        .build()
        .unwrap();
    let untrusted_verify_ca = VerifyCaServerVerifier {
        inner: untrusted_webpki,
    };
    let untrusted_err = untrusted_verify_ca
        .verify_server_cert(&leaf, &[], &matching_name, &[], valid_time)
        .unwrap_err();
    assert!(
        matches!(
            untrusted_err,
            RustlsError::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "expected UnknownIssuer from untrusted CA, got {untrusted_err:?}"
    );

    // Expired and not-yet-valid certificates are still rejected by VerifyCaServerVerifier.
    let expired_err = verify_ca
        .verify_server_cert(&leaf, &[], &matching_name, &[], expired_time)
        .unwrap_err();
    assert!(
        matches!(
            expired_err,
            RustlsError::InvalidCertificate(
                CertificateError::Expired | CertificateError::ExpiredContext { .. }
            )
        ),
        "expected Expired error, got {expired_err:?}"
    );

    let not_yet_err = verify_ca
        .verify_server_cert(&leaf, &[], &matching_name, &[], not_yet_valid_time)
        .unwrap_err();
    assert!(
        matches!(
            not_yet_err,
            RustlsError::InvalidCertificate(
                CertificateError::NotValidYet | CertificateError::NotValidYetContext { .. }
            )
        ),
        "expected NotValidYet error, got {not_yet_err:?}"
    );
}
