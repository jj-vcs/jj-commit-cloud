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

//! PostgreSQL TLS policy resolution, root CA loading, and `rustls` connector builders.
//!
//! # Cross-Process and RPC Boundaries
//! Implements libpq `sslmode` semantics (`disable`, `prefer`, `require`, `verify-ca`,
//! `verify-full`) over `rustls`, installs the `ring` crypto provider once per process, sets
//! ALPN `"postgresql"` for PostgreSQL 17 direct TLS negotiation, and fails closed when
//! `--postgres-ca-cert` is combined with plaintext downgrade modes or Unix-domain sockets.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use tokio_postgres::config::{Host, SslMode as PgSslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;

use crate::store::{StoreError, StoreResult};

static RUSTLS_CRYPTO_INIT: Once = Once::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ParsedSslMode {
    Unspecified,
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParsedPgTlsOptions {
    pub(super) ssl_mode: ParsedSslMode,
    pub(super) ssl_root_cert: Option<String>,
    pub(super) normalized_dsn: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ResolvedTlsPolicy {
    NoTls,
    EncryptOnly {
        pg_ssl_mode: PgSslMode,
    },
    VerifyCa {
        pg_ssl_mode: PgSslMode,
        ca_source: CaCertSource,
    },
    VerifyFull {
        ca_source: CaCertSource,
    },
}

impl ResolvedTlsPolicy {
    pub(super) fn pg_ssl_mode(&self) -> PgSslMode {
        match self {
            Self::NoTls => PgSslMode::Disable,
            Self::EncryptOnly { pg_ssl_mode } | Self::VerifyCa { pg_ssl_mode, .. } => *pg_ssl_mode,
            Self::VerifyFull { .. } => PgSslMode::Require,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CaCertSource {
    SystemRoots,
    /// CA path supplied explicitly via `--postgres-ca-cert` / `POSTGRES_CA_CERT`.
    CliPemFile(PathBuf),
    /// CA path extracted from a connection URI/DSN `sslrootcert` parameter; error messages refer
    /// to `redacted_url` rather than echoing the raw parameter value so malformed URIs cannot leak
    /// password bytes through filesystem error messages.
    UrlPemFile {
        path: PathBuf,
        redacted_url: String,
    },
}

pub(super) fn ensure_rustls_crypto_provider() {
    RUSTLS_CRYPTO_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn hosts_are_all_unix(hosts: &[Host]) -> bool {
    #[cfg(unix)]
    {
        !hosts.is_empty() && hosts.iter().all(|host| matches!(host, Host::Unix(_)))
    }
    #[cfg(not(unix))]
    {
        let _ = hosts;
        false
    }
}

fn hosts_contain_unix(hosts: &[Host]) -> bool {
    #[cfg(unix)]
    {
        hosts.iter().any(|host| matches!(host, Host::Unix(_)))
    }
    #[cfg(not(unix))]
    {
        let _ = hosts;
        false
    }
}

pub(super) fn resolve_tls_policy(
    hosts: &[Host],
    ssl_mode: ParsedSslMode,
    ca_cert_flag: Option<&Path>,
    ssl_root_cert_param: Option<&str>,
    redacted_url: &str,
) -> StoreResult<ResolvedTlsPolicy> {
    if let Some(ca_path) = ca_cert_flag {
        if hosts_contain_unix(hosts) {
            return Err(StoreError::InvalidArgument(
                "--postgres-ca-cert (POSTGRES_CA_CERT) requires TLS and cannot be used with \
                 Unix-domain socket hosts; unset --postgres-ca-cert or use TCP hosts only"
                    .to_string(),
            ));
        }
        let ca_source = CaCertSource::CliPemFile(ca_path.to_path_buf());
        return match ssl_mode {
            ParsedSslMode::Disable => Err(StoreError::InvalidArgument(
                "--postgres-ca-cert (POSTGRES_CA_CERT) requires TLS and cannot be combined with \
                 sslmode=disable; unset --postgres-ca-cert or use \
                 sslmode=verify-full/verify-ca/require"
                    .to_string(),
            )),
            ParsedSslMode::Prefer => Err(StoreError::InvalidArgument(
                "--postgres-ca-cert (POSTGRES_CA_CERT) requires TLS and cannot be combined with \
                 sslmode=prefer (which allows plaintext downgrade); omit sslmode or use \
                 sslmode=verify-full/verify-ca/require"
                    .to_string(),
            )),
            ParsedSslMode::Unspecified | ParsedSslMode::VerifyFull => {
                Ok(ResolvedTlsPolicy::VerifyFull { ca_source })
            }
            ParsedSslMode::VerifyCa | ParsedSslMode::Require => Ok(ResolvedTlsPolicy::VerifyCa {
                pg_ssl_mode: PgSslMode::Require,
                ca_source,
            }),
        };
    }

    if ssl_root_cert_param.is_some_and(|rc| rc.eq_ignore_ascii_case("system"))
        && ssl_mode != ParsedSslMode::VerifyFull
    {
        return Err(StoreError::InvalidArgument(
            "PostgreSQL sslrootcert=system is only allowed with sslmode=verify-full \
             (matching libpq)"
                .to_string(),
        ));
    }

    if hosts_are_all_unix(hosts) {
        return match ssl_mode {
            ParsedSslMode::Disable | ParsedSslMode::Unspecified | ParsedSslMode::Prefer => {
                Ok(ResolvedTlsPolicy::NoTls)
            }
            ParsedSslMode::Require | ParsedSslMode::VerifyCa | ParsedSslMode::VerifyFull => {
                Err(StoreError::InvalidArgument(
                    "PostgreSQL TLS sslmode (require/verify-ca/verify-full) cannot be used with \
                     Unix-domain socket hosts; use sslmode=disable or TCP hosts only"
                        .to_string(),
                ))
            }
        };
    }

    if hosts_contain_unix(hosts)
        && matches!(
            ssl_mode,
            ParsedSslMode::Require | ParsedSslMode::VerifyCa | ParsedSslMode::VerifyFull
        )
    {
        return Err(StoreError::InvalidArgument(
            "PostgreSQL TLS sslmode (require/verify-ca/verify-full) cannot be used with \
             Unix-domain socket hosts; use sslmode=disable or TCP hosts only"
                .to_string(),
        ));
    }

    let ca_source = if let Some(root_cert) = ssl_root_cert_param {
        if root_cert.eq_ignore_ascii_case("system") {
            CaCertSource::SystemRoots
        } else {
            CaCertSource::UrlPemFile {
                path: PathBuf::from(root_cert),
                redacted_url: redacted_url.to_string(),
            }
        }
    } else {
        CaCertSource::SystemRoots
    };
    let has_explicit_ca_file = !matches!(ca_source, CaCertSource::SystemRoots);

    match ssl_mode {
        ParsedSslMode::Disable => Ok(ResolvedTlsPolicy::NoTls),
        ParsedSslMode::VerifyFull => Ok(ResolvedTlsPolicy::VerifyFull { ca_source }),
        ParsedSslMode::VerifyCa => Ok(ResolvedTlsPolicy::VerifyCa {
            pg_ssl_mode: PgSslMode::Require,
            ca_source,
        }),
        ParsedSslMode::Require => {
            if has_explicit_ca_file {
                Ok(ResolvedTlsPolicy::VerifyCa {
                    pg_ssl_mode: PgSslMode::Require,
                    ca_source,
                })
            } else {
                Ok(ResolvedTlsPolicy::EncryptOnly {
                    pg_ssl_mode: PgSslMode::Require,
                })
            }
        }
        ParsedSslMode::Prefer | ParsedSslMode::Unspecified => {
            if has_explicit_ca_file {
                Ok(ResolvedTlsPolicy::VerifyCa {
                    pg_ssl_mode: PgSslMode::Prefer,
                    ca_source,
                })
            } else {
                Ok(ResolvedTlsPolicy::EncryptOnly {
                    pg_ssl_mode: PgSslMode::Prefer,
                })
            }
        }
    }
}

pub(super) fn load_root_cert_store(source: &CaCertSource) -> StoreResult<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    match source {
        CaCertSource::CliPemFile(path) => {
            let pem_bytes = std::fs::read(path).map_err(|e| {
                StoreError::InvalidArgument(format!(
                    "failed to read PostgreSQL CA certificate file '{}': {e}",
                    path.display()
                ))
            })?;
            let mut parsed = Vec::new();
            for item in CertificateDer::pem_slice_iter(&pem_bytes) {
                let cert = item.map_err(|e| {
                    StoreError::InvalidArgument(format!(
                        "failed to parse PEM certificate from '{}': {e}",
                        path.display()
                    ))
                })?;
                parsed.push(cert);
            }
            let (added, _ignored) = roots.add_parsable_certificates(parsed);
            if added == 0 {
                return Err(StoreError::InvalidArgument(format!(
                    "PostgreSQL CA certificate file '{}' contains no valid X.509 certificates",
                    path.display()
                )));
            }
        }
        CaCertSource::UrlPemFile { path, redacted_url } => {
            let pem_bytes = std::fs::read(path).map_err(|e| {
                StoreError::InvalidArgument(format!(
                    "failed to read PostgreSQL sslrootcert CA certificate configured in \
                     '{redacted_url}': {e}"
                ))
            })?;
            let mut parsed = Vec::new();
            for item in CertificateDer::pem_slice_iter(&pem_bytes) {
                let cert = item.map_err(|e| {
                    StoreError::InvalidArgument(format!(
                        "failed to parse PEM certificate from PostgreSQL sslrootcert configured \
                         in '{redacted_url}': {e}"
                    ))
                })?;
                parsed.push(cert);
            }
            let (added, _ignored) = roots.add_parsable_certificates(parsed);
            if added == 0 {
                return Err(StoreError::InvalidArgument(format!(
                    "PostgreSQL sslrootcert configured in '{redacted_url}' contains no valid \
                     X.509 certificates"
                )));
            }
        }
        CaCertSource::SystemRoots => {
            let cert_result = rustls_native_certs::load_native_certs();
            for err in &cert_result.errors {
                warn!("non-fatal error loading native TLS root certificate: {err}");
            }
            let (added, _ignored) = roots.add_parsable_certificates(cert_result.certs);
            if added == 0 {
                return Err(StoreError::Unavailable(
                    "no native TLS root certificates found for PostgreSQL TLS connection; \
                     install system CA certificates, set SSL_CERT_FILE, or pass --postgres-ca-cert"
                        .to_string(),
                ));
            }
        }
    }
    Ok(roots)
}

#[derive(Debug)]
pub(super) struct VerifyCaServerVerifier {
    pub(super) inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for VerifyCaServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(RustlsError::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(err) => Err(err),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[derive(Debug)]
struct EncryptOnlyServerVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for EncryptOnlyServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub(super) fn build_encrypt_only_rustls_config(
    direct_ssl: bool,
) -> StoreResult<rustls::ClientConfig> {
    ensure_rustls_crypto_provider();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = Arc::new(EncryptOnlyServerVerifier {
        provider: Arc::clone(&provider),
    });
    let mut tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| StoreError::Unavailable(format!("rustls protocol config error: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    if direct_ssl {
        // PostgreSQL 17 direct TLS negotiation (`sslnegotiation=direct`) requires ALPN "postgresql".
        tls_config.alpn_protocols = vec![b"postgresql".to_vec()];
    }
    Ok(tls_config)
}

pub(super) fn build_encrypt_only_rustls_connector(
    direct_ssl: bool,
) -> StoreResult<MakeRustlsConnect> {
    Ok(MakeRustlsConnect::new(build_encrypt_only_rustls_config(
        direct_ssl,
    )?))
}

pub(super) fn build_verified_rustls_config(
    source: &CaCertSource,
    verify_hostname: bool,
    direct_ssl: bool,
) -> StoreResult<rustls::ClientConfig> {
    ensure_rustls_crypto_provider();
    let roots = Arc::new(load_root_cert_store(source)?);
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls_config = if verify_hostname {
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| StoreError::Unavailable(format!("rustls protocol config error: {e}")))?
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else {
        let webpki_verifier =
            WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
                .build()
                .map_err(|e| {
                    StoreError::Unavailable(format!(
                        "failed to build PostgreSQL verify-ca verifier: {e}"
                    ))
                })?;
        let verifier = Arc::new(VerifyCaServerVerifier {
            inner: webpki_verifier,
        });
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| StoreError::Unavailable(format!("rustls protocol config error: {e}")))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    };
    if direct_ssl {
        tls_config.alpn_protocols = vec![b"postgresql".to_vec()];
    }
    Ok(tls_config)
}

pub(super) fn build_verified_rustls_connector(
    source: &CaCertSource,
    verify_hostname: bool,
    direct_ssl: bool,
) -> StoreResult<MakeRustlsConnect> {
    Ok(MakeRustlsConnect::new(build_verified_rustls_config(
        source,
        verify_hostname,
        direct_ssl,
    )?))
}
