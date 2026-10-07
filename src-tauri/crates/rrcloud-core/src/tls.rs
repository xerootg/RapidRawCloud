//! Android-only TLS client configuration for this crate's HTTP clients.
//!
//! ## Why this exists
//!
//! reqwest 0.13's `rustls` feature unconditionally verifies peers through
//! `rustls-platform-verifier`. On Android that delegates to the platform's
//! `PKIXRevocationChecker`, which in a **non-debuggable (release) build**
//! requires revocation data and only attempts OCSP — and Let's Encrypt
//! stopped embedding OCSP responder URLs in its certificates in 2025. The
//! result, observed live on a release APK against a Let's Encrypt-backed
//! endpoint, is a hard failure on every connection:
//!
//! ```text
//! [WARN] certificate was revoked: java.security.cert.CertPathValidatorException:
//!        Certificate does not specify OCSP responder
//! [ERROR] failed to verify TLS certificate: invalid peer certificate: Revoked
//! ```
//!
//! Debug builds never hit this (the verifier's Kotlin has a debug/test-only
//! escape), which is exactly why all device testing on `--debug` builds
//! passed. Upstream has no release-build fix yet
//! (rustls/rustls-platform-verifier#179; the 0.2.0 Android component only
//! *removed* the test escape).
//!
//! ## The fix
//!
//! On Android, build the HTTP clients on rustls with the bundled
//! webpki (Mozilla CCADB) root store instead of the platform verifier —
//! no revocation hard-dependency, same store Firefox ships. Trade-off:
//! user-installed/corporate CAs are not honored for *this crate's sync
//! connections* on Android (desktop keeps the platform verifier, and the
//! webview is untouched).
//!
//! Desktop (macOS/Windows/Linux) keeps `rustls-platform-verifier`: those
//! verifiers handle OCSP-less certificates fine.

#[cfg(target_os = "android")]
/// A rustls `ClientConfig` trusting the bundled webpki (Mozilla) roots,
/// for `reqwest::ClientBuilder::use_preconfigured_tls`. ALPN offers
/// http/1.1 only (this build of reqwest carries no http2 support).
pub fn android_webpki_tls_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    // Explicit provider: the unified dependency tree enables more than one
    // rustls crypto-provider feature (reqwest pulls aws-lc-rs, other deps
    // pull ring), so `ClientConfig::builder()`'s process-default lookup
    // panics ("Could not automatically determine the process-level
    // CryptoProvider") — seen live on the first release build carrying
    // this module. Pin aws-lc-rs, the same provider reqwest's own rustls
    // stack uses.
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS protocol defaults are always valid")
        .with_root_certificates(roots)
        .with_no_client_auth();
    // http/1.1 only: the app's reqwest is compiled without its `http2`
    // feature (default-features = false), so offering "h2" makes a server
    // that accepts it (Traefik does) route into hyper-util's unbuilt http2
    // path, which panics "http2 feature is not enabled" — seen live on the
    // second release build carrying this module.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

/// Applies the Android webpki TLS override to a reqwest builder; a no-op
/// passthrough on every other platform. Every HTTP client this crate (or
/// the app's sync layer) builds for S3/pairing traffic routes through
/// here so the policy lives in exactly one place.
pub fn apply_platform_tls(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    #[cfg(target_os = "android")]
    {
        builder.use_preconfigured_tls(android_webpki_tls_config())
    }
    #[cfg(not(target_os = "android"))]
    {
        builder
    }
}
