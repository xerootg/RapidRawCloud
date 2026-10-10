//! "Pair with cloud" — the zero-typing device setup (docs/CLOUD_SETUP.md §6).
//!
//! The user types ONE thing: the pairing service's URL (e.g.
//! `rrc.themissing.xyz`). Everything else is discovered:
//!
//! 1. `GET <service>/api/pairing-info` (public) → the OIDC issuer + the
//!    public client id.
//! 2. `GET <issuer>/.well-known/openid-configuration` → authorize + token
//!    endpoints.
//! 3. We open the system browser at the authorize endpoint with a PKCE
//!    (RFC 7636, S256) challenge and `redirect_uri=rapidraw://auth-callback`.
//! 4. The OS routes `rapidraw://auth-callback?code=…&state=…` back into the
//!    app (Android intent-filter + tauri-plugin-deep-link); the frontend
//!    hands the URL to [`sync_pair_complete`].
//! 5. We exchange the code (public client + PKCE verifier — no secret),
//!    call `GET <service>/api/config` with the bearer token, and apply the
//!    returned `{sync, credentials}` through the exact same paths the
//!    manual settings UI uses (`save_settings` + the credential store +
//!    `configure_core`).
//!
//! Security notes:
//! - PKCE S256 + a random `state` checked on completion; the pending
//!   verifier/state live only in process memory and are consumed (taken)
//!   by the first completion attempt.
//! - The access token is used for one HTTPS GET and dropped; nothing
//!   OAuth-related is persisted.
//! - The credential secret goes straight into the platform credential
//!   store (Keystore on Android), never through `settings.json` or back to
//!   the webview — same contract as `sync_set_credentials` (§3.6).

use std::sync::Mutex;

use base64::{Engine as _, engine::general_purpose};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Manager, State};

use crate::AppState;
use crate::app_settings::SyncSettings;

use super::commands::{configure_core, credential_store, set_credentials_core};

/// The one fixed redirect URI — must match the Android intent-filter AND a
/// registered redirect URI on the Authentik provider.
pub const REDIRECT_URI: &str = "rapidraw://auth-callback";

/// A pair flow waiting for its browser round-trip. One at a time, process
/// wide: starting a new flow replaces (invalidates) any previous one.
struct PendingPair {
    state: String,
    verifier: String,
    token_endpoint: String,
    client_id: String,
    /// Absolute URL of the service's config endpoint.
    config_url: String,
}

static PENDING: Mutex<Option<PendingPair>> = Mutex::new(None);

// ---------------------------------------------------------------------------
// Pure helpers (host-testable)
// ---------------------------------------------------------------------------

/// `rrc.example.com` → `https://rrc.example.com` (trailing slash trimmed).
/// An explicit `http://` is allowed for LAN/dev setups.
pub fn normalize_discovery_url(input: &str) -> Result<String, String> {
    let t = input.trim().trim_end_matches('/');
    if t.is_empty() {
        return Err("enter the pairing service URL (e.g. rrc.example.com)".into());
    }
    let url = if t.starts_with("http://") || t.starts_with("https://") {
        t.to_string()
    } else {
        format!("https://{t}")
    };
    // Must parse as a URL with a host.
    let parsed = reqwest::Url::parse(&url).map_err(|e| format!("invalid URL: {e}"))?;
    if parsed.host_str().is_none() {
        return Err("invalid URL: no host".into());
    }
    Ok(url)
}

/// RFC 7636: a high-entropy verifier and its S256 challenge, both
/// base64url-no-pad. 32 random bytes → 43-char verifier (the RFC minimum
/// length, maximum entropy per char).
pub fn pkce_pair() -> (String, String) {
    use rand::RngExt;
    let bytes: [u8; 32] = rand::rng().random();
    let verifier = general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let challenge = pkce_challenge(&verifier);
    (verifier, challenge)
}

/// S256 challenge for a given verifier (split out for the RFC test vector).
fn pkce_challenge(verifier: &str) -> String {
    general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// Random URL-safe `state` parameter.
fn random_state() -> String {
    use rand::RngExt;
    let bytes: [u8; 16] = rand::rng().random();
    general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// The browser authorize URL.
pub fn build_authorize_url(
    authorization_endpoint: &str,
    client_id: &str,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(authorization_endpoint)
        .map_err(|e| format!("bad authorization endpoint: {e}"))?;
    url.query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", REDIRECT_URI)
        .append_pair("response_type", "code")
        .append_pair("scope", "openid profile email")
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state);
    Ok(url.to_string())
}

/// Parse `rapidraw://auth-callback?...` into `(code, state)`. Surfaces the
/// provider's `error`/`error_description` (e.g. the user hit "Deny").
pub fn parse_callback(callback_url: &str) -> Result<(String, String), String> {
    let url = reqwest::Url::parse(callback_url).map_err(|e| format!("bad callback URL: {e}"))?;
    let mut code = None;
    let mut state = None;
    let mut error = None;
    let mut error_desc = None;
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            "error_description" => error_desc = Some(v.into_owned()),
            _ => {}
        }
    }
    if let Some(e) = error {
        let desc = error_desc.unwrap_or_default();
        return Err(format!("sign-in failed: {e} {desc}").trim_end().to_string());
    }
    match (code, state) {
        (Some(c), Some(s)) => Ok((c, s)),
        _ => Err("callback is missing code/state".into()),
    }
}

/// Merges a cloud config doc's sync settings onto this device's current
/// ones, preserving the DEVICE-LOCAL fields. A camera-roll watch list
/// describes one physical device's storage — `autoWatchDcim` and
/// `watchedMediaBuckets` must never be stomped by pairing (user decision,
/// 2026-10-07: "not persisted for all devices, be a per-device setting").
/// Everything else (endpoint/bucket/region/budgets/...) comes from the
/// cloud doc.
pub fn merge_cloud_sync(local: &SyncSettings, cloud: &SyncSettings) -> SyncSettings {
    let mut merged = cloud.clone();
    merged.auto_watch_dcim = local.auto_watch_dcim;
    merged.watched_media_buckets = local.watched_media_buckets.clone();
    merged
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairingInfo {
    issuer: String,
    client_id: String,
    /// Path of the config endpoint on the pairing service (e.g. `/api/config`).
    #[serde(default = "default_config_endpoint")]
    config_endpoint: String,
}

fn default_config_endpoint() -> String {
    "/api/config".into()
}

#[derive(Deserialize)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairedCreds {
    access_key_id: String,
    secret_access_key: String,
}

/// The pairing service's config doc. `sync` is the app's own [`SyncSettings`]
/// shape (camelCase), so it deserializes directly.
#[derive(Deserialize)]
struct ConfigDoc {
    sync: SyncSettings,
    credentials: PairedCreds,
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Starts a pair flow: discovers the OIDC coordinates from the service URL,
/// stores the PKCE state, opens the system browser at the authorize URL,
/// and returns that URL (so the UI can offer a manual "open again" link).
#[tauri::command]
pub async fn sync_pair_begin(discovery_url: String, app: AppHandle) -> Result<String, String> {
    let base = normalize_discovery_url(&discovery_url)?;
    // Android: webpki-roots TLS (see rrcloud_core::tls — the platform
    // verifier hard-fails OCSP-less Let's Encrypt certs on release builds).
    let http = rrcloud_core::tls::apply_platform_tls(
        reqwest::Client::builder().timeout(std::time::Duration::from_secs(15)),
    )
    .build()
    .map_err(|e| e.to_string())?;

    let info: PairingInfo = http
        .get(format!("{base}/api/pairing-info"))
        .send()
        .await
        .map_err(|e| format!("pairing service unreachable: {e}"))?
        .error_for_status()
        .map_err(|e| format!("pairing service error: {e}"))?
        .json()
        .await
        .map_err(|e| format!("bad pairing-info response: {e}"))?;

    let issuer = info.issuer.trim_end_matches('/');
    let oidc: OidcDiscovery = http
        .get(format!("{issuer}/.well-known/openid-configuration"))
        .send()
        .await
        .map_err(|e| format!("OIDC issuer unreachable: {e}"))?
        .error_for_status()
        .map_err(|e| format!("OIDC issuer error: {e}"))?
        .json()
        .await
        .map_err(|e| format!("bad OIDC discovery response: {e}"))?;

    let (verifier, challenge) = pkce_pair();
    let state = random_state();
    let authorize_url = build_authorize_url(
        &oidc.authorization_endpoint,
        &info.client_id,
        &state,
        &challenge,
    )?;

    let config_url = format!(
        "{base}{}",
        if info.config_endpoint.starts_with('/') {
            info.config_endpoint.clone()
        } else {
            format!("/{}", info.config_endpoint)
        }
    );
    *PENDING.lock().unwrap() = Some(PendingPair {
        state,
        verifier,
        token_endpoint: oidc.token_endpoint,
        client_id: info.client_id,
        config_url,
    });

    // Open the system browser (Custom Tab on Android). Failure to open is
    // not fatal: the URL is returned for the UI to present as a link.
    // `shell().open` is deprecated in favor of tauri-plugin-opener, but the
    // shell plugin is the one this app already registers (upstream parity —
    // adding the opener plugin would widen UPSTREAM_TOUCHES for no gain).
    #[allow(deprecated)]
    {
        use tauri_plugin_shell::ShellExt;
        if let Err(e) = app.shell().open(&authorize_url, None) {
            log::warn!("pair: could not open system browser: {e}");
        }
    }
    Ok(authorize_url)
}

/// Completes a pair flow from the `rapidraw://auth-callback?...` URL the
/// deep link delivered: code exchange (PKCE), config fetch, then apply —
/// credentials into the platform store, settings persisted, and the engine
/// reconfigured. Returns the applied [`SyncSettings`] so the UI can refresh.
///
/// If no library folder is open yet, settings + credentials are still fully
/// applied and the engine picks them up when a library opens — the returned
/// error string is replaced by success in that case (it is the expected
/// first-run ordering), matching how the manual flow behaves.
#[tauri::command]
pub async fn sync_pair_complete(
    callback_url: String,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<SyncSettings, String> {
    let (code, cb_state) = parse_callback(&callback_url)?;
    let pending = PENDING
        .lock()
        .unwrap()
        .take()
        .ok_or("no pair flow in progress — tap Pair with cloud first")?;
    if pending.state != cb_state {
        return Err("state mismatch — stale or forged callback; start pairing again".into());
    }

    // Same Android TLS override as `sync_pair_begin` (rrcloud_core::tls).
    let http = rrcloud_core::tls::apply_platform_tls(
        reqwest::Client::builder().timeout(std::time::Duration::from_secs(15)),
    )
    .build()
    .map_err(|e| e.to_string())?;

    // Code → token (public client: client_id + PKCE verifier, no secret).
    // Form body built by hand via Url's pair encoder: the app's reqwest is
    // compiled without the `form` helper's feature set.
    let form_body = {
        let mut u = reqwest::Url::parse("http://local/").expect("static URL");
        u.query_pairs_mut()
            .append_pair("grant_type", "authorization_code")
            .append_pair("code", code.as_str())
            .append_pair("redirect_uri", REDIRECT_URI)
            .append_pair("client_id", pending.client_id.as_str())
            .append_pair("code_verifier", pending.verifier.as_str());
        u.query().unwrap_or_default().to_string()
    };
    let token: TokenResponse = http
        .post(&pending.token_endpoint)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(form_body)
        .send()
        .await
        .map_err(|e| format!("token endpoint unreachable: {e}"))?
        .error_for_status()
        .map_err(|e| format!("code exchange rejected: {e}"))?
        .json()
        .await
        .map_err(|e| format!("bad token response: {e}"))?;

    // Token → the user's stored config.
    let resp = http
        .get(&pending.config_url)
        .bearer_auth(&token.access_token)
        .send()
        .await
        .map_err(|e| format!("pairing service unreachable: {e}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(
            "no cloud config stored for your account yet — sign in to the pairing site in a \
             browser first and add your library bucket"
                .into(),
        );
    }
    let doc: ConfigDoc = resp
        .error_for_status()
        .map_err(|e| format!("config fetch rejected: {e}"))?
        .json()
        .await
        .map_err(|e| format!("bad config response: {e}"))?;

    // Apply — exactly the manual UI's paths. Credentials first (so a
    // configure that runs picks them up), then settings + reconfigure.
    let store = credential_store(&app)?;
    set_credentials_core(
        &*store,
        doc.credentials.access_key_id,
        doc.credentials.secret_access_key,
    )?;

    let mut app_settings = crate::app_settings::load_settings(app.clone())?;
    // Device-local fields survive pairing (see merge_cloud_sync's doc).
    let applied_sync = merge_cloud_sync(&app_settings.sync, &doc.sync);
    app_settings.sync = applied_sync.clone();
    crate::app_settings::save_settings(app_settings.clone(), app.clone())?;

    // Reconfigure now if a library is open; otherwise the saved settings +
    // credentials engage on the next library open (expected on first run).
    let sync_root = app_settings
        .root_folders
        .first()
        .cloned()
        .or_else(|| app_settings.last_root_path.clone())
        .map(std::path::PathBuf::from);
    if let Some(sync_root) = sync_root {
        let state_dir = app
            .path()
            .app_data_dir()
            .map_err(|e| e.to_string())?
            .join("rrcloud");
        if let Err(e) = configure_core(
            &state.sync_manager,
            &*store,
            applied_sync.clone(),
            sync_root,
            state_dir,
        ) {
            // The redb state store is single-writer: a WorkManager sync/scan
            // cycle holding it at this exact moment is routine, not a
            // pairing failure — credentials and settings are already fully
            // applied above, and the engine picks them up on its next
            // cycle/foreground reconfigure. Seen live on-device: a re-pair
            // raced the half-hourly SyncCycleWorker and the UI showed a
            // scary "state db is already open in another process" even
            // though the pair had succeeded. Any OTHER configure failure
            // (bad endpoint, unwritable state dir, ...) still surfaces.
            if e.contains("already open in another process") {
                log::warn!(
                    "pair: configure deferred (state db busy — a background \
                     sync cycle holds it); settings + credentials applied: {e}"
                );
            } else {
                return Err(e);
            }
        }
    }

    Ok(applied_sync)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_adds_https_and_trims() {
        assert_eq!(
            normalize_discovery_url("rrc.example.com").unwrap(),
            "https://rrc.example.com"
        );
        assert_eq!(
            normalize_discovery_url("  rrc.example.com/  ").unwrap(),
            "https://rrc.example.com"
        );
        assert_eq!(
            normalize_discovery_url("http://192.168.1.5:8080").unwrap(),
            "http://192.168.1.5:8080"
        );
        assert!(normalize_discovery_url("").is_err());
        assert!(normalize_discovery_url("   ").is_err());
    }

    #[test]
    fn pkce_challenge_matches_the_rfc7636_test_vector() {
        // RFC 7636 appendix B.
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn pkce_pair_produces_valid_lengths_and_consistency() {
        let (v, c) = pkce_pair();
        // RFC 7636: verifier 43..=128 chars of [A-Za-z0-9-._~].
        assert!(v.len() >= 43 && v.len() <= 128, "verifier len {}", v.len());
        assert!(
            v.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.' | '_' | '~'))
        );
        assert_eq!(c, pkce_challenge(&v));
        // Two pairs differ (entropy sanity).
        assert_ne!(pkce_pair().0, v);
    }

    #[test]
    fn authorize_url_carries_all_oauth_params() {
        let url = build_authorize_url(
            "https://auth.example/application/o/authorize/",
            "client123",
            "st4te",
            "ch4llenge",
        )
        .unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let q: std::collections::HashMap<_, _> = parsed.query_pairs().collect();
        assert_eq!(q["client_id"], "client123");
        assert_eq!(q["redirect_uri"], REDIRECT_URI);
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["code_challenge"], "ch4llenge");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "st4te");
        assert!(q["scope"].contains("openid"));
    }

    #[test]
    fn parse_callback_extracts_code_and_state() {
        let (code, state) =
            parse_callback("rapidraw://auth-callback?code=abc123&state=xyz").unwrap();
        assert_eq!(code, "abc123");
        assert_eq!(state, "xyz");
    }

    #[test]
    fn parse_callback_surfaces_provider_errors_and_rejects_garbage() {
        let err = parse_callback(
            "rapidraw://auth-callback?error=access_denied&error_description=User+denied",
        )
        .unwrap_err();
        assert!(err.contains("access_denied"), "{err}");
        assert!(parse_callback("rapidraw://auth-callback").is_err());
        assert!(parse_callback("not a url").is_err());
    }

    #[test]
    fn merge_cloud_sync_preserves_device_local_dcim_fields() {
        // User decision (2026-10-07): a camera-roll watch list is a
        // per-device fact; pairing must never push one device's folders
        // onto another. Cloud wins for coordinates; device wins for DCIM.
        let local = SyncSettings {
            endpoint: "http://old.local".into(),
            auto_watch_dcim: true,
            watched_media_buckets: vec!["Camera".into(), "158ND750".into()],
            ..SyncSettings::default()
        };

        let cloud = SyncSettings {
            enabled: true,
            endpoint: "https://garage.example".into(),
            bucket: "my-photos".into(),
            region: "garage".into(),
            // The doc may carry ANY values here (older docs, other devices'
            // choices) — they must not matter.
            auto_watch_dcim: false,
            watched_media_buckets: vec!["SomeoneElsesFolder".into()],
            ..SyncSettings::default()
        };

        let merged = merge_cloud_sync(&local, &cloud);
        assert!(merged.enabled);
        assert_eq!(merged.endpoint, "https://garage.example");
        assert_eq!(merged.bucket, "my-photos");
        // Device-local fields preserved verbatim:
        assert!(merged.auto_watch_dcim);
        assert_eq!(merged.watched_media_buckets, vec!["Camera", "158ND750"]);
    }

    #[test]
    fn merge_cloud_sync_fresh_device_keeps_defaults_off() {
        // First pair on a fresh install: DCIM watch stays OFF regardless of
        // what the cloud doc says — enabling it is an on-device choice.
        let local = SyncSettings::default();
        let cloud = SyncSettings {
            auto_watch_dcim: true,
            watched_media_buckets: vec!["Camera".into()],
            ..SyncSettings::default()
        };
        let merged = merge_cloud_sync(&local, &cloud);
        assert!(!merged.auto_watch_dcim);
        assert!(merged.watched_media_buckets.is_empty());
    }

    #[test]
    fn config_doc_parses_the_services_shape() {
        let json = br#"{
            "version": 1,
            "updatedAt": "2026-10-07T00:00:00Z",
            "sync": {
                "enabled": true,
                "endpoint": "https://garage.example",
                "bucket": "my-photos",
                "region": "garage",
                "forcePathStyle": true,
                "cacheSizeGb": 8,
                "previewBudgetGb": 10,
                "previewPrefetchMonths": 12,
                "autoWatchDcim": false,
                "watchedMediaBuckets": [],
                "workerBackfill": true
            },
            "credentials": { "accessKeyId": "GK1", "secretAccessKey": "s3cr3t" }
        }"#;
        let doc: ConfigDoc = serde_json::from_slice(json).unwrap();
        assert_eq!(doc.sync.bucket, "my-photos");
        assert!(doc.sync.enabled);
        assert_eq!(doc.credentials.access_key_id, "GK1");
        assert_eq!(doc.credentials.secret_access_key, "s3cr3t");
    }
}
