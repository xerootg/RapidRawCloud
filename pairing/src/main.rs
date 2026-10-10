//! RapidRawCloud pairing / discovery service.
//!
//! "S3 is the config store." A small `rapidraw-admin` S3 bucket holds one
//! config document per user at `users/<username>/config.json`:
//!
//! ```json
//! { "version": 1, "updatedAt": "...",
//!   "sync": { ...app SyncSettings (camelCase)... },
//!   "credentials": { "accessKeyId": "...", "secretAccessKey": "..." } }
//! ```
//!
//! Two audiences, two auth styles (see ARCHITECTURE / docs/CLOUD_SETUP.md):
//!
//!  * **Browser** (`GET /`, `POST /save`): sits behind Authentik Traefik
//!    forward-auth, which sets `X-Authentik-Username`. A user signs in once
//!    and fills a form with their *own* library-bucket S3 coordinates; we
//!    write their `config.json`. Return visits show "already paired".
//!  * **App** (`GET /api/config`): the native app obtains an OIDC/PKCE
//!    access token and sends it as `Authorization: Bearer …`. We first
//!    require the token's `aud` claim to name the app's own OIDC client id
//!    (so tokens Authentik issued to other applications are refused), then
//!    validate it by calling Authentik's userinfo endpoint, and return that
//!    user's `config.json` so the app configures itself with zero typing.
//!
//! The service never creates buckets or mints credentials — users bring
//! their own bucket and paste its key once. The headless worker reads the
//! same admin bucket (read-only key) and backfills each user's library with
//! that user's credentials.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Form, Json, Router,
};
use base64::Engine;
use rrcloud_proto::{
    key_pairing_user_config, PairingConfigDoc, PairingCredentials, PairingInfo,
    PairingSyncSettings, PAIRING_CONFIG_PATH, PAIRING_INFO_PATH, PAIRING_REDIRECT_URI,
    PAIRING_REDIRECT_URI_RAW2DNG,
};
use s3::{creds::Credentials, Bucket, Region};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Configuration (from env)
// ---------------------------------------------------------------------------

struct Config {
    /// Admin bucket name (holds the per-user config docs).
    admin_bucket: String,
    /// S3 endpoint the SERVICE uses to reach the admin bucket (in-cluster,
    /// e.g. http://garage-external.garage.svc.cluster.local:3900).
    admin_s3_endpoint: String,
    admin_s3_region: String,
    admin_s3_access_key: String,
    admin_s3_secret_key: String,
    /// Authentik userinfo endpoint, used to validate app bearer tokens.
    oidc_userinfo_url: String,
    /// OIDC issuer URL for the APP's provider (public/PKCE), advertised via
    /// `GET /api/pairing-info` so a device needs only this service's URL to
    /// discover everything else (authorize/token endpoints come from the
    /// issuer's own `.well-known/openid-configuration`).
    oidc_issuer_url: String,
    /// OIDC client id of the APP's provider (public client, no secret).
    oidc_client_id: String,
    /// Default library endpoint pre-filled in the creds form (the public S3
    /// URL the *app* will use — e.g. https://garage.themissing.xyz).
    default_library_endpoint: String,
    default_library_region: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let get = |k: &str| std::env::var(k).map_err(|_| format!("missing env var {k}"));
        Ok(Config {
            admin_bucket: get("ADMIN_BUCKET")?,
            admin_s3_endpoint: get("ADMIN_S3_ENDPOINT")?,
            admin_s3_region: std::env::var("ADMIN_S3_REGION").unwrap_or_else(|_| "garage".into()),
            admin_s3_access_key: get("ADMIN_S3_ACCESS_KEY")?,
            admin_s3_secret_key: get("ADMIN_S3_SECRET_KEY")?,
            oidc_userinfo_url: get("OIDC_USERINFO_URL")?,
            oidc_issuer_url: get("OIDC_ISSUER_URL")?,
            oidc_client_id: get("OIDC_CLIENT_ID")?,
            default_library_endpoint: std::env::var("DEFAULT_LIBRARY_ENDPOINT").unwrap_or_default(),
            default_library_region: std::env::var("DEFAULT_LIBRARY_REGION")
                .unwrap_or_else(|_| "garage".into()),
        })
    }
}

struct AppState {
    config: Config,
    http: reqwest::Client,
}

impl AppState {
    /// A fresh handle to the admin bucket (path-style, static creds).
    fn admin_bucket(&self) -> Result<Box<Bucket>, String> {
        let region = Region::Custom {
            region: self.config.admin_s3_region.clone(),
            endpoint: self.config.admin_s3_endpoint.clone(),
        };
        let creds = Credentials::new(
            Some(&self.config.admin_s3_access_key),
            Some(&self.config.admin_s3_secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| format!("bad admin credentials: {e}"))?;
        Ok(Bucket::new(&self.config.admin_bucket, region, creds)
            .map_err(|e| format!("bad admin bucket: {e}"))?
            .with_path_style())
    }
}

// ---------------------------------------------------------------------------
// Config document model
// ---------------------------------------------------------------------------

/// The documents are the generated protocol SDK's ([`PairingConfigDoc`] =
/// `{version, updatedAt, sync: PairingSyncSettings, credentials}`,
/// camelCase): one definition shared with the app's pairing client and the
/// firmware, so a field added to `protocol/rrcloud.protocol.toml` reaches
/// all three. Device-local sync fields (`autoWatchDcim`,
/// `watchedMediaBuckets`) are written as inert defaults — a camera-roll
/// watch list belongs to one physical device, and the app ignores them when
/// applying a pair.
type ConfigDoc = PairingConfigDoc;

// ---------------------------------------------------------------------------
// Username handling
// ---------------------------------------------------------------------------

/// Restrict the username used as an S3 path segment to a safe charset so a
/// hostile `X-Authentik-Username`/token claim can never traverse the key
/// space (`../`) or inject control characters. Authentik usernames are
/// normally `[a-zA-Z0-9._-]`; anything else is rejected.
fn sanitize_username(raw: &str) -> Option<String> {
    let u = raw.trim();
    if u.is_empty() || u.len() > 128 {
        return None;
    }
    if u.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        && u != "."
        && u != ".."
    {
        Some(u.to_string())
    } else {
        None
    }
}

/// `users/<username>/config.json` — the SDK's key template; `username` has
/// already passed [`sanitize_username`], which is the charset the template
/// documents.
fn config_key(username: &str) -> String {
    key_pairing_user_config(username)
}

// ---------------------------------------------------------------------------
// S3 config-doc read / write
// ---------------------------------------------------------------------------

async fn read_config(state: &AppState, username: &str) -> Result<Option<ConfigDoc>, String> {
    let bucket = state.admin_bucket()?;
    match bucket.get_object(config_key(username)).await {
        Ok(resp) if resp.status_code() == 200 => {
            let doc: ConfigDoc = serde_json::from_slice(resp.bytes())
                .map_err(|e| format!("corrupt config doc: {e}"))?;
            Ok(Some(doc))
        }
        // 404 (or any other non-200) → treat as "not configured yet".
        Ok(_) => Ok(None),
        // rust-s3 surfaces a 404 as an error in some paths; treat a
        // not-found as "unconfigured", bubble anything else up.
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("404") || msg.to_lowercase().contains("not found") {
                Ok(None)
            } else {
                Err(format!("admin bucket read failed: {msg}"))
            }
        }
    }
}

async fn write_config(state: &AppState, username: &str, doc: &ConfigDoc) -> Result<(), String> {
    let bucket = state.admin_bucket()?;
    let body = serde_json::to_vec_pretty(doc).map_err(|e| e.to_string())?;
    let resp = bucket
        .put_object_with_content_type(config_key(username), &body, "application/json")
        .await
        .map_err(|e| format!("admin bucket write failed: {e}"))?;
    if (200..300).contains(&resp.status_code()) {
        Ok(())
    } else {
        Err(format!(
            "admin bucket write returned HTTP {}",
            resp.status_code()
        ))
    }
}

// ---------------------------------------------------------------------------
// OIDC bearer validation (app path)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct UserInfo {
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    sub: Option<String>,
}

/// Does this bearer token's `aud` claim name `client_id`?
///
/// Authentik's userinfo endpoint resolves ANY access token the instance has
/// issued — for any provider/application — to its user. So "userinfo said
/// 200" only proves the token is *some* valid Authentik token for *some*
/// user, not that it was issued to the RapidRAW app. A token minted for (or
/// leaked from) another application in the same Authentik must not unlock a
/// user's library credentials here, so we additionally require the token's
/// `aud` (string, or array of strings per RFC 7519 §4.1.3) to contain our
/// client id. The token is parsed as a JWT (Authentik access tokens are JWTs)
/// WITHOUT verifying its signature or expiry: userinfo remains the sole
/// authority for validity and identity; this is purely the audience gate.
/// Anything that is not a 3-segment JWT with a decodable JSON payload, or
/// whose `aud` is missing/does not match, is rejected.
fn token_audience_matches(token: &str, client_id: &str) -> bool {
    let mut parts = token.split('.');
    let (Some(_header), Some(payload), Some(_sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        tracing::debug!("bearer rejected: not a 3-segment JWT");
        return false;
    };
    // Authentik emits unpadded base64url; tolerate padded input too.
    let payload = payload.trim_end_matches('=');
    let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload) else {
        tracing::debug!("bearer rejected: JWT payload is not base64url");
        return false;
    };
    let Ok(claims) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        tracing::debug!("bearer rejected: JWT payload is not JSON");
        return false;
    };
    let matches = match claims.get("aud") {
        Some(serde_json::Value::String(aud)) => aud == client_id,
        Some(serde_json::Value::Array(auds)) => auds.iter().any(|a| a.as_str() == Some(client_id)),
        _ => false,
    };
    if !matches {
        tracing::warn!("bearer rejected: token audience does not include this client id");
    }
    matches
}

/// Validate an app's bearer token: first require its `aud` claim to name our
/// OIDC client id (see [`token_audience_matches`]), then call Authentik's
/// userinfo endpoint and return the sanitized username. `None` →
/// invalid/unusable token.
async fn validate_bearer(state: &AppState, bearer: &str) -> Option<String> {
    if !token_audience_matches(bearer, &state.config.oidc_client_id) {
        return None;
    }
    let resp = state
        .http
        .get(&state.config.oidc_userinfo_url)
        .bearer_auth(bearer)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let info: UserInfo = resp.json().await.ok()?;
    let name = info.preferred_username.or(info.sub)?;
    sanitize_username(&name)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn healthz() -> &'static str {
    "ok"
}

/// Browser username from the forward-auth header, sanitized. The route is
/// only reachable behind Authentik forward-auth (see the IngressRoute), so a
/// missing header means a misconfiguration, not an anonymous user.
fn browser_user(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-Authentik-Username")
        .and_then(|v| v.to_str().ok())
        .and_then(sanitize_username)
}

async fn index(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(user) = browser_user(&headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            "Not authenticated (this page must be reached through Authentik).",
        )
            .into_response();
    };
    match read_config(&state, &user).await {
        Ok(Some(doc)) => Html(render_page(&user, Some(&doc), None, &state.config)).into_response(),
        Ok(None) => Html(render_page(&user, None, None, &state.config)).into_response(),
        Err(e) => {
            tracing::error!("index read_config: {e}");
            (StatusCode::BAD_GATEWAY, "Config store unavailable.").into_response()
        }
    }
}

#[derive(Deserialize)]
struct SaveForm {
    endpoint: String,
    bucket: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    force_path_style: Option<String>,
    #[serde(default)]
    worker_backfill: Option<String>,
    #[serde(default)]
    cache_size_gb: Option<u32>,
    #[serde(default)]
    preview_budget_gb: Option<u32>,
}

async fn save(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<SaveForm>,
) -> Response {
    let Some(user) = browser_user(&headers) else {
        return (StatusCode::UNAUTHORIZED, "Not authenticated.").into_response();
    };

    let endpoint = form.endpoint.trim().to_string();
    let bucket = form.bucket.trim().to_string();
    let region = form.region.trim().to_string();
    let access_key_id = form.access_key_id.trim().to_string();
    let secret_access_key = form.secret_access_key.trim().to_string();

    if endpoint.is_empty() || bucket.is_empty() || access_key_id.is_empty() {
        return Html(render_page(
            &user,
            None,
            Some("Endpoint, bucket, and access key are required."),
            &state.config,
        ))
        .into_response();
    }
    if secret_access_key.is_empty() {
        return Html(render_page(
            &user,
            None,
            Some("Secret key is required (it is write-only; re-enter to change it)."),
            &state.config,
        ))
        .into_response();
    }

    // Start from the protocol defaults (what `{}` decodes to) so every field
    // the form does not collect — the prefetch window, the device-local
    // camera-roll fields, the upload-policy flags — carries its documented
    // default rather than a value invented here.
    let defaults = PairingSyncSettings::default();
    let doc = ConfigDoc {
        version: 1,
        updated_at: now_iso8601(),
        sync: PairingSyncSettings {
            enabled: true,
            endpoint,
            bucket,
            region,
            force_path_style: form.force_path_style.is_some(),
            cache_size_gb: form.cache_size_gb.unwrap_or(defaults.cache_size_gb),
            preview_budget_gb: form.preview_budget_gb.unwrap_or(defaults.preview_budget_gb),
            worker_backfill: form.worker_backfill.is_some(),
            ..defaults
        },
        credentials: PairingCredentials {
            access_key_id,
            secret_access_key,
        },
    };

    match write_config(&state, &user, &doc).await {
        Ok(()) => Redirect::to("/?saved=1").into_response(),
        Err(e) => {
            tracing::error!("save write_config: {e}");
            (StatusCode::BAD_GATEWAY, "Could not save config.").into_response()
        }
    }
}

/// Public pairing discovery: everything a device needs to start the OIDC/
/// PKCE flow, keyed only by this service's URL. Deliberately unauthenticated
/// (none of this is secret — the client id is a public client, and the
/// issuer's own `.well-known/openid-configuration` is public too).
async fn api_pairing_info(State(state): State<Arc<AppState>>) -> Response {
    Json(PairingInfo {
        version: 1,
        issuer: state.config.oidc_issuer_url.clone(),
        client_id: state.config.oidc_client_id.clone(),
        config_endpoint: PAIRING_CONFIG_PATH.to_string(),
        // Kept for older clients; `redirect_uris` is the full list.
        redirect_uri: Some(PAIRING_REDIRECT_URI.to_string()),
        // Every app that pairs through this service, each with its own
        // scheme so two apps on one phone never fight over a callback. The
        // Authentik provider must list all of them as allowed redirect URIs.
        //  - RapidRAW (the editor):            rapidraw://auth-callback
        //  - Raw2DNG (RawImageSnapseedBridge): raw2dng://auth-callback
        redirect_uris: vec![
            PAIRING_REDIRECT_URI.to_string(),
            PAIRING_REDIRECT_URI_RAW2DNG.to_string(),
        ],
    })
    .into_response()
}

/// App config endpoint. `Authorization: Bearer <oidc access token>`.
async fn api_config(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(bearer) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
    };
    let Some(user) = validate_bearer(&state, bearer.trim()).await else {
        return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
    };
    match read_config(&state, &user).await {
        Ok(Some(doc)) => Json(doc).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            "no config for this user yet — sign in at the pairing site and add your bucket first",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("api_config read_config: {e}");
            (StatusCode::BAD_GATEWAY, "config store unavailable").into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// HTML (minimal, dependency-free)
// ---------------------------------------------------------------------------

fn esc(s: &str) -> String {
    html_escape::encode_double_quoted_attribute(s).to_string()
}

fn page_shell(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: system-ui, sans-serif; max-width: 640px; margin: 3rem auto; padding: 0 1rem; line-height: 1.5; }}
  h1 {{ font-size: 1.4rem; }}
  label {{ display:block; margin: 0.8rem 0 0.2rem; font-weight:600; }}
  input[type=text], input[type=password], input[type=number] {{ width:100%; padding:0.5rem; font-size:1rem; box-sizing:border-box; }}
  .row {{ display:flex; gap:1rem; }} .row > div {{ flex:1; }}
  .check {{ font-weight:400; margin-top:0.8rem; }} .check input {{ width:auto; margin-right:0.4rem; }}
  button {{ margin-top:1.2rem; padding:0.6rem 1.2rem; font-size:1rem; cursor:pointer; }}
  .note {{ color:#666; font-size:0.9rem; }} .err {{ color:#c00; font-weight:600; }}
  .ok {{ color:#0a0; font-weight:600; }}
  code {{ background:rgba(127,127,127,0.15); padding:0.1rem 0.3rem; border-radius:3px; }}
</style></head><body>{body}</body></html>"#
    )
}

/// Single page renderer for both first-time pairing and the paired/edit
/// view. `existing` pre-fills the form (and shows the "paired" banner +
/// device-pairing instructions); `None` shows the intro + an empty form
/// seeded with the configured defaults. The secret key is never rendered.
fn render_page(
    user: &str,
    existing: Option<&ConfigDoc>,
    error: Option<&str>,
    cfg: &Config,
) -> String {
    let err = error
        .map(|e| format!("<p class=\"err\">{}</p>", esc(e)))
        .unwrap_or_default();

    let (ep, bucket, region, akid, fps, wbf, cache, prev) = match existing {
        Some(d) => (
            esc(&d.sync.endpoint),
            esc(&d.sync.bucket),
            esc(&d.sync.region),
            esc(&d.credentials.access_key_id),
            d.sync.force_path_style,
            d.sync.worker_backfill,
            d.sync.cache_size_gb,
            d.sync.preview_budget_gb,
        ),
        None => {
            let d = PairingSyncSettings::default();
            (
                esc(&cfg.default_library_endpoint),
                String::new(),
                esc(&cfg.default_library_region),
                String::new(),
                d.force_path_style,
                true,
                d.cache_size_gb,
                d.preview_budget_gb,
            )
        }
    };
    let ck = |b: bool| if b { "checked" } else { "" };
    let secret_ph = if existing.is_some() {
        "re-enter to save"
    } else {
        ""
    };

    let form = format!(
        r#"<form method="post" action="/save">
  <label>S3 endpoint</label>
  <input type="text" name="endpoint" value="{ep}" placeholder="https://s3.example.com" required>
  <div class="row">
    <div><label>Bucket</label><input type="text" name="bucket" value="{bucket}" placeholder="my-photos" required></div>
    <div><label>Region</label><input type="text" name="region" value="{region}"></div>
  </div>
  <label>Access key ID</label>
  <input type="text" name="access_key_id" value="{akid}" autocomplete="off" required>
  <label>Secret access key</label>
  <input type="password" name="secret_access_key" autocomplete="off" required placeholder="{secret_ph}">
  <p class="note">Stored in the admin config bucket so your devices and the worker can use it. Use a key scoped to this one bucket. The secret is write-only here — re-enter it on every save.</p>
  <label class="check"><input type="checkbox" name="force_path_style" {fps}> Path-style addressing (Garage / MinIO / B2 / R2)</label>
  <label class="check"><input type="checkbox" name="worker_backfill" {wbf}> Let the worker generate previews for this library</label>
  <p class="note">Camera-roll auto-import (which folders each phone watches) is a per-device choice — set it on the device, in Settings → Sync.</p>
  <div class="row">
    <div><label>Cache budget (GB)</label><input type="number" name="cache_size_gb" value="{cache}" min="1"></div>
    <div><label>Preview budget (GB)</label><input type="number" name="preview_budget_gb" value="{prev}" min="1"></div>
  </div>
  <button type="submit">Save</button>
</form>"#,
        ep = ep,
        bucket = bucket,
        region = region,
        akid = akid,
        secret_ph = secret_ph,
        fps = ck(fps),
        wbf = ck(wbf),
        cache = cache,
        prev = prev,
    );

    let header = if existing.is_some() {
        format!(
            r#"<h1>RapidRawCloud — paired</h1>
<p class="ok">You're paired, <strong>{user}</strong>.</p>
<h2 style="font-size:1.1rem">Pair a device</h2>
<p>Open <strong>RapidRAW → Settings → Sync → Pair with cloud</strong> and sign in.
The app configures itself — no typing.</p>
<h2 style="font-size:1.1rem">Your library settings</h2>"#,
            user = esc(user)
        )
    } else {
        format!(
            r#"<h1>RapidRawCloud — pair your library</h1>
<p>Signed in as <strong>{user}</strong>. Enter your photo library's S3 bucket
once here; every device you pair (and the backfill worker) picks it up
automatically.</p>"#,
            user = esc(user)
        )
    };

    page_shell("RapidRawCloud", &format!("{header}{err}{form}"))
}

// ---------------------------------------------------------------------------
// Time (no chrono dep)
// ---------------------------------------------------------------------------

fn now_iso8601() -> String {
    // Seconds since epoch is enough for an "updatedAt" marker; format as a
    // plain RFC3339-ish UTC string without pulling in chrono.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Minimal civil-time conversion (UTC).
    let (y, mo, d, h, mi, s) = civil_from_unix(secs as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Days-based civil date conversion (Howard Hinnant's algorithm), UTC.
fn civil_from_unix(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, mi, s) = (
        (rem / 3600) as u32,
        ((rem % 3600) / 60) as u32,
        (rem % 60) as u32,
    );
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let mo = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if mo <= 2 { y + 1 } else { y };
    (y, mo, d, h, mi, s)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rrcloud-pairing: configuration error: {e}");
            std::process::exit(2);
        }
    };
    let bind: std::net::SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .expect("BIND_ADDR must be host:port");

    let state = Arc::new(AppState {
        config,
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("http client"),
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(index))
        .route("/save", post(save))
        .route(PAIRING_CONFIG_PATH, get(api_config))
        .route(PAIRING_INFO_PATH, get(api_pairing_info))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    tracing::info!("rrcloud-pairing listening on {bind}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! Audience-binding tests for `GET /api/config`.
    //!
    //! Authentik's userinfo endpoint resolves ANY access token the instance
    //! has issued, no matter which OAuth2 provider/application it was issued
    //! to. So "userinfo said 200" only proves "this is *some* valid Authentik
    //! token for *some* user" — not "this token was issued to the RapidRAW
    //! app". A token minted for another client (Grafana, Nextcloud, …) in the
    //! same Authentik, or leaked from one, must not unlock the user's library
    //! S3 credentials here. Authentik access tokens are JWTs whose `aud` (and
    //! `azp`) carry the client id, and `Config.oidc_client_id` is already
    //! known to the service, so the service can and should check it.
    //!
    //! The fakes below are tiny axum routers on 127.0.0.1:0; the real
    //! `api_config` handler is exercised over TCP with reqwest (already a
    //! runtime dependency), so no extra test transport crates are needed.

    use super::*;
    use axum::http::Uri;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use serde_json::{json, Value};

    /// Distinctive marker for alice's library access key id: if this string
    /// shows up in a response body, alice's S3 credentials leaked.
    const ALICE_AKID: &str = "GK-ALICE-AKID";
    const ALICE_SECRET: &str = "alice-secret-access-key-do-not-leak";
    /// The client id of THIS service's OIDC provider (the RapidRAW app).
    const OUR_CLIENT_ID: &str = "rapidraw-app";
    /// Another OAuth2 application registered in the same Authentik instance.
    const OTHER_CLIENT_ID: &str = "grafana";
    const ADMIN_BUCKET: &str = "rapidraw-admin";
    const ISSUER: &str = "https://auth.example.test/application/o/rapidraw/";

    /// Bind an axum router on an ephemeral loopback port and return its base URL.
    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Fake Authentik userinfo endpoint. Mirrors the real behaviour that
    /// matters for this bug: any non-empty bearer token the instance knows
    /// resolves to its user, regardless of the provider it was issued to.
    /// (It does NOT check `aud` — neither does Authentik.) Returns the URL of
    /// the userinfo route.
    async fn fake_userinfo() -> String {
        async fn userinfo(headers: HeaderMap) -> Response {
            let token = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(str::trim)
                .unwrap_or("");
            if token.is_empty() {
                return (StatusCode::UNAUTHORIZED, "no token").into_response();
            }
            Json(json!({
                "preferred_username": "alice",
                "sub": "6b9a0e2c-alice-sub",
                "email": "alice@example.test",
            }))
            .into_response()
        }
        let base = serve(Router::new().route("/application/o/userinfo/", get(userinfo))).await;
        format!("{base}/application/o/userinfo/")
    }

    /// Fake S3 (path-style) admin bucket holding only alice's config doc.
    async fn fake_s3() -> String {
        async fn get_object(uri: Uri) -> Response {
            if uri.path() == format!("/{ADMIN_BUCKET}/{}", config_key("alice")) {
                let doc = ConfigDoc {
                    version: 1,
                    updated_at: "2026-01-01T00:00:00Z".into(),
                    sync: PairingSyncSettings {
                        enabled: true,
                        endpoint: "https://garage.example.test".into(),
                        bucket: "alice-photos".into(),
                        region: "garage".into(),
                        force_path_style: true,
                        worker_backfill: true,
                        ..PairingSyncSettings::default()
                    },
                    credentials: PairingCredentials {
                        access_key_id: ALICE_AKID.into(),
                        secret_access_key: ALICE_SECRET.into(),
                    },
                };
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    serde_json::to_vec(&doc).unwrap(),
                )
                    .into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchKey</Code></Error>",
                )
                    .into_response()
            }
        }
        serve(Router::new().fallback(get_object)).await
    }

    /// Build the real service (same `api_config` handler and state shape as
    /// `main`) wired to the fakes, and return its base URL.
    async fn pairing_service() -> String {
        let state = Arc::new(AppState {
            config: Config {
                admin_bucket: ADMIN_BUCKET.into(),
                admin_s3_endpoint: fake_s3().await,
                admin_s3_region: "garage".into(),
                admin_s3_access_key: "GKadmin".into(),
                admin_s3_secret_key: "adminsecret".into(),
                oidc_userinfo_url: fake_userinfo().await,
                oidc_issuer_url: ISSUER.into(),
                oidc_client_id: OUR_CLIENT_ID.into(),
                default_library_endpoint: String::new(),
                default_library_region: "garage".into(),
            },
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
        });
        serve(
            Router::new()
                .route("/api/config", get(api_config))
                .route("/api/pairing-info", get(api_pairing_info))
                .with_state(state),
        )
        .await
    }

    /// `GET /api/config` with the given bearer; returns (status, body).
    async fn get_config(bearer: &str) -> (StatusCode, String) {
        let base = pairing_service().await;
        let resp = reqwest::Client::new()
            .get(format!("{base}/api/config"))
            .bearer_auth(bearer)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        (StatusCode::from_u16(status.as_u16()).unwrap(), body)
    }

    fn b64url(bytes: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// A well-formed (header.payload.signature) JWT with the given claims.
    /// The signature is not valid — and does not need to be: the service
    /// treats userinfo as the authority for token validity and identity, so
    /// the test only cares about the *claims* carried in the payload.
    fn jwt(claims: Value) -> String {
        let header = json!({ "alg": "RS256", "typ": "JWT", "kid": "test-key" });
        format!(
            "{}.{}.{}",
            b64url(&serde_json::to_vec(&header).unwrap()),
            b64url(&serde_json::to_vec(&claims).unwrap()),
            b64url(b"not-a-real-signature"),
        )
    }

    /// Claims as Authentik issues them for a user `alice` logging into the
    /// application with client id `client_id` (aud == azp == client id).
    fn alice_claims_for(client_id: &str) -> Value {
        json!({
            "iss": ISSUER,
            "sub": "6b9a0e2c-alice-sub",
            "aud": client_id,
            "azp": client_id,
            "exp": 4_102_444_800u64,
            "iat": 1_700_000_000u64,
            "preferred_username": "alice",
            "scope": "openid profile email",
        })
    }

    fn assert_creds_leaked_free(status: StatusCode, body: &str, what: &str) {
        assert!(
            !body.contains(ALICE_AKID),
            "{what}: alice's library credentials LEAKED (status {status}): {body}"
        );
        assert!(
            !body.contains(ALICE_SECRET),
            "{what}: alice's library SECRET leaked (status {status}): {body}"
        );
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{what}: expected 401, got {status} with body: {body}"
        );
    }

    // --- positive controls -------------------------------------------------

    /// (c) A token Authentik issued to OUR client id unlocks alice's config.
    #[tokio::test]
    async fn api_config_accepts_token_issued_to_our_client() {
        let token = jwt(alice_claims_for(OUR_CLIENT_ID));
        let (status, body) = get_config(&token).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        let doc: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(doc["credentials"]["accessKeyId"], ALICE_AKID);
        assert_eq!(doc["sync"]["bucket"], "alice-photos");
    }

    /// (b) `aud` may be an array (RFC 7519 §4.1.3); it is accepted when it
    /// contains our client id.
    #[tokio::test]
    async fn api_config_accepts_aud_array_containing_our_client() {
        let mut claims = alice_claims_for(OUR_CLIENT_ID);
        claims["aud"] = json!([OUR_CLIENT_ID, "other-resource"]);
        let token = jwt(claims);
        let (status, body) = get_config(&token).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        let doc: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(doc["credentials"]["accessKeyId"], ALICE_AKID);
    }

    // --- the bug: tokens not issued to this client are honoured ------------

    /// (a) THE BUG. A valid Authentik access token issued to a *different*
    /// application (here Grafana) in the same instance. Authentik's userinfo
    /// happily resolves it to alice, and today the service hands out alice's
    /// library S3 credentials. It must be rejected (401) because `aud` is not
    /// our client id.
    #[tokio::test]
    async fn api_config_rejects_token_issued_to_another_client() {
        let token = jwt(alice_claims_for(OTHER_CLIENT_ID));
        let (status, body) = get_config(&token).await;
        assert_creds_leaked_free(status, &body, "token with aud=\"grafana\"");
    }

    /// (a') Array-form `aud` that does NOT include our client id.
    #[tokio::test]
    async fn api_config_rejects_aud_array_without_our_client() {
        let mut claims = alice_claims_for(OTHER_CLIENT_ID);
        claims["aud"] = json!([OTHER_CLIENT_ID, "other-resource"]);
        let token = jwt(claims);
        let (status, body) = get_config(&token).await;
        assert_creds_leaked_free(
            status,
            &body,
            "token with aud=[\"grafana\",\"other-resource\"]",
        );
    }

    /// (e) A JWT with no `aud` claim at all carries no proof it was issued to
    /// this client; it must be rejected.
    #[tokio::test]
    async fn api_config_rejects_jwt_without_aud_claim() {
        let mut claims = alice_claims_for(OUR_CLIENT_ID);
        claims.as_object_mut().unwrap().remove("aud");
        claims.as_object_mut().unwrap().remove("azp");
        let token = jwt(claims);
        let (status, body) = get_config(&token).await;
        assert_creds_leaked_free(status, &body, "JWT without aud");
    }

    /// (d) An opaque (non-JWT) token. Authentik issues JWT access tokens by
    /// default, so the service cannot establish the audience of anything
    /// else; the expected behaviour is to reject it (401) rather than fall
    /// back to the audience-less userinfo-only check.
    #[tokio::test]
    async fn api_config_rejects_opaque_non_jwt_token() {
        let (status, body) = get_config("opaque-token-from-some-other-client").await;
        assert_creds_leaked_free(status, &body, "opaque non-JWT token");
    }

    /// Sanity: a token whose payload segment is not base64url/JSON must not
    /// crash the handler or be accepted.
    #[tokio::test]
    async fn api_config_rejects_jwt_with_garbage_payload() {
        let token = format!("{}.!!not-base64!!.{}", b64url(b"{}"), b64url(b"sig"));
        let (status, body) = get_config(&token).await;
        assert_creds_leaked_free(status, &body, "JWT with undecodable payload");
    }
}
