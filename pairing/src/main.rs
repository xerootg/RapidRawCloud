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
//!    access token and sends it as `Authorization: Bearer …`. We validate
//!    it by calling Authentik's userinfo endpoint, then return that user's
//!    `config.json` so the app configures itself with zero typing.
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
use s3::{creds::Credentials, Bucket, Region};
use serde::{Deserialize, Serialize};

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

/// Mirrors the app's `SyncSettings` (camelCase). Only the fields the app
/// actually consumes; extra fields the app writes later are preserved by the
/// browser form round-trip only for the ones below (v1 keeps it minimal).
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct SyncSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    bucket: String,
    #[serde(default)]
    region: String,
    #[serde(default = "default_true")]
    force_path_style: bool,
    #[serde(default = "default_cache_gb")]
    cache_size_gb: u32,
    #[serde(default = "default_preview_gb")]
    preview_budget_gb: u32,
    #[serde(default = "default_prefetch_months")]
    preview_prefetch_months: u32,
    #[serde(default)]
    auto_watch_dcim: bool,
    #[serde(default)]
    watched_media_buckets: Vec<String>,
    #[serde(default)]
    worker_backfill: bool,
}

fn default_true() -> bool {
    true
}
fn default_cache_gb() -> u32 {
    8
}
fn default_preview_gb() -> u32 {
    10
}
fn default_prefetch_months() -> u32 {
    12
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Creds {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ConfigDoc {
    version: u32,
    updated_at: String,
    sync: SyncSettings,
    credentials: Creds,
}

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

fn config_key(username: &str) -> String {
    format!("users/{username}/config.json")
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

/// Validate an app's bearer token by calling Authentik's userinfo endpoint,
/// returning the sanitized username. `None` → invalid/unusable token.
async fn validate_bearer(state: &AppState, bearer: &str) -> Option<String> {
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

    let doc = ConfigDoc {
        version: 1,
        updated_at: now_iso8601(),
        sync: SyncSettings {
            enabled: true,
            endpoint,
            bucket,
            region,
            force_path_style: form.force_path_style.is_some(),
            cache_size_gb: form.cache_size_gb.unwrap_or_else(default_cache_gb),
            preview_budget_gb: form.preview_budget_gb.unwrap_or_else(default_preview_gb),
            preview_prefetch_months: default_prefetch_months(),
            // DEVICE-LOCAL fields — deliberately NOT collected here and
            // written as inert defaults: a camera-roll watch list belongs
            // to one physical device, and the app ignores these two doc
            // fields when applying a pair (merge_cloud_sync in the app's
            // sync::pairing preserves the device's own values).
            auto_watch_dcim: false,
            watched_media_buckets: Vec::new(),
            worker_backfill: form.worker_backfill.is_some(),
        },
        credentials: Creds {
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
    Json(serde_json::json!({
        "version": 1,
        "issuer": state.config.oidc_issuer_url,
        "clientId": state.config.oidc_client_id,
        "configEndpoint": "/api/config",
        // Kept for older clients; `redirectUris` is the full list.
        "redirectUri": "rapidraw://auth-callback",
        // Every app that pairs through this service, each with its own
        // scheme so two apps on one phone never fight over a callback. The
        // Authentik provider must list all of them as allowed redirect URIs.
        //  - RapidRAW (the editor):            rapidraw://auth-callback
        //  - Raw2DNG (RawImageSnapseedBridge): raw2dng://auth-callback
        "redirectUris": ["rapidraw://auth-callback", "raw2dng://auth-callback"],
    }))
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
        None => (
            esc(&cfg.default_library_endpoint),
            String::new(),
            esc(&cfg.default_library_region),
            String::new(),
            true,
            true,
            default_cache_gb(),
            default_preview_gb(),
        ),
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
        .route("/api/config", get(api_config))
        .route("/api/pairing-info", get(api_pairing_info))
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
