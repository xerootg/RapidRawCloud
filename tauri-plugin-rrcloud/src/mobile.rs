use serde::de::DeserializeOwned;
use tauri::{
    plugin::{PluginApi, PluginHandle},
    AppHandle, Runtime,
};

use serde::{Deserialize, Serialize};

use crate::commands::{DcimAccessStatus, DockRpcReply};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DockAddressArgs {
    address: String,
}

/// The Kotlin side takes and returns bodies as JSON *text* (`bodyJson`) so
/// arbitrary documents cross the invoke bridge without a typed schema.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DockRpcArgs {
    method: String,
    path: String,
    body_json: Option<String>,
    auth: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DockRpcRaw {
    status: i32,
    body_json: String,
}

/// Initializes the Kotlin plugin class. `RrcloudPlugin` (package
/// `com.plugin.rrcloud`, matching the JNI bridge's package in
/// `rrcloud_core::android::bridge`) currently exposes no commands — see
/// `src/commands.rs`'s doc — but registration happens here regardless so
/// the Kotlin class (and, through it, `RrcloudBridge`'s native-library
/// load) is wired into the app's plugin set from the moment this crate is
/// a dependency, not deferred to whenever the first command lands.
///
/// iOS is out of scope for P5 (ARCHITECTURE.md §5 is Android-only); this
/// plugin was scaffolded `--android` only, so `mobile.rs` only ever
/// compiles for `target_os = "android"` in this repo.
pub fn init<R: Runtime, C: DeserializeOwned>(
    _app: &AppHandle<R>,
    api: PluginApi<R, C>,
) -> crate::Result<Rrcloud<R>> {
    let handle = api.register_android_plugin("com.plugin.rrcloud", "RrcloudPlugin")?;
    Ok(Rrcloud(handle))
}

/// Mobile side of the plugin: calls into the Kotlin `RrcloudPlugin`
/// `@Command` methods (`startForegroundSync`/`checkPartialMediaAccess`)
/// via the standard Tauri mobile-plugin invoke bridge.
#[allow(dead_code)]
pub struct Rrcloud<R: Runtime>(PluginHandle<R>);

impl<R: Runtime> Rrcloud<R> {
    /// §5.1 point 2: starts `SyncForegroundService` via the Kotlin
    /// `startForegroundSync` command. Only valid while the app is
    /// foreground — Kotlin is responsible for the
    /// `ForegroundServiceStartNotAllowedException` that would otherwise
    /// throw from the background (§5.1's whole "honest model").
    pub fn start_foreground_sync(&self) -> crate::Result<()> {
        self.0
            .run_mobile_plugin("startForegroundSync", ())
            .map_err(Into::into)
    }

    /// §5.2 partial-photo-access detection via the Kotlin
    /// `checkPartialMediaAccess` command.
    pub fn dcim_access_status(&self) -> crate::Result<DcimAccessStatus> {
        self.0
            .run_mobile_plugin("checkPartialMediaAccess", ())
            .map_err(Into::into)
    }

    pub fn dock_scan_start(&self) -> crate::Result<()> {
        self.0
            .run_mobile_plugin("dockScanStart", ())
            .map_err(Into::into)
    }

    pub fn dock_scan_stop(&self) -> crate::Result<()> {
        self.0
            .run_mobile_plugin("dockScanStop", ())
            .map_err(Into::into)
    }

    pub fn dock_connect(&self, address: String) -> crate::Result<rrcloud_proto::DockBleInfo> {
        self.0
            .run_mobile_plugin("dockConnect", DockAddressArgs { address })
            .map_err(Into::into)
    }

    pub fn dock_disconnect(&self) -> crate::Result<()> {
        self.0
            .run_mobile_plugin("dockDisconnect", ())
            .map_err(Into::into)
    }

    pub fn dock_rpc(
        &self,
        method: String,
        path: String,
        body: Option<serde_json::Value>,
        auth: Option<String>,
    ) -> crate::Result<DockRpcReply> {
        let body_json = body.map(|b| b.to_string());
        let raw: DockRpcRaw = self.0.run_mobile_plugin(
            "dockRpc",
            DockRpcArgs {
                method,
                path,
                body_json,
                auth,
            },
        )?;
        let body = serde_json::from_str(&raw.body_json).unwrap_or(serde_json::Value::Null);
        Ok(DockRpcReply {
            status: raw.status,
            body,
        })
    }
}
