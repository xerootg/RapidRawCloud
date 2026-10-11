use serde::de::DeserializeOwned;
use tauri::{plugin::PluginApi, AppHandle, Runtime};

use crate::commands::DcimAccessStatus;

pub fn init<R: Runtime, C: DeserializeOwned>(
    app: &AppHandle<R>,
    _api: PluginApi<R, C>,
) -> crate::Result<Rrcloud<R>> {
    Ok(Rrcloud(app.clone()))
}

/// Desktop side of the plugin. The §5 Android platform work (WorkManager,
/// Keystore, DCIM `ContentObserver`, JNI bridge) has no desktop
/// counterpart — `SyncManager` already drives the engine directly in the
/// foreground on desktop (ARCHITECTURE.md §3.3) — so this handle exists
/// only so `RrcloudExt` resolves on every platform.
#[allow(dead_code)]
pub struct Rrcloud<R: Runtime>(AppHandle<R>);

impl<R: Runtime> Rrcloud<R> {
    /// No-op: the desktop engine already runs freely in the foreground
    /// (§5.1 point 1) — there is no separate "foreground sync mode" to
    /// enter.
    pub fn start_foreground_sync(&self) -> crate::Result<()> {
        Ok(())
    }

    /// Desktop has no partial-media-access grant model — always full
    /// access.
    pub fn dcim_access_status(&self) -> crate::Result<DcimAccessStatus> {
        Ok(DcimAccessStatus { partial: false })
    }

    pub fn dock_scan_start(&self) -> crate::Result<()> {
        Err(crate::Error::DockUnsupported)
    }

    pub fn dock_scan_stop(&self) -> crate::Result<()> {
        Err(crate::Error::DockUnsupported)
    }

    pub fn dock_connect(&self, _address: String) -> crate::Result<rrcloud_proto::DockBleInfo> {
        Err(crate::Error::DockUnsupported)
    }

    pub fn dock_disconnect(&self) -> crate::Result<()> {
        Err(crate::Error::DockUnsupported)
    }

    pub fn dock_rpc(
        &self,
        _method: String,
        _path: String,
        _body: Option<serde_json::Value>,
        _auth: Option<String>,
    ) -> crate::Result<crate::commands::DockRpcReply> {
        Err(crate::Error::DockUnsupported)
    }
}
