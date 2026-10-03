use serde::de::DeserializeOwned;
use tauri::{plugin::PluginApi, AppHandle, Runtime};

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
/// only so `RrcloudExt` resolves on every platform; it carries no methods
/// yet.
#[allow(dead_code)]
pub struct Rrcloud<R: Runtime>(AppHandle<R>);
