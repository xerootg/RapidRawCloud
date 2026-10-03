use serde::de::DeserializeOwned;
use tauri::{
  plugin::{PluginApi, PluginHandle},
  AppHandle, Runtime,
};

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

/// Mobile side of the plugin. No methods yet — see `src/commands.rs`'s
/// doc; `sync_start_foreground_sync` lands here in the P5 green pass.
#[allow(dead_code)]
pub struct Rrcloud<R: Runtime>(PluginHandle<R>);
