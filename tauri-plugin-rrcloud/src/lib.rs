use tauri::{
  plugin::{Builder, TauriPlugin},
  Manager, Runtime,
};

#[cfg(desktop)]
mod desktop;
#[cfg(mobile)]
mod mobile;

mod commands;
mod error;
mod models;

pub use error::{Error, Result};

#[cfg(desktop)]
use desktop::Rrcloud;
#[cfg(mobile)]
use mobile::Rrcloud;

/// Extensions to [`tauri::App`], [`tauri::AppHandle`] and [`tauri::Window`] to access the rrcloud APIs.
pub trait RrcloudExt<R: Runtime> {
  fn rrcloud(&self) -> &Rrcloud<R>;
}

impl<R: Runtime, T: Manager<R>> crate::RrcloudExt<R> for T {
  fn rrcloud(&self) -> &Rrcloud<R> {
    self.state::<Rrcloud<R>>().inner()
  }
}

/// Initializes the plugin.
///
/// No JS-facing commands yet (see `src/commands.rs`'s doc) — `setup` below
/// still registers the Kotlin plugin class on Android, which is what wires
/// `RrcloudBridge`'s native-library load into the app's lifecycle; the
/// WorkManager jobs, `ContentObserver`, and JNI bridge it exposes are all
/// OS-triggered, not invoked through this `invoke_handler`.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
  Builder::new("rrcloud")
    .invoke_handler(tauri::generate_handler![])
    .setup(|app, api| {
      #[cfg(mobile)]
      let rrcloud = mobile::init(app, api)?;
      #[cfg(desktop)]
      let rrcloud = desktop::init(app, api)?;
      app.manage(rrcloud);
      Ok(())
    })
    .build()
}
