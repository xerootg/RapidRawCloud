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
/// `sync_start_foreground_sync`/`sync_dcim_access_status` (§5.1 point 2 /
/// §5.2) are the only JS-facing commands (see `src/commands.rs`'s doc) —
/// `setup` below still registers the Kotlin plugin class on Android, which
/// is what wires `RrcloudBridge`'s native-library load into the app's
/// lifecycle; the WorkManager jobs, `ContentObserver`, and JNI bridge it
/// exposes otherwise are all OS-triggered, not invoked through this
/// `invoke_handler`.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("rrcloud")
        .invoke_handler(tauri::generate_handler![
            commands::sync_start_foreground_sync,
            commands::sync_dcim_access_status,
            commands::dock_scan_start,
            commands::dock_scan_stop,
            commands::dock_connect,
            commands::dock_disconnect,
            commands::dock_rpc,
        ])
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
