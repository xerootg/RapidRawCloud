// The §5.1/§5.2 WorkManager jobs, ContentObserver DCIM watch, and JNI
// bridge are all OS-triggered, not called from the webview (ARCHITECTURE.md
// §5) — these two are the only genuine JS-facing commands this plugin
// needs: `sync_start_foreground_sync` (§5.1 point 2, the user-initiated
// long-operation trigger) and `sync_dcim_access_status` (§5.2 partial-
// photo-access detection).
const COMMANDS: &[&str] = &["sync_start_foreground_sync", "sync_dcim_access_status"];

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .android_path("android")
        .build();
}
