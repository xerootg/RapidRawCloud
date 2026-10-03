// No JS-facing commands yet: the §5.1/§5.2 WorkManager jobs, the
// ContentObserver DCIM watch, and the JNI bridge are all OS-triggered, not
// called from the webview (ARCHITECTURE.md §5). `sync_start_foreground_sync`
// (§5.1 point 2, the user-initiated-long-operation trigger) lands in the
// P5 green pass as the one genuine command this plugin needs.
const COMMANDS: &[&str] = &[];

fn main() {
  tauri_plugin::Builder::new(COMMANDS)
    .android_path("android")
    .build();
}
