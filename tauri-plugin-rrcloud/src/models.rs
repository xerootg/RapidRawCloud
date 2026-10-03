//! No JS-facing request/response models yet: the P5 scope's Kotlin/JNI
//! surface (WorkManager jobs, the ContentObserver, the Keystore
//! `CredentialStore` callback) is OS-triggered, not called from the
//! webview (ARCHITECTURE.md §5). The one genuine command this plugin needs
//! — `sync_start_foreground_sync` (§5.1 point 2) — lands with its own
//! argument type in the P5 green pass.
