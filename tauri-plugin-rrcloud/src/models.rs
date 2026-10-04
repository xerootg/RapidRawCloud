//! The P5 scope's Kotlin/JNI surface (WorkManager jobs, the
//! `ContentObserver`, the Keystore `CredentialStore` callback) is
//! OS-triggered, not called from the webview (ARCHITECTURE.md §5), so it
//! needs no request/response models here. [`crate::commands::DcimAccessStatus`]
//! is the one response type this plugin's JS-facing surface returns.
