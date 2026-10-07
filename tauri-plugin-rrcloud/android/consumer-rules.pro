# RrcloudBridge's five @JvmStatic callback methods (ARCHITECTURE.md §5.1)
# are invoked ONLY via `rrcloud_core`'s JNI bridge, through
# `JNIEnv::call_static_method` with a hardcoded method name/signature
# string (`bridge.rs`, `android_integration.rs`) — there is no Kotlin/Java
# call site anywhere in this app for R8 to trace as "used". Android's
# default `proguard-android-optimize.txt` keeps native (`external fun`)
# methods and their declaring class, which protects the OTHER direction of
# this contract (`runSyncCycle`/`dcimScanDecisions`/`dcimRecordImport`/
# `dirtyUnbackedCount`/`releaseStateLock`/`reacquireStateLock`), but it has
# no rule for plain reflection-invoked statics like these.
#
# Without this keep rule, a minified release build (`gen/android/app`'s
# release `buildType` has `isMinifyEnabled = true`) can rename or strip
# these methods: `GetStaticMethodID` then returns null and every
# credential load/store/clear + settings read + expedited-sync enqueue
# throws a pending JNI exception from the native side — sync silently
# degrades to permanently "not configured" in exactly the build users
# actually install (the mandated `--debug` build gate can't catch this,
# since `isMinifyEnabled = false` for debug). P5 review round-1 major.
#
# Consumed automatically by the app module via `consumerProguardFiles`
# (`tauri-plugin-rrcloud/android/build.gradle.kts`) — no edit to
# `gen/android/app`'s own proguard rules needed.
-keepclassmembers class com.plugin.rrcloud.RrcloudBridge {
    java.lang.String loadCredentialsJson(android.content.Context);
    boolean storeCredentialsJson(android.content.Context, java.lang.String);
    boolean clearCredentials(android.content.Context);
    java.lang.String loadSyncSettingsJson(android.content.Context);
    void enqueueExpeditedSync(android.content.Context);
}

# androidx.security:security-crypto pulls in Google Tink, which references
# the compile-only JSR-305/concurrency annotations. They are absent from
# the runtime classpath by design, but R8 treats a missing referenced
# class as fatal in a minified release build:
#   Missing class javax.annotation.concurrent.GuardedBy
#       (referenced from: com.google.crypto.tink.KeysetManager ...)
# killed `:app:minifyUniversalReleaseWithR8` on the v1.6.4-cloud.1 release
# pipeline. Tink's own README prescribes exactly these -dontwarn rules.
-dontwarn javax.annotation.Nullable
-dontwarn javax.annotation.concurrent.GuardedBy
