package com.plugin.rrcloud

import android.content.Context
import android.content.SharedPreferences
import android.util.Log
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import org.json.JSONObject

/**
 * Keystore-backed S3 credential storage (ARCHITECTURE.md §5.1/§3.6): an
 * `EncryptedSharedPreferences` file whose AES key is generated and held in
 * the Android Keystore (never exported, never backed up), wrapping a single
 * JSON blob `{"access_key":"...","secret_key":"..."}` under one preference
 * key. Mirrors `sync::credentials::FileCredentialStore`'s desktop contract
 * (`load`/`store`/`clear`) exactly, so the same `Credentials` struct
 * round-trips through either platform's store unchanged.
 *
 * Every entry point here fails loud: a Keystore/crypto failure is always
 * logged at `Log.e` (never silently swallowed), per this unit's standard
 * for security-relevant paths. [store]/[clear] additionally surface the
 * failure to their caller as `false` (never a quiet success) — but
 * [load]'s return type is the ambiguous one: a plain Kotlin `null` has to
 * mean BOTH "nothing stored yet" (benign — sync simply isn't configured)
 * and "the Keystore/crypto layer itself failed" (a real security-relevant
 * failure, e.g. a Keystore key invalidated by a lock-screen change) to
 * every direct Kotlin caller. [load] therefore does NOT collapse the two:
 * it returns plain `null` only for the former, and *throws*
 * [CredentialStoreUnavailable] for the latter — which crosses the JNI
 * boundary as a pending Java exception that both Rust callers
 * (`android_integration.rs::android_credential_store_load` and
 * `rrcloud_core::android::bridge`'s `call_static_string_method`) already
 * detect and log distinctly from a clean `null` return (see each site's
 * own `Err`-vs-null branch) — this fix only needed to stop [load] from
 * erasing that distinction before it ever reached either of them.
 */
object AndroidCredentialStore {
    private const val TAG = "RrcloudCredentialStore"
    private const val PREFS_FILE = "rrcloud_credentials"
    private const val KEY_JSON = "credentials_json"

    /** Thrown by [load] for a genuine Keystore/crypto failure — never for
     * "nothing stored yet", which is a plain `null` return. Lets a real
     * hardware/crypto failure cross the JNI boundary as a pending Java
     * exception instead of being silently reported as "unconfigured". */
    class CredentialStoreUnavailable(message: String, cause: Throwable) : RuntimeException(message, cause)

    private fun openPrefsOrThrow(context: Context): SharedPreferences {
        try {
            val masterKey = MasterKey.Builder(context)
                .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
                .build()
            return EncryptedSharedPreferences.create(
                context,
                PREFS_FILE,
                masterKey,
                EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
                EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
            )
        } catch (e: Exception) {
            Log.e(TAG, "Failed to open Keystore-backed credential store", e)
            throw CredentialStoreUnavailable("Failed to open Keystore-backed credential store", e)
        }
    }

    /** [store]/[clear]'s view of the store: `null` on a Keystore failure
     * (already logged by [openPrefsOrThrow]), matching their existing
     * Boolean-`false`-on-failure contract — only [load] needs the
     * throwing variant, since only its return type would otherwise
     * conflate "unconfigured" with "failed". */
    private fun prefs(context: Context): SharedPreferences? =
        try {
            openPrefsOrThrow(context)
        } catch (e: CredentialStoreUnavailable) {
            null
        }

    /**
     * `null` when nothing is stored yet (benign). Throws
     * [CredentialStoreUnavailable] — logged, never swallowed — on a
     * genuine Keystore/crypto failure, so that failure is never mistaken
     * for "sync not configured" by a caller on the other side of the JNI
     * boundary.
     */
    fun load(context: Context): String? {
        val sp = openPrefsOrThrow(context)
        return try {
            sp.getString(KEY_JSON, null)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to read stored credentials", e)
            throw CredentialStoreUnavailable("Failed to read stored credentials", e)
        }
    }

    /** `json` must already be a well-formed `{"access_key":...,"secret_key":...}` object
     * (matching the Rust-side `AndroidCredentials`/`Credentials` structs' serde shape —
     * no `rename_all`, so plain snake_case; Kotlin only validates the blob is
     * well-formed JSON here and never inspects field names itself). */
    fun store(context: Context, json: String): Boolean {
        val sp = prefs(context) ?: return false
        return try {
            // Validate shape before committing — a malformed write here
            // would otherwise surface much later as an opaque sync failure.
            JSONObject(json)
            sp.edit().putString(KEY_JSON, json).commit()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to store credentials", e)
            false
        }
    }

    fun clear(context: Context): Boolean {
        val sp = prefs(context) ?: return false
        return try {
            sp.edit().remove(KEY_JSON).commit()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to clear credentials", e)
            false
        }
    }
}
