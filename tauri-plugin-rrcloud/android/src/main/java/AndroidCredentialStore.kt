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
 * JSON blob `{"accessKey":"...","secretKey":"..."}` under one preference
 * key. Mirrors `sync::credentials::FileCredentialStore`'s desktop contract
 * (`load`/`store`/`clear`) exactly, so the same `Credentials` struct
 * round-trips through either platform's store unchanged.
 *
 * Every entry point here fails loud: a Keystore/crypto failure is logged at
 * `Log.e` and surfaced as `null`/`false` to the caller (never silently
 * swallowed), per this unit's standard for security-relevant paths.
 */
object AndroidCredentialStore {
    private const val TAG = "RrcloudCredentialStore"
    private const val PREFS_FILE = "rrcloud_credentials"
    private const val KEY_JSON = "credentials_json"

    private fun prefs(context: Context): SharedPreferences? {
        return try {
            val masterKey = MasterKey.Builder(context)
                .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
                .build()
            EncryptedSharedPreferences.create(
                context,
                PREFS_FILE,
                masterKey,
                EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
                EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
            )
        } catch (e: Exception) {
            Log.e(TAG, "Failed to open Keystore-backed credential store", e)
            null
        }
    }

    /** `null` when nothing is stored yet, or on a Keystore failure (logged). */
    fun load(context: Context): String? {
        val sp = prefs(context) ?: return null
        return try {
            sp.getString(KEY_JSON, null)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to read stored credentials", e)
            null
        }
    }

    /** `json` must already be a well-formed `{"accessKey":...,"secretKey":...}` object. */
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
