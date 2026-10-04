package com.plugin.rrcloud

import android.content.ContentUris
import android.content.Context
import android.os.Build
import android.provider.MediaStore
import android.util.Log
import androidx.work.ListenableWorker
import androidx.work.Worker
import androidx.work.WorkerParameters
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.util.Locale

/**
 * The §5.2 durable DCIM scan path: queries `MediaStore.Images.Media.
 * EXTERNAL_CONTENT_URI` for rows added since the last cursor, filters to
 * the configured bucket(s) and RAW mimetypes/extensions, batches the
 * dedupe decision through [RrcloudBridge.dcimScanDecisions], and for every
 * `rehash`/`new` candidate streams the bytes into
 * `<library>/DCIM-import/.rr.part-<name>` then renames, finally recording
 * the import via [RrcloudBridge.dcimRecordImport].
 *
 * Cursor: on API 30+, `MediaStore.getGeneration()`/`GENERATION_ADDED` — a
 * monotonic counter immune to back-dated rows. On API 24-29 (no generation
 * counter), `DATE_ADDED` with a 48h overlap re-scan — the same formula as
 * `rrcloud_core::android::scan_window::effective_floor` (the Rust side is
 * the tested, authoritative version; this is a 3-line restatement in
 * Kotlin, not a separate implementation — there is no JNI accessor for a
 * single scalar formula, so duplicating it here is the pragmatic minimal
 * extension rather than widening the bridge's JNI surface for one `i64`
 * computation).
 */
class DcimScanWorker(context: Context, params: WorkerParameters) : Worker(context, params) {
    override fun doWork(): ListenableWorker.Result {
        val settings = RrcloudSyncSettingsReader.readWorkConstraintSettings(applicationContext)
        if (!settings.enabled || !settings.autoWatchDcim) {
            return ListenableWorker.Result.success()
        }

        val watchedBuckets = watchedBucketNames(settings)
        val prefs = applicationContext.getSharedPreferences(PREFS_FILE, Context.MODE_PRIVATE)
        return try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                scanWithGeneration(prefs, watchedBuckets)
            } else {
                scanWithDateAdded(prefs, watchedBuckets)
            }
            ListenableWorker.Result.success()
        } catch (e: Exception) {
            Log.e(TAG, "DCIM scan failed", e)
            ListenableWorker.Result.retry()
        }
    }

    private fun scanWithGeneration(prefs: android.content.SharedPreferences, watchedBuckets: Set<String>) {
        val volume = MediaStore.VOLUME_EXTERNAL
        val lastGeneration = prefs.getLong(KEY_GENERATION, 0L)
        // Snapshot the generation BEFORE issuing the query, not after
        // processing it (regression, P5 review round 0: a photo whose
        // GENERATION_ADDED lands between the pre-query snapshot and the
        // query itself is still > lastGeneration and so is included in
        // THIS pass's results; reading the generation only after the slow
        // per-file processCandidates() call would instead let it slip
        // between "not in this pass's query results" and "<= the newly
        // stored cursor", i.e. silently and permanently lost).
        val nowGeneration = MediaStore.getGeneration(applicationContext, volume)
        val selection = if (lastGeneration > 0) {
            "${MediaStore.MediaColumns.GENERATION_ADDED} > ?"
        } else {
            null
        }
        val args = if (lastGeneration > 0) arrayOf(lastGeneration.toString()) else null
        val candidates = queryCandidates(selection, args, watchedBuckets)
        processCandidates(candidates)
        prefs.edit().putLong(KEY_GENERATION, nowGeneration).apply()
    }

    private fun scanWithDateAdded(prefs: android.content.SharedPreferences, watchedBuckets: Set<String>) {
        val lastCursor = if (prefs.contains(KEY_DATE_ADDED_CURSOR)) prefs.getLong(KEY_DATE_ADDED_CURSOR, 0L) else null
        val nowUnix = System.currentTimeMillis() / 1000L
        val floor = effectiveFloor(lastCursor, nowUnix)
        val candidates = queryCandidates(
            "${MediaStore.MediaColumns.DATE_ADDED} > ?",
            arrayOf(floor.toString()),
            watchedBuckets
        )
        processCandidates(candidates)
        prefs.edit().putLong(KEY_DATE_ADDED_CURSOR, nowUnix).apply()
    }

    /** Mirrors `rrcloud_core::android::scan_window::effective_floor` — see
     * the class doc. */
    private fun effectiveFloor(cursorUnix: Long?, nowUnix: Long): Long {
        if (cursorUnix == null) return 0L
        val overlapSecs = 48L * 60L * 60L
        return (cursorUnix - overlapSecs).coerceAtLeast(0L).coerceAtMost(nowUnix.coerceAtLeast(0L))
    }

    private data class Candidate(val id: Long, val path: String, val size: Long, val mtimeUnix: Long, val displayName: String)

    private fun queryCandidates(
        selection: String?,
        args: Array<String>?,
        watchedBuckets: Set<String>
    ): List<Candidate> {
        // The dedupe-key "path" must survive MediaStore row-id churn (§5.2:
        // the key "survives re-scans, re-mounts, and MediaStore id churn").
        // A row's `_ID` is NOT stable across a provider rebuild (factory
        // reset+restore, SD card reinsert, OS media-DB rescan) — exactly
        // the events §5.2 calls out — so it must never be embedded in the
        // key (P5 review round 1: the previous `uri.toString()` key baked
        // the `_ID` into a `content://.../media/<id>` string and broke the
        // invariant). Instead: RELATIVE_PATH+DISPLAY_NAME on API 29+ (both
        // populated under scoped storage), or the legacy DATA column on
        // API 24-28 (pre-scoped-storage, still fully populated for reads).
        // `_ID` is kept on the candidate only to build the content:// URI
        // used for the streaming read/import, never as the dedupe key.
        val useRelativePath = Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q
        val projection = arrayOf(
            MediaStore.MediaColumns._ID,
            MediaStore.MediaColumns.SIZE,
            MediaStore.MediaColumns.DATE_MODIFIED,
            MediaStore.MediaColumns.DISPLAY_NAME,
            MediaStore.MediaColumns.BUCKET_DISPLAY_NAME,
            MediaStore.MediaColumns.MIME_TYPE,
            if (useRelativePath) MediaStore.MediaColumns.RELATIVE_PATH else MediaStore.MediaColumns.DATA
        )
        val out = mutableListOf<Candidate>()
        applicationContext.contentResolver.query(
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            projection,
            selection,
            args,
            null
        )?.use { cursor ->
            val idCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns._ID)
            val sizeCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns.SIZE)
            val mtimeCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns.DATE_MODIFIED)
            val nameCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns.DISPLAY_NAME)
            val bucketCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns.BUCKET_DISPLAY_NAME)
            val mimeCol = cursor.getColumnIndexOrThrow(MediaStore.MediaColumns.MIME_TYPE)
            val stablePathCol = cursor.getColumnIndexOrThrow(
                if (useRelativePath) MediaStore.MediaColumns.RELATIVE_PATH else MediaStore.MediaColumns.DATA
            )
            while (cursor.moveToNext()) {
                val bucket = cursor.getString(bucketCol)
                if (watchedBuckets.isNotEmpty() && bucket !in watchedBuckets) {
                    continue
                }
                val name = cursor.getString(nameCol) ?: continue
                val mime = cursor.getString(mimeCol) ?: ""
                if (!isRawCandidate(name, mime)) {
                    continue
                }
                val id = cursor.getLong(idCol)
                val stablePath = if (useRelativePath) {
                    val relativePath = cursor.getString(stablePathCol)
                    if (relativePath.isNullOrEmpty()) name else relativePath + name
                } else {
                    cursor.getString(stablePathCol)?.takeIf { it.isNotEmpty() } ?: name
                }
                out.add(
                    Candidate(
                        id = id,
                        path = stablePath,
                        size = cursor.getLong(sizeCol),
                        mtimeUnix = cursor.getLong(mtimeCol),
                        displayName = name
                    )
                )
            }
        }
        return out
    }

    private fun watchedBucketNames(settings: WorkConstraintSettings): Set<String> {
        // ARCHITECTURE.md §5.2: "Filtered to configured bucket ids (default
        // `DCIM/Camera`)". The configured list (`watchedMediaBuckets`) is
        // read from the same `settings.json` block as everything else;
        // empty means "not configured yet" and falls back to the default.
        return if (settings.watchedMediaBuckets.isEmpty()) {
            DEFAULT_WATCHED_BUCKETS
        } else {
            settings.watchedMediaBuckets.toSet()
        }
    }

    private fun isRawCandidate(displayName: String, mimeType: String): Boolean {
        if (mimeType.startsWith("image/x-") || mimeType == "image/x-adobe-dng") {
            return true
        }
        val lower = displayName.lowercase(Locale.ROOT)
        return RAW_EXTENSIONS.any { lower.endsWith(it) }
    }

    private fun processCandidates(candidates: List<Candidate>) {
        if (candidates.isEmpty()) {
            return
        }
        val candidatesJson = JSONArray()
        for (c in candidates) {
            candidatesJson.put(
                JSONObject().apply {
                    put("path", c.path)
                    put("size", c.size)
                    put("mtimeUnix", c.mtimeUnix)
                }
            )
        }
        val decisionsJson = RrcloudBridge.dcimScanDecisions(applicationContext, candidatesJson.toString())
            ?: run {
                Log.w(TAG, "dcimScanDecisions returned null; will retry this batch next scan")
                return
            }
        val decisions = JSONArray(decisionsJson)
        val byPath = candidates.associateBy { it.path }
        for (i in 0 until decisions.length()) {
            val entry = decisions.getJSONObject(i)
            val path = entry.getString("path")
            val decision = entry.getString("decision")
            if (decision == "skip") {
                continue
            }
            val candidate = byPath[path] ?: continue
            importCandidate(candidate)
        }
    }

    private fun importCandidate(candidate: Candidate) {
        val libraryRoot = libraryRoot() ?: return
        val importDir = File(libraryRoot, "DCIM-import").apply { mkdirs() }
        val finalFile = File(importDir, candidate.displayName)
        val partFile = File(importDir, ".rr.part-${candidate.displayName}")

        val uri = ContentUris.withAppendedId(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, candidate.id)
        try {
            applicationContext.contentResolver.openInputStream(uri)?.use { input ->
                partFile.outputStream().use { output ->
                    val buffer = ByteArray(256 * 1024)
                    while (true) {
                        val read = input.read(buffer)
                        if (read < 0) break
                        output.write(buffer, 0, read)
                    }
                }
            } ?: run {
                Log.w(TAG, "openInputStream returned null for $uri")
                return
            }
            if (!partFile.renameTo(finalFile)) {
                Log.e(TAG, "Failed to rename ${partFile.path} to ${finalFile.path}")
                partFile.delete()
                return
            }
        } catch (e: Exception) {
            Log.e(TAG, "Streaming copy failed for $uri", e)
            partFile.delete()
            return
        }

        val code = RrcloudBridge.dcimRecordImport(
            applicationContext,
            candidate.path,
            candidate.size,
            candidate.mtimeUnix,
            finalFile.absolutePath
        )
        if (code != RrcloudBridge.RESULT_SUCCESS) {
            Log.w(TAG, "dcimRecordImport returned code $code for ${finalFile.path}")
        }
    }

    private fun libraryRoot(): File? {
        val base = applicationContext.externalMediaDirs.firstOrNull() ?: return null
        return File(base, ".library")
    }

    companion object {
        private const val TAG = "RrcloudDcimScanWorker"
        private const val PREFS_FILE = "rrcloud_dcim_scan"
        private const val KEY_GENERATION = "generation"
        private const val KEY_DATE_ADDED_CURSOR = "date_added_cursor"
        private val DEFAULT_WATCHED_BUCKETS = setOf("Camera")

        private val RAW_EXTENSIONS = listOf(
            ".dng", ".crw", ".cr2", ".cr3", ".raw", ".erf", ".raf", ".3fr", ".fff", ".iiq",
            ".dcr", ".mos", ".rwl", ".mrw", ".nef", ".nrw", ".orf", ".rw2", ".pef", ".srw",
            ".arw", ".srf", ".sr2"
        )
    }
}
