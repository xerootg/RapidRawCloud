package com.plugin.rrcloud

import android.content.Context
import android.database.ContentObserver
import android.net.Uri
import android.os.Handler
import android.os.Looper

/**
 * The §5.2 live DCIM-watch path: a `ContentObserver` on
 * `MediaStore.Images.Media.EXTERNAL_CONTENT_URI`, registered while the
 * process is alive (from [RrcloudPlugin.load]) and debounced — a burst of
 * `onChange` calls (a multi-shot camera burst, a bulk import) coalesces
 * into one [RrcloudWorkScheduler.enqueueDcimScanNow] trigger instead of one
 * per row.
 */
class DcimContentObserver(private val context: Context) : ContentObserver(Handler(Looper.getMainLooper())) {
    private val handler = Handler(Looper.getMainLooper())
    private val debounced = Runnable {
        RrcloudWorkScheduler.enqueueDcimScanNow(context)
    }

    override fun onChange(selfChange: Boolean, uri: Uri?) {
        super.onChange(selfChange, uri)
        handler.removeCallbacks(debounced)
        handler.postDelayed(debounced, DEBOUNCE_MS)
    }

    companion object {
        private const val DEBOUNCE_MS = 3_000L
    }
}
