package com.plugin.rrcloud

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.ParcelUuid
import android.util.Log
import app.tauri.plugin.JSObject
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.util.ArrayDeque
import java.util.UUID

/**
 * BLE central for the camera dock's admin service (protocol definition
 * `DOCK_BLE_*`, firmware/rrcloud-ingest/main/ble.c).
 *
 * One dock at a time. A request is a JSON document fragmented over the RX
 * characteristic (flags byte + u16 LE total length on the first fragment);
 * the reply arrives as notifications on TX with the same framing. Writing RX
 * needs an encrypted link, so the first write makes Android bond with the
 * dock ("Just Works" — the dock has no display); the OS shows its own
 * pairing dialog and retries the write. All callbacks run on the main looper.
 */
@SuppressLint("MissingPermission")
class DockBleClient(private val context: Context, private val emit: (String, JSObject) -> Unit) {
    companion object {
        private const val TAG = "DockBle"
        val SERVICE: UUID = UUID.fromString("75969b8f-4f5b-4f23-931f-79d1f9dd5114")
        val RX: UUID = UUID.fromString("0b0fd650-dbcf-4b31-974f-932a29b5a481")
        val TX: UUID = UUID.fromString("ffe3977d-ab87-4993-9537-27bd8900962b")
        val INFO: UUID = UUID.fromString("fba1763b-c749-4d9e-b69d-9143ad134c84")
        val CCCD: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        const val FLAG_FIRST = 1
        const val FLAG_LAST = 2
        const val MAX_RESPONSE = 32768
        const val RPC_TIMEOUT_MS = 30_000L
        const val CONNECT_TIMEOUT_MS = 25_000L
        const val SCAN_MS = 12_000L
    }

    private class Rpc(val id: Int, val payload: ByteArray, val cb: (Result<Pair<Int, String>>) -> Unit)

    private val main = Handler(Looper.getMainLooper())
    private val adapter: BluetoothAdapter?
        get() = (context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter

    private var scanCallback: ScanCallback? = null
    private var gatt: BluetoothGatt? = null
    private var rx: BluetoothGattCharacteristic? = null
    private var tx: BluetoothGattCharacteristic? = null
    private var info: BluetoothGattCharacteristic? = null
    private var mtu = 23
    private var ready = false
    private var connectCb: ((Result<JSONObject>) -> Unit)? = null
    private var infoDoc: JSONObject? = null

    private val rpcQueue = ArrayDeque<Rpc>()
    private var current: Rpc? = null
    private val writeQueue = ArrayDeque<ByteArray>()
    private var writing = false
    private val assembly = ByteArrayOutputStream()
    private var assemblyTotal = -1
    private var nextId = 1
    private var deadline: Runnable? = null

    val isConnected: Boolean get() = gatt != null && ready

    // ---- scanning ---------------------------------------------------------------
    fun startScan(): String? {
        val a = adapter ?: return "this device has no Bluetooth"
        if (!a.isEnabled) return "Bluetooth is turned off"
        val scanner = a.bluetoothLeScanner ?: return "Bluetooth LE scanning is unavailable"
        stopScan()
        val cb = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                val o = JSObject()
                o.put("address", result.device.address)
                o.put("name", result.scanRecord?.deviceName ?: result.device.name ?: "")
                o.put("rssi", result.rssi)
                emit("dock-found", o)
            }
            override fun onScanFailed(errorCode: Int) {
                Log.w(TAG, "scan failed: $errorCode")
                val o = JSObject(); o.put("error", "scan failed ($errorCode)"); emit("dock-scan", o)
            }
        }
        val filters = listOf(ScanFilter.Builder().setServiceUuid(ParcelUuid(SERVICE)).build())
        val settings = ScanSettings.Builder().setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY).build()
        scanner.startScan(filters, settings, cb)
        scanCallback = cb
        main.postDelayed({ if (scanCallback === cb) { stopScan(); val o = JSObject(); o.put("done", true); emit("dock-scan", o) } }, SCAN_MS)
        return null
    }

    fun stopScan() {
        val cb = scanCallback ?: return
        scanCallback = null
        try { adapter?.bluetoothLeScanner?.stopScan(cb) } catch (e: Exception) { Log.w(TAG, "stopScan: $e") }
    }

    // ---- connection -------------------------------------------------------------
    fun connect(address: String, cb: (Result<JSONObject>) -> Unit) {
        val a = adapter ?: return cb(Result.failure(IllegalStateException("this device has no Bluetooth")))
        if (!a.isEnabled) return cb(Result.failure(IllegalStateException("Bluetooth is turned off")))
        stopScan()
        disconnect()
        val device: BluetoothDevice = try { a.getRemoteDevice(address) } catch (e: IllegalArgumentException) {
            return cb(Result.failure(IllegalArgumentException("invalid dock address")))
        }
        connectCb = cb
        ready = false
        infoDoc = null
        gatt = device.connectGatt(context, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        main.postDelayed({ if (!ready && connectCb != null) fail("the dock did not answer (timeout)") }, CONNECT_TIMEOUT_MS)
        state("connecting")
    }

    fun disconnect() {
        val g = gatt ?: return
        gatt = null
        ready = false
        try { g.disconnect(); g.close() } catch (e: Exception) { Log.w(TAG, "close: $e") }
        failAll("disconnected")
        state("disconnected")
    }

    private fun state(s: String, detail: String? = null) {
        val o = JSObject(); o.put("state", s); if (detail != null) o.put("detail", detail)
        emit("dock-state", o)
    }

    private fun fail(msg: String) {
        val cb = connectCb; connectCb = null
        disconnect()
        cb?.invoke(Result.failure(IllegalStateException(msg)))
    }

    private fun failAll(msg: String) {
        current?.cb?.invoke(Result.failure(IllegalStateException(msg))); current = null
        while (rpcQueue.isNotEmpty()) rpcQueue.poll()?.cb?.invoke(Result.failure(IllegalStateException(msg)))
        writeQueue.clear(); writing = false
        assembly.reset(); assemblyTotal = -1
        deadline?.let { main.removeCallbacks(it) }; deadline = null
    }

    // ---- RPC --------------------------------------------------------------------
    fun rpc(method: String, path: String, bodyJson: String?, auth: String?, cb: (Result<Pair<Int, String>>) -> Unit) {
        if (!isConnected) return cb(Result.failure(IllegalStateException("not connected to a dock")))
        val id = nextId++; if (nextId > 0x7fffffff) nextId = 1
        val sb = StringBuilder("{\"id\":").append(id).append(",\"m\":").append(JSONObject.quote(method)).append(",\"p\":").append(JSONObject.quote(path))
        if (!bodyJson.isNullOrEmpty()) sb.append(",\"b\":").append(bodyJson)
        if (!auth.isNullOrEmpty()) sb.append(",\"auth\":").append(JSONObject.quote(auth))
        sb.append('}')
        val payload = sb.toString().toByteArray(Charsets.UTF_8)
        if (payload.size > 8192) return cb(Result.failure(IllegalArgumentException("request too large")))
        rpcQueue.add(Rpc(id, payload, cb))
        pumpRpc()
    }

    private fun pumpRpc() {
        if (current != null || rpcQueue.isEmpty()) return
        val r = rpcQueue.poll() ?: return
        current = r
        assembly.reset(); assemblyTotal = -1
        val maxPayload = (mtu - 3).coerceAtLeast(20)
        var off = 0; var first = true
        val tag = (r.id and 0xF) shl 4
        while (first || off < r.payload.size) {
            val hl = if (first) 3 else 1
            val room = (maxPayload - hl).coerceAtLeast(1)
            val n = minOf(room, r.payload.size - off)
            val last = off + n >= r.payload.size
            val frag = ByteArray(hl + n)
            frag[0] = ((if (first) FLAG_FIRST else 0) or (if (last) FLAG_LAST else 0) or tag).toByte()
            if (first) { frag[1] = (r.payload.size and 0xFF).toByte(); frag[2] = ((r.payload.size shr 8) and 0xFF).toByte() }
            System.arraycopy(r.payload, off, frag, hl, n)
            writeQueue.add(frag)
            off += n; first = false
        }
        val d = Runnable { if (current === r) { current = null; failAll("the dock did not reply in time") } }
        deadline = d; main.postDelayed(d, RPC_TIMEOUT_MS)
        pumpWrite()
    }

    private fun pumpWrite() {
        if (writing) return
        val g = gatt ?: return
        val c = rx ?: return
        val frag = writeQueue.poll() ?: return
        writing = true
        val ok = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            g.writeCharacteristic(c, frag, BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT) == BluetoothGatt.GATT_SUCCESS
        } else {
            @Suppress("DEPRECATION")
            run { c.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT; c.value = frag; g.writeCharacteristic(c) }
        }
        if (!ok) { writing = false; current = null; failAll("write to the dock failed") }
    }

    private fun onFragment(data: ByteArray) {
        if (data.isEmpty()) return
        val flags = data[0].toInt() and 0xFF
        var off = 1
        if (flags and FLAG_FIRST != 0) {
            if (data.size < 3) return
            assemblyTotal = (data[1].toInt() and 0xFF) or ((data[2].toInt() and 0xFF) shl 8)
            assembly.reset(); off = 3
            if (assemblyTotal > MAX_RESPONSE) { assemblyTotal = -1; return }
        } else if (assemblyTotal < 0) return
        assembly.write(data, off, data.size - off)
        if (flags and FLAG_LAST != 0) {
            val text = assembly.toString("UTF-8")
            assembly.reset(); assemblyTotal = -1
            val r = current ?: return
            try {
                val o = JSONObject(text)
                if (o.optInt("id", -1) != r.id) return // stale reply for an earlier request
                deadline?.let { main.removeCallbacks(it) }; deadline = null
                current = null
                r.cb(Result.success(Pair(o.optInt("s", 500), o.opt("b")?.toString() ?: "null")))
            } catch (e: Exception) {
                deadline?.let { main.removeCallbacks(it) }; deadline = null
                current = null
                r.cb(Result.failure(IllegalStateException("the dock sent an unreadable reply")))
            }
            pumpRpc()
        }
    }

    // ---- GATT callbacks -----------------------------------------------------------
    private val gattCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
            main.post {
                if (g !== gatt) return@post
                if (newState == BluetoothProfile.STATE_CONNECTED && status == BluetoothGatt.GATT_SUCCESS) {
                    state("connected")
                    if (!g.requestMtu(517)) g.discoverServices()
                } else {
                    val why = if (status == BluetoothGatt.GATT_SUCCESS) "disconnected" else "connection lost ($status)"
                    if (connectCb != null) fail(why) else { disconnect() }
                }
            }
        }

        override fun onMtuChanged(g: BluetoothGatt, newMtu: Int, status: Int) {
            main.post {
                if (g !== gatt) return@post
                if (status == BluetoothGatt.GATT_SUCCESS) mtu = newMtu
                g.discoverServices()
            }
        }

        override fun onServicesDiscovered(g: BluetoothGatt, status: Int) {
            main.post {
                if (g !== gatt) return@post
                val svc = g.getService(SERVICE)
                rx = svc?.getCharacteristic(RX); tx = svc?.getCharacteristic(TX); info = svc?.getCharacteristic(INFO)
                if (svc == null || rx == null || tx == null || info == null) { fail("not a RapidRawCloud dock (service missing)"); return@post }
                g.readCharacteristic(info)
            }
        }

        private fun handleRead(g: BluetoothGatt, c: BluetoothGattCharacteristic, value: ByteArray, status: Int) {
            main.post {
                if (g !== gatt) return@post
                if (c.uuid == INFO) {
                    if (status != BluetoothGatt.GATT_SUCCESS) { fail("could not read the dock's identity ($status)"); return@post }
                    infoDoc = try { JSONObject(String(value, Charsets.UTF_8)) } catch (e: Exception) { JSONObject() }
                    val t = tx ?: return@post
                    g.setCharacteristicNotification(t, true)
                    val d = t.getDescriptor(CCCD)
                    if (d == null) { fail("dock reply characteristic has no notification descriptor"); return@post }
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                        g.writeDescriptor(d, BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE)
                    } else {
                        @Suppress("DEPRECATION")
                        run { d.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE; g.writeDescriptor(d) }
                    }
                }
            }
        }

        @Deprecated("pre-33")
        override fun onCharacteristicRead(g: BluetoothGatt, c: BluetoothGattCharacteristic, status: Int) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) handleRead(g, c, c.value ?: ByteArray(0), status)
        }

        override fun onCharacteristicRead(g: BluetoothGatt, c: BluetoothGattCharacteristic, value: ByteArray, status: Int) {
            handleRead(g, c, value, status)
        }

        override fun onDescriptorWrite(g: BluetoothGatt, d: BluetoothGattDescriptor, status: Int) {
            main.post {
                if (g !== gatt) return@post
                if (status != BluetoothGatt.GATT_SUCCESS) { fail("could not subscribe to the dock ($status)"); return@post }
                ready = true
                val cb = connectCb; connectCb = null
                state("ready")
                cb?.invoke(Result.success(infoDoc ?: JSONObject()))
            }
        }

        override fun onCharacteristicWrite(g: BluetoothGatt, c: BluetoothGattCharacteristic, status: Int) {
            main.post {
                if (g !== gatt) return@post
                writing = false
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    // 5 / 15 = insufficient authentication/encryption: Android bonds and the
                    // caller retries; anything else ends the request.
                    val r = current; current = null
                    failAll(if (status == 5 || status == 15 || status == 8) "pairing with the dock is required — accept the pairing request and try again" else "the dock rejected the write ($status)")
                    if (r != null) Log.w(TAG, "write failed status=$status for rpc ${r.id}")
                    return@post
                }
                pumpWrite()
            }
        }

        @Deprecated("pre-33")
        override fun onCharacteristicChanged(g: BluetoothGatt, c: BluetoothGattCharacteristic) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) { val v = c.value ?: return; main.post { if (g === gatt && c.uuid == TX) onFragment(v) } }
        }

        override fun onCharacteristicChanged(g: BluetoothGatt, c: BluetoothGattCharacteristic, value: ByteArray) {
            main.post { if (g === gatt && c.uuid == TX) onFragment(value) }
        }
    }
}
