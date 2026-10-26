import { useCallback, useEffect, useRef, useState } from 'react';
import { fetch } from '@tauri-apps/plugin-http';

/**
 * The dock's own firmware (firmware/rrcloud-ingest main/ota.c), over the same Bluetooth
 * RPC as the rest of DockSection:
 *   - the dock can check and install by itself when it has internet (`/api/ota/check`,
 *     `/api/ota/install`);
 *   - or this phone downloads the image (the dock's channel: the rolling `firmware-latest`
 *     release, or `firmware-dev` test builds), checks its SHA-256 and sends it in
 *     `/api/ota/upload/*` chunks — for a dock with no internet of its own.
 * The dock refuses an image built for other hardware or another chip revision, and a new
 * firmware starts on trial: it rolls back by itself if it does not come up cleanly.
 */

const REPO = 'xerootg/RapidRawCloud';
const CHUNK = 4096; // the dock's OTA_UPLOAD_CHUNK; base64 keeps a request under its 8 KiB limit

export interface DockOta {
  running: string;
  built: string;
  hardware: string;
  variant: string;
  chip_rev: number;
  channel: 'release' | 'dev';
  state: string;
  message: string;
  done: number;
  total: number;
  upload_sha256: string;
  trial: boolean;
  rollback: boolean;
  rolled_back: string;
  checked: boolean;
  available: { version: string; tag: string; commit: string; channel: string; size: number; newer: boolean } | null;
}

interface ManifestImage {
  variant: string;
  file: string;
  size: number;
  sha256: string;
  chip_rev_min?: number;
  chip_rev_max?: number;
}

interface Manifest {
  version: string;
  tag: string;
  commit: string;
  images: ManifestImage[];
}

interface Sending {
  done: number;
  from: number; // where the current stretch started (after a resume, not 0): for the rate
  total: number;
  started: number;
  version: string;
  reconnects: number;
}

type Rpc = <T>(method: 'GET' | 'POST', path: string, body?: unknown) => Promise<T>;

const BUSY = ['checking', 'downloading', 'receiving', 'verifying', 'writing', 'restarting'];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function fmtMB(n: number): string {
  return `${(n / 1048576).toFixed(2)} MB`;
}

function toBase64(bytes: Uint8Array): string {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

async function sha256Hex(bytes: Uint8Array<ArrayBuffer>): Promise<string> {
  const d = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return [...d].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export default function DockFirmware({
  rpc,
  connected,
  reconnect,
  onSending,
  onError,
  onMessage,
}: {
  rpc: Rpc;
  connected: boolean;
  /** Reconnects to the dock this panel last talked to; false when it is not reachable. */
  reconnect: () => Promise<boolean>;
  /** DockSection keeps this panel mounted across a dropped link while a transfer runs. */
  onSending: (active: boolean) => void;
  onError: (e: unknown) => void;
  onMessage: (m: string) => void;
}) {
  const [ota, setOta] = useState<DockOta | null>(null);
  const [sending, setSending] = useState<Sending | null>(null);
  const cancelRef = useRef(false);

  const refresh = useCallback(async () => {
    try {
      setOta(await rpc<DockOta>('GET', '/api/ota'));
    } catch {
      /* the status poll in DockSection reports connection errors */
    }
  }, [rpc]);

  useEffect(() => {
    if (!connected) return;
    void refresh();
    const id = setInterval(() => void refresh(), sending ? 10000 : 3000);
    return () => clearInterval(id);
  }, [connected, refresh, sending]);

  const setChannel = async (dev: boolean) => {
    try {
      await rpc('POST', '/api/config', { ota_dev_channel: dev });
      await refresh();
    } catch (e) {
      onError(e);
    }
  };

  const dockCheck = async () => {
    try {
      await rpc('POST', '/api/ota/check', {});
      onMessage('The dock is checking for an update');
      await refresh();
    } catch (e) {
      onError(e);
    }
  };

  const dockInstall = async () => {
    try {
      await rpc('POST', '/api/ota/install', {});
      onMessage('The dock is downloading the update; it restarts into it');
      await refresh();
    } catch (e) {
      onError(e);
    }
  };

  // The phone does the download: for a dock without internet of its own.
  const sendFromPhone = async () => {
    if (!ota) return;
    cancelRef.current = false;
    try {
      const tag = ota.channel === 'dev' ? 'firmware-dev' : 'firmware-latest';
      const base = `https://github.com/${REPO}/releases/download`;
      const mr = await fetch(`${base}/${tag}/rrcloud-ingest-ota.json`);
      if (!mr.ok)
        throw new Error(`no ${ota.channel === 'dev' ? 'test build' : 'release'} manifest (HTTP ${mr.status})`);
      const m = (await mr.json()) as Manifest;
      const img = m.images.find(
        (i) =>
          i.variant === ota.variant &&
          (i.chip_rev_min === undefined || ota.chip_rev >= i.chip_rev_min) &&
          (i.chip_rev_max === undefined || ota.chip_rev <= i.chip_rev_max),
      );
      if (!img)
        throw new Error(
          `${m.version} has no image for ${ota.variant} on chip v${Math.floor(ota.chip_rev / 100)}.${ota.chip_rev % 100}`,
        );
      if (m.version === ota.running && !window.confirm(`The dock already runs ${m.version}. Send it again?`)) return;
      if (
        !window.confirm(
          `Send ${m.version} (${fmtMB(img.size)}) to the dock over Bluetooth? This takes several minutes; the dock restarts into it.`,
        )
      )
        return;
      // the versioned release, not the rolling one: it cannot change under the download
      const release = ota.channel === 'dev' || !m.tag ? tag : m.tag;
      // from here on the panel stays mounted (a dropped link must not orphan the transfer)
      // and the buttons stay disabled
      onSending(true);
      setSending({ done: 0, from: 0, total: img.size, started: Date.now(), version: m.version, reconnects: 0 });
      onMessage(`Downloading ${m.version}…`);
      const ir = await fetch(`${base}/${release}/${img.file}`);
      if (!ir.ok) throw new Error(`download failed (HTTP ${ir.status})`);
      const bytes = new Uint8Array(await ir.arrayBuffer());
      if (bytes.length !== img.size)
        throw new Error(`download is ${bytes.length} bytes, the manifest says ${img.size}`);
      const sha = await sha256Hex(bytes);
      if (sha !== img.sha256) throw new Error('download does not match the SHA-256 in the manifest');

      // Where the dock is: an upload a dropped link interrupted is still held there (for
      // two minutes) and carries on from its offset instead of starting over.
      const resumeAt = async (): Promise<number | null> => {
        const now = await rpc<DockOta>('GET', '/api/ota');
        // only this image: another one of the same size would fail the dock's SHA-256 check at the end
        return now.state === 'receiving' && now.total === bytes.length && now.upload_sha256 === sha ? now.done : null;
      };
      let off = (await resumeAt()) ?? -1;
      if (off < 0) {
        await rpc('POST', '/api/ota/upload/begin', { size: bytes.length, sha256: sha });
        off = 0;
      }
      let started = Date.now();
      let from = off;
      let reconnects = 0;
      let stalled = 0; // reconnects in a row that brought no progress
      setSending({ done: off, from, total: bytes.length, started, version: m.version, reconnects });
      while (off < bytes.length) {
        if (cancelRef.current) throw new Error('cancelled');
        const body = { off, data: toBase64(bytes.subarray(off, Math.min(off + CHUNK, bytes.length))) };
        try {
          // a request lost in flight is sent again: the dock ignores a chunk it already has
          const r = await rpc<{ expected: number }>('POST', '/api/ota/upload/chunk', body).catch(() =>
            rpc<{ expected: number }>('POST', '/api/ota/upload/chunk', body),
          );
          off = r.expected;
        } catch (e) {
          // The Bluetooth link to the dock drops every few minutes under sustained traffic
          // (its radio sits behind the ESP32-C6's hosted HCI): reconnect and resume.
          let back = false;
          for (let i = 0; i < 6 && !back && !cancelRef.current; i++) {
            await sleep(3000);
            back = await reconnect();
          }
          const at = back ? await resumeAt().catch(() => null) : null;
          if (at === null) throw e;
          stalled = at > off ? 0 : stalled + 1;
          if (stalled >= 3)
            throw new Error('the dock stopped taking the image (three reconnects without progress)', { cause: e });
          off = from = at;
          started = Date.now();
          reconnects++;
        }
        setSending({ done: off, from, total: bytes.length, started, version: m.version, reconnects });
      }
      await rpc('POST', '/api/ota/upload/finish', {});
      onMessage(`Sent ${m.version}; the dock checks it and restarts into it`);
    } catch (e) {
      onError(e);
      // a cancel throws away what the dock received; a lost link keeps it for a resume
      if (cancelRef.current) await rpc('POST', '/api/ota/cancel', {}).catch(() => undefined);
    } finally {
      setSending(null);
      onSending(false);
      void refresh();
    }
  };

  if (!ota) return null;
  const busy = BUSY.includes(ota.state) || sending !== null;
  // an upload the dock is still holding (the link dropped) can be resumed from this phone
  const canSend = connected && sending === null && (!BUSY.includes(ota.state) || ota.state === 'receiving');
  const sent = sending ? sending.done - sending.from : 0;
  const eta =
    sending && sent > 0
      ? Math.round((((Date.now() - sending.started) / sent) * (sending.total - sending.done)) / 1000)
      : 0;

  return (
    <>
      <h4 className="text-text-primary text-sm font-medium pt-1">Dock firmware</h4>
      <p className="text-text-secondary text-sm">
        {ota.running} · {ota.variant} · chip v{Math.floor(ota.chip_rev / 100)}.{ota.chip_rev % 100}
        {ota.trial && ' · on trial (kept after a minute if the dock can be reached)'}
        {ota.rolled_back && ` · ${ota.rolled_back} did not start cleanly and was rolled back`}
        {!ota.rollback && " · this dock's bootloader cannot roll back a bad update (flash it over USB once)"}
      </p>
      {ota.available && (
        <p className="text-text-secondary text-sm">
          The dock found {ota.available.channel === 'dev' ? 'test build' : 'release'} {ota.available.version}
          {ota.available.newer ? '' : ' (already running)'}
        </p>
      )}
      {(ota.state !== 'idle' || ota.message) && !sending && (
        <p className={`text-sm ${ota.state === 'failed' ? 'text-red-400' : 'text-text-secondary'}`}>
          {ota.state}
          {ota.message && `: ${ota.message}`}
          {ota.total > 0 && BUSY.includes(ota.state) && ` (${Math.round((100 * ota.done) / ota.total)} %)`}
        </p>
      )}
      {sending && (
        <div className="flex flex-col gap-1 text-sm">
          <progress max={sending.total} value={sending.done} />
          <span className="text-text-secondary">
            Sending {sending.version}: {fmtMB(sending.done)} of {fmtMB(sending.total)}
            {eta > 0 && ` · about ${Math.ceil(eta / 60)} min left`}
            {sending.reconnects > 0 && ` · reconnected ${sending.reconnects}×`}
          </span>
        </div>
      )}
      <label className="flex items-center justify-between gap-2">
        <span className="text-text-secondary text-sm">Test builds (the rolling firmware-dev prerelease)</span>
        <input
          type="checkbox"
          checked={ota.channel === 'dev'}
          disabled={busy || !connected}
          onChange={(e) => void setChannel(e.target.checked)}
        />
      </label>
      <div className="flex flex-wrap gap-2">
        <button
          className="px-2 py-1 rounded-md bg-accent text-button-text text-sm disabled:opacity-50"
          disabled={!canSend}
          onClick={() => void sendFromPhone()}
        >
          {ota.state === 'receiving' && !sending ? 'Resume from this phone' : 'Update from this phone'}
        </button>
        <button
          className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
          disabled={busy || !connected}
          onClick={() => void dockCheck()}
        >
          Dock: check online
        </button>
        <button
          className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
          disabled={busy || !connected || !ota.available}
          onClick={() => void dockInstall()}
        >
          Dock: install
        </button>
        {sending && (
          <button
            className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm"
            onClick={() => {
              cancelRef.current = true;
            }}
          >
            Cancel
          </button>
        )}
      </div>
    </>
  );
}
