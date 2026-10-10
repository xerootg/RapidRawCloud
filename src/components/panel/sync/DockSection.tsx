import { useCallback, useEffect, useRef, useState } from 'react';
import { addPluginListener, invoke, PluginListener } from '@tauri-apps/api/core';
import { open } from '@tauri-apps/plugin-shell';
import { Invokes } from '../../ui/AppProperties';
import { useSettingsStore } from '../../../store/useSettingsStore';

/**
 * Camera ingest dock (firmware/rrcloud-ingest) configured over Bluetooth LE.
 *
 * The dock exposes the same `/api/…` operations as its web UI through one GATT
 * service (protocol `DOCK_BLE_*`); `dock_rpc` carries them. Everything the web
 * page does is here: status, what to sync, cloud (manual S3 or pairing with the
 * cloud through the pairing service), device settings, radio firmware, log.
 * Android only — the plugin rejects the commands elsewhere.
 */

interface DockFound {
  address: string;
  name: string;
  rssi: number;
}

interface DockInfo {
  proto: number;
  device_id: string;
  name: string;
  hostname: string;
  version: string;
}

interface DockRpcReply {
  status: number;
  body: unknown;
}

interface DockSyncStatus {
  phase: string;
  camera_attached: boolean;
  camera_model: string;
  camera_kind: string;
  s3_ready: boolean;
  current_file: string;
  current_done: number;
  current_size: number;
  run_done: number;
  run_skipped: number;
  run_failed: number;
  run_total: number;
  lifetime_uploaded: number;
  lifetime_bytes: number;
  last_run_ts: number;
  last_error: string;
}

interface DockRadio {
  linked: boolean;
  linking: boolean;
  version: string;
  host_lib: string;
  compatible: boolean;
  error: string;
  update: { state: string; message: string };
}

interface DockStatus {
  sync: DockSyncStatus;
  network: string;
  eth_link: boolean;
  coproc?: DockRadio;
}

interface DockPair {
  state: string;
  message?: string;
  user_code?: string;
  verification_uri?: string;
  verification_uri_complete?: string;
}

type DockError = { error?: string };

interface DockConfig {
  s3_endpoint: string;
  s3_bucket: string;
  s3_region: string;
  s3_access_key: string;
  s3_tls_insecure: boolean;
  pairing_url: string;
  include_globs: string;
  exclude_globs: string;
  key_template: string;
  msc_root: string;
  auto_sync: boolean;
  upload_videos: boolean;
  min_size_kb: number;
  device_name: string;
  wifi_ssid: string;
  hostname: string;
  admin_auth: boolean;
  usb_debug: boolean;
  ble_enabled: boolean;
  has_s3_secret: boolean;
  has_wifi_password: boolean;
  has_admin_password: boolean;
  device_id: string;
}

const STR_FIELDS: (keyof DockConfig)[] = [
  'include_globs',
  'exclude_globs',
  'msc_root',
  'key_template',
  's3_endpoint',
  's3_bucket',
  's3_region',
  's3_access_key',
  'device_name',
  'hostname',
  'wifi_ssid',
];
const BOOL_FIELDS: (keyof DockConfig)[] = [
  'auto_sync',
  'upload_videos',
  's3_tls_insecure',
  'admin_auth',
  'usb_debug',
  'ble_enabled',
];

function fmtBytes(n: number | undefined): string {
  const v = n ?? 0;
  if (v < 1024) return `${v} B`;
  if (v < 1048576) return `${(v / 1024).toFixed(1)} KB`;
  if (v < 1073741824) return `${(v / 1048576).toFixed(1)} MB`;
  return `${(v / 1073741824).toFixed(2)} GB`;
}

function fmtTs(ts: number | undefined): string {
  if (!ts) return 'never';
  return new Date(ts * 1000).toLocaleString();
}

class DockAuthRequired extends Error {}

export default function DockSection() {
  const appSettings = useSettingsStore((s) => s.appSettings);
  const [scanning, setScanning] = useState(false);
  const [found, setFound] = useState<DockFound[]>([]);
  const [connecting, setConnecting] = useState<string | null>(null);
  const [info, setInfo] = useState<DockInfo | null>(null);
  const [status, setStatus] = useState<DockStatus | null>(null);
  const [config, setConfig] = useState<DockConfig | null>(null);
  const [form, setForm] = useState<Partial<DockConfig> & { s3_secret_key?: string; wifi_password?: string; admin_password?: string }>({});
  const [auth, setAuth] = useState('');
  const [needAuth, setNeedAuth] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [pairUrl, setPairUrl] = useState('');
  const [pair, setPair] = useState<DockPair | null>(null);
  const [log, setLog] = useState<string[]>([]);
  const [showLog, setShowLog] = useState(false);
  const authRef = useRef(auth);
  authRef.current = auth;
  const connected = info !== null;

  const say = (m: string) => {
    setMessage(m);
    setError('');
  };
  const fail = (e: unknown) => {
    setError(String(e instanceof Error ? e.message : e));
  };

  // One request to the dock; a 401 asks for the dock's admin password once.
  const rpc = useCallback(async <T,>(method: 'GET' | 'POST', path: string, body?: unknown): Promise<T> => {
    const reply = await invoke<DockRpcReply>(Invokes.DockRpc, {
      method,
      path,
      body: body ?? null,
      auth: authRef.current || null,
    });
    if (reply.status === 401) {
      setNeedAuth(true);
      throw new DockAuthRequired('the dock requires its admin password');
    }
    if (reply.status >= 400) {
      throw new Error((reply.body as DockError | null)?.error ?? `dock answered ${reply.status}`);
    }
    return reply.body as T;
  }, []);

  // ---- scan / connect -------------------------------------------------------
  useEffect(() => {
    let found$: PluginListener | undefined;
    let scan$: PluginListener | undefined;
    let state$: PluginListener | undefined;
    void (async () => {
      try {
        found$ = await addPluginListener<DockFound>('rrcloud', 'dock-found', (d) => {
          setFound((list) => {
            const i = list.findIndex((x) => x.address === d.address);
            if (i >= 0) {
              const next = [...list];
              next[i] = d;
              return next;
            }
            return [...list, d];
          });
        });
        scan$ = await addPluginListener<{ done?: boolean; error?: string }>('rrcloud', 'dock-scan', (d) => {
          setScanning(false);
          if (d.error) setError(d.error);
        });
        state$ = await addPluginListener<{ state: string; detail?: string }>('rrcloud', 'dock-state', (d) => {
          if (d.state === 'disconnected') {
            setInfo(null);
            setStatus(null);
            setConfig(null);
            setPair(null);
          }
        });
      } catch {
        /* not the Android build: the section stays usable for scanning errors */
      }
    })();
    return () => {
      void found$?.unregister();
      void scan$?.unregister();
      void state$?.unregister();
    };
  }, []);

  const startScan = async () => {
    setFound([]);
    setError('');
    try {
      await invoke(Invokes.DockScanStart);
      setScanning(true);
      say('Looking for docks nearby…');
    } catch (e) {
      fail(e);
    }
  };

  const loadConfig = useCallback(async () => {
    const c = await rpc<DockConfig>('GET', '/api/config');
    setConfig(c);
    const next: Record<string, string | number | boolean> = {};
    STR_FIELDS.forEach((f) => (next[f] = String(c[f] ?? '')));
    BOOL_FIELDS.forEach((f) => (next[f] = !!c[f]));
    next.min_size_kb = c.min_size_kb ?? 0;
    setForm(next as Partial<DockConfig>);
    setPairUrl((u) => u || c.pairing_url || appSettings?.sync?.endpoint?.replace(/^https?:\/\/garage\./, 'https://rrc.') || '');
  }, [rpc, appSettings?.sync?.endpoint]);

  const connect = async (address: string) => {
    setConnecting(address);
    setError('');
    try {
      await invoke(Invokes.DockScanStop).catch(() => undefined);
      setScanning(false);
      const i = await invoke<DockInfo>(Invokes.DockConnect, { address });
      setInfo(i);
      say(`Connected to ${i.hostname} (firmware ${i.version})`);
      try {
        await loadConfig();
      } catch (e) {
        if (!(e instanceof DockAuthRequired)) fail(e);
      }
    } catch (e) {
      fail(e);
    } finally {
      setConnecting(null);
    }
  };

  const disconnect = async () => {
    await invoke(Invokes.DockDisconnect).catch(() => undefined);
    setInfo(null);
    setStatus(null);
    setConfig(null);
    setPair(null);
    say('Disconnected');
  };

  // ---- status poll ------------------------------------------------------------
  useEffect(() => {
    if (!connected) return;
    let stop = false;
    const tick = async () => {
      try {
        const s = await rpc<DockStatus>('GET', '/api/status');
        if (!stop) setStatus(s);
        if (pair && pair.state !== 'done' && pair.state !== 'failed' && pair.state !== 'idle') {
          const p = await rpc<DockPair>('GET', '/api/pair/status');
          if (!stop) setPair(p);
          if (p.state === 'done') {
            say('Dock paired with the cloud');
            await loadConfig().catch(() => undefined);
          }
        }
        if (showLog) {
          const l = await rpc<string[]>('GET', '/api/log');
          if (!stop && Array.isArray(l)) setLog(l);
        }
      } catch (e) {
        if (!(e instanceof DockAuthRequired) && !stop) setError(String(e instanceof Error ? e.message : e));
      }
    };
    void tick();
    const id = setInterval(() => void tick(), 2500);
    return () => {
      stop = true;
      clearInterval(id);
    };
  }, [connected, rpc, showLog, pair, auth, loadConfig]);

  // ---- actions ------------------------------------------------------------------
  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(true);
    setError('');
    try {
      await fn();
      say(label);
    } catch (e) {
      if (!(e instanceof DockAuthRequired)) fail(e);
    } finally {
      setBusy(false);
    }
  };

  const saveConfig = () =>
    run('Saved to the dock', async () => {
      const patch: Record<string, unknown> = {};
      const f = form as Record<string, unknown>;
      STR_FIELDS.forEach((k) => (patch[k] = f[k] ?? ''));
      BOOL_FIELDS.forEach((k) => (patch[k] = !!f[k]));
      patch.min_size_kb = form.min_size_kb ?? 0;
      if (form.s3_secret_key) patch.s3_secret_key = form.s3_secret_key;
      if (form.wifi_password) patch.wifi_password = form.wifi_password;
      if (form.admin_password) patch.admin_password = form.admin_password;
      await rpc('POST', '/api/config', patch);
      setForm((f) => ({ ...f, s3_secret_key: '', wifi_password: '', admin_password: '' }));
      await loadConfig();
    });

  const useAppCloud = () => {
    const s = appSettings?.sync;
    if (!s) return;
    setForm((f) => ({ ...f, s3_endpoint: s.endpoint, s3_bucket: s.bucket, s3_region: s.region }));
    say('Endpoint, bucket and region copied from this phone; enter the access key and secret, then save');
  };

  const beginPair = () =>
    run('Pairing started', async () => {
      await rpc('POST', '/api/pair/begin', { url: pairUrl });
      setPair({ state: 'discovering', message: 'contacting the pairing service…' });
    });

  const cancelPair = () =>
    run('Pairing cancelled', async () => {
      await rpc('POST', '/api/pair/cancel', {});
      setPair(null);
    });

  const patchForm = (next: Partial<typeof form>) => setForm((f) => ({ ...f, ...next }));

  const y = status?.sync;
  const active = y && (y.phase === 'uploading' || y.phase === 'enumerating' || y.phase === 'publishing');
  const radio = status?.coproc;

  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-text-primary font-medium">Camera dock (Bluetooth)</h3>
      <p className="text-text-secondary text-sm">
        Set up a RapidRawCloud camera dock without a network: what it uploads, which cloud it
        uploads to, Wi‑Fi, and its radio firmware.
      </p>

      {!connected && (
        <div className="flex flex-col gap-2">
          <button
            className="self-start px-3 py-1.5 rounded-md bg-accent text-button-text disabled:opacity-50"
            disabled={scanning}
            onClick={() => void startScan()}
          >
            {scanning ? 'Scanning…' : 'Find docks'}
          </button>
          {found.map((d) => (
            <div key={d.address} className="flex items-center justify-between gap-2 px-2 py-1 rounded-md bg-bg-primary border border-border-color/40">
              <span className="text-text-primary text-sm">
                {d.name || 'dock'} <span className="text-text-secondary">· {d.address} · {d.rssi} dBm</span>
              </span>
              <button
                className="px-2 py-1 rounded-md bg-accent text-button-text text-sm disabled:opacity-50"
                disabled={connecting !== null}
                onClick={() => void connect(d.address)}
              >
                {connecting === d.address ? 'Connecting…' : 'Connect'}
              </button>
            </div>
          ))}
        </div>
      )}

      {connected && info && (
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <span className="text-text-primary text-sm">
              {info.hostname} · firmware {info.version}
            </span>
            <button className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm" onClick={() => void disconnect()}>
              Disconnect
            </button>
          </div>

          {needAuth && (
            <div className="flex flex-col gap-1">
              <span className="text-text-secondary text-sm">This dock requires its admin password</span>
              <input
                type="password"
                value={auth}
                placeholder="admin password"
                onChange={(e) => {
                  setAuth(e.target.value);
                  setNeedAuth(false);
                }}
                className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary"
              />
            </div>
          )}

          {y && (
            <div className="flex flex-col gap-1 text-sm px-2 py-2 rounded-md bg-bg-primary border border-border-color/40">
              <div className="flex justify-between">
                <span className="text-text-secondary">Status</span>
                <span className="text-text-primary">{y.phase}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-secondary">Camera</span>
                <span className="text-text-primary">
                  {y.camera_attached ? `${y.camera_model} (${String(y.camera_kind).toUpperCase()})` : 'none attached'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-secondary">Cloud</span>
                <span className="text-text-primary">{y.s3_ready ? 'configured' : 'not configured'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-secondary">Network</span>
                <span className="text-text-primary">{status.network || 'no address'}{status.eth_link ? '' : ' (no ethernet link)'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-secondary">Uploaded</span>
                <span className="text-text-primary">{y.lifetime_uploaded} files · {fmtBytes(y.lifetime_bytes)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-secondary">Last run</span>
                <span className="text-text-primary">{fmtTs(y.last_run_ts)}</span>
              </div>
              {active && (
                <div className="text-text-secondary">
                  {y.current_file ? `${y.current_file} — ${fmtBytes(y.current_done)} / ${fmtBytes(y.current_size)}` : 'scanning camera…'}
                  {' · '}
                  {y.run_done} uploaded · {y.run_skipped} skipped · {y.run_failed} failed · {y.run_total} queued
                </div>
              )}
              {y.last_error && <div className="text-red-400">{y.last_error}</div>}
              <div className="flex gap-2 pt-1">
                <button
                  className="px-2 py-1 rounded-md bg-accent text-button-text text-sm disabled:opacity-50"
                  disabled={busy || !y.camera_attached || active}
                  onClick={() => void run('Sync requested', () => rpc('POST', '/api/sync/now', {}))}
                >
                  Sync now
                </button>
                <button
                  className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
                  disabled={busy || !active}
                  onClick={() => void run('Cancelling', () => rpc('POST', '/api/sync/cancel', {}))}
                >
                  Cancel
                </button>
                <button
                  className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
                  disabled={busy}
                  onClick={() => void run('USB port power-cycled', () => rpc('POST', '/api/usb/reset', {}))}
                >
                  Reset USB
                </button>
              </div>
            </div>
          )}

          {config && (
            <>
              <h4 className="text-text-primary text-sm font-medium pt-1">Pair the dock with the cloud</h4>
              <Field label="Pairing service URL" value={pairUrl} onChange={setPairUrl} placeholder="https://rrc.example.com" />
              {!pair || pair.state === 'idle' || pair.state === 'done' || pair.state === 'failed' ? (
                <button
                  className="self-start px-3 py-1.5 rounded-md bg-accent text-button-text disabled:opacity-50"
                  disabled={busy || !pairUrl}
                  onClick={() => void beginPair()}
                >
                  Pair with cloud
                </button>
              ) : (
                <div className="flex flex-col gap-1 text-sm">
                  <span className="text-text-secondary">{pair.message || pair.state}</span>
                  {pair.user_code && (
                    <>
                      <span className="text-text-primary">
                        Sign in and enter code <code className="font-mono">{pair.user_code}</code>
                      </span>
                      <div className="flex gap-2">
                        <button
                          className="px-2 py-1 rounded-md bg-accent text-button-text text-sm"
                          onClick={() => void open(pair.verification_uri_complete || pair.verification_uri || '')}
                        >
                          Open sign-in
                        </button>
                        <button
                          className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm"
                          onClick={() => void cancelPair()}
                        >
                          Cancel
                        </button>
                      </div>
                    </>
                  )}
                </div>
              )}
              {pair?.state === 'failed' && <p className="text-sm text-red-400">{pair.message}</p>}

              <h4 className="text-text-primary text-sm font-medium pt-1">Cloud (manual)</h4>
              <button
                className="self-start px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
                disabled={!appSettings?.sync?.endpoint}
                onClick={useAppCloud}
              >
                Use this phone's endpoint and bucket
              </button>
              <Field label="S3 endpoint" value={form.s3_endpoint ?? ''} onChange={(v) => patchForm({ s3_endpoint: v })} placeholder="https://garage.example.com" />
              <Field label="Bucket" value={form.s3_bucket ?? ''} onChange={(v) => patchForm({ s3_bucket: v })} />
              <Field label="Region" value={form.s3_region ?? ''} onChange={(v) => patchForm({ s3_region: v })} />
              <Field label="Access key ID" value={form.s3_access_key ?? ''} onChange={(v) => patchForm({ s3_access_key: v })} />
              <Field
                label={`Secret access key ${config.has_s3_secret ? '(stored; leave blank to keep)' : '(not set)'}`}
                value={form.s3_secret_key ?? ''}
                onChange={(v) => patchForm({ s3_secret_key: v })}
                type="password"
              />
              <Check label="Skip TLS certificate verification (self-signed endpoint)" value={!!form.s3_tls_insecure} onChange={(v) => patchForm({ s3_tls_insecure: v })} />

              <h4 className="text-text-primary text-sm font-medium pt-1">What to sync</h4>
              <Field label="Include patterns" value={form.include_globs ?? ''} onChange={(v) => patchForm({ include_globs: v })} />
              <Field label="Exclude patterns" value={form.exclude_globs ?? ''} onChange={(v) => patchForm({ exclude_globs: v })} />
              <Field label="Library path template" value={form.key_template ?? ''} onChange={(v) => patchForm({ key_template: v })} />
              <Field label="Mass-storage scan folder" value={form.msc_root ?? ''} onChange={(v) => patchForm({ msc_root: v })} placeholder="DCIM" />
              <Check label="Sync automatically when a camera is plugged in" value={!!form.auto_sync} onChange={(v) => patchForm({ auto_sync: v })} />
              <Check label="Also upload videos (*.mov *.mp4)" value={!!form.upload_videos} onChange={(v) => patchForm({ upload_videos: v })} />
              <NumField label="Minimum file size (KB)" value={form.min_size_kb ?? 0} onChange={(v) => patchForm({ min_size_kb: v })} />

              <h4 className="text-text-primary text-sm font-medium pt-1">Device</h4>
              <Field label="Name in the device registry" value={form.device_name ?? ''} onChange={(v) => patchForm({ device_name: v })} />
              <Field label="Hostname" value={form.hostname ?? ''} onChange={(v) => patchForm({ hostname: v })} />
              <Field label="Wi‑Fi SSID (optional; Ethernet is always on)" value={form.wifi_ssid ?? ''} onChange={(v) => patchForm({ wifi_ssid: v })} />
              <Field
                label={`Wi‑Fi password ${config.has_wifi_password ? '(stored; leave blank to keep)' : ''}`}
                value={form.wifi_password ?? ''}
                onChange={(v) => patchForm({ wifi_password: v })}
                type="password"
              />
              <Check label="Require a password for the dock's admin page and app access" value={!!form.admin_auth} onChange={(v) => patchForm({ admin_auth: v })} />
              <Field
                label={`Admin password ${config.has_admin_password ? '(stored; leave blank to keep)' : ''}`}
                value={form.admin_password ?? ''}
                onChange={(v) => patchForm({ admin_password: v })}
                type="password"
              />
              <Check label="Bluetooth setup from this app" value={!!form.ble_enabled} onChange={(v) => patchForm({ ble_enabled: v })} />
              <Check label="Verbose USB logging on the dock's console" value={!!form.usb_debug} onChange={(v) => patchForm({ usb_debug: v })} />
              <div className="flex gap-2">
                <button className="px-3 py-1.5 rounded-md bg-accent text-button-text disabled:opacity-50" disabled={busy} onClick={() => void saveConfig()}>
                  Save to dock
                </button>
                <button
                  className="px-3 py-1.5 rounded-md bg-bg-primary border border-border-color/40 text-text-primary disabled:opacity-50"
                  disabled={busy}
                  onClick={() => void run('Dock is rebooting', () => rpc('POST', '/api/reboot', {}))}
                >
                  Reboot dock
                </button>
              </div>

              {radio && (
                <>
                  <h4 className="text-text-primary text-sm font-medium pt-1">Radio co-processor</h4>
                  <p className="text-text-secondary text-sm">
                    {radio.linked
                      ? `ESP32-C6 running ESP-Hosted ${radio.version}${radio.compatible ? ' · compatible' : ` · needs ${radio.host_lib} — update it`}`
                      : radio.linking
                        ? 'linking over SDIO…'
                        : `not linked${radio.error ? ' — ' + radio.error : ''}`}
                    {radio.update?.state && radio.update.state !== 'idle' && ` · update ${radio.update.state}: ${radio.update.message}`}
                  </p>
                  <div className="flex gap-2">
                    <button
                      className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
                      disabled={busy || !radio.linked || radio.update?.state === 'running'}
                      onClick={() => void run('Radio update started; the dock restarts when done', () => rpc('POST', '/api/coproc/update', {}))}
                    >
                      Update radio firmware
                    </button>
                    <button
                      className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-sm disabled:opacity-50"
                      disabled={busy}
                      onClick={() => void run('Paired phones forgotten', () => rpc('POST', '/api/ble/forget', {}))}
                    >
                      Forget paired phones
                    </button>
                  </div>
                </>
              )}

              <button className="self-start text-text-secondary text-sm underline" onClick={() => setShowLog((v) => !v)}>
                {showLog ? 'Hide dock log' : 'Show dock log'}
              </button>
              {showLog && (
                <pre className="text-xs text-text-secondary whitespace-pre-wrap max-h-64 overflow-auto px-2 py-1 rounded-md bg-bg-primary border border-border-color/40">
                  {log.join('\n')}
                </pre>
              )}
            </>
          )}
        </div>
      )}

      {message && !error && <p className="text-sm text-text-secondary">{message}</p>}
      {error && <p className="text-sm text-red-400">{error}</p>}
    </section>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  type = 'text',
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-text-secondary text-sm">{label}</span>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        autoComplete="off"
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary"
      />
    </label>
  );
}

function Check({ label, value, onChange }: { label: string; value: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="flex items-center justify-between gap-2">
      <span className="text-text-secondary text-sm">{label}</span>
      <input type="checkbox" checked={value} onChange={(e) => onChange(e.target.checked)} />
    </label>
  );
}

function NumField({ label, value, onChange }: { label: string; value: number; onChange: (v: number) => void }) {
  return (
    <label className="flex items-center justify-between gap-2">
      <span className="text-text-secondary text-sm">{label}</span>
      <input
        type="number"
        value={value}
        min={0}
        onChange={(e) => onChange(Number(e.target.value) || 0)}
        className="w-24 px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary"
      />
    </label>
  );
}
