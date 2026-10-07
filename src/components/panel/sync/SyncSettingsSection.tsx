import { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { useSettingsStore } from '../../../store/useSettingsStore';
import { useSyncStore } from '../../../store/useSyncStore';
import { useSyncActions } from '../../../hooks/useSyncActions';
import { Invokes, SyncSettings } from '../../ui/AppProperties';
import DeviceManagementPanel from './DeviceManagementPanel';
import RecentlyDeletedView from './RecentlyDeletedView';

const DEFAULT_SYNC: SyncSettings = {
  enabled: false,
  endpoint: '',
  bucket: '',
  region: '',
  forcePathStyle: true,
  uploadRequiresUnmetered: false,
  uploadRequiresCharging: false,
  cacheSizeGb: 8,
  previewBudgetGb: 10,
  previewPrefetchMonths: 12,
  autoWatchDcim: false,
  watchedMediaBuckets: [],
  workerBackfill: false,
};

/**
 * The "Sync" settings category (ARCHITECTURE.md §3.6/§3.8). Endpoint / bucket /
 * region / enable, the cache + preview budgets, DCIM watch + watched buckets,
 * worker backfill, and a credentials entry that calls `sync_set_credentials`
 * and only ever shows `credentialsConfigured` (the secret is never read back).
 * Also hosts the device-management panel and the Recently Deleted view.
 */
export default function SyncSettingsSection() {
  const appSettings = useSettingsStore((s) => s.appSettings);
  const handleSettingsChange = useSettingsStore((s) => s.handleSettingsChange);
  const status = useSyncStore((s) => s.status);
  const { refreshStatus, configure, setCredentials } = useSyncActions();

  const [form, setForm] = useState<SyncSettings>(appSettings?.sync ?? DEFAULT_SYNC);
  const [accessKey, setAccessKey] = useState('');
  const [secretKey, setSecretKey] = useState('');
  const [savingCreds, setSavingCreds] = useState(false);
  const [pairUrl, setPairUrl] = useState('');
  const [pickerOpen, setPickerOpen] = useState(false);
  const [pickerBuckets, setPickerBuckets] = useState<string[] | null>(null);
  const [pickerManual, setPickerManual] = useState('');
  const [pairState, setPairState] = useState<'idle' | 'waiting' | 'applying' | 'done' | 'error'>(
    'idle',
  );
  const [pairMessage, setPairMessage] = useState('');

  useEffect(() => {
    void refreshStatus();
  }, [refreshStatus]);

  // "Pair with cloud" (docs/CLOUD_SETUP.md §6): the Rust side forwards the
  // rapidraw://auth-callback deep link as this event; completing it fetches
  // and applies the whole sync config + credentials.
  useEffect(() => {
    const un = listen<string>('rrcloud-pair-callback', async (event) => {
      setPairState('applying');
      setPairMessage('Finishing sign-in…');
      try {
        const applied = await invoke<SyncSettings>(Invokes.SyncPairComplete, {
          callbackUrl: event.payload,
        });
        setForm(applied);
        if (appSettings) {
          await handleSettingsChange({ ...appSettings, sync: applied });
        }
        setPairState('done');
        setPairMessage('Paired! Cloud sync is configured.');
        await refreshStatus();
      } catch (err) {
        setPairState('error');
        setPairMessage(String(err));
      }
    });
    return () => {
      void un.then((f) => f());
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appSettings, handleSettingsChange, refreshStatus]);

  const beginPair = async () => {
    setPairState('waiting');
    setPairMessage('Opening your browser to sign in…');
    try {
      await invoke<string>(Invokes.SyncPairBegin, { discoveryUrl: pairUrl });
      setPairMessage('Finish signing in — this screen updates automatically.');
    } catch (err) {
      setPairState('error');
      setPairMessage(String(err));
    }
  };

  useEffect(() => {
    if (appSettings?.sync) setForm(appSettings.sync);
  }, [appSettings?.sync]);

  const patch = (next: Partial<SyncSettings>) => setForm((f) => ({ ...f, ...next }));

  const persist = async () => {
    if (appSettings) {
      await handleSettingsChange({ ...appSettings, sync: form });
    }
    await configure(form);
    await refreshStatus();
  };

  const saveCreds = async () => {
    setSavingCreds(true);
    try {
      await setCredentials(accessKey, secretKey);
      setAccessKey('');
      setSecretKey('');
      await refreshStatus();
    } finally {
      setSavingCreds(false);
    }
  };

  // Per-device DCIM watch list (user decision 2026-10-07: device-local,
  // folder picker — never a synced comma string). "Add folder" lists the
  // device's actual MediaStore buckets via sync_list_media_buckets.
  const openPicker = async () => {
    setPickerOpen(true);
    setPickerBuckets(null);
    try {
      const buckets = await invoke<string[]>(Invokes.SyncListMediaBuckets);
      setPickerBuckets(buckets);
    } catch {
      setPickerBuckets([]);
    }
  };

  const addWatched = (name: string) => {
    const n = name.trim();
    if (!n) return;
    setForm((f) =>
      f.watchedMediaBuckets.includes(n)
        ? f
        : { ...f, watchedMediaBuckets: [...f.watchedMediaBuckets, n] },
    );
    setPickerOpen(false);
    setPickerManual('');
  };

  const removeWatched = (name: string) => {
    setForm((f) => ({
      ...f,
      watchedMediaBuckets: f.watchedMediaBuckets.filter((b) => b !== name),
    }));
  };

  const credsConfigured = status?.credentialsConfigured ?? false;

  return (
    <div className="flex flex-col gap-6">
      <section className="flex flex-col gap-3">
        <h3 className="text-text-primary font-medium">Pair with cloud</h3>
        <p className="text-text-secondary text-sm">
          Have a pairing service? Enter its URL and sign in — endpoint, bucket, and
          credentials are configured for you.
        </p>
        <Field
          label="Pairing service URL"
          value={pairUrl}
          onChange={setPairUrl}
          placeholder="rrc.example.com"
        />
        <button
          className="self-start px-3 py-1.5 rounded-md bg-accent text-button-text disabled:opacity-50"
          disabled={!pairUrl || pairState === 'waiting' || pairState === 'applying'}
          onClick={() => void beginPair()}
        >
          Pair with cloud
        </button>
        {pairMessage && (
          <p
            className={`text-sm ${
              pairState === 'error'
                ? 'text-red-400'
                : pairState === 'done'
                  ? 'text-green-400'
                  : 'text-text-secondary'
            }`}
          >
            {pairMessage}
          </p>
        )}
      </section>

      <section className="flex flex-col gap-3">
        <h3 className="text-text-primary font-medium">Cloud Sync</h3>
        <label className="flex items-center justify-between">
          <span className="text-text-secondary">Enable sync</span>
          <input
            type="checkbox"
            checked={form.enabled}
            onChange={(e) => patch({ enabled: e.target.checked })}
          />
        </label>
        <Field label="Endpoint" value={form.endpoint} onChange={(v) => patch({ endpoint: v })} placeholder="https://garage.example" />
        <Field label="Bucket" value={form.bucket} onChange={(v) => patch({ bucket: v })} />
        <Field label="Region" value={form.region} onChange={(v) => patch({ region: v })} placeholder="garage" />
        <NumField label="Cache budget (GB)" value={form.cacheSizeGb} onChange={(v) => patch({ cacheSizeGb: v })} />
        <NumField label="Preview budget (GB)" value={form.previewBudgetGb} onChange={(v) => patch({ previewBudgetGb: v })} />
        <label className="flex items-center justify-between">
          <span className="text-text-secondary">Auto-watch DCIM</span>
          <input
            type="checkbox"
            checked={form.autoWatchDcim}
            onChange={(e) => patch({ autoWatchDcim: e.target.checked })}
          />
        </label>
        <div className="flex flex-col gap-1">
          <span className="text-text-secondary text-sm">Watched camera-roll folders (this device)</span>
          {form.watchedMediaBuckets.length === 0 && (
            <p className="text-text-secondary text-xs">None — new photos are not auto-imported.</p>
          )}
          {form.watchedMediaBuckets.map((b) => (
            <div key={b} className="flex items-center justify-between px-2 py-1 rounded-md bg-bg-primary border border-border-color/40">
              <span className="text-text-primary text-sm">{b}</span>
              <button
                className="text-text-secondary hover:text-red-400 px-2"
                onClick={() => removeWatched(b)}
                aria-label={`Stop watching ${b}`}
              >
                ✕
              </button>
            </div>
          ))}
          <button
            className="self-start px-3 py-1 rounded-md bg-surface border border-border-color/40 text-text-primary text-sm"
            onClick={() => void openPicker()}
          >
            Add folder…
          </button>
          {pickerOpen && (
            <div className="flex flex-col gap-1 mt-1 p-2 rounded-md bg-bg-primary border border-border-color/40">
              <span className="text-text-secondary text-xs">
                {pickerBuckets === null ? 'Loading folders…' : 'Folders with photos on this device:'}
              </span>
              {(pickerBuckets ?? [])
                .filter((b) => !form.watchedMediaBuckets.includes(b))
                .map((b) => (
                  <button
                    key={b}
                    className="text-left px-2 py-1 rounded hover:bg-surface text-text-primary text-sm"
                    onClick={() => addWatched(b)}
                  >
                    {b}
                  </button>
                ))}
              {pickerBuckets !== null &&
                pickerBuckets.filter((b) => !form.watchedMediaBuckets.includes(b)).length === 0 && (
                  <p className="text-text-secondary text-xs">No other folders found.</p>
                )}
              <div className="flex gap-2 items-center mt-1">
                <input
                  type="text"
                  value={pickerManual}
                  placeholder="Folder name…"
                  onChange={(e) => setPickerManual(e.target.value)}
                  className="flex-1 px-2 py-1 rounded-md bg-surface border border-border-color/40 text-text-primary text-sm"
                />
                <button
                  className="px-2 py-1 rounded-md bg-accent text-button-text text-sm disabled:opacity-50"
                  disabled={!pickerManual.trim()}
                  onClick={() => addWatched(pickerManual)}
                >
                  Add
                </button>
                <button
                  className="px-2 py-1 rounded-md bg-surface border border-border-color/40 text-text-secondary text-sm"
                  onClick={() => {
                    setPickerOpen(false);
                    setPickerManual('');
                  }}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
        </div>
        <label className="flex items-center justify-between">
          <span className="text-text-secondary">Worker backfill (this device)</span>
          <input
            type="checkbox"
            checked={form.workerBackfill}
            onChange={(e) => patch({ workerBackfill: e.target.checked })}
          />
        </label>
        <button className="self-start px-3 py-1.5 rounded-md bg-accent text-button-text" onClick={() => void persist()}>
          Save & reconfigure
        </button>
      </section>

      <section className="flex flex-col gap-3">
        <h3 className="text-text-primary font-medium">Credentials</h3>
        <p className="text-text-secondary text-sm">
          {credsConfigured ? 'Credentials are configured (stored locally, never shown).' : 'No credentials configured.'}
        </p>
        <Field label="Access key" value={accessKey} onChange={setAccessKey} />
        <Field label="Secret key" value={secretKey} onChange={setSecretKey} type="password" />
        <button
          className="self-start px-3 py-1.5 rounded-md bg-surface border border-border-color/40 text-text-primary disabled:opacity-50"
          disabled={savingCreds || !accessKey || !secretKey}
          onClick={() => void saveCreds()}
        >
          Save credentials
        </button>
      </section>

      <DeviceManagementPanel />
      <RecentlyDeletedView />
    </div>
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
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary"
      />
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
