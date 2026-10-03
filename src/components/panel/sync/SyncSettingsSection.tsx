import { useEffect, useState } from 'react';
import { useSettingsStore } from '../../../store/useSettingsStore';
import { useSyncStore } from '../../../store/useSyncStore';
import { useSyncActions } from '../../../hooks/useSyncActions';
import { SyncSettings } from '../../ui/AppProperties';
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

  useEffect(() => {
    void refreshStatus();
  }, [refreshStatus]);

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

  const credsConfigured = status?.credentialsConfigured ?? false;

  return (
    <div className="flex flex-col gap-6">
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
        <Field
          label="Watched buckets (comma-separated)"
          value={form.watchedMediaBuckets.join(', ')}
          onChange={(v) => patch({ watchedMediaBuckets: v.split(',').map((s) => s.trim()).filter(Boolean) })}
        />
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
