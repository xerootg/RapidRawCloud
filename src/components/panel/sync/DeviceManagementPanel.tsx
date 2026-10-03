import { Laptop, Trash2 } from 'lucide-react';
import { useSyncStore } from '../../../store/useSyncStore';
import { useSyncActions } from '../../../hooks/useSyncActions';

/**
 * Device list / retire panel in settings (ARCHITECTURE.md §2.10/§3.8). Lists
 * the shared device registry from `sync_status().peerDevices` and retires a
 * dead device via `sync_retire_device`. The current device cannot retire
 * itself out from under the running engine.
 */
export default function DeviceManagementPanel() {
  const status = useSyncStore((s) => s.status);
  const { retireDevice, refreshStatus } = useSyncActions();

  const devices = status?.peerDevices ?? [];

  const onRetire = async (deviceId: string) => {
    await retireDevice(deviceId);
    await refreshStatus();
  };

  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-text-primary font-medium">Devices</h3>
      {devices.length === 0 ? (
        <p className="text-text-secondary text-sm">No devices registered yet.</p>
      ) : (
        <ul className="flex flex-col gap-1">
          {devices.map((d) => (
            <li
              key={d.deviceId}
              className="flex items-center justify-between px-2 py-1.5 rounded-md bg-surface border border-border-color/30"
            >
              <span className="flex items-center gap-2 text-text-primary">
                <Laptop size={14} />
                <span className="font-mono text-xs">{d.deviceId}</span>
                {d.isSelf && <span className="text-xs text-accent">this device</span>}
                {d.retired && <span className="text-xs text-text-secondary">retired</span>}
              </span>
              {!d.isSelf && !d.retired && (
                <button
                  className="flex items-center gap-1 text-xs text-red-400 hover:text-red-300"
                  onClick={() => void onRetire(d.deviceId)}
                >
                  <Trash2 size={14} /> Retire
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
