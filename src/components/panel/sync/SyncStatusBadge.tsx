import { useEffect } from 'react';
import { Cloud, CloudOff, RefreshCw, AlertTriangle } from 'lucide-react';
import { useSyncStore } from '../../../store/useSyncStore';
import { useSyncActions } from '../../../hooks/useSyncActions';

/**
 * Cloud-sync status badge for the MainLibrary header (ARCHITECTURE.md §3.8):
 * idle / syncing / offline / error plus pending counts. Renders nothing until
 * the first `sync_status` probe confirms the engine is available (so an
 * upstream `--no-default-features` build shows no sync chrome at all).
 */
export default function SyncStatusBadge() {
  const available = useSyncStore((s) => s.available);
  const status = useSyncStore((s) => s.status);
  const { refreshStatus } = useSyncActions();

  useEffect(() => {
    // Probe once on mount; the `sync-status` listener keeps it live after.
    void refreshStatus();
  }, [refreshStatus]);

  if (available !== true || !status) return null;

  const pending = status.pendingUp + status.pendingDown;
  const { icon, label, tone } = describe(status.state, pending, status.dirtyUnbacked);

  return (
    <div
      className="flex items-center gap-1.5 px-2 py-1 rounded-md bg-surface border border-border-color/30 text-xs select-none"
      title={`up ${status.pendingUp} · down ${status.pendingDown} · ${status.dirtyUnbacked} not backed up`}
    >
      <span className={tone}>{icon}</span>
      <span className="text-text-secondary">{label}</span>
      {pending > 0 && <span className="text-text-primary font-medium">{pending}</span>}
    </div>
  );
}

function describe(state: string, pending: number, dirty: number) {
  switch (state) {
    case 'syncing':
      return { icon: <RefreshCw size={14} className="animate-spin" />, label: 'Syncing', tone: 'text-accent' };
    case 'offline':
      return { icon: <CloudOff size={14} />, label: 'Offline', tone: 'text-text-secondary' };
    case 'error':
      return { icon: <AlertTriangle size={14} />, label: 'Sync error', tone: 'text-red-400' };
    default:
      return {
        icon: <Cloud size={14} />,
        label: dirty > 0 ? 'Pending backup' : 'Synced',
        tone: dirty > 0 ? 'text-yellow-400' : 'text-text-secondary',
      };
  }
}
