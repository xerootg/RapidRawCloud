import { AlertTriangle } from 'lucide-react';
import { useSyncStore } from '../../../store/useSyncStore';
import { useSyncActions } from '../../../hooks/useSyncActions';

/**
 * Toast stack for `sync-conflict` events (ARCHITECTURE.md §2.6/§3.8). Each
 * conflict offers "Keep winner" or "Keep both" (a loser copy), routed to
 * `sync_resolve_conflict`. Renders nothing when there are no conflicts.
 */
export default function SyncConflictToast() {
  const conflicts = useSyncStore((s) => s.conflicts);
  const dismissConflict = useSyncStore((s) => s.dismissConflict);
  const { resolveConflict } = useSyncActions();

  if (conflicts.length === 0) return null;

  const resolve = async (path: string, keep: 'winner' | 'copy') => {
    await resolveConflict(path, keep);
    dismissConflict(path);
  };

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      {conflicts.map((c) => (
        <div
          key={c.path}
          className="w-80 p-3 rounded-lg bg-surface border border-yellow-400/40 shadow-lg flex flex-col gap-2"
        >
          <div className="flex items-center gap-2 text-yellow-400">
            <AlertTriangle size={16} />
            <span className="font-medium text-text-primary">Sync conflict</span>
          </div>
          <p className="text-text-secondary text-xs truncate" title={c.path}>
            {c.path}
          </p>
          <p className="text-text-secondary text-xs">Edited on another device ({c.winnerDevice}).</p>
          <div className="flex gap-2">
            <button
              className="flex-1 px-2 py-1 rounded-md bg-accent text-button-text text-xs"
              onClick={() => void resolve(c.path, 'winner')}
            >
              Keep winner
            </button>
            <button
              className="flex-1 px-2 py-1 rounded-md bg-bg-primary border border-border-color/40 text-text-primary text-xs"
              onClick={() => void resolve(c.path, 'copy')}
            >
              Keep both
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
