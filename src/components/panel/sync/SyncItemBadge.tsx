import { Cloud, CloudOff, UploadCloud, DownloadCloud, AlertTriangle } from 'lucide-react';
import { useSyncStore } from '../../../store/useSyncStore';

/**
 * Per-item cloud-state badge for the library grid (ARCHITECTURE.md §3.8). Fed
 * by the batched `sync-item-state` map (falling back to the image's own
 * `sync_state` field from the P2 listing). Shows a CloudOff stub indicator for
 * a 0-byte cloud stub, an up/down arrow while transferring, and nothing for a
 * fully-local item.
 */
export default function SyncItemBadge({ path, fallbackState }: { path: string; fallbackState?: string | null }) {
  const live = useSyncStore((s) => s.itemStates[path]);
  const state = live ?? fallbackState ?? null;
  if (!state) return null;

  switch (state) {
    case 'stub':
      return <CloudOff size={14} className="text-text-secondary" aria-label="cloud stub" />;
    case 'pending_up':
      return <UploadCloud size={14} className="text-accent" aria-label="uploading" />;
    case 'pending_down':
      return <DownloadCloud size={14} className="text-accent" aria-label="downloading" />;
    case 'corrupt_remote':
      return <AlertTriangle size={14} className="text-red-400" aria-label="remote copy damaged" />;
    case 'synced':
    case 'hydrated':
      return <Cloud size={14} className="text-text-secondary/60" aria-label="synced" />;
    default:
      return null;
  }
}
