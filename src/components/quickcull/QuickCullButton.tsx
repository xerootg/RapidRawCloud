import { useMemo } from 'react';
import { Eye, EyeOff, Layers } from 'lucide-react';
import { useLibraryStore } from '../../store/useLibraryStore';
import { useQuickCullStore } from '../../store/useQuickCullStore';
import { FlagStatus } from '../../types/library';
import { undecidedQueue } from './QuickCullOverlay';

/**
 * Library-header entry point for Quick Cull: shows how many images in the current view are still undecided and opens
 * the overlay on them, newest first. The eye toggle flips upstream's own "exclude rejected" filter, which is how
 * archived (Reject) images disappear from the grid — no fork filter code.
 */
export default function QuickCullButton() {
  const imageList = useLibraryStore((s) => s.imageList);
  const imageRatings = useLibraryStore((s) => s.imageRatings);
  const flagStatus = useLibraryStore((s) => s.filterCriteria.flagStatus);
  const setFilterCriteria = useLibraryStore((s) => s.setFilterCriteria);
  const open = useQuickCullStore((s) => s.open);

  const queue = useMemo(() => undecidedQueue(imageList, imageRatings), [imageList, imageRatings]);
  const hidingArchived = flagStatus === FlagStatus.ExcludeRejected;

  return (
    <div className="flex items-center rounded-md bg-surface border border-border-color/30 text-xs select-none">
      <button
        className="flex items-center gap-1.5 px-2 py-1 hover:bg-bg-primary rounded-l-md disabled:opacity-50"
        onClick={() => open(queue)}
        disabled={queue.length === 0}
        title="Quick Cull: swipe right to keep, left to archive"
      >
        <Layers size={14} />
        <span className="text-text-secondary">Cull</span>
        {queue.length > 0 && <span className="text-text-primary font-medium tabular-nums">{queue.length}</span>}
      </button>
      <button
        className="px-1.5 py-1 border-l border-border-color/30 hover:bg-bg-primary rounded-r-md text-text-secondary"
        onClick={() => setFilterCriteria({ flagStatus: hidingArchived ? FlagStatus.All : FlagStatus.ExcludeRejected })}
        title={hidingArchived ? 'Show archived (rejected) images' : 'Hide archived (rejected) images'}
        aria-label={hidingArchived ? 'Show archived images' : 'Hide archived images'}
      >
        {hidingArchived ? <EyeOff size={14} /> : <Eye size={14} />}
      </button>
    </div>
  );
}
