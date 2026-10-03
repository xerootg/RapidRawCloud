import { useCallback, useEffect, useState } from 'react';
import { RotateCcw, Trash2 } from 'lucide-react';
import { useSyncActions } from '../../../hooks/useSyncActions';
import { SyncRecentlyDeleted } from '../../ui/AppProperties';

/**
 * "Recently Deleted" view in settings (ARCHITECTURE.md §2.7/§3.8): lists
 * soft-deleted items from `sync_recently_deleted` and restores one with
 * `sync_restore`.
 */
export default function RecentlyDeletedView() {
  const { recentlyDeleted, restore } = useSyncActions();
  const [items, setItems] = useState<SyncRecentlyDeleted[]>([]);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setItems(await recentlyDeleted());
    } catch (_err) {
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [recentlyDeleted]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const onRestore = async (path: string) => {
    await restore(path);
    await refresh();
  };

  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-text-primary font-medium flex items-center gap-2">
        <Trash2 size={14} /> Recently Deleted
      </h3>
      {loading ? (
        <p className="text-text-secondary text-sm">Loading…</p>
      ) : items.length === 0 ? (
        <p className="text-text-secondary text-sm">Nothing has been deleted recently.</p>
      ) : (
        <ul className="flex flex-col gap-1">
          {items.map((it) => (
            <li
              key={it.relkey}
              className="flex items-center justify-between px-2 py-1.5 rounded-md bg-surface border border-border-color/30"
            >
              <span className="truncate text-text-primary text-sm" title={it.path}>
                {it.relkey}
              </span>
              <button
                className="flex items-center gap-1 text-xs text-accent hover:opacity-80"
                onClick={() => void onRestore(it.path)}
              >
                <RotateCcw size={14} /> Restore
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
