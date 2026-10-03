import { useCallback } from 'react';
import { CloudDownload, HardDriveDownload, Pin, PinOff } from 'lucide-react';
import { createElement } from 'react';
import { Option } from '../components/ui/AppProperties';
import { useSyncStore } from '../store/useSyncStore';
import { useSyncActions } from './useSyncActions';

/**
 * Builds the per-item cloud-sync context-menu entries (ARCHITECTURE.md §3.5):
 * "Make available offline" (`sync_hydrate`), "Free up space" (`sync_free_space`),
 * and Pin / Unpin. Returns an empty list when sync is unavailable, so the menu
 * is inert on an upstream build. GREEN wires these into useAppContextMenus.
 */
export function useSyncItemMenu() {
  const available = useSyncStore((s) => s.available);
  const itemStates = useSyncStore((s) => s.itemStates);
  const { hydrate, freeSpace, pinPaths, unpinPaths } = useSyncActions();

  return useCallback(
    (paths: string[]): Option[] => {
      if (available !== true || paths.length === 0) return [];
      const anyStub = paths.some((p) => itemStates[p] === 'stub');

      // Pin is an orthogonal record flag, not a `sync-item-state` lane, so the
      // menu has no reliable local source of truth for the current pin state.
      // Both Pin and Unpin are therefore always offered (both commands are
      // idempotent — a no-op flips 0 items), so Unpin stays reachable.
      return [
        {
          icon: createElement(CloudDownload, { size: 16 }),
          label: 'Make available offline',
          disabled: !anyStub,
          onClick: () => void Promise.all(paths.map((p) => hydrate(p))),
        },
        {
          icon: createElement(HardDriveDownload, { size: 16 }),
          label: 'Free up space',
          onClick: () => void freeSpace(paths),
        },
        {
          icon: createElement(Pin, { size: 16 }),
          label: 'Pin (keep offline)',
          onClick: () => void pinPaths(paths),
        },
        {
          icon: createElement(PinOff, { size: 16 }),
          label: 'Unpin',
          onClick: () => void unpinPaths(paths),
        },
      ];
    },
    [available, itemStates, hydrate, freeSpace, pinPaths, unpinPaths],
  );
}
