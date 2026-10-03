import { create } from 'zustand';
import { SyncConflict, SyncStatusDto } from '../components/ui/AppProperties';

/**
 * Cloud-sync UI state (ARCHITECTURE.md §3.8). Fed by the `sync-*` Tauri
 * listeners in useTauriListeners.ts (status at ~1 Hz, item-state batched) and
 * read by the MainLibrary status badge, the per-item grid badges, the hydrate
 * progress UI, the conflict toast, and the settings panels.
 *
 * `available` is the frontend call-guard (§ parity): `null` until the first
 * `sync_status` probe, then `true`/`false`. On an upstream build the sync
 * commands are absent and the probe rejects, so `available` stays `false` and
 * all sync UI stays hidden.
 */
interface HydrateProgress {
  bytes: number;
  total: number;
}

interface SyncState {
  available: boolean | null;
  status: SyncStatusDto | null;
  // Per-path lane state for grid badges, keyed by absolute path (§3.8).
  itemStates: Record<string, string>;
  // Per-path hydration byte progress (§3.5), keyed by absolute path.
  hydrateProgress: Record<string, HydrateProgress>;
  conflicts: SyncConflict[];
  lastError: { path: string | null; message: string } | null;

  setAvailable: (available: boolean) => void;
  // Accepts a partial snapshot and MERGES it over the last status, so a live
  // `sync-status` event (the 6-field §3.8 subset) never clobbers the
  // command-only facts (configured/credentialsConfigured/deviceId/peerDevices)
  // learned from the full `sync_status` probe.
  setStatus: (status: Partial<SyncStatusDto>) => void;
  applyItemStates: (updates: Array<{ path: string; state: string }>) => void;
  setHydrateProgress: (path: string, progress: HydrateProgress) => void;
  clearHydrateProgress: (path: string) => void;
  pushConflict: (conflict: SyncConflict) => void;
  dismissConflict: (path: string) => void;
  setError: (error: { path: string | null; message: string } | null) => void;
}

export const useSyncStore = create<SyncState>((set) => ({
  available: null,
  status: null,
  itemStates: {},
  hydrateProgress: {},
  conflicts: [],
  lastError: null,

  setAvailable: (available) => set({ available }),

  setStatus: (status) =>
    set((state) => ({
      status: state.status
        ? { ...state.status, ...status }
        : (status as SyncStatusDto),
    })),

  applyItemStates: (updates) =>
    set((state) => {
      if (updates.length === 0) return state;
      const next = { ...state.itemStates };
      for (const u of updates) next[u.path] = u.state;
      return { itemStates: next };
    }),

  setHydrateProgress: (path, progress) =>
    set((state) => ({ hydrateProgress: { ...state.hydrateProgress, [path]: progress } })),

  clearHydrateProgress: (path) =>
    set((state) => {
      if (!(path in state.hydrateProgress)) return state;
      const next = { ...state.hydrateProgress };
      delete next[path];
      return { hydrateProgress: next };
    }),

  pushConflict: (conflict) =>
    set((state) => ({
      conflicts: state.conflicts.some((c) => c.path === conflict.path)
        ? state.conflicts
        : [...state.conflicts, conflict],
    })),

  dismissConflict: (path) =>
    set((state) => ({ conflicts: state.conflicts.filter((c) => c.path !== path) })),

  setError: (error) => set({ lastError: error }),
}));
