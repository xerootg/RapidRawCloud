import { useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { Invokes } from '../components/ui/AppProperties';
import { useSyncActions } from './useSyncActions';
import { useSyncStore } from '../store/useSyncStore';

/**
 * Drives the foreground sync cycle (ARCHITECTURE.md §3.3). The Android sync
 * engine's background WorkManager cycle runs at most ~hourly and bypasses the
 * app-crate manager, so without a foreground driver the user never sees sync
 * progress and the bucket is slow to bootstrap. This kicks an in-process cycle
 * — which emits the live `sync-*` events the badges consume — at the moments
 * that matter: on mount, when the app regains focus (resume), right after an
 * import completes, and on a light interval while the app is open.
 *
 * Each cycle returns immediately (it runs on its own thread) and is coalesced
 * by the backend guard, so overlapping triggers never stack up. On an upstream
 * `--no-default-features` build `runCycle` marks sync unavailable and this
 * becomes inert.
 */
export function useSyncDriver() {
  const { runCycle } = useSyncActions();
  const available = useSyncStore((s) => s.available);

  useEffect(() => {
    // Known non-sync build: stay inert.
    if (available === false) return;

    // Make the in-process engine live for the session (the app-crate manager is
    // otherwise only configured after the user saves sync settings), THEN kick
    // the first cycle. Both are no-ops on a non-sync build / when sync is off.
    void invoke(Invokes.SyncEnsureConfigured)
      .catch(() => {})
      .finally(() => void runCycle());

    const onFocus = () => void runCycle();
    window.addEventListener('focus', onFocus);

    const intervalId = window.setInterval(() => void runCycle(), 60_000);

    let unlisten: (() => void) | undefined;
    void listen('import-complete', () => void runCycle()).then((fn) => {
      unlisten = fn;
    });

    return () => {
      window.removeEventListener('focus', onFocus);
      clearInterval(intervalId);
      unlisten?.();
    };
  }, [available, runCycle]);
}
