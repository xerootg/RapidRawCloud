import { useCallback } from 'react';
import { invoke } from '@tauri-apps/api/core';
import {
  Invokes,
  SyncRecentlyDeleted,
  SyncSettings,
  SyncStatusDto,
} from '../components/ui/AppProperties';
import { useSyncStore } from '../store/useSyncStore';

/**
 * Thin wrappers over the `sync_*` Tauri commands (ARCHITECTURE.md
 * §3.3/§3.5/§3.6/§3.8). Every call is guarded: on an upstream
 * `--no-default-features` build these commands are not registered, so `invoke`
 * rejects; we catch that, mark sync unavailable, and let the UI stay hidden.
 *
 * `sync_set_credentials` returns nothing — the secret is written to the
 * Rust-only store and never echoed back to the webview (§3.6).
 */
export function useSyncActions() {
  const setAvailable = useSyncStore((s) => s.setAvailable);
  const setStatus = useSyncStore((s) => s.setStatus);

  const refreshStatus = useCallback(async (): Promise<SyncStatusDto | null> => {
    try {
      const status = await invoke<SyncStatusDto>(Invokes.SyncStatus);
      setAvailable(true);
      setStatus(status);
      return status;
    } catch (_err) {
      // Command absent (upstream build) or engine error: treat as unavailable.
      setAvailable(false);
      return null;
    }
  }, [setAvailable, setStatus]);

  const configure = useCallback(async (settings: SyncSettings): Promise<void> => {
    await invoke(Invokes.SyncConfigure, { settings });
  }, []);

  const setCredentials = useCallback(async (accessKey: string, secretKey: string): Promise<void> => {
    // Intentionally returns void: the secret never comes back (§3.6).
    await invoke(Invokes.SyncSetCredentials, { accessKey, secretKey });
  }, []);

  const pinPaths = useCallback(
    (paths: string[]): Promise<number> => invoke<number>(Invokes.SyncPinPaths, { paths }),
    [],
  );

  const unpinPaths = useCallback(
    (paths: string[]): Promise<number> => invoke<number>(Invokes.SyncUnpinPaths, { paths }),
    [],
  );

  const freeSpace = useCallback(
    (paths: string[]): Promise<number> => invoke<number>(Invokes.SyncFreeSpace, { paths }),
    [],
  );

  const hydrate = useCallback(
    (path: string): Promise<void> => invoke(Invokes.SyncHydrate, { path }),
    [],
  );

  const recentlyDeleted = useCallback(
    (): Promise<SyncRecentlyDeleted[]> => invoke<SyncRecentlyDeleted[]>(Invokes.SyncRecentlyDeleted),
    [],
  );

  const restore = useCallback(
    (path: string): Promise<string[]> => invoke<string[]>(Invokes.SyncRestore, { path }),
    [],
  );

  const resolveConflict = useCallback(
    (path: string, keep: 'winner' | 'copy'): Promise<void> =>
      invoke(Invokes.SyncResolveConflict, { path, keep }),
    [],
  );

  const retireDevice = useCallback(
    (deviceId: string): Promise<void> => invoke(Invokes.SyncRetireDevice, { deviceId }),
    [],
  );

  const verifyLibrary = useCallback(
    (): Promise<unknown> => invoke(Invokes.SyncVerifyLibrary),
    [],
  );

  return {
    refreshStatus,
    configure,
    setCredentials,
    pinPaths,
    unpinPaths,
    freeSpace,
    hydrate,
    recentlyDeleted,
    restore,
    resolveConflict,
    retireDevice,
    verifyLibrary,
  };
}
