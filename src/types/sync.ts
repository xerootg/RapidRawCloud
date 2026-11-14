// Cloud-sync types (RapidRawCloud fork; see docs/ARCHITECTURE.md). Kept in their
// own module so upstream's type files only carry one-line references to them.

/// Cloud sync settings mirrored from the Rust `SyncSettings` (§3.6). Credentials
/// are NEVER part of this — the webview only ever learns `credentialsConfigured`.
export interface SyncSettings {
  enabled: boolean;
  endpoint: string;
  bucket: string;
  region: string;
  forcePathStyle: boolean;
  uploadRequiresUnmetered: boolean;
  uploadRequiresCharging: boolean;
  cacheSizeGb: number;
  previewBudgetGb: number;
  previewPrefetchMonths: number;
  autoWatchDcim: boolean;
  watchedMediaBuckets: string[];
  workerBackfill: boolean;
}

/// A device in the shared registry for the settings device panel (§2.10/§3.8).
export interface SyncPeerDevice {
  deviceId: string;
  isSelf: boolean;
  lastSeenUnix: number;
  retired: boolean;
}

/// The §3.8 status snapshot (`sync_status` command). Carries only
/// `credentialsConfigured: boolean` — never secret material (§3.6).
export interface SyncStatusDto {
  state: 'idle' | 'syncing' | 'offline' | 'error';
  pendingUp: number;
  pendingDown: number;
  bytesUp: number;
  bytesDown: number;
  dirtyUnbacked: number;
  configured: boolean;
  credentialsConfigured: boolean;
  deviceId: string | null;
  peerDevices: SyncPeerDevice[];
}

/// A soft-deleted item in the "Recently Deleted" view (§3.8).
export interface SyncRecentlyDeleted {
  path: string;
  relkey: string;
  deletedUnix: number;
}

/// Per-path item-state update from the batched `sync-item-state` event (§3.8).
export interface SyncItemStateUpdate {
  path: string;
  state: string;
}

/// A `sync-conflict` event payload (§3.8).
export interface SyncConflict {
  path: string;
  copyPath: string;
  winnerDevice: string;
}
