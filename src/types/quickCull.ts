import { ImageFlag } from './library';

/**
 * Quick Cull backend commands (`src-tauri/src/sync/cull.rs`). Kept in this fork-owned file instead of upstream's
 * `Invokes` enum so the feature adds no upstream edit beyond its mount points. Always registered (not `sync`-gated).
 */
export const QuickCullInvokes = {
  Apply: 'cull_apply',
  Preview: 'cull_preview',
  // Upstream commands reused for the "render full-res" fallback on raws with a small embedded JPEG.
  LoadMetadata: 'load_metadata',
  GeneratePreviewForPath: 'generate_preview_for_path',
  UpdateThumbnailQueue: 'update_thumbnail_queue',
} as const;

export type PreviewLevel = 'local' | 'full';

/** Exact target values for one image (not a delta). Keep, archive and undo are all this shape. */
export interface CullEntry {
  path: string;
  rating: number;
  flag: ImageFlag | null;
}

export interface CullSnapshot {
  rating: number;
  flag: ImageFlag | null;
}

export type CullDecision = 'keep' | 'archive' | 'skip';

/** Keep never downgrades: 4/5 stars stay, anything lower becomes 3. Keep clears Reject. */
export const KEEP_MIN_RATING = 3;

export const targetFor = (decision: CullDecision, before: CullSnapshot): CullSnapshot => {
  switch (decision) {
    case 'keep':
      return {
        rating: Math.max(before.rating, KEEP_MIN_RATING),
        flag: before.flag === ImageFlag.Reject ? null : before.flag,
      };
    case 'archive':
      return { rating: before.rating, flag: ImageFlag.Reject };
    case 'skip':
      return before;
  }
};

/** Undecided = never rated and never flagged. Anything the user already touched is out of the inbox. */
export const isUndecided = (rating: number, flag: ImageFlag | null) => rating === 0 && flag === null;
