import { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { useProcessStore } from '../../store/useProcessStore';
import { PreviewLevel, QuickCullInvokes } from '../../types/quickCull';

/**
 * Progressive inspection source for one image, cheapest first and never downloading an original on its own:
 *
 * 1. the grid's 1280 px medium thumbnail (exists for cloud stubs too — the bucket's `_medium` JPEG);
 * 2. `cull_preview(level: 'local')`: the original's embedded JPEG when local, or a locally cached proxy's preview;
 * 3. only when the caller asks (`full`): hydrate the original (explicit "Load full") or, for a local raw whose embedded
 *    JPEG is small, upstream's full render.
 *
 * Decoded previews are cached as blob URLs in a small LRU shared across the session so flipping between pinned
 * images in focus-compare is instant.
 */

const LRU_MAX = 10;
const cache = new Map<string, { url: string; w: number; h: number }>();
const inflight = new Map<string, Promise<{ url: string; w: number; h: number } | null>>();

const cacheKey = (path: string, level: string) => `${level}|${path}`;

const put = (key: string, value: { url: string; w: number; h: number }) => {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > LRU_MAX) {
    const [oldKey, old] = cache.entries().next().value as [string, { url: string }];
    URL.revokeObjectURL(old.url);
    cache.delete(oldKey);
  }
};

const decodeSize = (url: string) =>
  new Promise<{ w: number; h: number }>((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve({ w: img.naturalWidth, h: img.naturalHeight });
    img.onerror = reject;
    img.src = url;
  });

const toBlobUrl = async (bytes: ArrayBuffer | Uint8Array | number[]) => {
  const data = bytes instanceof ArrayBuffer ? bytes : new Uint8Array(bytes as ArrayLike<number>);
  const url = URL.createObjectURL(new Blob([data as BlobPart]));
  try {
    return { url, ...(await decodeSize(url)) };
  } catch (e) {
    URL.revokeObjectURL(url);
    throw e;
  }
};

export type SourceKind = 'thumbnail' | 'preview' | 'full' | 'render';

async function fetchLevel(path: string, level: PreviewLevel | 'render') {
  const key = cacheKey(path, level);
  const hit = cache.get(key);
  if (hit) {
    put(key, hit);
    return hit;
  }
  let p = inflight.get(key);
  if (!p) {
    p = (async () => {
      try {
        let bytes: ArrayBuffer;
        if (level === 'render') {
          const meta = await invoke<{ adjustments?: unknown } | null>(QuickCullInvokes.LoadMetadata, { path });
          bytes = await invoke(QuickCullInvokes.GeneratePreviewForPath, {
            path,
            jsAdjustments: meta?.adjustments ?? {},
          });
        } else {
          bytes = await invoke(QuickCullInvokes.Preview, { path, level });
        }
        const value = await toBlobUrl(bytes);
        put(key, value);
        return value;
      } catch (e) {
        if (String(e) !== 'no-local-preview') console.warn(`quick cull preview (${level}) failed for ${path}:`, e);
        return null;
      } finally {
        inflight.delete(key);
      }
    })();
    inflight.set(key, p);
  }
  return p;
}

/** Warm the local preview of upcoming images without rendering them. */
export const preloadLocal = (paths: string[]) => paths.forEach((p) => void fetchLevel(p, 'local'));

export function useBestSource(path: string | null, requested: 'local' | 'full' | 'render') {
  const thumb = useProcessStore((s) => (path ? s.mediumThumbnails?.[path] || s.thumbnails?.[path] : undefined));
  const [best, setBest] = useState<{ path: string; url: string; w: number; h: number; kind: SourceKind } | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!path) return;
    let alive = true;
    const levels: Array<[PreviewLevel | 'render', SourceKind]> = [['local', 'preview']];
    if (requested === 'full') levels.push(['full', 'full']);
    if (requested === 'render') levels.push(['render', 'render']);
    setLoading(requested !== 'local');
    (async () => {
      for (const [level, kind] of levels) {
        const v = await fetchLevel(path, level);
        if (!alive) return;
        if (v) setBest((cur) => (cur?.path === path && cur.w >= v.w ? cur : { path, kind, ...v }));
      }
      if (alive) setLoading(false);
    })();
    return () => {
      alive = false;
    };
  }, [path, requested]);

  const current = best && best.path === path ? best : null;
  return { thumb, best: current, loading };
}
