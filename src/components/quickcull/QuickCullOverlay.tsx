import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { toast } from 'react-toastify';
import { Archive, Check, ChevronUp, CloudDownload, Maximize, Pin, PinOff, Undo2, X } from 'lucide-react';
import { useLibraryStore } from '../../store/useLibraryStore';
import { MAX_PINS, UndoStep, useQuickCullStore } from '../../store/useQuickCullStore';
import { ImageFile } from '../../types/library';
import { CullDecision, CullSnapshot, QuickCullInvokes, isUndecided, targetFor } from '../../types/quickCull';
import { FIT, Size, ZOOM_EPS, fitSize } from './viewport';
import { useCullGestures } from './useCullGestures';
import { preloadLocal, useBestSource } from './useBestSource';

/**
 * Quick Cull: full-screen, one image at a time. Swipe right = keep (≥ 3★), left = archive (Reject), up = skip.
 * Undo restores the exact pre-swipe `{rating, flag}`. Decisions are upstream's own rating/flag, written atomically by
 * `cull_apply`; the Reject flag syncs (§2.5) and upstream's "exclude rejected" filter hides archived images.
 *
 * Fork-owned and self-contained: while open it captures the keyboard (so upstream shortcuts don't fire underneath)
 * and wraps the Android back handler, so no upstream hook needs to know it exists.
 */

const snapshotOf = (path: string): CullSnapshot => {
  const { imageList, imageRatings } = useLibraryStore.getState();
  const image = imageList.find((i) => i.path === path);
  return { rating: imageRatings[path] || 0, flag: image?.flag ?? null };
};

const applyOptimistic = (path: string, v: CullSnapshot) =>
  useLibraryStore.getState().setLibrary((state) => ({
    imageRatings: { ...state.imageRatings, [path]: v.rating },
    imageList: state.imageList.map((i: ImageFile) => (i.path === path ? { ...i, flag: v.flag } : i)),
  }));

const persist = async (path: string, v: CullSnapshot, rollback: CullSnapshot) => {
  try {
    const failed: string[] = await invoke(QuickCullInvokes.Apply, {
      entries: [{ path, rating: v.rating, flag: v.flag }],
    });
    if (failed.length > 0) throw new Error('write failed');
  } catch (e) {
    applyOptimistic(path, rollback);
    toast.error(`Quick Cull: could not save decision (${e})`);
  }
};

const isStub = (image?: ImageFile) => !!image && (image.is_cloud_placeholder || image.sync_state === 'stub');

export default function QuickCullOverlay() {
  const { isOpen, queue, index, pinned, compareIndex, kept, archived, undo, close, set } = useQuickCullStore();
  const imageList = useLibraryStore((s) => s.imageList);
  const stageRef = useRef<HTMLDivElement>(null);
  const [stage, setStage] = useState<Size>({ w: 0, h: 0 });
  const [natural, setNatural] = useState<Size>({ w: 3, h: 2 });
  const [requested, setRequested] = useState<'local' | 'full' | 'render'>('local');
  const pointerPos = useRef({ x: 0, y: 0 });

  const comparing = pinned.length >= 2;
  const currentPath = comparing ? pinned[compareIndex % pinned.length] : (queue[index] ?? null);
  const image = useMemo(() => imageList.find((i) => i.path === currentPath), [imageList, currentPath]);
  const done = !comparing && index >= queue.length;

  const { thumb, best, loading } = useBestSource(isOpen ? currentPath : null, requested);
  const src = best?.url ?? thumb;
  const fit = fitSize(natural, stage);

  // Stage size tracking.
  useEffect(() => {
    if (!isOpen || !stageRef.current) return;
    const el = stageRef.current;
    const ro = new ResizeObserver(() => setStage({ w: el.clientWidth, h: el.clientHeight }));
    ro.observe(el);
    setStage({ w: el.clientWidth, h: el.clientHeight });
    return () => ro.disconnect();
  }, [isOpen]);

  // Request thumbnails + warm local previews for the next few images.
  useEffect(() => {
    if (!isOpen) return;
    const ahead = queue.slice(index, index + 5);
    if (ahead.length) invoke(QuickCullInvokes.UpdateThumbnailQueue, { paths: ahead }).catch(() => {});
    preloadLocal(queue.slice(index + 1, index + 3));
  }, [isOpen, queue, index]);

  const decide = useCallback(
    (decision: CullDecision, path = currentPath) => {
      if (!path) return;
      const before = snapshotOf(path);
      const after = targetFor(decision, before);
      if (decision !== 'skip') {
        applyOptimistic(path, after);
        void persist(path, after, before);
      }
      const step: UndoStep = { path, index: queue.indexOf(path), decision, before };
      set((s) => {
        const pins = s.pinned.filter((p) => p !== path);
        return {
          undo: [...s.undo, step],
          kept: s.kept + (decision === 'keep' ? 1 : 0),
          archived: s.archived + (decision === 'archive' ? 1 : 0),
          pinned: pins,
          compareIndex: pins.length ? s.compareIndex % pins.length : 0,
          // Outside compare, advance past the decided image; in compare the queue position is unchanged unless the
          // decided image is the one under the cursor.
          index: s.pinned.length >= 2 && s.queue[s.index] !== path ? s.index : s.index + 1,
        };
      });
    },
    [currentPath, queue, set],
  );

  const undoLast = useCallback(() => {
    const step = useQuickCullStore.getState().undo.at(-1);
    if (!step) return;
    if (step.decision !== 'skip') {
      const now = snapshotOf(step.path);
      applyOptimistic(step.path, step.before);
      void persist(step.path, step.before, now);
    }
    set((s) => ({
      undo: s.undo.slice(0, -1),
      kept: s.kept - (step.decision === 'keep' ? 1 : 0),
      archived: s.archived - (step.decision === 'archive' ? 1 : 0),
      index: step.index >= 0 ? step.index : s.index,
    }));
  }, [set]);

  const flip = useCallback(() => {
    if (comparing) set((s) => ({ compareIndex: (s.compareIndex + 1) % s.pinned.length }));
  }, [comparing, set]);

  const togglePin = useCallback(() => {
    if (!currentPath) return;
    set((s) => {
      if (s.pinned.includes(currentPath)) {
        const pins = s.pinned.filter((p) => p !== currentPath);
        return { pinned: pins, compareIndex: 0 };
      }
      if (s.pinned.length >= MAX_PINS) {
        toast.info(`Compare holds ${MAX_PINS} images`);
        return {};
      }
      // Pinning advances so the next frame can be pinned too (a burst is pinned frame by frame).
      return { pinned: [...s.pinned, currentPath], index: Math.min(s.index + 1, s.queue.length) };
    });
  }, [currentPath, set]);

  const gestures = useCullGestures({
    stage,
    fit,
    natural,
    swipeEnabled: !done,
    onSwipe: (o) => decide(o),
    onTap: flip,
  });
  const { view, swipe, reset, zoomTo, oneToOne } = gestures;

  // New image: back to fit and the cheapest source — except in compare, where the shared zoom point is the point.
  useEffect(() => {
    setRequested('local');
    if (!comparing) reset();
  }, [currentPath, comparing, reset]);

  // Keyboard (capture phase, swallow everything so upstream shortcuts don't fire under the overlay).
  useEffect(() => {
    if (!isOpen) return;
    let spaceRestore: typeof FIT | null = null;
    const onKey = (e: KeyboardEvent) => {
      e.stopPropagation();
      const mod = e.metaKey || e.ctrlKey;
      const k = e.key;
      const handled = () => e.preventDefault();
      if (k === 'Escape') {
        handled();
        if (comparing) set({ pinned: [], compareIndex: 0 });
        else close();
      } else if ((mod && k.toLowerCase() === 'z') || k === 'Backspace') {
        handled();
        undoLast();
      } else if (k === 'ArrowRight' || k.toLowerCase() === 'k') {
        handled();
        decide('keep');
      } else if (k === 'ArrowLeft' || k.toLowerCase() === 'a') {
        handled();
        decide('archive');
      } else if (k === 'ArrowUp') {
        handled();
        decide('skip');
      } else if (k === 'Tab') {
        handled();
        flip();
      } else if (k.toLowerCase() === 'p') {
        handled();
        togglePin();
      } else if (k.toLowerCase() === 'f') {
        handled();
        setRequested('full');
      } else if (k.toLowerCase() === 'z' || k === '0') {
        handled();
        if (view.s > ZOOM_EPS) reset();
        else zoomTo(oneToOne);
      } else if (k === ' ' && !e.repeat && !spaceRestore) {
        handled();
        spaceRestore = view;
        const r = stageRef.current?.getBoundingClientRect();
        const p = r
          ? { x: pointerPos.current.x - r.left - r.width / 2, y: pointerPos.current.y - r.top - r.height / 2 }
          : { x: 0, y: 0 };
        zoomTo(Math.max(oneToOne, view.s), p);
      }
    };
    const onKeyUp = (e: KeyboardEvent) => {
      e.stopPropagation();
      if (e.key === ' ' && spaceRestore) {
        gestures.setView(spaceRestore);
        spaceRestore = null;
      }
    };
    window.addEventListener('keydown', onKey, true);
    window.addEventListener('keyup', onKeyUp, true);
    return () => {
      window.removeEventListener('keydown', onKey, true);
      window.removeEventListener('keyup', onKeyUp, true);
    };
  }, [isOpen, comparing, view, oneToOne, decide, undoLast, flip, togglePin, close, reset, zoomTo, set, gestures]);

  // Android back gesture closes compare, then the overlay (never undoes).
  useEffect(() => {
    if (!isOpen) return;
    const w = window as Window & { __handleAndroidBack?: () => void };
    const previous = w.__handleAndroidBack;
    w.__handleAndroidBack = () => {
      const s = useQuickCullStore.getState();
      if (s.pinned.length >= 2) s.set({ pinned: [], compareIndex: 0 });
      else s.close();
    };
    return () => {
      w.__handleAndroidBack = previous;
    };
  }, [isOpen]);

  if (!isOpen) return null;

  const progress = queue.length ? Math.min(index + 1, queue.length) : 0;
  const tint = Math.min(1, Math.abs(swipe.dx) / Math.max(1, stage.w * 0.35));
  const stub = isStub(image);
  const smallPreview = !!best && Math.max(best.w, best.h) < 3000 && !stub && image?.is_raw;
  const sourceLabel = best
    ? `${best.kind === 'thumbnail' ? 'thumbnail' : best.kind} · ${Math.max(best.w, best.h)}px`
    : thumb
      ? 'thumbnail · 1280px'
      : 'loading…';

  return (
    <div className="fixed inset-0 z-[100] bg-black text-white flex flex-col select-none" role="dialog" aria-modal>
      {/* Top bar */}
      <div
        className="flex items-center gap-3 px-3 py-2 text-sm shrink-0"
        style={{ paddingTop: 'max(0.5rem, env(safe-area-inset-top))' }}
      >
        <button className="p-2 rounded-full hover:bg-white/10" onClick={close} aria-label="Close Quick Cull">
          <X size={20} />
        </button>
        <span className="tabular-nums">
          {comparing
            ? `Compare ${(compareIndex % pinned.length) + 1}/${pinned.length}`
            : `${progress} / ${queue.length}`}
        </span>
        <span className="text-green-400 tabular-nums">✓ {kept}</span>
        <span className="text-amber-400 tabular-nums">▣ {archived}</span>
        <span className="ml-auto text-white/50 text-xs truncate">{sourceLabel}</span>
        {!done && (
          <button
            className={`p-2 rounded-full hover:bg-white/10 ${currentPath && pinned.includes(currentPath) ? 'text-sky-400' : ''}`}
            onClick={togglePin}
            aria-label="Pin for focus compare (P)"
            title="Pin for focus compare (P)"
          >
            {currentPath && pinned.includes(currentPath) ? <PinOff size={18} /> : <Pin size={18} />}
          </button>
        )}
      </div>

      {/* Stage */}
      <div
        ref={stageRef}
        className="relative flex-1 overflow-hidden touch-none"
        onPointerMoveCapture={(e) => (pointerPos.current = { x: e.clientX, y: e.clientY })}
        {...(done ? {} : gestures.handlers)}
      >
        {done ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-4 text-center px-6">
            <p className="text-2xl font-medium">All caught up</p>
            <p className="text-white/70">
              Kept {kept} · Archived {archived}
            </p>
            <div className="flex gap-3">
              {undo.length > 0 && (
                <button className="px-4 py-2 rounded-lg bg-white/10" onClick={undoLast}>
                  Undo last
                </button>
              )}
              <button className="px-4 py-2 rounded-lg bg-white text-black" onClick={close}>
                Done
              </button>
            </div>
          </div>
        ) : (
          <div
            className="absolute inset-0"
            style={{
              transform: `translate(${swipe.dx}px, ${swipe.dy}px) rotate(${(swipe.dx / Math.max(1, stage.w)) * 12}deg)`,
              transition: swipe.animating ? 'transform 170ms ease-out' : 'none',
            }}
          >
            {src && (
              <img
                key={currentPath ?? ''}
                src={src}
                alt=""
                draggable={false}
                onLoad={(e) => setNatural({ w: e.currentTarget.naturalWidth, h: e.currentTarget.naturalHeight })}
                className="absolute left-1/2 top-1/2 max-w-none pointer-events-none"
                style={{
                  width: fit.w,
                  height: fit.h,
                  transform: `translate(-50%, -50%) translate(${view.tx}px, ${view.ty}px) scale(${view.s})`,
                  imageRendering: view.s > 3 ? 'pixelated' : 'auto',
                }}
              />
            )}
            {swipe.dx !== 0 && (
              <div
                className={`absolute top-6 ${swipe.dx > 0 ? 'left-6 text-green-400 border-green-400' : 'right-6 text-amber-400 border-amber-400'} border-4 rounded-xl px-4 py-1 text-2xl font-bold`}
                style={{ opacity: tint }}
              >
                {swipe.dx > 0 ? 'KEEP' : 'ARCHIVE'}
              </div>
            )}
            {swipe.dy < -20 && (
              <div className="absolute top-6 left-1/2 -translate-x-1/2 text-white/80 text-xl font-bold">SKIP</div>
            )}
          </div>
        )}
        {loading && <div className="absolute bottom-3 left-1/2 -translate-x-1/2 text-xs text-white/70">Loading…</div>}
      </div>

      {/* Bottom bar */}
      {!done && (
        <div
          className="flex items-center justify-center gap-4 px-3 py-3 shrink-0"
          style={{ paddingBottom: 'max(0.75rem, env(safe-area-inset-bottom))' }}
        >
          <button
            className="flex flex-col items-center gap-1 px-4 py-2 rounded-xl bg-amber-500/20 text-amber-300"
            onClick={() => decide('archive')}
            aria-label="Archive (←)"
          >
            <Archive size={22} />
            <span className="text-xs">Archive</span>
          </button>
          <button
            className="flex flex-col items-center gap-1 px-3 py-2 rounded-xl bg-white/10 disabled:opacity-30"
            onClick={undoLast}
            disabled={undo.length === 0}
            aria-label="Undo (Ctrl+Z)"
          >
            <Undo2 size={20} />
            <span className="text-xs">Undo</span>
          </button>
          <button
            className="flex flex-col items-center gap-1 px-3 py-2 rounded-xl bg-white/10"
            onClick={() => decide('skip')}
            aria-label="Skip (↑)"
          >
            <ChevronUp size={20} />
            <span className="text-xs">Skip</span>
          </button>
          {stub && best?.kind !== 'full' && (
            <button
              className="flex flex-col items-center gap-1 px-3 py-2 rounded-xl bg-sky-500/20 text-sky-300"
              onClick={() => setRequested('full')}
              aria-label="Load full original (downloads it)"
              title="Downloads the original from the cloud (F)"
            >
              <CloudDownload size={20} />
              <span className="text-xs">Load full</span>
            </button>
          )}
          {smallPreview && best?.kind !== 'render' && (
            <button
              className="flex flex-col items-center gap-1 px-3 py-2 rounded-xl bg-sky-500/20 text-sky-300"
              onClick={() => setRequested('render')}
              aria-label="Render full resolution"
              title="This raw's embedded preview is small; render the original at full resolution"
            >
              <Maximize size={20} />
              <span className="text-xs">Full res</span>
            </button>
          )}
          <button
            className="flex flex-col items-center gap-1 px-4 py-2 rounded-xl bg-green-500/20 text-green-300"
            onClick={() => decide('keep')}
            aria-label="Keep (→)"
          >
            <Check size={22} />
            <span className="text-xs">Keep</span>
          </button>
        </div>
      )}
    </div>
  );
}

/** Undecided images of the current library view, newest first. */
export const undecidedQueue = (imageList: ImageFile[], imageRatings: Record<string, number>) =>
  imageList
    .filter((i) => isUndecided(imageRatings[i.path] || 0, i.flag ?? null))
    .sort((a, b) => b.modified - a.modified)
    .map((i) => i.path);
