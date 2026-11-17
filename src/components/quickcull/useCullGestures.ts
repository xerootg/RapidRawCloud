import { useCallback, useEffect, useRef, useState } from 'react';
import { FIT, Size, SwipeOutcome, View, ZOOM_EPS, clampView, oneToOneScale, swipeOutcome, zoomAt } from './viewport';

/**
 * Pointer Events gesture machine for Quick Cull (touch, pen and mouse alike):
 *
 * - at fit scale a drag is a **swipe** (right keep / left archive / up skip); swipes are disabled while zoomed, so a
 *   pan can never accidentally cull;
 * - **pinch** zooms around the finger centroid; one-finger drag while zoomed **pans** (edge-clamped);
 * - **double-tap** toggles fit ↔ 100% at the tap point;
 * - **press-and-hold** (350 ms, no movement) peeks at 100% under the finger and follows it, released → back;
 * - single **tap** is reported (after the double-tap window) for focus-compare flipping;
 * - **wheel** / trackpad pinch zooms at the cursor.
 */

const HOLD_MS = 350;
const MOVE_SLOP = 8;
const DOUBLE_TAP_MS = 280;
const TAP_MS = 250;

type Mode = 'idle' | 'pending' | 'swipe' | 'pan' | 'pinch' | 'hold';

interface Args {
  stage: Size;
  fit: Size;
  natural: Size;
  swipeEnabled: boolean;
  onSwipe: (outcome: Exclude<SwipeOutcome, null>) => void;
  onTap?: () => void;
}

export function useCullGestures({ stage, fit, natural, swipeEnabled, onSwipe, onTap }: Args) {
  const [view, setView] = useState<View>(FIT);
  const [swipe, setSwipe] = useState({ dx: 0, dy: 0, animating: false });

  const pointers = useRef(new Map<number, { x: number; y: number }>());
  const mode = useRef<Mode>('idle');
  const start = useRef({ x: 0, y: 0, t: 0 });
  const last = useRef({ x: 0, y: 0, t: 0, vx: 0 });
  const pinch = useRef({ dist: 1, view: FIT });
  const holdTimer = useRef<number | undefined>(undefined);
  const holdRestore = useRef<View>(FIT);
  const lastTap = useRef({ t: 0, x: 0, y: 0 });
  const tapTimer = useRef<number | undefined>(undefined);
  const viewRef = useRef(view);
  viewRef.current = view;

  const maxScale = oneToOneScale(natural, fit, window.devicePixelRatio || 1) * 2;
  const oneToOne = oneToOneScale(natural, fit, window.devicePixelRatio || 1);

  // Stage-centre-relative point from a client coordinate.
  const rel = useCallback(
    (el: HTMLElement, x: number, y: number) => {
      const r = el.getBoundingClientRect();
      return { x: x - r.left - stage.w / 2, y: y - r.top - stage.h / 2 };
    },
    [stage.w, stage.h],
  );

  const reset = useCallback(() => setView(FIT), []);
  const zoomTo = useCallback(
    (s: number, p = { x: 0, y: 0 }) => setView((v) => zoomAt(v, s, p, fit, stage, maxScale)),
    [fit, stage, maxScale],
  );

  useEffect(
    () => () => {
      window.clearTimeout(holdTimer.current);
      window.clearTimeout(tapTimer.current);
    },
    [],
  );

  const commitOrSpringBack = useCallback(
    (dx: number, dy: number, vx: number) => {
      const outcome = swipeOutcome(dx, dy, vx, stage);
      if (!outcome) {
        setSwipe({ dx: 0, dy: 0, animating: true });
        return;
      }
      navigator.vibrate?.(10);
      const off =
        outcome === 'skip' ? { dx: 0, dy: -stage.h * 1.2 } : { dx: (outcome === 'keep' ? 1 : -1) * stage.w * 1.3, dy };
      setSwipe({ ...off, animating: true });
      window.setTimeout(() => {
        onSwipe(outcome);
        setSwipe({ dx: 0, dy: 0, animating: false });
      }, 170);
    },
    [stage, onSwipe],
  );

  const onPointerDown = (e: React.PointerEvent<HTMLElement>) => {
    e.currentTarget.setPointerCapture(e.pointerId);
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    window.clearTimeout(holdTimer.current);

    if (pointers.current.size === 2) {
      const [a, b] = [...pointers.current.values()];
      pinch.current = { dist: Math.hypot(a.x - b.x, a.y - b.y) || 1, view: viewRef.current };
      if (mode.current === 'swipe') setSwipe({ dx: 0, dy: 0, animating: true });
      mode.current = 'pinch';
      return;
    }
    if (pointers.current.size > 2) return;

    start.current = { x: e.clientX, y: e.clientY, t: performance.now() };
    last.current = { x: e.clientX, y: e.clientY, t: start.current.t, vx: 0 };
    mode.current = 'pending';
    const el = e.currentTarget;
    holdTimer.current = window.setTimeout(() => {
      if (mode.current !== 'pending') return;
      mode.current = 'hold';
      holdRestore.current = viewRef.current;
      navigator.vibrate?.(5);
      const p = rel(el, start.current.x, start.current.y);
      setView((v) => zoomAt(v, Math.max(oneToOne, v.s), p, fit, stage, maxScale));
    }, HOLD_MS);
  };

  const onPointerMove = (e: React.PointerEvent<HTMLElement>) => {
    const prev = pointers.current.get(e.pointerId);
    if (!prev) return;
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });

    if (mode.current === 'pinch' && pointers.current.size >= 2) {
      const [a, b] = [...pointers.current.values()];
      const dist = Math.hypot(a.x - b.x, a.y - b.y);
      const centroid = rel(e.currentTarget, (a.x + b.x) / 2, (a.y + b.y) / 2);
      const base = pinch.current.view;
      setView(zoomAt(base, (base.s * dist) / pinch.current.dist, centroid, fit, stage, maxScale));
      return;
    }

    const now = performance.now();
    const dt = Math.max(1, now - last.current.t);
    last.current = { x: e.clientX, y: e.clientY, t: now, vx: (e.clientX - last.current.x) / dt };
    const dx = e.clientX - start.current.x;
    const dy = e.clientY - start.current.y;

    if (mode.current === 'pending' && Math.hypot(dx, dy) > MOVE_SLOP) {
      window.clearTimeout(holdTimer.current);
      mode.current = viewRef.current.s > ZOOM_EPS ? 'pan' : swipeEnabled ? 'swipe' : 'idle';
    }
    if (mode.current === 'swipe') {
      setSwipe({ dx, dy: Math.min(0, dy), animating: false });
    } else if (mode.current === 'pan' || mode.current === 'hold') {
      // Hold-peek follows the finger inversely so the area under it stays in view.
      const k = mode.current === 'hold' ? -1 : 1;
      const mx = (e.clientX - prev.x) * k;
      const my = (e.clientY - prev.y) * k;
      setView((v) => clampView({ s: v.s, tx: v.tx + mx, ty: v.ty + my }, fit, stage));
    }
  };

  const onPointerUp = (e: React.PointerEvent<HTMLElement>) => {
    pointers.current.delete(e.pointerId);
    window.clearTimeout(holdTimer.current);
    const m = mode.current;

    if (m === 'pinch') {
      // Lifting one finger of a pinch must not turn into a swipe or pan jump.
      if (pointers.current.size === 0) mode.current = 'idle';
      else {
        const [p] = [...pointers.current.values()];
        start.current = { x: p.x, y: p.y, t: performance.now() };
        mode.current = 'idle';
      }
      if (viewRef.current.s <= ZOOM_EPS) reset();
      return;
    }
    if (pointers.current.size > 0) return;
    mode.current = 'idle';

    if (m === 'swipe') {
      commitOrSpringBack(e.clientX - start.current.x, Math.min(0, e.clientY - start.current.y), last.current.vx);
      return;
    }
    if (m === 'hold') {
      setView(holdRestore.current);
      return;
    }
    if (m === 'pending' && performance.now() - start.current.t < TAP_MS) {
      const now = performance.now();
      const isDouble =
        now - lastTap.current.t < DOUBLE_TAP_MS &&
        Math.hypot(e.clientX - lastTap.current.x, e.clientY - lastTap.current.y) < 30;
      if (isDouble) {
        window.clearTimeout(tapTimer.current);
        lastTap.current = { t: 0, x: 0, y: 0 };
        const p = rel(e.currentTarget, e.clientX, e.clientY);
        setView((v) => (v.s > ZOOM_EPS ? FIT : zoomAt(v, oneToOne, p, fit, stage, maxScale)));
      } else {
        lastTap.current = { t: now, x: e.clientX, y: e.clientY };
        window.clearTimeout(tapTimer.current);
        tapTimer.current = window.setTimeout(() => onTap?.(), DOUBLE_TAP_MS);
      }
    }
  };

  const onWheel = (e: React.WheelEvent<HTMLElement>) => {
    const p = rel(e.currentTarget, e.clientX, e.clientY);
    const factor = Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.0025));
    setView((v) => {
      const next = zoomAt(v, v.s * factor, p, fit, stage, maxScale);
      return next.s <= ZOOM_EPS ? FIT : next;
    });
  };

  return {
    view,
    setView,
    reset,
    zoomTo,
    oneToOne,
    swipe,
    handlers: {
      onPointerDown,
      onPointerMove,
      onPointerUp,
      onPointerCancel: onPointerUp,
      onWheel,
    },
  };
}
