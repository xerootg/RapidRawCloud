/**
 * Pure viewport math for Quick Cull's zoom/pan. Scale 1 = the image fitted (contain) to the stage; the transform is
 * `translate(tx, ty) scale(s)` around the stage centre. Kept free of React/DOM so it is trivially testable.
 */

export interface View {
  s: number;
  tx: number;
  ty: number;
}

export interface Size {
  w: number;
  h: number;
}

export const FIT: View = { s: 1, tx: 0, ty: 0 };
export const ZOOM_EPS = 1.01;

/** Contain-fit size of an image of `img` natural size inside `stage`. */
export const fitSize = (img: Size, stage: Size): Size => {
  if (img.w <= 0 || img.h <= 0 || stage.w <= 0 || stage.h <= 0) return { w: 0, h: 0 };
  const k = Math.min(stage.w / img.w, stage.h / img.h);
  return { w: img.w * k, h: img.h * k };
};

/** Clamp the pan so the image never leaves a gap at an edge it could cover. */
export const clampView = (v: View, fit: Size, stage: Size): View => {
  const maxX = Math.max(0, (fit.w * v.s - stage.w) / 2);
  const maxY = Math.max(0, (fit.h * v.s - stage.h) / 2);
  return { s: v.s, tx: Math.min(maxX, Math.max(-maxX, v.tx)), ty: Math.min(maxY, Math.max(-maxY, v.ty)) };
};

/**
 * Zoom to scale `s2` keeping the stage point `p` (relative to the stage centre) fixed under the finger/cursor.
 */
export const zoomAt = (v: View, s2: number, p: { x: number; y: number }, fit: Size, stage: Size, maxS: number) => {
  const s = Math.min(maxS, Math.max(1, s2));
  const k = s / v.s;
  return clampView({ s, tx: p.x - (p.x - v.tx) * k, ty: p.y - (p.y - v.ty) * k }, fit, stage);
};

/**
 * The scale at which one source pixel is one device pixel ("100%"), never below 2.5× so a 1280 px thumbnail still
 * gives a useful close look.
 */
export const oneToOneScale = (natural: Size, fit: Size, dpr = 1) =>
  fit.w > 0 ? Math.max(2.5, natural.w / dpr / fit.w) : 2.5;

export type SwipeOutcome = 'keep' | 'archive' | 'skip' | null;

/** Swipe commit rule: past 35% of the width or a fast flick (≥ 0.6 px/ms) horizontally; up past 25% = skip. */
export const swipeOutcome = (dx: number, dy: number, vx: number, stage: Size): SwipeOutcome => {
  const horizontal = Math.abs(dx) >= Math.abs(dy);
  if (horizontal && (Math.abs(dx) > stage.w * 0.35 || (Math.abs(vx) > 0.6 && Math.abs(dx) > 30))) {
    return dx > 0 ? 'keep' : 'archive';
  }
  if (!horizontal && dy < -stage.h * 0.25) return 'skip';
  return null;
};
