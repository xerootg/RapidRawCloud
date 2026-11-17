import { create } from 'zustand';
import { CullDecision, CullSnapshot } from '../types/quickCull';

/** One reversible step: the exact pre-decision values, so undo restores bit-for-bit. */
export interface UndoStep {
  path: string;
  index: number;
  decision: CullDecision;
  before: CullSnapshot;
}

interface QuickCullState {
  isOpen: boolean;
  /** Snapshot of the queue taken at open, so decided images never reshuffle under the user's thumb. */
  queue: string[];
  index: number;
  undo: UndoStep[];
  kept: number;
  archived: number;
  /** Focus-compare pins (max 3), flipped at a shared zoom/pan point. */
  pinned: string[];
  compareIndex: number;
  open: (queue: string[]) => void;
  close: () => void;
  set: (partial: Partial<QuickCullState> | ((s: QuickCullState) => Partial<QuickCullState>)) => void;
}

export const MAX_PINS = 3;

export const useQuickCullStore = create<QuickCullState>((set) => ({
  isOpen: false,
  queue: [],
  index: 0,
  undo: [],
  kept: 0,
  archived: 0,
  pinned: [],
  compareIndex: 0,
  open: (queue) => set({ isOpen: true, queue, index: 0, undo: [], kept: 0, archived: 0, pinned: [], compareIndex: 0 }),
  close: () => set({ isOpen: false, pinned: [], compareIndex: 0 }),
  set: (partial) => set(partial),
}));
