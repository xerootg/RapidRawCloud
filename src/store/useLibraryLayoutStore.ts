import { create } from 'zustand';

/**
 * Fork library-layout preferences. They are per-device view state, not synced
 * settings, so they live in localStorage rather than AppSettings.
 *
 * `folderSections` decides how the Recursive display mode lays out the grid:
 * one section per folder (upstream behaviour, the default), or every photo below
 * the current folder in one continuous grid in the chosen sort order. The second
 * is the "all photos" timeline a cloud library needs, because synced photos
 * live in deep date folders (`Camera Import/<model>/<yyyy>/<mm>/<dd>`).
 */
const FOLDER_SECTIONS_KEY = 'rrcloud.library.folderSections';

function readFolderSections(): boolean {
  try {
    return localStorage.getItem(FOLDER_SECTIONS_KEY) !== 'off';
  } catch {
    return true;
  }
}

interface LibraryLayoutState {
  folderSections: boolean;
  setFolderSections: (on: boolean) => void;
}

export const useLibraryLayoutStore = create<LibraryLayoutState>((set) => ({
  folderSections: readFolderSections(),
  setFolderSections: (on) => {
    try {
      localStorage.setItem(FOLDER_SECTIONS_KEY, on ? 'on' : 'off');
    } catch {
      // Storage unavailable: the choice still applies for this session.
    }
    set({ folderSections: on });
  },
}));
