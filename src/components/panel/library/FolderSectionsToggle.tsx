import Switch from '../../ui/Switch';
import { LibraryViewMode } from '../../ui/AppProperties';
import { useLibraryLayoutStore } from '../../../store/useLibraryLayoutStore';

/**
 * The fork's "Group by folder" preference (see `useLibraryLayoutStore`): whether
 * the library grid splits a Recursive listing into per-folder sections.
 */
export function useFolderSections(): boolean {
  return useLibraryLayoutStore((s) => s.folderSections);
}

/**
 * The View Options control for that preference, mounted under Display Mode. It
 * only shows in Recursive mode; the Current Folder mode never has sections.
 */
export default function FolderSectionsToggle({ libraryViewMode }: { libraryViewMode: LibraryViewMode }) {
  const folderSections = useLibraryLayoutStore((s) => s.folderSections);
  const setFolderSections = useLibraryLayoutStore((s) => s.setFolderSections);
  if (libraryViewMode !== LibraryViewMode.Recursive) {
    return null;
  }
  return (
    <div className="px-3 mt-3">
      <Switch
        label="Group by folder"
        checked={folderSections}
        onChange={setFolderSections}
        tooltip="Off: every photo below this folder in one grid, in the sort order above"
      />
    </div>
  );
}
