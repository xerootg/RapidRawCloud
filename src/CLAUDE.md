# src/ — frontend (React 19 + TypeScript)

Also read the root `CLAUDE.md`.

- Backend calls: add the command name to the `Invokes` enum in `components/ui/AppProperties.tsx`, then
  `invoke(Invokes.X, { camelCaseArgs })`. DTO types for sync live in the same file.
- State: zustand stores in `store/`. Sync UI state is `store/useSyncStore.ts`, fed by the `sync-*`
  listeners in `hooks/useTauriListeners.ts`.
- Fork-owned UI: `components/panel/sync/*`, `hooks/useSync*.ts`, `store/useSyncStore.ts`. Add new fork
  UI as new files there. In upstream components, limit edits to a one-line mount point (an import plus
  a JSX element) and record it in `docs/UPSTREAM_TOUCHES.md`.
- Sync UI must stay hidden on a build without the `sync` feature: gate it on
  `useSyncStore(s => s.available)`, never assume sync commands exist.
- Upstream components use i18next (`useTranslation`, keys in `i18n/locales/en.json`); the fork's sync
  panels currently use English literals. Follow the convention of the file you are editing. If you add
  translation keys, add them to `en.json` and run `npm run i18n:check`.
- Before pushing: `npm run typecheck && npm run lint && npm run format:check` from the repo root.
  Prettier: single quotes, semicolons, trailing commas, width 120.
