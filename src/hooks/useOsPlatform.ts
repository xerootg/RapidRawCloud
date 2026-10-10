import { useMemo } from 'react';
import { platform, type } from '@tauri-apps/plugin-os';

export function useOsPlatform() {
  return useMemo(() => {
    try {
      return platform();
    } catch (_error) {
      return '';
    }
  }, []);
}

/**
 * True inside the Android build. The os plugin's `platform()`/`type()` are the
 * source of truth; the WebView's user agent is the fallback when the plugin's
 * injected globals are not available yet.
 */
export function useIsAndroid() {
  return useMemo(() => {
    try {
      if (platform() === 'android' || type() === 'android') return true;
    } catch (_error) {
      /* fall through */
    }
    return typeof navigator !== 'undefined' && /Android/i.test(navigator.userAgent);
  }, []);
}
