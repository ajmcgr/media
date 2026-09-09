type AnalyticsParams = Record<string, unknown>;

declare global {
  interface Window {
    gtag?: (command: "event", eventName: string, params?: AnalyticsParams) => void;
  }
}

export function trackEvent(eventName: string, params: AnalyticsParams = {}) {
  if (typeof window === "undefined" || typeof window.gtag !== "function") return;
  window.gtag("event", eventName, params);
}

/** Records a funnel milestone once per browser session without storing PII. */
export function trackOncePerSession(eventName: string, params: AnalyticsParams = {}, key = eventName) {
  if (typeof window === "undefined") return;
  const storageKey = `mediaai.analytics.${key}`;
  try {
    if (window.sessionStorage.getItem(storageKey)) return;
    window.sessionStorage.setItem(storageKey, "1");
  } catch {
    // Analytics should never prevent a user action when storage is unavailable.
  }
  trackEvent(eventName, params);
}
