/**
 * 941 Analytics - TypeScript Definitions
 *
 * Privacy-first analytics for 941 Apps projects.
 * https://github.com/blakecrosley/941analytics
 */

declare global {
  interface Window {
    /**
     * 941 Analytics public API
     * Available after the tracking script loads
     */
    analytics: Analytics941;

    /**
     * Namespaced alias to avoid conflicts
     */
    _941analytics: Analytics941;

    /**
     * Queue for events fired before script loads
     * @internal
     */
    _941q: Analytics941Queue;
  }
}

/**
 * Custom event properties
 * Any JSON-serializable key-value pairs
 */
export interface EventProperties {
  [key: string]: string | number | boolean | null | undefined;
}

/**
 * 941 Analytics API
 */
export interface Analytics941 {
  /**
   * Track a custom event
   *
   * @param eventName - Name of the event (e.g., 'button_click', 'signup_complete')
   * @param properties - Optional properties to attach to the event
   *
   * @example
   * // Simple event
   * analytics.track('newsletter_signup');
   *
   * // Event with properties
   * analytics.track('purchase', {
   *   product_id: 'sku-123',
   *   price: 29.99,
   *   currency: 'USD'
   * });
   */
  track(eventName: string, properties?: EventProperties): void;

  /**
   * Track a pageview manually
   * Useful for SPAs that need to track route changes
   *
   * @param url - Optional URL to track (defaults to current URL)
   * @param title - Optional page title (defaults to document.title)
   *
   * @example
   * // Track current page
   * analytics.page();
   *
   * // Track specific route
   * analytics.page('/dashboard', 'Dashboard - My App');
   */
  page(url?: string, title?: string): void;

  /**
   * Get the current session ID
   * Useful for debugging or correlating with backend logs
   *
   * @returns The session ID string
   *
   * @example
   * const sessionId = analytics.getSessionId();
   * console.log('Session:', sessionId);
   */
  getSessionId(): string;

  /**
   * Identify the current user (optional)
   * The user ID is stored in sessionStorage and included with events
   *
   * Note: This is for your own correlation only.
   * 941 Analytics does not track users across sessions.
   *
   * @param userId - Your application's user identifier
   *
   * @example
   * // After user logs in
   * analytics.identify(user.id);
   */
  identify(userId: string | number): void;
}

/**
 * Queue interface for events fired before script loads
 * @internal
 */
export interface Analytics941Queue {
  push(args: [keyof Analytics941, ...unknown[]]): void;
}

export {};
