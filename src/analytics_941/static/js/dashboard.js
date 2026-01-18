/**
 * 941 Analytics Dashboard JavaScript
 * Alpine.js components and HTMX integration
 */

// =============================================================================
// TIMEZONE UTILITIES
// =============================================================================

/**
 * Get the site's configured timezone from the body data attribute
 */
function getSiteTimezone() {
    return document.body.dataset.timezone || 'America/New_York';
}

/**
 * Format a UTC timestamp in the site's timezone
 * @param {string|Date} utcTimestamp - UTC timestamp
 * @param {object} options - Intl.DateTimeFormat options
 * @returns {string} Formatted date/time string
 */
function formatInSiteTimezone(utcTimestamp, options = {}) {
    const tz = getSiteTimezone();
    const date = typeof utcTimestamp === 'string' ? new Date(utcTimestamp + 'Z') : utcTimestamp;

    const defaultOptions = {
        timeZone: tz,
        ...options
    };

    return new Intl.DateTimeFormat('en-US', defaultOptions).format(date);
}

/**
 * Format a timestamp for display (short format)
 * @param {string|Date} utcTimestamp - UTC timestamp
 * @returns {string} Formatted time (e.g., "2:30 PM")
 */
function formatTime(utcTimestamp) {
    return formatInSiteTimezone(utcTimestamp, {
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
    });
}

/**
 * Format a timestamp for display (date only)
 * @param {string|Date} utcTimestamp - UTC timestamp
 * @returns {string} Formatted date (e.g., "Jan 15")
 */
function formatDate(utcTimestamp) {
    return formatInSiteTimezone(utcTimestamp, {
        month: 'short',
        day: 'numeric'
    });
}

/**
 * Format a timestamp for display (date and time)
 * @param {string|Date} utcTimestamp - UTC timestamp
 * @returns {string} Formatted datetime (e.g., "Jan 15, 2:30 PM")
 */
function formatDateTime(utcTimestamp) {
    return formatInSiteTimezone(utcTimestamp, {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
    });
}

// Expose timezone utilities globally for use in templates
window._941tz = {
    getSiteTimezone,
    formatInSiteTimezone,
    formatTime,
    formatDate,
    formatDateTime
};

// =============================================================================
// ALPINE.JS COMPONENTS
// =============================================================================

document.addEventListener('alpine:init', () => {
    /**
     * Activity Feed Component
     * Handles real-time activity polling with pause/resume and event filtering
     */
    Alpine.data('activityFeedComponent', () => ({
        paused: false,
        eventTypeFilter: 'all',
        pollInterval: null,
        seenEventIds: new Set(),

        startPolling() {
            // HTMX handles the actual polling via hx-trigger
            // This component just manages the paused state
            this.setupPollingControl();
        },

        setupPollingControl() {
            // Listen for HTMX config request to enable/disable polling
            const container = this.$refs.feedContainer;
            if (container) {
                // Update hx-trigger based on paused state
                this.$watch('paused', (paused) => {
                    if (paused) {
                        // Stop HTMX polling
                        htmx.off(container, 'htmx:configRequest');
                    } else {
                        // Trigger immediate refresh when resuming
                        this.refresh();
                    }
                });
            }
        },

        togglePause() {
            this.paused = !this.paused;
            if (!this.paused) {
                this.refresh();
            }
        },

        refresh() {
            if (this.paused) return;

            const container = document.getElementById('activity-feed-container');
            if (container) {
                let url = './partials/activity-feed';
                if (this.eventTypeFilter && this.eventTypeFilter !== 'all') {
                    url += `?event_type=${encodeURIComponent(this.eventTypeFilter)}`;
                }
                htmx.ajax('GET', url, {
                    target: container,
                    swap: 'innerHTML'
                });
            }
        },

        destroy() {
            if (this.pollInterval) {
                clearInterval(this.pollInterval);
            }
        }
    }));

    /**
     * Date Picker Component
     * Handles custom date range selection with presets, comparison, and keyboard shortcuts
     */
    Alpine.data('datePickerComponent', () => ({
        customOpen: false,
        compareOpen: false,
        startDate: '',
        endDate: '',
        error: '',
        today: new Date().toISOString().split('T')[0],
        activeTab: 'overview',
        compareMode: 'previous',
        showKeyboardHints: false,

        init() {
            // Initialize from URL params or data attributes
            const urlParams = new URLSearchParams(window.location.search);
            this.startDate = urlParams.get('start') || this.$el.dataset.start || '';
            this.endDate = urlParams.get('end') || this.$el.dataset.end || '';
            this.activeTab = this.$el.dataset.activeTab || 'overview';
            this.compareMode = urlParams.get('compare') || this.$el.dataset.compare || 'previous';
        },

        get isValid() {
            if (!this.startDate || !this.endDate) return false;
            return this.startDate <= this.endDate && this.endDate <= this.today;
        },

        setPreset(preset) {
            const today = new Date();
            let start, end;

            switch (preset) {
                case 'yesterday':
                    const yesterday = new Date(today);
                    yesterday.setDate(yesterday.getDate() - 1);
                    start = end = yesterday;
                    break;
                case 'thisWeek':
                    start = new Date(today);
                    start.setDate(today.getDate() - today.getDay());
                    end = today;
                    break;
                case 'lastWeek':
                    end = new Date(today);
                    end.setDate(today.getDate() - today.getDay() - 1);
                    start = new Date(end);
                    start.setDate(end.getDate() - 6);
                    break;
                case 'thisMonth':
                    start = new Date(today.getFullYear(), today.getMonth(), 1);
                    end = today;
                    break;
                case 'lastMonth':
                    start = new Date(today.getFullYear(), today.getMonth() - 1, 1);
                    end = new Date(today.getFullYear(), today.getMonth(), 0);
                    break;
            }

            this.startDate = start.toISOString().split('T')[0];
            this.endDate = end.toISOString().split('T')[0];
            this.error = '';
        },

        applyDates() {
            this.error = '';

            if (!this.startDate || !this.endDate) {
                this.error = 'Please select both start and end dates';
                return;
            }

            if (this.startDate > this.endDate) {
                this.error = 'End date must be after start date';
                return;
            }

            if (this.endDate > this.today) {
                this.error = 'End date cannot be in the future';
                return;
            }

            // Build URL with custom dates and compare mode
            let url = `./partials/${this.activeTab}?period=custom&start=${this.startDate}&end=${this.endDate}`;
            if (this.compareMode !== 'none') {
                url += `&compare=${this.compareMode}`;
            }

            // Update browser URL
            const browserUrl = new URL(window.location.href);
            browserUrl.searchParams.set('period', 'custom');
            browserUrl.searchParams.set('start', this.startDate);
            browserUrl.searchParams.set('end', this.endDate);
            if (this.compareMode !== 'none') {
                browserUrl.searchParams.set('compare', this.compareMode);
            } else {
                browserUrl.searchParams.delete('compare');
            }
            window.history.pushState({}, '', browserUrl);

            // Trigger HTMX request programmatically
            htmx.ajax('GET', url, {
                target: '#main-content',
                swap: 'innerHTML'
            });

            this.customOpen = false;
        },

        applyCompare() {
            // Update URL with compare mode
            const browserUrl = new URL(window.location.href);
            if (this.compareMode !== 'none') {
                browserUrl.searchParams.set('compare', this.compareMode);
            } else {
                browserUrl.searchParams.delete('compare');
            }
            window.history.pushState({}, '', browserUrl);

            // Build partial URL from current params
            const period = browserUrl.searchParams.get('period') || '30d';
            let url = `./partials/${this.activeTab}?period=${period}`;
            if (this.startDate && this.endDate) {
                url += `&start=${this.startDate}&end=${this.endDate}`;
            }
            if (this.compareMode !== 'none') {
                url += `&compare=${this.compareMode}`;
            }

            // Trigger HTMX request
            htmx.ajax('GET', url, {
                target: '#main-content',
                swap: 'innerHTML'
            });

            this.compareOpen = false;
        },

        handleKeyboard(event) {
            const key = event.key;

            // Escape key always closes popups
            if (key === 'Escape') {
                if (this.customOpen || this.compareOpen || this.showKeyboardHints) {
                    event.preventDefault();
                    this.customOpen = false;
                    this.compareOpen = false;
                    this.showKeyboardHints = false;
                    return;
                }
            }

            // Don't trigger shortcuts if user is typing in an input
            if (event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA') {
                return;
            }

            // Don't trigger with modifier keys (except for ?)
            if (event.ctrlKey || event.altKey || event.metaKey) {
                return;
            }

            // Toggle keyboard hints with ?
            if (key === '?' || (event.shiftKey && key === '/')) {
                event.preventDefault();
                this.showKeyboardHints = !this.showKeyboardHints;
                return;
            }

            // Quick period shortcuts
            let period = null;
            switch (key) {
                case '7':
                    period = '7d';
                    break;
                case '3':
                    period = '30d';
                    break;
                case '9':
                    period = '90d';
                    break;
                case '1':
                    period = '24h';
                    break;
                case 'y':
                case 'Y':
                    period = 'year';
                    break;
                case 'a':
                case 'A':
                    period = 'all';
                    break;
            }

            if (period) {
                event.preventDefault();
                this.customOpen = false;
                this.compareOpen = false;

                // Build URL with period
                let url = `./partials/${this.activeTab}?period=${period}`;
                if (this.compareMode !== 'none') {
                    url += `&compare=${this.compareMode}`;
                }

                // Update browser URL
                const browserUrl = new URL(window.location.href);
                browserUrl.searchParams.set('period', period);
                browserUrl.searchParams.delete('start');
                browserUrl.searchParams.delete('end');
                window.history.pushState({}, '', browserUrl);

                // Trigger HTMX request
                htmx.ajax('GET', url, {
                    target: '#main-content',
                    swap: 'innerHTML'
                });
            }
        }
    }));
});

// =============================================================================
// GLOBAL KEYBOARD NAVIGATION
// =============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Global Escape key handler to close any open dropdowns/modals
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            // Find and click any visible close buttons or close dropdown triggers
            const openDropdowns = document.querySelectorAll('.analytics-dropdown.open, [x-show="true"]');
            openDropdowns.forEach(dropdown => {
                // Attempt to close via Alpine.js
                if (dropdown._x_dataStack) {
                    const data = dropdown._x_dataStack[0];
                    if (data && typeof data.customOpen !== 'undefined') {
                        data.customOpen = false;
                    }
                    if (data && typeof data.compareOpen !== 'undefined') {
                        data.compareOpen = false;
                    }
                }
            });
        }
    });

    // Make buttons activatable with Enter and Space keys
    document.addEventListener('keydown', (event) => {
        const target = event.target;

        // Check if target is a button-like element
        if (target.classList.contains('analytics-btn') ||
            target.classList.contains('analytics-nav__link') ||
            target.classList.contains('analytics-chart-toggle') ||
            target.classList.contains('analytics-date-picker-popup__preset')) {

            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                target.click();
            }
        }
    });

    // Trap focus within modals/popups when open
    document.addEventListener('keydown', (event) => {
        if (event.key !== 'Tab') return;

        const activePopup = document.querySelector('.analytics-date-picker-popup:not([style*="display: none"])');
        if (!activePopup) return;

        const focusableElements = activePopup.querySelectorAll(
            'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );

        if (focusableElements.length === 0) return;

        const firstElement = focusableElements[0];
        const lastElement = focusableElements[focusableElements.length - 1];

        if (event.shiftKey && document.activeElement === firstElement) {
            event.preventDefault();
            lastElement.focus();
        } else if (!event.shiftKey && document.activeElement === lastElement) {
            event.preventDefault();
            firstElement.focus();
        }
    });
});
