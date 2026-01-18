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
     * Date Picker Component
     * Handles custom date range selection with presets
     */
    Alpine.data('datePickerComponent', () => ({
        customOpen: false,
        startDate: '',
        endDate: '',
        error: '',
        today: new Date().toISOString().split('T')[0],
        activeTab: 'overview',

        init() {
            // Initialize from URL params or data attributes
            const urlParams = new URLSearchParams(window.location.search);
            this.startDate = urlParams.get('start') || this.$el.dataset.start || '';
            this.endDate = urlParams.get('end') || this.$el.dataset.end || '';
            this.activeTab = this.$el.dataset.activeTab || 'overview';
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

            // Build URL with custom dates
            const url = `./partials/${this.activeTab}?period=custom&start=${this.startDate}&end=${this.endDate}`;

            // Update browser URL
            const browserUrl = new URL(window.location.href);
            browserUrl.searchParams.set('period', 'custom');
            browserUrl.searchParams.set('start', this.startDate);
            browserUrl.searchParams.set('end', this.endDate);
            window.history.pushState({}, '', browserUrl);

            // Trigger HTMX request programmatically
            htmx.ajax('GET', url, {
                target: '#main-content',
                swap: 'innerHTML'
            });

            this.customOpen = false;
        }
    }));
});
