/**
 * 941 Analytics - Tracking Script v2.0
 *
 * Privacy-first analytics with session tracking and auto-events.
 * Target size: < 1.5KB gzipped
 */
(function() {
    'use strict';

    // Configuration from script tag data attributes
    var script = document.currentScript;
    var endpoint = script.dataset.endpoint;
    var site = script.dataset.site;

    if (!endpoint || !site) return;

    // Session management
    var SESSION_TIMEOUT = 30 * 60 * 1000; // 30 minutes
    var HEARTBEAT_INTERVAL = 15 * 1000; // 15 seconds
    var sessionId = null;
    var lastActivity = Date.now();
    var scrollDepths = { 25: false, 50: false, 75: false, 100: false };

    // Generate session ID
    function generateId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
    }

    // Get or create session
    function getSession() {
        try {
            var stored = sessionStorage.getItem('_941_session');
            if (stored) {
                var data = JSON.parse(stored);
                if (Date.now() - data.lastActivity < SESSION_TIMEOUT) {
                    sessionId = data.id;
                    lastActivity = Date.now();
                    saveSession();
                    return sessionId;
                }
            }
        } catch (e) {}

        sessionId = generateId();
        saveSession();
        return sessionId;
    }

    function saveSession() {
        try {
            sessionStorage.setItem('_941_session', JSON.stringify({
                id: sessionId,
                lastActivity: lastActivity
            }));
        } catch (e) {}
    }

    // Send beacon
    function send(type, data) {
        if (!sessionId) getSession();

        var payload = Object.assign({
            type: type,
            site: site,
            session_id: sessionId,
            url: location.href,
            referrer: document.referrer || null,
            title: document.title,
            screen_width: window.screen.width,
            screen_height: window.screen.height,
            language: navigator.language,
            timestamp: new Date().toISOString()
        }, data || {});

        // Use sendBeacon for reliability
        if (navigator.sendBeacon) {
            navigator.sendBeacon(endpoint, JSON.stringify(payload));
        } else {
            // Fallback for older browsers
            var xhr = new XMLHttpRequest();
            xhr.open('POST', endpoint, true);
            xhr.setRequestHeader('Content-Type', 'application/json');
            xhr.send(JSON.stringify(payload));
        }
    }

    // Track pageview
    function trackPageview() {
        getSession();
        send('pageview');
    }

    // Track event
    function trackEvent(name, eventType, eventData) {
        send('event', {
            event_name: name,
            event_type: eventType || 'custom',
            event_data: eventData || null
        });
    }

    // Heartbeat for session duration
    function heartbeat() {
        if (document.visibilityState === 'visible') {
            lastActivity = Date.now();
            saveSession();
            send('heartbeat');
        }
    }

    // Scroll depth tracking
    function trackScroll() {
        var scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        var docHeight = document.documentElement.scrollHeight - window.innerHeight;
        var scrollPercent = docHeight > 0 ? Math.round((scrollTop / docHeight) * 100) : 0;

        [25, 50, 75, 100].forEach(function(depth) {
            if (scrollPercent >= depth && !scrollDepths[depth]) {
                scrollDepths[depth] = true;
                trackEvent('scroll_' + depth, 'scroll', { depth: depth });
            }
        });
    }

    // Outbound link tracking
    function trackOutbound(event) {
        var link = event.target.closest('a');
        if (!link) return;

        var href = link.href;
        if (!href) return;

        try {
            var url = new URL(href);
            if (url.hostname !== location.hostname) {
                trackEvent('outbound_click', 'click', {
                    url: href,
                    text: link.textContent.trim().substring(0, 100)
                });
            }
        } catch (e) {}
    }

    // Download tracking
    function trackDownload(event) {
        var link = event.target.closest('a');
        if (!link) return;

        var href = link.href || '';
        var ext = href.split('.').pop().toLowerCase().split('?')[0];
        var downloads = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar', 'gz', 'dmg', 'exe'];

        if (downloads.indexOf(ext) > -1) {
            trackEvent('download', 'click', {
                url: href,
                extension: ext
            });
        }
    }

    // Form submission tracking
    function trackForm(event) {
        var form = event.target;
        if (form.tagName !== 'FORM') return;

        trackEvent('form_submit', 'form', {
            form_id: form.id || null,
            form_name: form.name || null,
            form_action: form.action || null
        });
    }

    // Error tracking
    function trackError(event) {
        trackEvent('js_error', 'error', {
            message: event.message,
            source: event.filename,
            line: event.lineno,
            column: event.colno
        });
    }

    // 404 tracking
    function track404() {
        if (document.title.toLowerCase().indexOf('404') > -1 ||
            document.body.textContent.indexOf('Page not found') > -1) {
            trackEvent('404', 'error', { url: location.href });
        }
    }

    // Initialize
    function init() {
        // Track initial pageview
        trackPageview();

        // Check for 404
        setTimeout(track404, 1000);

        // Set up heartbeat
        setInterval(heartbeat, HEARTBEAT_INTERVAL);

        // Scroll tracking (throttled)
        var scrollTimeout;
        window.addEventListener('scroll', function() {
            if (!scrollTimeout) {
                scrollTimeout = setTimeout(function() {
                    trackScroll();
                    scrollTimeout = null;
                }, 200);
            }
        }, { passive: true });

        // Click tracking for outbound links and downloads
        document.addEventListener('click', function(e) {
            trackOutbound(e);
            trackDownload(e);
        });

        // Form submission tracking
        document.addEventListener('submit', trackForm);

        // Error tracking
        window.addEventListener('error', trackError);

        // Track page visibility changes
        document.addEventListener('visibilitychange', function() {
            if (document.visibilityState === 'visible') {
                lastActivity = Date.now();
                saveSession();
            }
        });

        // Track page unload
        window.addEventListener('pagehide', function() {
            send('session_end');
        });

        // SPA support - track navigation changes
        var pushState = history.pushState;
        history.pushState = function() {
            pushState.apply(history, arguments);
            setTimeout(function() {
                scrollDepths = { 25: false, 50: false, 75: false, 100: false };
                trackPageview();
            }, 100);
        };
    }

    // Expose public API
    window._941 = {
        track: trackEvent,
        pageview: trackPageview
    };

    // Start tracking when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
