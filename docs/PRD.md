# 941 Analytics v2.0 - Implementation PRD

**Owner**: Blake Crosley
**Site**: blakecrosley.com (personal hub)
**Philosophy**: Do it right. Performance is sacred.

---

## Design Principles

1. **Tracking script < 1KB** - Non-blocking, async, invisible to users
2. **Dashboard loads fast** - Static assets cached, heavy components lazy-loaded
3. **Globe is a destination** - Dedicated geography page, not loaded by default
4. **Full auto-tracking** - No manual instrumentation needed
5. **Privacy-first** - No cookies, hashed visitor IDs, GDPR compliant

---

## Feature Set

### Core Metrics
- [x] Page views (human vs bot)
- [x] Unique visitors (daily hash rotation)
- [ ] **Sessions** (NEW) - Group pageviews into sessions
- [ ] **Bounce rate** (NEW) - Single-page sessions < 10s
- [ ] **Time on site** (NEW) - Session duration tracking
- [ ] **Time on page** (NEW) - Per-page duration

### Traffic Sources
- [x] Referrer classification (direct/organic/social/email/referral)
- [x] Top referrers by domain
- [x] UTM campaign tracking
- [ ] **Entry/exit pages** (NEW)

### Demographics
- [x] Country/Region/City (Cloudflare MaxMind)
- [x] Device type (mobile/tablet/desktop)
- [x] Browser & version
- [x] Operating system
- [ ] **Language** (NEW - Accept-Language header)
- [ ] **Screen resolution** (NEW)

### Auto-Tracked Events
- [ ] **Outbound link clicks** (NEW)
- [ ] **File downloads** (NEW - pdf, zip, etc.)
- [ ] **Scroll depth** (NEW - 25%, 50%, 75%, 100%)
- [ ] **Rage clicks** (NEW - 3+ clicks in 500ms on same element)
- [ ] **Form submissions** (NEW)
- [ ] **Video engagement** (NEW - play, 25%, 50%, 75%, complete)
- [ ] **404 errors** (NEW)
- [ ] **JavaScript errors** (NEW)

### Dashboard Features
- [x] Stats cards (views, visitors, bots, live)
- [x] Traffic chart over time
- [ ] **Custom date range picker** (NEW)
- [ ] **Comparison mode** (NEW - this period vs previous)
- [ ] **Filtering** (NEW - by country, device, source, page)
- [ ] **Real-time live view** (NEW - current visitors)
- [ ] **CSV export** (NEW)
- [ ] **Globe on dedicated page** (MOVED)

---

## Architecture

### File Structure
```
src/analytics_941/
├── __init__.py                 # setup_analytics() - public API
├── config.py                   # Settings dataclass
│
├── core/
│   ├── client.py               # D1 database client
│   ├── models.py               # Pydantic models
│   ├── queries.py              # SQL query builders
│   └── stats.py                # Stats computation
│
├── detection/
│   ├── bots.py                 # Bot detection (existing)
│   ├── user_agent.py           # UA parsing (existing)
│   ├── referrer.py             # Referrer classification (existing)
│   └── utm.py                  # UTM parsing (existing)
│
├── auth/
│   ├── passkey.py              # Simple passkey
│   ├── webauthn.py             # WebAuthn/FIDO2
│   └── sessions.py             # Session management
│
├── routes/
│   ├── __init__.py             # create_dashboard_router()
│   ├── dashboard.py            # Main dashboard
│   ├── realtime.py             # Live visitors page
│   ├── sources.py              # Traffic sources page
│   ├── geography.py            # Globe page
│   ├── technology.py           # Browsers/devices page
│   ├── events.py               # Events page
│   ├── api.py                  # JSON API
│   ├── export.py               # CSV export
│   └── auth_routes.py          # Auth endpoints
│
├── templates/
│   ├── base.html               # Shell layout
│   ├── components/
│   │   ├── nav.html            # Tab navigation
│   │   ├── stats_cards.html    # Metric cards
│   │   ├── chart.html          # Bar/line chart
│   │   ├── table.html          # Data table
│   │   ├── date_picker.html    # Date range selector
│   │   └── filters.html        # Filter chips
│   └── pages/
│       ├── dashboard.html      # Overview
│       ├── realtime.html       # Live visitors
│       ├── sources.html        # Traffic sources
│       ├── geography.html      # Globe + map
│       ├── technology.html     # Browsers/devices
│       ├── events.html         # Auto-tracked events
│       └── login.html          # Login page
│
└── static/
    ├── css/
    │   └── analytics.css       # All styles (minified)
    ├── js/
    │   ├── analytics.js        # Dashboard interactivity
    │   └── globe.js            # Three.js globe (lazy)
    └── tracking.js             # Tracking script (< 1KB)
```

### Database Schema (D1)

```sql
-- Core pageview tracking
CREATE TABLE page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- Page info
    url TEXT NOT NULL,
    page_title TEXT,

    -- Session (NEW)
    session_id TEXT NOT NULL,
    visitor_hash TEXT NOT NULL,

    -- Referrer
    referrer TEXT,
    referrer_type TEXT,
    referrer_domain TEXT,

    -- UTM
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_term TEXT,
    utm_content TEXT,

    -- Geography
    country TEXT,
    region TEXT,
    city TEXT,
    latitude REAL,
    longitude REAL,

    -- Technology
    device_type TEXT,
    browser TEXT,
    browser_version TEXT,
    os TEXT,
    os_version TEXT,
    screen_width INTEGER,
    screen_height INTEGER,
    language TEXT,

    -- Bot detection
    is_bot INTEGER DEFAULT 0,
    bot_name TEXT,
    bot_category TEXT
);

-- Session tracking (NEW)
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    session_id TEXT NOT NULL UNIQUE,
    visitor_hash TEXT NOT NULL,

    started_at DATETIME NOT NULL,
    last_activity_at DATETIME NOT NULL,
    ended_at DATETIME,

    duration_seconds INTEGER DEFAULT 0,
    pageview_count INTEGER DEFAULT 1,
    event_count INTEGER DEFAULT 0,

    is_bounce INTEGER DEFAULT 1,

    entry_page TEXT,
    exit_page TEXT,

    -- Attribution (first touch)
    referrer_type TEXT,
    referrer_domain TEXT,
    utm_source TEXT,
    utm_campaign TEXT,

    -- Demographics
    country TEXT,
    region TEXT,
    device_type TEXT,
    browser TEXT,
    os TEXT
);

-- Auto-tracked events (NEW)
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    session_id TEXT NOT NULL,
    visitor_hash TEXT NOT NULL,

    event_type TEXT NOT NULL,  -- click, scroll, form, video, error, etc.
    event_name TEXT NOT NULL,  -- outbound_click, scroll_50, form_submit, etc.
    event_data TEXT,           -- JSON: {url, element, depth, error_message, etc.}

    page_url TEXT,

    -- Demographics for filtering
    country TEXT,
    device_type TEXT
);

-- Hourly aggregates (NEW - for faster queries)
CREATE TABLE hourly_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    hour DATETIME NOT NULL,  -- Rounded to hour

    views INTEGER DEFAULT 0,
    visitors INTEGER DEFAULT 0,
    sessions INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    bot_views INTEGER DEFAULT 0,

    total_duration_seconds INTEGER DEFAULT 0,

    UNIQUE(site, hour)
);

-- Daily aggregates (enhanced)
CREATE TABLE daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    date DATE NOT NULL,

    -- Core metrics
    views INTEGER DEFAULT 0,
    visitors INTEGER DEFAULT 0,
    sessions INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    bot_views INTEGER DEFAULT 0,

    -- Duration
    total_duration_seconds INTEGER DEFAULT 0,
    avg_session_duration REAL DEFAULT 0,

    -- Events
    total_events INTEGER DEFAULT 0,
    outbound_clicks INTEGER DEFAULT 0,
    scroll_depths TEXT,  -- JSON: {25: n, 50: n, 75: n, 100: n}

    -- Aggregated dimensions (JSON)
    top_pages TEXT,
    entry_pages TEXT,
    exit_pages TEXT,
    top_referrers TEXT,
    referrer_types TEXT,
    countries TEXT,
    regions TEXT,
    devices TEXT,
    browsers TEXT,
    operating_systems TEXT,
    languages TEXT,
    utm_sources TEXT,
    utm_campaigns TEXT,
    event_breakdown TEXT,

    UNIQUE(site, date)
);

-- Indexes for performance
CREATE INDEX idx_pv_site_ts ON page_views(site, timestamp);
CREATE INDEX idx_pv_session ON page_views(site, session_id);
CREATE INDEX idx_sessions_site_started ON sessions(site, started_at);
CREATE INDEX idx_sessions_visitor ON sessions(site, visitor_hash);
CREATE INDEX idx_events_site_ts ON events(site, timestamp);
CREATE INDEX idx_events_type ON events(site, event_type, timestamp);
CREATE INDEX idx_hourly_site ON hourly_stats(site, hour);
CREATE INDEX idx_daily_site ON daily_stats(site, date);
```

### Tracking Script (< 1KB minified)

```javascript
// analytics-941 tracking script v2
(function() {
    const W = window, D = document, L = location, N = navigator;
    const ENDPOINT = '{{WORKER_URL}}';
    const SITE = '{{SITE_NAME}}';

    // Session management (30 min timeout)
    const SESSION_KEY = '_941s';
    const SESSION_TIMEOUT = 30 * 60 * 1000;

    function getSession() {
        let s = sessionStorage.getItem(SESSION_KEY);
        if (s) {
            s = JSON.parse(s);
            if (Date.now() - s.t < SESSION_TIMEOUT) {
                s.t = Date.now();
                sessionStorage.setItem(SESSION_KEY, JSON.stringify(s));
                return s.id;
            }
        }
        // New session
        s = { id: Math.random().toString(36).substr(2, 9), t: Date.now() };
        sessionStorage.setItem(SESSION_KEY, JSON.stringify(s));
        return s.id;
    }

    // Core tracking
    function track(type, data = {}) {
        const payload = {
            site: SITE,
            type: type,
            url: L.pathname,
            title: D.title,
            ref: D.referrer ? new URL(D.referrer).hostname : '',
            sid: getSession(),
            sw: screen.width,
            sh: screen.height,
            lang: N.language,
            ...data
        };

        // Add UTM params on pageview
        if (type === 'pv') {
            const p = new URLSearchParams(L.search);
            ['source','medium','campaign','term','content'].forEach(k => {
                const v = p.get('utm_' + k);
                if (v) payload['utm_' + k] = v;
            });
        }

        // Send via beacon (non-blocking)
        const url = ENDPOINT + '/collect?' + new URLSearchParams(payload);
        if (N.sendBeacon) {
            N.sendBeacon(url);
        } else {
            new Image().src = url;
        }
    }

    // Pageview tracking
    let lastPath = '';
    function trackPageview() {
        if (L.pathname === lastPath) return;
        lastPath = L.pathname;
        track('pv');
    }

    // SPA support
    const _push = history.pushState;
    history.pushState = function() {
        _push.apply(history, arguments);
        trackPageview();
    };
    W.addEventListener('popstate', trackPageview);

    // Time on page (heartbeat every 15s while visible)
    let pageTime = 0;
    let heartbeatInterval;

    function startHeartbeat() {
        heartbeatInterval = setInterval(() => {
            if (!D.hidden) {
                pageTime += 15;
                track('hb', { t: pageTime });
            }
        }, 15000);
    }

    function stopHeartbeat() {
        clearInterval(heartbeatInterval);
        if (pageTime > 0) {
            track('leave', { t: pageTime });
        }
    }

    D.addEventListener('visibilitychange', () => {
        if (D.hidden) {
            stopHeartbeat();
        } else {
            startHeartbeat();
        }
    });

    W.addEventListener('beforeunload', stopHeartbeat);

    // Auto-track: Outbound links
    D.addEventListener('click', e => {
        const a = e.target.closest('a[href]');
        if (!a) return;

        try {
            const url = new URL(a.href);
            if (url.hostname !== L.hostname) {
                track('ev', {
                    n: 'outbound_click',
                    d: JSON.stringify({ url: a.href, text: a.innerText.slice(0, 100) })
                });
            }
        } catch {}
    });

    // Auto-track: Downloads
    D.addEventListener('click', e => {
        const a = e.target.closest('a[href]');
        if (!a) return;

        const ext = a.href.split('.').pop().toLowerCase();
        if (['pdf','zip','doc','docx','xls','xlsx','ppt','pptx','mp3','mp4'].includes(ext)) {
            track('ev', {
                n: 'download',
                d: JSON.stringify({ url: a.href, type: ext })
            });
        }
    });

    // Auto-track: Scroll depth
    let maxScroll = 0;
    const scrollThresholds = [25, 50, 75, 100];
    let trackedDepths = new Set();

    function trackScroll() {
        const scrollTop = W.scrollY;
        const docHeight = D.documentElement.scrollHeight - W.innerHeight;
        const percent = Math.round((scrollTop / docHeight) * 100);

        if (percent > maxScroll) {
            maxScroll = percent;
            scrollThresholds.forEach(t => {
                if (percent >= t && !trackedDepths.has(t)) {
                    trackedDepths.add(t);
                    track('ev', { n: 'scroll_' + t, d: JSON.stringify({ depth: t }) });
                }
            });
        }
    }

    let scrollTimeout;
    W.addEventListener('scroll', () => {
        clearTimeout(scrollTimeout);
        scrollTimeout = setTimeout(trackScroll, 150);
    }, { passive: true });

    // Auto-track: Rage clicks (3+ clicks in 500ms)
    let clickTimes = [];
    let lastClickTarget = null;

    D.addEventListener('click', e => {
        const now = Date.now();
        const target = e.target;

        if (target === lastClickTarget) {
            clickTimes.push(now);
            clickTimes = clickTimes.filter(t => now - t < 500);

            if (clickTimes.length >= 3) {
                track('ev', {
                    n: 'rage_click',
                    d: JSON.stringify({
                        element: target.tagName,
                        class: target.className,
                        text: target.innerText?.slice(0, 50)
                    })
                });
                clickTimes = [];
            }
        } else {
            lastClickTarget = target;
            clickTimes = [now];
        }
    });

    // Auto-track: Form submissions
    D.addEventListener('submit', e => {
        const form = e.target;
        track('ev', {
            n: 'form_submit',
            d: JSON.stringify({
                id: form.id,
                action: form.action,
                fields: form.elements.length
            })
        });
    });

    // Auto-track: JS errors
    W.addEventListener('error', e => {
        track('ev', {
            n: 'js_error',
            d: JSON.stringify({
                message: e.message?.slice(0, 200),
                file: e.filename,
                line: e.lineno
            })
        });
    });

    // Auto-track: 404 pages
    if (D.title.toLowerCase().includes('404') ||
        D.body?.innerText?.toLowerCase().includes('page not found')) {
        track('ev', { n: '404', d: JSON.stringify({ url: L.pathname }) });
    }

    // Initialize
    trackPageview();
    startHeartbeat();

})();
```

**Minified size target**: < 1.5KB gzipped

---

## Implementation Order

### Phase 1: Foundation (Do First)
1. Create new file structure
2. Extract templates from routes.py
3. Set up static asset serving with fingerprinting
4. Create base.html with HTMX/Alpine
5. Migrate existing dashboard to templates

### Phase 2: Session & Duration
1. Update tracking script with session management
2. Add heartbeat endpoint to Worker
3. Create sessions table and aggregation
4. Display bounce rate & time on site

### Phase 3: Auto-Event Tracking
1. Add event tracking to script (all auto events)
2. Create events table
3. Add events endpoint to Worker
4. Build events dashboard page

### Phase 4: Enhanced Dashboard
1. Custom date range picker
2. Comparison mode
3. Filtering by dimension
4. Real-time page with SSE

### Phase 5: Globe & Geography
1. Move globe to dedicated `/geography` page
2. Lazy load Three.js only on that page
3. Add 2D map fallback for quick view
4. Region/city drill-down

### Phase 6: Export & Polish
1. CSV export endpoints
2. API documentation
3. Performance optimization
4. Mobile responsive fixes

---

## Performance Budgets

| Asset | Target | Notes |
|-------|--------|-------|
| Tracking script | < 1.5KB gzip | Non-blocking, async |
| Dashboard CSS | < 10KB gzip | Single file |
| Dashboard JS | < 5KB gzip | Excludes globe |
| Globe JS | < 150KB gzip | Lazy loaded |
| Initial HTML | < 15KB gzip | HTMX partial updates |
| API response | < 50ms | Cached aggregates |

---

## Success Criteria

- [ ] Lighthouse performance score > 95 on tracked pages
- [ ] Dashboard loads in < 1 second
- [ ] Globe page loads in < 2 seconds (with lazy load)
- [ ] All auto-events tracking without configuration
- [ ] Bounce rate and time on site displayed
- [ ] Custom date ranges working
- [ ] CSV export functional
