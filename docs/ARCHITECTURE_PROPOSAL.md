# 941 Analytics: Architecture Redesign Proposal

**Date**: 2026-01-14
**Status**: Draft for Review

---

## Executive Summary

This document proposes a comprehensive redesign of 941 Analytics to match modern privacy-first analytics standards (Plausible, Fathom, Simple Analytics) while maintaining our unique strengths: self-hosted, Cloudflare-native, and zero-dependency.

---

## Part 1: Current State Analysis

### What We Have (Working Well)

| Component | Status | Notes |
|-----------|--------|-------|
| Privacy-first collection | ✅ Excellent | No cookies, no IPs, daily-rotating visitor hash |
| Bot detection | ✅ Good | 100+ patterns, 11 categories |
| Geographic tracking | ✅ Good | Country/region/city via Cloudflare |
| UTM attribution | ✅ Good | Full campaign tracking |
| Referrer classification | ✅ Good | Direct/organic/social/email/referral |
| WebAuthn authentication | ✅ Good | Passkey support |
| 3D Globe visualization | ✅ Unique | Three.js with drill-down |

### Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Browser (Client)                          │
├─────────────────────────────────────────────────────────────────┤
│  Tracking Script (700 bytes)                                     │
│  └─> GET /collect?site=...&url=...&ref=...                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Cloudflare Worker (Edge)                       │
├─────────────────────────────────────────────────────────────────┤
│  • Rate limiting (KV)                                            │
│  • Origin validation                                             │
│  • Bot detection                                                 │
│  • UA parsing                                                    │
│  • Geolocation (CF headers)                                      │
│  • Visitor hash generation                                       │
│  └─> INSERT INTO page_views                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      D1 Database (SQLite)                        │
├─────────────────────────────────────────────────────────────────┤
│  page_views (raw events)     │  daily_stats (aggregates)        │
│  passkeys, sessions          │  challenges                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   FastAPI Dashboard (Python)                     │
├─────────────────────────────────────────────────────────────────┤
│  routes.py (2,597 lines!)                                        │
│  • All HTML inline                                               │
│  • All CSS inline                                                │
│  • All JavaScript inline (Three.js globe, charts)                │
│  • Authentication logic                                          │
│  • API endpoints                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Critical Problems

#### 1. **Monolithic Dashboard File** (2,597 lines)
- All HTML, CSS, JavaScript embedded in Python
- No separation of concerns
- Impossible to maintain or extend
- No caching of static assets
- Every page load re-renders everything

#### 2. **Performance Issues**
- Three.js + TopoJSON loaded on every dashboard visit (~500KB)
- No lazy loading
- No code splitting
- No static asset caching
- CSS/JS not minified or fingerprinted

#### 3. **Missing Core Features** (vs Plausible/Fathom)

| Feature | Plausible | Fathom | 941 Analytics |
|---------|-----------|--------|---------------|
| Custom date ranges | ✅ | ✅ | ❌ |
| Real-time live view | ✅ | ✅ | ⚠️ Polling only |
| Goals/Conversions | ✅ | ✅ | ❌ |
| Event tracking | ✅ | ✅ | ❌ |
| Bounce rate | ✅ | ✅ | ❌ |
| Session duration | ✅ | ✅ | ❌ |
| Comparison periods | ✅ | ✅ | ❌ |
| CSV export | ✅ | ✅ | ❌ |
| Email reports | ✅ | ✅ | ❌ |
| Filtering (country, device) | ✅ | ✅ | ❌ |
| API access | ✅ | ✅ | ⚠️ Basic |
| Public dashboards | ✅ | ✅ | ❌ |
| Funnels | ✅ | ❌ | ❌ |

#### 4. **Data Model Limitations**
- No session concept (can't calculate bounce rate, time on site)
- Daily aggregation only (no hourly granularity)
- No event tracking schema
- No goals/conversions table

---

## Part 2: Proposed Architecture

### New File Structure

```
941analytics/
├── src/analytics_941/
│   ├── __init__.py              # Public API: setup_analytics()
│   ├── config.py                # Configuration dataclass
│   │
│   ├── core/                    # Core business logic
│   │   ├── client.py            # D1 database client
│   │   ├── models.py            # Pydantic models
│   │   ├── queries.py           # SQL query builders
│   │   └── aggregations.py      # Stats computation
│   │
│   ├── detection/               # Traffic analysis
│   │   ├── bots.py              # Bot detection
│   │   ├── user_agent.py        # Browser/OS parsing
│   │   ├── referrer.py          # Referrer classification
│   │   └── utm.py               # Campaign parsing
│   │
│   ├── auth/                    # Authentication
│   │   ├── passkey.py           # Simple passkey auth
│   │   ├── webauthn.py          # WebAuthn/FIDO2
│   │   └── sessions.py          # Session management
│   │
│   ├── routes/                  # FastAPI routers (SEPARATED)
│   │   ├── __init__.py          # Router factory
│   │   ├── dashboard.py         # Main dashboard page
│   │   ├── api.py               # JSON API endpoints
│   │   ├── auth.py              # Auth endpoints
│   │   └── export.py            # CSV/data export
│   │
│   ├── templates/               # Jinja2 templates (NEW)
│   │   ├── base.html            # Base layout
│   │   ├── dashboard.html       # Main dashboard
│   │   ├── login.html           # Login page
│   │   ├── components/          # Reusable partials
│   │   │   ├── stats_card.html
│   │   │   ├── chart.html
│   │   │   ├── table.html
│   │   │   ├── globe.html
│   │   │   └── filters.html
│   │   └── pages/
│   │       ├── realtime.html
│   │       ├── sources.html
│   │       ├── geography.html
│   │       ├── technology.html
│   │       └── events.html
│   │
│   └── static/                  # Static assets (NEW)
│       ├── css/
│       │   ├── dashboard.css    # Main styles
│       │   └── components.css   # Component styles
│       ├── js/
│       │   ├── dashboard.js     # Core interactivity
│       │   ├── charts.js        # Chart rendering
│       │   └── globe.js         # 3D globe (lazy loaded)
│       └── vendor/
│           └── (htmx, alpine - optionally bundled)
│
├── worker/                      # Cloudflare Worker
│   ├── src/
│   │   ├── index.ts             # Main entry
│   │   ├── collect.ts           # Collection endpoint
│   │   ├── events.ts            # Event tracking (NEW)
│   │   ├── realtime.ts          # Real-time endpoint (NEW)
│   │   └── scheduled.ts         # Cron jobs
│   ├── schema.sql               # D1 schema
│   └── wrangler.toml
│
└── docs/
    ├── ARCHITECTURE.md
    ├── API.md
    └── DEPLOYMENT.md
```

### New Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      Browser (Enhanced)                          │
├─────────────────────────────────────────────────────────────────┤
│  Tracking Script v2 (~1KB)                                       │
│  ├─> Pageview: GET /collect                                      │
│  ├─> Event: POST /event (button clicks, form submits)            │
│  ├─> Session: heartbeat every 30s (for duration)                 │
│  └─> Exit: sendBeacon on page unload                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               Cloudflare Worker (Enhanced Edge)                  │
├─────────────────────────────────────────────────────────────────┤
│  /collect     → Page views (existing)                            │
│  /event       → Custom events (NEW)                              │
│  /heartbeat   → Session duration updates (NEW)                   │
│  /realtime    → Current visitor count (NEW - Durable Objects?)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    D1 Database (Enhanced)                        │
├─────────────────────────────────────────────────────────────────┤
│  page_views       │  sessions (NEW)      │  events (NEW)         │
│  daily_stats      │  goals (NEW)         │  hourly_stats (NEW)   │
│  passkeys         │  auth_sessions       │  challenges           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                FastAPI Dashboard (Modular)                       │
├─────────────────────────────────────────────────────────────────┤
│  /admin/analytics/                  → Dashboard overview         │
│  /admin/analytics/realtime          → Live visitors (NEW)        │
│  /admin/analytics/sources           → Traffic sources detail     │
│  /admin/analytics/geography         → Map & regions              │
│  /admin/analytics/technology        → Browsers, devices, OS      │
│  /admin/analytics/events            → Custom events (NEW)        │
│  /admin/analytics/api/...           → JSON API                   │
│  /admin/analytics/export/...        → CSV downloads (NEW)        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Part 3: Feature PRDs

### PRD 1: Modular Dashboard Architecture

**Goal**: Split monolithic routes.py into maintainable components.

**Deliverables**:
1. Separate Jinja2 templates (base, components, pages)
2. Static CSS/JS files with fingerprinting
3. Lazy-load Three.js globe only when needed
4. HTMX-powered navigation (no full page reloads)

**Performance Target**:
- Initial dashboard load: < 100KB (excluding globe)
- Time to interactive: < 1 second
- Globe lazy load: triggered on geography tab click

**Files to Create**:
- `templates/base.html` - Shell with nav, includes HTMX/Alpine
- `templates/dashboard.html` - Overview page
- `templates/components/*.html` - Reusable partials
- `static/css/dashboard.css` - Extracted and minified
- `static/js/charts.js` - Lightweight chart library

---

### PRD 2: Session Tracking (Bounce Rate + Duration)

**Goal**: Enable bounce rate and average session duration metrics.

**How It Works**:
```
1. Pageview → Generate session_id (hash of visitor_hash + timestamp rounded to 30min)
2. Heartbeat → POST /heartbeat every 30s while page visible
3. Exit → sendBeacon on beforeunload with final duration
4. Bounce = session with only 1 pageview and duration < 10s
```

**Schema Changes**:
```sql
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY,
  site TEXT,
  session_id TEXT,  -- Hash, not trackable
  visitor_hash TEXT,
  started_at DATETIME,
  ended_at DATETIME,
  duration_seconds INTEGER,
  pageview_count INTEGER,
  is_bounce INTEGER,
  entry_page TEXT,
  exit_page TEXT,
  country TEXT,
  device_type TEXT
);
```

**Privacy Consideration**: Session ID rotates with visitor hash (daily).

---

### PRD 3: Custom Event Tracking

**Goal**: Track button clicks, form submissions, downloads.

**Tracking Script Addition**:
```javascript
// Manual event tracking
analytics.event('signup_click', { plan: 'pro' });

// Auto-track forms
analytics.trackForms();

// Auto-track outbound links
analytics.trackOutbound();
```

**Schema**:
```sql
CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  site TEXT,
  timestamp DATETIME,
  session_id TEXT,
  visitor_hash TEXT,
  event_name TEXT,
  event_data TEXT,  -- JSON
  page_url TEXT,
  country TEXT,
  device_type TEXT
);
```

**Dashboard**: Events page with:
- Event counts over time
- Top events table
- Event properties breakdown

---

### PRD 4: Goals & Conversions

**Goal**: Define success metrics and track conversion rates.

**How It Works**:
```
Goal Types:
1. Pageview goal: Visit specific URL (e.g., /thank-you)
2. Event goal: Fire specific event (e.g., signup_complete)
3. Revenue goal: Event with revenue property
```

**Schema**:
```sql
CREATE TABLE goals (
  id INTEGER PRIMARY KEY,
  site TEXT,
  name TEXT,
  goal_type TEXT,  -- pageview, event, revenue
  match_pattern TEXT,  -- URL pattern or event name
  created_at DATETIME
);

CREATE TABLE conversions (
  id INTEGER PRIMARY KEY,
  site TEXT,
  goal_id INTEGER,
  timestamp DATETIME,
  session_id TEXT,
  visitor_hash TEXT,
  revenue REAL,
  page_url TEXT,
  referrer_type TEXT
);
```

**Dashboard**: Conversion funnel visualization, conversion rate by source.

---

### PRD 5: Custom Date Ranges & Comparison

**Goal**: Select any date range and compare to previous period.

**UI**:
```
[Today] [7D] [30D] [90D] [Custom ▼]
                         ├─> Date picker
                         └─> Compare to previous period ☑
```

**API Change**:
```
GET /api/stats?start=2026-01-01&end=2026-01-14&compare=true
```

**Response**:
```json
{
  "current": { "views": 1000, "visitors": 500 },
  "previous": { "views": 800, "visitors": 400 },
  "change": { "views": "+25%", "visitors": "+25%" }
}
```

---

### PRD 6: Filtering & Drill-Down

**Goal**: Filter dashboard by any dimension.

**Filter Dimensions**:
- Country / Region / City
- Device type (mobile/tablet/desktop)
- Browser / OS
- Referrer type / specific referrer
- UTM source / campaign
- Entry page

**UI**: Chips above dashboard, click to filter, combine multiple.

**API**:
```
GET /api/stats?country=US&device=mobile&period=7d
```

---

### PRD 7: Real-Time Dashboard

**Goal**: See current visitors with live updates.

**Implementation Options**:

1. **Polling (Simple)**: HTMX `hx-trigger="every 5s"`
2. **Server-Sent Events (Better)**: `/api/realtime/stream`
3. **Durable Objects (Best)**: Cloudflare real-time state

**Real-Time Display**:
```
┌─────────────────────────────────────────┐
│  🟢 12 visitors online now              │
├─────────────────────────────────────────┤
│  Page                    │ Visitors     │
│  /blog/analytics-guide   │ 5            │
│  /                       │ 4            │
│  /pricing                │ 3            │
├─────────────────────────────────────────┤
│  Country   │ Device   │ Source          │
│  🇺🇸 US 8   │ 📱 6    │ Google 5        │
│  🇬🇧 UK 3   │ 💻 6    │ Direct 4        │
│  🇩🇪 DE 1   │          │ Twitter 3      │
└─────────────────────────────────────────┘
```

---

### PRD 8: Data Export & API

**Goal**: Full data access for power users.

**Export Endpoints**:
```
GET /export/pageviews.csv?period=30d
GET /export/events.csv?period=30d
GET /export/conversions.csv?period=30d
```

**Public API** (token-authenticated):
```
GET /api/v1/stats?period=7d
GET /api/v1/pages?period=7d&limit=100
GET /api/v1/sources?period=7d
GET /api/v1/countries?period=7d
GET /api/v1/events?period=7d
GET /api/v1/realtime
```

**Rate Limit**: 100 requests/minute per API key.

---

### PRD 9: Email Reports

**Goal**: Weekly/monthly summary emails.

**Implementation**: Cloudflare Worker scheduled job → SendGrid/Resend.

**Report Contents**:
- Total views & visitors (with % change)
- Top 5 pages
- Top 3 sources
- Geographic highlights
- Goal conversions (if configured)

**Configuration**: Per-site settings in D1.

---

## Part 4: Implementation Phases

### Phase 1: Architecture Refactor (Foundation)
**Priority**: Critical
**Effort**: 2-3 days

1. Extract templates from routes.py
2. Create static CSS/JS files
3. Set up asset fingerprinting
4. Implement HTMX navigation
5. Lazy-load globe

**Result**: Same features, cleaner code, faster loading.

---

### Phase 2: Custom Date Ranges + Filtering
**Priority**: High
**Effort**: 1-2 days

1. Date picker component
2. Filter chips UI
3. Query builder for filters
4. Comparison mode

**Result**: Much more useful dashboard.

---

### Phase 3: Session Tracking
**Priority**: High
**Effort**: 2 days

1. Session schema + Worker changes
2. Heartbeat endpoint
3. Bounce rate calculation
4. Duration display

**Result**: Key missing metrics.

---

### Phase 4: Event Tracking + Goals
**Priority**: Medium
**Effort**: 2-3 days

1. Event tracking script
2. Events table + API
3. Goals configuration UI
4. Conversion tracking

**Result**: Actionable analytics.

---

### Phase 5: Real-Time + Export
**Priority**: Medium
**Effort**: 1-2 days

1. Real-time page with SSE
2. CSV export endpoints
3. Public API with auth

**Result**: Complete feature parity.

---

### Phase 6: Email Reports (Optional)
**Priority**: Low
**Effort**: 1 day

1. Report template
2. Scheduled Worker job
3. Email configuration

**Result**: Passive monitoring.

---

## Part 5: Open Questions

Before proceeding, I need your input on:

### 1. **Scope: Personal vs Multi-Tenant**
Is this for your personal sites only, or should it be a proper multi-tenant SaaS that others could use?
- Personal: Simpler auth, single dashboard
- Multi-tenant: User accounts, API keys, billing integration

### 2. **Globe Visualization**
The 3D globe is unique but heavy (~500KB). Options:
- A) Keep it, lazy-load on geography tab
- B) Replace with lightweight 2D map (Leaflet, ~40KB)
- C) Make it optional/toggleable

### 3. **Real-Time Complexity**
How important is true real-time (< 1s latency)?
- A) Polling every 5-10s is fine
- B) Need SSE/WebSocket for instant updates
- C) Durable Objects for shared state (most complex)

### 4. **Event Tracking Scope**
How much auto-tracking should we do?
- A) Manual only: `analytics.event('name')`
- B) Auto-track forms and outbound links
- C) Full: rage clicks, scroll depth, video plays

### 5. **Hosting Templates**
Where should dashboard templates live?
- A) Bundled in Python package (current approach)
- B) Separate static site (hosted on Cloudflare Pages)
- C) Both: embedded default, customizable override

---

## References

- [Privacy-Focused Analytics Comparison](https://userbird.com/blog/privacy-focused-analytics)
- [Plausible vs Fathom vs DataSag](https://www.datasag.com/blog/analytics-tools-compared-plausible-fathom-datasag-ga4)
- [Dashboard Design Principles 2025](https://www.uxpin.com/studio/blog/dashboard-design-principles/)
- [Web Analytics Dashboard Best Practices](https://improvado.io/blog/web-analytics-dashboard)
- [Fathom Analytics Features](https://usefathom.com/features)
- [Plausible Documentation](https://plausible.io/docs)
