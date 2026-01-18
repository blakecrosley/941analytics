# 941 Analytics

Privacy-first analytics for 941 Apps projects. No cookies, no fingerprinting, no consent banners needed.

## Features

- **Privacy by design**: No cookies, no IP storage, no fingerprinting
- **Daily visitor counting**: Uses rotating hashes (can't track across days)
- **Cloudflare-powered**: Edge collection via Workers, SQLite storage via D1
- **FastAPI integration**: Drop-in dashboard routes
- **Multi-site support**: One Worker serves all your projects
- **SPA navigation**: Tracks pushState/replaceState for HTMX and SPAs
- **UTM attribution**: Captures campaign parameters automatically
- **Interactive globe**: 3D visualization with country → state → city drill-down
- **Auto-tracked events**: Scroll depth, outbound clicks, downloads, forms, JS errors
- **Custom events API**: Track your own events with `analytics.track()`

## What We Track

| Data | Source | Privacy |
|------|--------|---------|
| Page URL | Request | Path only, no query params |
| Page title | JavaScript | Document title |
| Referrer | Header | Domain only |
| Country/Region/City | Cloudflare | MaxMind geolocation, no IP stored |
| Device type | Viewport | mobile/tablet/desktop |
| Browser/OS | User-Agent | Parsed on server, UA not stored |
| UTM parameters | Query string | Campaign attribution |
| Visitor hash | Computed | Rotates daily, can't identify individuals |

**NOT collected**: IP addresses, cookies, user-agent strings, device IDs, personal data.

## Installation

### 1. Install the Python package

```bash
pip install git+https://github.com/blakecrosley/941analytics.git
```

### 2. Set up Cloudflare infrastructure

```bash
cd worker

# Create D1 database
wrangler d1 create 941-analytics

# Update wrangler.toml with the database_id

# Initialize schema
wrangler d1 execute 941-analytics --file=./schema.sql

# Set the analytics secret
wrangler secret put ANALYTICS_SECRET

# Deploy worker
npm install
npm run deploy
```

### 3. Integrate into your FastAPI app

```python
from analytics_941 import setup_analytics

analytics = setup_analytics(
    site_name="941return.com",
    worker_url="https://941-analytics.941apps.workers.dev",
    d1_database_id="your-d1-database-id",
    cf_account_id="your-cloudflare-account-id",
    cf_api_token="your-cloudflare-api-token",
)

# Add dashboard routes (protected by your auth)
app.include_router(
    analytics.dashboard_router,
    prefix="/admin/analytics",
    dependencies=[Depends(require_admin)]  # your auth
)
```

### 4. Add tracking script to your templates

In your Jinja2 base template:

```jinja2
{# Before </body> #}
{{ analytics.tracking_script() | safe }}
```

## Custom Events

Track custom events from your application code using the JavaScript API.

### Basic Usage

```javascript
// Track a simple event
analytics.track('button_click');

// Track an event with properties
analytics.track('signup_complete', {
  plan: 'pro',
  source: 'landing_page'
});

// Track a purchase
analytics.track('purchase', {
  product_id: 'sku-123',
  price: 29.99,
  currency: 'USD'
});
```

### API Methods

```javascript
// Track custom event
analytics.track(eventName, properties?)

// Track pageview manually (for SPAs)
analytics.page(url?, title?)

// Get current session ID
analytics.getSessionId()

// Identify user (optional, for your own correlation)
analytics.identify(userId)
```

### Queue for Early Events

If you need to track events before the script loads, use the queue:

```html
<script>
  window._941q = window._941q || [];
  window._941q.push(['track', 'early_event', { timing: 'before_load' }]);
</script>
```

Events in the queue are processed automatically when the script loads.

### TypeScript Support

Type definitions are available at `types/analytics.d.ts`:

```typescript
// Reference the types
/// <reference path="node_modules/941analytics/types/analytics.d.ts" />

// Full type safety
analytics.track('purchase', {
  product_id: 'sku-123',
  price: 29.99
});
```

### Auto-Tracked Events

The following events are tracked automatically (no code required):

| Event | Type | Data Captured |
|-------|------|---------------|
| `scroll_25`, `scroll_50`, etc. | scroll | Scroll depth milestones |
| `outbound_click` | click | Destination URL, link text |
| `file_download` | click | Filename, extension, URL |
| `form_submit` | form | Form ID/name, action, method |
| `js_error` | error | Message, source, line, stack |

## Dashboard

Access at `/admin/analytics` (after adding auth):

- **Stats cards**: Total views, unique visitors, bot traffic, live visitors
- **Traffic chart**: Views over time (today, 7d, 30d)
- **3D Globe**: Interactive world map with drill-down (country → state → city)
- **Top pages**: Most visited URLs
- **Traffic sources**: Referrer breakdown by type (direct, organic, social, etc.)
- **Top referrers**: Individual referring domains
- **UTM campaigns**: Campaign and source attribution
- **Devices**: Mobile/tablet/desktop split
- **Browsers & OS**: Browser and operating system breakdown
- **Geography**: Top regions and cities
- **Bot breakdown**: Categorized bot traffic (search engines, AI crawlers, etc.)

## API Endpoints

The dashboard router exposes:

- `GET /admin/analytics` - HTML dashboard
- `GET /admin/analytics/api/stats?period=7d` - JSON stats
- `GET /admin/analytics/api/realtime` - Visitors in last 5 minutes

## Privacy Compliance

This implementation is designed to be:

- **GDPR compliant**: No personal data collected
- **CCPA compliant**: No data sale, no tracking
- **ePrivacy compliant**: No cookies or local storage

Add this to your privacy policy:

```markdown
## Website Analytics

We use privacy-respecting analytics to understand visitor patterns.

**Collected:** Page views, referrers, country (via Cloudflare), device type
**NOT collected:** IP addresses, cookies, personal identifiers

Data is aggregated and raw pageviews are deleted after 90 days.
No data is shared with third parties.
```

## License

MIT
