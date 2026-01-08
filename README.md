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
