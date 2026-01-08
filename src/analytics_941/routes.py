"""FastAPI routes for the analytics dashboard."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from .client import AnalyticsClient


def create_dashboard_router(client: AnalyticsClient, site_name: str) -> APIRouter:
    """Create a FastAPI router for the analytics dashboard."""
    router = APIRouter(tags=["analytics"])

    @router.get("", response_class=HTMLResponse)
    @router.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request, period: str = "7d"):
        """Render the analytics dashboard."""
        data = await client.get_dashboard_data(period)

        # Simple HTML dashboard (can be replaced with Jinja2 template)
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics - {site_name}</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <style>
        :root {{
            --bg: #0a0d12;
            --surface: #12161d;
            --border: #1e2530;
            --text: #e8edf3;
            --muted: #9ba3ad;
            --accent: #59b2cc;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg);
            color: var(--text);
            padding: 2rem;
            line-height: 1.5;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 1.5rem; font-weight: 500; margin-bottom: 1.5rem; }}
        .period-tabs {{
            display: flex;
            gap: 0.5rem;
            margin-bottom: 2rem;
        }}
        .period-tabs a {{
            padding: 0.5rem 1rem;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--muted);
            text-decoration: none;
            font-size: 0.875rem;
        }}
        .period-tabs a.active, .period-tabs a:hover {{
            background: var(--accent);
            color: var(--bg);
            border-color: var(--accent);
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .stat-card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1.5rem;
        }}
        .stat-card h3 {{
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--muted);
            margin-bottom: 0.5rem;
        }}
        .stat-card .value {{
            font-size: 2rem;
            font-weight: 600;
        }}
        .section {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }}
        .section h2 {{
            font-size: 1rem;
            font-weight: 500;
            margin-bottom: 1rem;
            color: var(--muted);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            text-align: left;
            padding: 0.75rem 0;
            border-bottom: 1px solid var(--border);
        }}
        th {{ color: var(--muted); font-weight: 500; font-size: 0.875rem; }}
        td {{ font-size: 0.875rem; }}
        .bar {{
            height: 4px;
            background: var(--accent);
            border-radius: 2px;
            margin-top: 0.25rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Analytics: {site_name}</h1>

        <div class="period-tabs">
            <a href="?period=today" class="{'active' if period == 'today' else ''}">Today</a>
            <a href="?period=7d" class="{'active' if period == '7d' else ''}">7 Days</a>
            <a href="?period=30d" class="{'active' if period == '30d' else ''}">30 Days</a>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Views</h3>
                <div class="value">{data.total_views:,}</div>
            </div>
            <div class="stat-card">
                <h3>Unique Visitors</h3>
                <div class="value">{data.unique_visitors:,}</div>
            </div>
        </div>

        <div class="section">
            <h2>Top Pages</h2>
            <table>
                <thead><tr><th>Page</th><th>Views</th></tr></thead>
                <tbody>
                    {''.join(f'<tr><td>{p["url"]}</td><td>{p["views"]:,}</td></tr>' for p in data.top_pages)}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Top Referrers</h2>
            <table>
                <thead><tr><th>Source</th><th>Views</th></tr></thead>
                <tbody>
                    {''.join(f'<tr><td>{r["referrer"] or "Direct"}</td><td>{r["views"]:,}</td></tr>' for r in data.top_referrers) or '<tr><td colspan="2">No referrer data</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Countries</h2>
            <table>
                <thead><tr><th>Country</th><th>Views</th></tr></thead>
                <tbody>
                    {''.join(f'<tr><td>{c["country"]}</td><td>{c["views"]:,}</td></tr>' for c in data.countries) or '<tr><td colspan="2">No country data</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>Devices</h2>
            <table>
                <thead><tr><th>Type</th><th>Views</th></tr></thead>
                <tbody>
                    {''.join(f'<tr><td>{d.title()}</td><td>{v:,}</td></tr>' for d, v in data.devices.items()) or '<tr><td colspan="2">No device data</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""
        return HTMLResponse(content=html)

    @router.get("/api/stats")
    async def api_stats(period: str = "7d"):
        """API endpoint for dashboard data."""
        data = await client.get_dashboard_data(period)
        return data.model_dump()

    @router.get("/api/realtime")
    async def api_realtime():
        """Get realtime visitor count (last 5 minutes)."""
        count = await client.get_realtime_count()
        return {"visitors": count}

    return router
