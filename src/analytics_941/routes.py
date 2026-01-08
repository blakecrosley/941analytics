"""FastAPI routes for the analytics dashboard."""

import hashlib
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, Form, Response, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse

from .client import AnalyticsClient


# Simple token-based auth
AUTH_COOKIE_NAME = "analytics_auth"
AUTH_COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30 days


def _hash_passkey(passkey: str, site_name: str) -> str:
    """Hash the passkey with the site name as salt."""
    return hashlib.sha256(f"{site_name}:{passkey}".encode()).hexdigest()


def _verify_auth(auth_cookie: Optional[str], expected_hash: str) -> bool:
    """Verify the auth cookie matches the expected hash."""
    if not auth_cookie:
        return False
    return secrets.compare_digest(auth_cookie, expected_hash)


def create_dashboard_router(
    client: AnalyticsClient,
    site_name: str,
    passkey: Optional[str] = None
) -> APIRouter:
    """Create a FastAPI router for the analytics dashboard.

    Args:
        client: The analytics client for querying data
        site_name: The site name for the dashboard title
        passkey: Optional passkey to protect the dashboard. If set, users must
                 enter this passkey to access the dashboard.
    """
    router = APIRouter(tags=["analytics"])

    # Pre-compute the expected hash if passkey is set
    expected_hash = _hash_passkey(passkey, site_name) if passkey else None

    def _render_views_chart(views_by_day: list[dict]) -> str:
        """Render a simple bar chart for views over time."""
        if not views_by_day:
            return '<div style="color: var(--muted); text-align: center; padding: 2rem;">No data for this period</div>'

        max_views = max((d.get("views", 0) for d in views_by_day), default=1)
        if max_views == 0:
            max_views = 1

        bars = []
        for day in views_by_day:
            views = day.get("views", 0)
            height_pct = (views / max_views) * 100 if max_views else 0
            date_str = day.get("date", "")
            # Show abbreviated date in tooltip
            bars.append(
                f'<div class="chart-bar" style="height: {max(height_pct, 2)}%;">'
                f'<div class="chart-tooltip">{date_str}<br><strong>{views:,}</strong> views</div>'
                f'</div>'
            )

        # Date labels (first, middle, last)
        first_date = views_by_day[0].get("date", "") if views_by_day else ""
        last_date = views_by_day[-1].get("date", "") if views_by_day else ""

        return f'''
            <div style="display: flex; align-items: flex-end; height: 180px; gap: 2px;">
                {"".join(bars)}
            </div>
            <div class="chart-labels">
                <span>{first_date}</span>
                <span>{last_date}</span>
            </div>
        '''

    def _render_login_page(error: str = "") -> str:
        """Render the login page HTML."""
        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics Login - {site_name}</title>
    <style>
        :root {{
            --bg: #0a0d12;
            --surface: #12161d;
            --border: #1e2530;
            --text: #e8edf3;
            --muted: #9ba3ad;
            --accent: #59b2cc;
            --error: #e74c3c;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .login-card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 2.5rem;
            width: 100%;
            max-width: 360px;
        }}
        h1 {{
            font-size: 1.25rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
            text-align: center;
        }}
        .subtitle {{
            color: var(--muted);
            font-size: 0.875rem;
            text-align: center;
            margin-bottom: 1.5rem;
        }}
        .error {{
            background: rgba(231, 76, 60, 0.1);
            border: 1px solid var(--error);
            color: var(--error);
            padding: 0.75rem;
            border-radius: 6px;
            font-size: 0.875rem;
            margin-bottom: 1rem;
            text-align: center;
        }}
        label {{
            display: block;
            font-size: 0.875rem;
            color: var(--muted);
            margin-bottom: 0.5rem;
        }}
        input[type="password"] {{
            width: 100%;
            padding: 0.75rem 1rem;
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text);
            font-size: 1rem;
            margin-bottom: 1.5rem;
        }}
        input[type="password"]:focus {{
            outline: none;
            border-color: var(--accent);
        }}
        button {{
            width: 100%;
            padding: 0.75rem 1rem;
            background: var(--accent);
            border: none;
            border-radius: 6px;
            color: var(--bg);
            font-size: 1rem;
            font-weight: 500;
            cursor: pointer;
            transition: opacity 0.2s;
        }}
        button:hover {{
            opacity: 0.9;
        }}
    </style>
</head>
<body>
    <div class="login-card">
        <h1>Analytics</h1>
        <p class="subtitle">{site_name}</p>
        {f'<div class="error">{error}</div>' if error else ''}
        <form method="POST" action="login">
            <label for="passkey">Passkey</label>
            <input type="password" id="passkey" name="passkey" placeholder="Enter passkey" autofocus required>
            <button type="submit">Access Dashboard</button>
        </form>
    </div>
</body>
</html>
"""

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(error: str = ""):
        """Show the login page."""
        if not passkey:
            # No passkey configured, redirect to dashboard
            return RedirectResponse(url="./", status_code=302)
        return HTMLResponse(content=_render_login_page(error))

    @router.post("/login")
    async def login_submit(
        request: Request,
        passkey_input: str = Form(..., alias="passkey")
    ):
        """Handle login form submission."""
        if not passkey:
            return RedirectResponse(url="./", status_code=302)

        if passkey_input == passkey:
            # Valid passkey - set auth cookie and redirect
            redirect = RedirectResponse(url="./", status_code=302)
            # Only set Secure flag on HTTPS (production)
            is_secure = request.url.scheme == "https"
            redirect.set_cookie(
                key=AUTH_COOKIE_NAME,
                value=expected_hash,
                max_age=AUTH_COOKIE_MAX_AGE,
                httponly=True,
                secure=is_secure,
                samesite="lax"
            )
            return redirect
        else:
            # Invalid passkey
            return HTMLResponse(
                content=_render_login_page("Invalid passkey"),
                status_code=401
            )

    @router.get("/logout")
    async def logout():
        """Clear the auth cookie and redirect to login."""
        redirect = RedirectResponse(url="./login", status_code=302)
        redirect.delete_cookie(key=AUTH_COOKIE_NAME)
        return redirect

    @router.get("", response_class=HTMLResponse)
    @router.get("/", response_class=HTMLResponse)
    async def dashboard(
        request: Request,
        period: str = "7d",
        analytics_auth: Optional[str] = Cookie(None)
    ):
        """Render the analytics dashboard."""
        # Check auth if passkey is configured
        if passkey and not _verify_auth(analytics_auth, expected_hash):
            # Use path from current URL to construct proper relative redirect
            base_path = str(request.url.path).rstrip("/")
            return RedirectResponse(url=f"{base_path}/login", status_code=302)

        try:
            data = await client.get_dashboard_data(period)
        except Exception as e:
            # Show error page instead of 500
            error_html = f"""
<!DOCTYPE html>
<html>
<head><title>Analytics Error</title></head>
<body style="font-family: system-ui; padding: 2rem; background: #0a0d12; color: #e8edf3;">
<h1>Dashboard Error</h1>
<p>Failed to load analytics data:</p>
<pre style="background: #1a1f29; padding: 1rem; border-radius: 6px; overflow: auto;">{str(e)}</pre>
<p><a href="./login" style="color: #59b2cc;">Back to login</a></p>
</body>
</html>"""
            return HTMLResponse(content=error_html, status_code=500)

        # Build country rows with globe data
        country_rows = []
        globe_data = []
        max_views = max((c["views"] for c in data.countries), default=1)

        for c in data.countries:
            country_rows.append(
                f'<tr><td>{c["country"]}</td><td>{c["views"]:,}</td></tr>'
            )
            # Normalize for globe visualization (0-1 scale)
            globe_data.append({
                "country": c["country"],
                "views": c["views"],
                "normalized": c["views"] / max_views if max_views > 0 else 0
            })

        # Build region data for drill-down (states for US, etc.)
        region_data = []
        for r in data.regions:
            region_data.append({
                "country": r["country"],
                "region": r["region"],
                "views": r["views"]
            })

        # Build city data for further drill-down
        city_data = []
        for city in data.cities:
            city_data.append({
                "country": city["country"],
                "region": city["region"],
                "city": city["city"],
                "views": city["views"]
            })

        # Simple HTML dashboard
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics - {site_name}</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://unpkg.com/topojson-client@3"></script>
    <script type="importmap">
    {{
        "imports": {{
            "three": "https://unpkg.com/three@0.160.0/build/three.module.js",
            "three/addons/": "https://unpkg.com/three@0.160.0/examples/jsm/"
        }}
    }}
    </script>
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
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
        }}
        h1 {{ font-size: 1.5rem; font-weight: 500; }}
        .logout {{
            color: var(--muted);
            text-decoration: none;
            font-size: 0.875rem;
        }}
        .logout:hover {{ color: var(--text); }}
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
        .main-grid {{
            display: grid;
            grid-template-columns: 1fr 400px;
            gap: 1.5rem;
        }}
        @media (max-width: 900px) {{
            .main-grid {{ grid-template-columns: 1fr; }}
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
        /* Chart styles */
        .chart-section {{ padding: 1.5rem; }}
        .chart-container {{ height: 200px; display: flex; align-items: flex-end; gap: 2px; padding-top: 1rem; }}
        .chart-bar {{
            flex: 1;
            min-width: 8px;
            background: linear-gradient(to top, rgba(89, 178, 204, 0.6), rgba(89, 178, 204, 0.9));
            border-radius: 2px 2px 0 0;
            position: relative;
            transition: all 0.2s;
        }}
        .chart-bar:hover {{
            background: linear-gradient(to top, rgba(89, 178, 204, 0.8), var(--accent));
        }}
        .chart-bar:hover .chart-tooltip {{
            display: block;
        }}
        .chart-tooltip {{
            display: none;
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: var(--surface);
            border: 1px solid var(--border);
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.7rem;
            white-space: nowrap;
            z-index: 10;
            margin-bottom: 4px;
        }}
        .chart-labels {{
            display: flex;
            justify-content: space-between;
            margin-top: 0.5rem;
            font-size: 0.7rem;
            color: var(--muted);
        }}
        .loading-dot {{
            display: inline-block;
            width: 10px;
            height: 10px;
            background: var(--accent);
            border-radius: 50%;
            animation: pulse 1.5s infinite;
        }}
        @keyframes pulse {{
            0%, 100% {{ opacity: 0.4; transform: scale(0.8); }}
            50% {{ opacity: 1; transform: scale(1); }}
        }}
        #realtime-card .value {{ color: var(--accent); }}
        .two-column-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
        }}
        @media (max-width: 600px) {{
            .two-column-grid {{ grid-template-columns: 1fr; }}
        }}
        #globe-container {{
            width: 100%;
            height: 350px;
            background: var(--bg);
            border-radius: 8px;
            margin-bottom: 1rem;
            position: relative;
        }}
        .globe-title {{
            position: absolute;
            top: 1rem;
            left: 1rem;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--muted);
            z-index: 10;
        }}
        #globe-tooltip {{
            display: none;
            position: absolute;
            background: var(--surface);
            border: 1px solid var(--accent);
            border-radius: 4px;
            padding: 8px 12px;
            font-size: 0.75rem;
            pointer-events: none;
            z-index: 100;
            box-shadow: 0 0 20px rgba(89, 178, 204, 0.3);
        }}
        #back-btn {{
            display: none;
            position: absolute;
            top: 1rem;
            right: 1rem;
            background: var(--surface);
            border: 1px solid var(--accent);
            color: var(--accent);
            padding: 0.5rem 1rem;
            border-radius: 6px;
            font-size: 0.75rem;
            cursor: pointer;
            z-index: 10;
            transition: all 0.2s;
        }}
        #back-btn:hover {{
            background: var(--accent);
            color: var(--bg);
        }}
        #detail-panel {{
            display: none;
            position: absolute;
            bottom: 1rem;
            left: 1rem;
            background: rgba(18, 22, 29, 0.9);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1rem;
            z-index: 10;
            text-align: center;
            min-width: 120px;
        }}
        #fullscreen-btn {{
            position: absolute;
            top: 1rem;
            right: 8rem;
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--muted);
            width: 32px;
            height: 32px;
            border-radius: 6px;
            font-size: 1rem;
            cursor: pointer;
            z-index: 10;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        #fullscreen-btn:hover {{
            border-color: var(--accent);
            color: var(--accent);
        }}
        /* Fullscreen Modal */
        .globe-modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: var(--bg);
            z-index: 1000;
        }}
        .globe-modal.active {{
            display: block;
        }}
        .globe-modal-content {{
            position: relative;
            width: 100%;
            height: 100%;
        }}
        #modal-globe-container {{
            width: 100%;
            height: 100%;
        }}
        .modal-close {{
            position: absolute;
            top: 1.5rem;
            right: 1.5rem;
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--muted);
            width: 40px;
            height: 40px;
            border-radius: 8px;
            font-size: 1.2rem;
            cursor: pointer;
            z-index: 10;
            transition: all 0.2s;
        }}
        .modal-close:hover {{
            border-color: var(--accent);
            color: var(--accent);
        }}
        .modal-back {{
            display: none;
            position: absolute;
            top: 1.5rem;
            left: 1.5rem;
            background: var(--surface);
            border: 1px solid var(--accent);
            color: var(--accent);
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            font-size: 0.875rem;
            cursor: pointer;
            z-index: 10;
            transition: all 0.2s;
        }}
        .modal-back:hover {{
            background: var(--accent);
            color: var(--bg);
        }}
        #modal-detail-panel {{
            display: none;
            position: absolute;
            bottom: 2rem;
            left: 2rem;
            background: rgba(18, 22, 29, 0.95);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            z-index: 10;
            min-width: 200px;
            max-width: 350px;
        }}
        #modal-tooltip {{
            display: none;
            position: absolute;
            background: var(--surface);
            border: 1px solid var(--accent);
            border-radius: 6px;
            padding: 10px 14px;
            font-size: 0.8rem;
            pointer-events: none;
            z-index: 100;
            box-shadow: 0 0 25px rgba(89, 178, 204, 0.4);
        }}
        /* City markers (different color from country markers) */
        .city-marker {{
            background: #f39c12;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Analytics: {site_name}</h1>
            {'<a href="./logout" class="logout">Logout</a>' if passkey else ''}
        </div>

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
            <div class="stat-card">
                <h3>Bot Traffic</h3>
                <div class="value" style="color: var(--muted);">{data.bot_views:,}</div>
            </div>
            <div class="stat-card" id="realtime-card">
                <h3>Live Visitors</h3>
                <div class="value" id="realtime-count" hx-get="api/realtime" hx-trigger="load, every 30s" hx-swap="innerHTML">
                    <span class="loading-dot"></span>
                </div>
                <div style="font-size: 0.75rem; color: var(--muted);">last 5 min</div>
            </div>
        </div>

        <!-- Views Over Time Chart -->
        <div class="section chart-section" style="margin-bottom: 1.5rem;">
            <h2>Views Over Time</h2>
            <div class="chart-container" id="views-chart">
                {_render_views_chart(data.views_by_day)}
            </div>
        </div>

        <div class="main-grid">
            <div class="left-column">
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
                    <h2>Traffic Sources</h2>
                    <table>
                        <thead><tr><th>Type</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join(f'<tr><td>{t.title() if t else "Direct"}</td><td>{v:,}</td></tr>' for t, v in sorted(data.referrer_types.items(), key=lambda x: x[1], reverse=True)) or '<tr><td colspan="2">No data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="section">
                    <h2>Top Referrers</h2>
                    <table>
                        <thead><tr><th>Domain</th><th>Type</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join('<tr><td>' + r.get("domain", "Direct") + '</td><td style="color:var(--muted)">' + r.get("type", "direct") + '</td><td>' + f'{r["views"]:,}' + '</td></tr>' for r in data.top_referrers) or '<tr><td colspan="3">No referrer data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="section">
                    <h2>UTM Campaigns</h2>
                    <table>
                        <thead><tr><th>Campaign</th><th>Source</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join('<tr><td>' + c.get("campaign", "-") + '</td><td style="color:var(--muted)">' + c.get("source", "-") + '</td><td>' + f'{c["views"]:,}' + '</td></tr>' for c in data.utm_campaigns) or '<tr><td colspan="3">No campaign data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="section">
                    <h2>Devices</h2>
                    <table>
                        <thead><tr><th>Type</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join(f'<tr><td>{d.title() if d else "Unknown"}</td><td>{v:,}</td></tr>' for d, v in sorted(data.devices.items(), key=lambda x: x[1], reverse=True)) or '<tr><td colspan="2">No device data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="section">
                    <h2>Browsers</h2>
                    <table>
                        <thead><tr><th>Browser</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join(f'<tr><td>{b}</td><td>{v:,}</td></tr>' for b, v in data.browsers.items()) or '<tr><td colspan="2">No browser data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="section">
                    <h2>Operating Systems</h2>
                    <table>
                        <thead><tr><th>OS</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join(f'<tr><td>{os}</td><td>{v:,}</td></tr>' for os, v in data.operating_systems.items()) or '<tr><td colspan="2">No OS data</td></tr>'}
                        </tbody>
                    </table>
                </div>

                <div class="two-column-grid">
                    <div class="section">
                        <h2>Bot Breakdown</h2>
                        <table>
                            <thead><tr><th>Category</th><th>Views</th></tr></thead>
                            <tbody>
                                {''.join(f'<tr><td>{cat.replace("_", " ").title()}</td><td>{v:,}</td></tr>' for cat, v in sorted(data.bot_breakdown.items(), key=lambda x: x[1], reverse=True)) or '<tr><td colspan="2">No bot traffic</td></tr>'}
                            </tbody>
                        </table>
                    </div>

                    <div class="section">
                        <h2>UTM Sources</h2>
                        <table>
                            <thead><tr><th>Source</th><th>Views</th></tr></thead>
                            <tbody>
                                {''.join(f'<tr><td>{s.get("source", "-")}</td><td>{s["views"]:,}</td></tr>' for s in data.utm_sources[:10]) or '<tr><td colspan="2">No UTM data</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="two-column-grid">
                    <div class="section">
                        <h2>Top Regions</h2>
                        <table>
                            <thead><tr><th>Region</th><th>Country</th><th>Views</th></tr></thead>
                            <tbody>
                                {''.join(f'<tr><td>{r.get("region", "-")}</td><td style="color:var(--muted)">{r.get("country", "-")}</td><td>{r["views"]:,}</td></tr>' for r in data.regions[:10]) or '<tr><td colspan="3">No region data</td></tr>'}
                            </tbody>
                        </table>
                    </div>

                    <div class="section">
                        <h2>Top Cities</h2>
                        <table>
                            <thead><tr><th>City</th><th>Region</th><th>Views</th></tr></thead>
                            <tbody>
                                {''.join(f'<tr><td>{c.get("city", "-")}</td><td style="color:var(--muted)">{c.get("region", "-")}</td><td>{c["views"]:,}</td></tr>' for c in data.cities[:10]) or '<tr><td colspan="3">No city data</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div class="right-column">
                <div class="section" style="padding: 0; overflow: hidden;">
                    <div id="globe-container">
                        <span class="globe-title">Visitors by Country</span>
                        <button id="back-btn">← Back to World</button>
                        <button id="fullscreen-btn" title="Fullscreen">⛶</button>
                        <div id="detail-panel"></div>
                        <div id="globe-tooltip"></div>
                    </div>
                    <div style="padding: 1.5rem; padding-top: 0;">
                        <table>
                            <thead><tr><th>Country</th><th>Views</th></tr></thead>
                            <tbody>
                                {''.join(country_rows) or '<tr><td colspan="2">No country data</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Fullscreen Globe Modal -->
    <div id="globe-modal" class="globe-modal">
        <div class="globe-modal-content">
            <button id="modal-close-btn" class="modal-close">✕</button>
            <button id="modal-back-btn" class="modal-back">← Back</button>
            <div id="modal-globe-container"></div>
            <div id="modal-detail-panel"></div>
            <div id="modal-tooltip"></div>
        </div>
    </div>

    <script type="module">
        import * as THREE from 'three';
        import {{ OrbitControls }} from 'three/addons/controls/OrbitControls.js';

        // Visitor data from server
        const globeData = {str(globe_data).replace("'", '"')};
        const regionData = {str(region_data).replace("'", '"')};
        const cityData = {str(city_data).replace("'", '"')};

        // Country centroids (lat, lon)
        const COUNTRY_COORDS = {{
            'US': [39.8, -98.5], 'CN': [35.0, 105.0], 'CA': [56.0, -106.0], 'SG': [1.35, 103.8],
            'PT': [39.5, -8.0], 'DE': [51.0, 10.5], 'VN': [16.0, 108.0], 'PK': [30.0, 70.0],
            'GB': [54.0, -2.0], 'FR': [46.0, 2.0], 'JP': [36.0, 138.0], 'IN': [22.0, 78.0],
            'BR': [-10.0, -55.0], 'AU': [-25.0, 135.0], 'KR': [36.0, 128.0], 'NL': [52.0, 5.0],
            'IT': [42.0, 12.0], 'ES': [40.0, -4.0], 'CH': [47.0, 8.0], 'SE': [62.0, 15.0],
            'NO': [62.0, 10.0], 'DK': [56.0, 10.0], 'FI': [64.0, 26.0], 'IE': [53.0, -8.0],
            'RU': [60.0, 100.0], 'MX': [23.0, -102.0], 'AR': [-34.0, -64.0], 'CL': [-33.0, -71.0],
            'CO': [4.0, -72.0], 'PE': [-10.0, -76.0], 'EG': [27.0, 30.0], 'NG': [10.0, 8.0],
            'ZA': [-29.0, 24.0], 'SA': [24.0, 45.0], 'AE': [24.0, 54.0], 'IL': [31.0, 35.0],
            'TR': [39.0, 35.0], 'PL': [52.0, 20.0], 'UA': [49.0, 32.0], 'CZ': [50.0, 15.0],
            'HK': [22.3, 114.2], 'TW': [24.0, 121.0], 'MY': [4.0, 109.0], 'TH': [15.0, 101.0],
            'ID': [-2.0, 118.0], 'PH': [13.0, 122.0], 'NZ': [-42.0, 174.0], 'AT': [47.5, 14.5],
            'BE': [50.8, 4.5], 'GR': [39.0, 22.0], 'HU': [47.0, 20.0], 'RO': [46.0, 25.0],
            'BD': [24.0, 90.0], 'KE': [-1.0, 38.0]
        }};

        const CONFIG = {{
            globeRadius: 100,
            backgroundColor: '#0a0d12',
            oceanColor: '#0a0a0a',
            borderColor: '#59b2cc',
            pointColor: '#59b2cc',
            atmosphereColor: '#59b2cc',
            animationDuration: 800,
            countriesUrl: 'https://unpkg.com/world-atlas@2.0.2/countries-110m.json'
        }};

        // Country names lookup
        const COUNTRY_NAMES = {{
            'US': 'United States', 'CN': 'China', 'CA': 'Canada', 'SG': 'Singapore',
            'PT': 'Portugal', 'DE': 'Germany', 'VN': 'Vietnam', 'PK': 'Pakistan',
            'GB': 'United Kingdom', 'FR': 'France', 'JP': 'Japan', 'IN': 'India',
            'BR': 'Brazil', 'AU': 'Australia', 'KR': 'South Korea', 'NL': 'Netherlands',
            'IT': 'Italy', 'ES': 'Spain', 'CH': 'Switzerland', 'SE': 'Sweden',
            'NO': 'Norway', 'DK': 'Denmark', 'FI': 'Finland', 'IE': 'Ireland',
            'RU': 'Russia', 'MX': 'Mexico', 'AR': 'Argentina', 'CL': 'Chile',
            'CO': 'Colombia', 'PE': 'Peru', 'EG': 'Egypt', 'NG': 'Nigeria',
            'ZA': 'South Africa', 'SA': 'Saudi Arabia', 'AE': 'UAE', 'IL': 'Israel',
            'TR': 'Turkey', 'PL': 'Poland', 'UA': 'Ukraine', 'CZ': 'Czech Republic',
            'HK': 'Hong Kong', 'TW': 'Taiwan', 'MY': 'Malaysia', 'TH': 'Thailand',
            'ID': 'Indonesia', 'PH': 'Philippines', 'NZ': 'New Zealand', 'AT': 'Austria',
            'BE': 'Belgium', 'GR': 'Greece', 'HU': 'Hungary', 'RO': 'Romania',
            'BD': 'Bangladesh', 'KE': 'Kenya'
        }};

        // US State centroids (lat, lon) for drill-down
        const US_STATE_COORDS = {{
            'AL': [32.7, -86.7], 'AK': [64.0, -153.0], 'AZ': [34.3, -111.7], 'AR': [34.9, -92.4],
            'CA': [37.2, -119.4], 'CO': [39.0, -105.5], 'CT': [41.6, -72.7], 'DE': [39.0, -75.5],
            'FL': [28.6, -82.4], 'GA': [32.6, -83.4], 'HI': [20.8, -156.3], 'ID': [44.4, -114.6],
            'IL': [40.0, -89.2], 'IN': [40.0, -86.3], 'IA': [42.0, -93.5], 'KS': [38.5, -98.4],
            'KY': [37.8, -85.7], 'LA': [31.1, -92.0], 'ME': [45.4, -69.2], 'MD': [39.0, -76.8],
            'MA': [42.2, -71.5], 'MI': [44.2, -85.4], 'MN': [46.3, -94.2], 'MS': [32.7, -89.7],
            'MO': [38.4, -92.5], 'MT': [47.0, -109.6], 'NE': [41.5, -99.8], 'NV': [39.3, -116.6],
            'NH': [43.7, -71.6], 'NJ': [40.1, -74.7], 'NM': [34.4, -106.1], 'NY': [42.9, -75.5],
            'NC': [35.5, -79.4], 'ND': [47.4, -100.5], 'OH': [40.4, -82.8], 'OK': [35.6, -97.5],
            'OR': [44.0, -120.5], 'PA': [40.9, -77.8], 'RI': [41.7, -71.5], 'SC': [33.9, -80.9],
            'SD': [44.4, -100.2], 'TN': [35.8, -86.3], 'TX': [31.5, -99.4], 'UT': [39.3, -111.7],
            'VT': [44.1, -72.7], 'VA': [37.5, -78.8], 'WA': [47.4, -120.5], 'WV': [38.9, -80.5],
            'WI': [44.6, -89.8], 'WY': [43.0, -107.5], 'DC': [38.9, -77.0]
        }};

        // US State names lookup
        const US_STATE_NAMES = {{
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'Washington DC'
        }};

        // Reverse lookup: state name -> code (Cloudflare returns full names like "California")
        const STATE_NAME_TO_CODE = {{}};
        Object.entries(US_STATE_NAMES).forEach(([code, name]) => {{
            STATE_NAME_TO_CODE[name] = code;
            STATE_NAME_TO_CODE[name.toLowerCase()] = code;
            STATE_NAME_TO_CODE[code] = code;  // Also allow code lookup
        }});

        // Helper to normalize state identifier to code
        function normalizeStateCode(region) {{
            if (!region) return null;
            // Try direct lookup (handles both "CA" and "California")
            return STATE_NAME_TO_CODE[region] || STATE_NAME_TO_CODE[region.toLowerCase()] || null;
        }}

        // City coordinates (approximate) for major cities worldwide
        const CITY_COORDS = {{
            // US Cities
            'New York|NY|US': [40.71, -74.01], 'Los Angeles|CA|US': [34.05, -118.24],
            'Chicago|IL|US': [41.88, -87.63], 'Houston|TX|US': [29.76, -95.37],
            'Phoenix|AZ|US': [33.45, -112.07], 'San Francisco|CA|US': [37.77, -122.42],
            'Seattle|WA|US': [47.61, -122.33], 'Miami|FL|US': [25.76, -80.19],
            'Boston|MA|US': [42.36, -71.06], 'Denver|CO|US': [39.74, -104.99],
            'Atlanta|GA|US': [33.75, -84.39], 'Dallas|TX|US': [32.78, -96.80],
            'Austin|TX|US': [30.27, -97.74], 'San Diego|CA|US': [32.72, -117.16],
            'Portland|OR|US': [45.52, -122.68], 'Las Vegas|NV|US': [36.17, -115.14],
            // International Cities
            'London||GB': [51.51, -0.13], 'Manchester||GB': [53.48, -2.24],
            'Toronto|ON|CA': [43.65, -79.38], 'Vancouver|BC|CA': [49.28, -123.12],
            'Montreal|QC|CA': [45.50, -73.57], 'Sydney|NSW|AU': [-33.87, 151.21],
            'Melbourne|VIC|AU': [-37.81, 144.96], 'Berlin||DE': [52.52, 13.41],
            'Munich||DE': [48.14, 11.58], 'Frankfurt||DE': [50.11, 8.68],
            'Paris||FR': [48.86, 2.35], 'Lyon||FR': [45.76, 4.83],
            'Tokyo||JP': [35.68, 139.65], 'Osaka||JP': [34.69, 135.50],
            'Singapore||SG': [1.35, 103.82], 'Mumbai||IN': [19.08, 72.88],
            'Delhi||IN': [28.70, 77.10], 'Bangalore||IN': [12.97, 77.59],
            'Seoul||KR': [37.57, 126.98], 'Amsterdam||NL': [52.37, 4.90],
            'Stockholm||SE': [59.33, 18.07], 'Dublin||IE': [53.35, -6.26],
            'Zurich||CH': [47.38, 8.54], 'Madrid||ES': [40.42, -3.70],
            'Barcelona||ES': [41.39, 2.17], 'Rome||IT': [41.90, 12.50],
            'Milan||IT': [45.46, 9.19], 'Sao Paulo||BR': [-23.55, -46.63],
            'Mexico City||MX': [19.43, -99.13], 'Hong Kong||HK': [22.32, 114.17]
        }};

        // Fullscreen mode state
        let isFullscreen = false;
        let originalContainer = null;  // Store original container for restoring

        let isAnimating = false;
        let autoRotate = true;
        let currentView = 'world';
        let selectedCountry = null;

        let scene, camera, renderer, controls, globeGroup;
        let tooltip;

        function latLonToVector3(lat, lon, radius = CONFIG.globeRadius) {{
            const phi = (90 - lat) * (Math.PI / 180);
            const theta = (lon + 180) * (Math.PI / 180);
            return new THREE.Vector3(
                -(radius * Math.sin(phi) * Math.cos(theta)),
                radius * Math.cos(phi),
                radius * Math.sin(phi) * Math.sin(theta)
            );
        }}

        function createGlowTexture() {{
            const canvas = document.createElement('canvas');
            canvas.width = 64;
            canvas.height = 64;
            const ctx = canvas.getContext('2d');
            const gradient = ctx.createRadialGradient(32, 32, 0, 32, 32, 32);
            gradient.addColorStop(0, 'rgba(89, 178, 204, 1)');
            gradient.addColorStop(0.3, 'rgba(89, 178, 204, 0.5)');
            gradient.addColorStop(1, 'rgba(89, 178, 204, 0)');
            ctx.fillStyle = gradient;
            ctx.fillRect(0, 0, 64, 64);
            return new THREE.CanvasTexture(canvas);
        }}

        function easeInOutCubic(t) {{
            return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
        }}

        function animateCameraTo(lat, lon, distance = 180) {{
            if (isAnimating) return;
            isAnimating = true;
            autoRotate = false;

            const targetPos = latLonToVector3(lat, lon, distance + CONFIG.globeRadius);
            const startPos = camera.position.clone();
            const startTime = performance.now();

            function animate() {{
                const elapsed = performance.now() - startTime;
                const t = Math.min(elapsed / CONFIG.animationDuration, 1);
                const eased = easeInOutCubic(t);

                camera.position.lerpVectors(startPos, targetPos, eased);
                camera.lookAt(0, 0, 0);

                if (t < 1) {{
                    requestAnimationFrame(animate);
                }} else {{
                    isAnimating = false;
                }}
            }}
            animate();
        }}

        function animateCameraToWorld() {{
            if (isAnimating) return;
            isAnimating = true;

            // Clear all drill-down markers when returning to world view
            clearStateMarkers();
            clearCityMarkers();

            // Show all country markers again
            showAllCountryMarkers();

            const targetPos = new THREE.Vector3(0, 0, 280);
            const startPos = camera.position.clone();
            const startTime = performance.now();

            function animate() {{
                const elapsed = performance.now() - startTime;
                const t = Math.min(elapsed / CONFIG.animationDuration, 1);
                const eased = easeInOutCubic(t);

                camera.position.lerpVectors(startPos, targetPos, eased);
                camera.lookAt(0, 0, 0);

                if (t < 1) {{
                    requestAnimationFrame(animate);
                }} else {{
                    isAnimating = false;
                    autoRotate = true;
                    currentView = 'world';
                    selectedCountry = null;
                    updateDetailPanel(null);
                }}
            }}
            animate();
        }}

        let stateMarkers = [];  // Track state markers for cleanup
        let countryMarkerMap = {{}};  // Track country markers by code for hide/show

        function clearStateMarkers() {{
            stateMarkers.forEach(m => {{
                if (m.mesh) globeGroup.remove(m.mesh);
                if (m.sprite) globeGroup.remove(m.sprite);
            }});
            stateMarkers = [];
        }}

        function hideCountryMarker(countryCode) {{
            const marker = countryMarkerMap[countryCode];
            if (marker) {{
                if (marker.mesh) marker.mesh.visible = false;
                if (marker.sprite) marker.sprite.visible = false;
            }}
        }}

        function showAllCountryMarkers() {{
            Object.values(countryMarkerMap).forEach(marker => {{
                if (marker.mesh) marker.mesh.visible = true;
                if (marker.sprite) marker.sprite.visible = true;
            }});
        }}

        function addStateMarkers(countryCode) {{
            clearStateMarkers();
            if (countryCode !== 'US') return;

            // Get US region data
            const usRegions = regionData.filter(r => r.country === 'US');
            if (usRegions.length === 0) return;

            const maxViews = Math.max(...usRegions.map(r => r.views), 1);
            const glowTexture = createGlowTexture();

            usRegions.forEach(item => {{
                // Normalize state name to code (Cloudflare returns "California", we need "CA")
                const stateCode = normalizeStateCode(item.region);
                const coords = stateCode ? US_STATE_COORDS[stateCode] : null;
                if (!coords) return;

                const [lat, lon] = coords;
                const position = latLonToVector3(lat, lon, CONFIG.globeRadius + 5);  // Higher above globe

                // Size based on views (log scale) - make them MUCH bigger
                const logViews = Math.log10(item.views + 1);
                const logMax = Math.log10(maxViews + 1);
                const size = 3 + (logViews / logMax) * 5;  // Much bigger: 3-8

                // Marker sphere - red for states
                const mesh = new THREE.Mesh(
                    new THREE.SphereGeometry(size, 16, 16),
                    new THREE.MeshBasicMaterial({{ color: '#ff6b6b', transparent: true, opacity: 0.95 }})
                );
                mesh.position.copy(position);
                mesh.userData = {{ state: stateCode, stateName: item.region, views: item.views, isState: true }};
                globeGroup.add(mesh);

                // Glow sprite - brighter glow
                const spriteMat = new THREE.SpriteMaterial({{
                    map: glowTexture, color: '#ff6b6b', transparent: true, opacity: 0.6, blending: THREE.AdditiveBlending
                }});
                const sprite = new THREE.Sprite(spriteMat);
                sprite.scale.set(size * 6, size * 6, 1);
                sprite.position.copy(position);
                globeGroup.add(sprite);

                stateMarkers.push({{ mesh, sprite }});
            }});
        }}

        let cityMarkers = [];  // Track city markers for cleanup

        function clearCityMarkers() {{
            cityMarkers.forEach(m => {{
                if (m.mesh) globeGroup.remove(m.mesh);
                if (m.sprite) globeGroup.remove(m.sprite);
            }});
            cityMarkers = [];
        }}

        function addCityMarkers(countryCode) {{
            clearCityMarkers();

            // Get cities for this country
            const countryCities = cityData.filter(c => c.country === countryCode);
            if (countryCities.length === 0) return;

            const maxViews = Math.max(...countryCities.map(c => c.views), 1);
            const glowTexture = createGlowTexture();

            countryCities.slice(0, 15).forEach(item => {{
                // Try to find city coordinates
                const cityKey = `${{item.city}}|${{item.region || ''}}|${{countryCode}}`;
                let coords = CITY_COORDS[cityKey];

                // Try without region
                if (!coords) {{
                    coords = CITY_COORDS[`${{item.city}}||${{countryCode}}`];
                }}

                // Fall back to country coords with offset
                if (!coords) {{
                    const countryCoords = COUNTRY_COORDS[countryCode];
                    if (countryCoords) {{
                        const offset = Math.random() * 8 - 4;
                        coords = [countryCoords[0] + offset, countryCoords[1] + offset];
                    }}
                }}

                if (!coords) return;

                const [lat, lon] = coords;
                const position = latLonToVector3(lat, lon, CONFIG.globeRadius + 1.5);

                // Size based on views (log scale)
                const logViews = Math.log10(item.views + 1);
                const logMax = Math.log10(maxViews + 1);
                const size = 0.6 + (logViews / logMax) * 1.8;

                // Marker sphere - orange for cities
                const mesh = new THREE.Mesh(
                    new THREE.SphereGeometry(size, 16, 16),
                    new THREE.MeshBasicMaterial({{ color: '#f39c12', transparent: true, opacity: 0.9 }})
                );
                mesh.position.copy(position);
                mesh.userData = {{ city: item.city, region: item.region, views: item.views, isCity: true }};
                globeGroup.add(mesh);

                // Glow sprite
                const spriteMat = new THREE.SpriteMaterial({{
                    map: glowTexture, color: '#f39c12', transparent: true, opacity: 0.4, blending: THREE.AdditiveBlending
                }});
                const sprite = new THREE.Sprite(spriteMat);
                sprite.scale.set(size * 4, size * 4, 1);
                sprite.position.copy(position);
                globeGroup.add(sprite);

                cityMarkers.push({{ mesh, sprite }});
            }});
        }}

        let selectedState = null;  // Track selected state for back navigation

        function drillToCountry(code, views) {{
            const coords = COUNTRY_COORDS[code];
            if (!coords) return;

            currentView = 'country';
            selectedCountry = {{ code, views, name: COUNTRY_NAMES[code] || code }};
            selectedState = null;

            // Hide the country marker we're drilling into
            hideCountryMarker(code);

            // For US, show states in detail panel and state markers
            if (code === 'US') {{
                const usStates = regionData.filter(r => r.country === 'US');
                selectedCountry.regions = usStates;
                selectedCountry.isUS = true;

                animateCameraTo(coords[0], coords[1], 80);
                setTimeout(() => {{
                    addStateMarkers('US');
                }}, CONFIG.animationDuration / 2);
            }} else {{
                // For other countries, get cities and show city markers
                const countryCities = cityData.filter(c => c.country === code);
                selectedCountry.cities = countryCities;
                selectedCountry.isUS = false;

                animateCameraTo(coords[0], coords[1], 100);
                setTimeout(() => {{
                    addCityMarkers(code);
                }}, CONFIG.animationDuration / 2);
            }}
            updateDetailPanel(selectedCountry);
        }}

        function drillToState(stateCode, views, stateName) {{
            // stateCode should already be normalized (e.g., "CA")
            const coords = US_STATE_COORDS[stateCode];
            if (!coords) return;

            currentView = 'state';
            const displayName = stateName || US_STATE_NAMES[stateCode] || stateCode;

            // Get cities for this state - try both code and full name since DB might have either
            const stateCities = cityData.filter(c => {{
                if (c.country !== 'US') return false;
                const cityStateCode = normalizeStateCode(c.region);
                return cityStateCode === stateCode || c.region === stateCode;
            }});

            selectedState = {{
                code: stateCode,
                name: displayName,
                views: views,
                cities: stateCities
            }};

            // Clear state markers, add city markers for this state
            clearStateMarkers();
            addStateCityMarkers(stateCode);

            updateDetailPanel(selectedState);
            animateCameraTo(coords[0], coords[1], 40);
        }}

        function addStateCityMarkers(stateCode) {{
            clearCityMarkers();

            // Get cities for this state
            const stateCities = cityData.filter(c => c.country === 'US' && c.region === stateCode);
            if (stateCities.length === 0) return;

            const maxViews = Math.max(...stateCities.map(c => c.views), 1);
            const glowTexture = createGlowTexture();

            stateCities.slice(0, 20).forEach(item => {{
                // Try to find city coordinates
                const cityKey = `${{item.city}}|${{stateCode}}|US`;
                let coords = CITY_COORDS[cityKey];

                // Fall back to state coords with offset
                if (!coords) {{
                    const stateCoords = US_STATE_COORDS[stateCode];
                    if (stateCoords) {{
                        const offset = (Math.random() - 0.5) * 4;
                        coords = [stateCoords[0] + offset, stateCoords[1] + offset];
                    }}
                }}

                if (!coords) return;

                const [lat, lon] = coords;
                const position = latLonToVector3(lat, lon, CONFIG.globeRadius + 1.5);

                // Size based on views
                const logViews = Math.log10(item.views + 1);
                const logMax = Math.log10(maxViews + 1);
                const size = 0.5 + (logViews / logMax) * 1.5;

                // Marker sphere - orange for cities
                const mesh = new THREE.Mesh(
                    new THREE.SphereGeometry(size, 16, 16),
                    new THREE.MeshBasicMaterial({{ color: '#f39c12', transparent: true, opacity: 0.9 }})
                );
                mesh.position.copy(position);
                mesh.userData = {{ city: item.city, region: stateCode, views: item.views, isCity: true }};
                globeGroup.add(mesh);

                // Glow sprite
                const spriteMat = new THREE.SpriteMaterial({{
                    map: glowTexture, color: '#f39c12', transparent: true, opacity: 0.4, blending: THREE.AdditiveBlending
                }});
                const sprite = new THREE.Sprite(spriteMat);
                sprite.scale.set(size * 4, size * 4, 1);
                sprite.position.copy(position);
                globeGroup.add(sprite);

                cityMarkers.push({{ mesh, sprite }});
            }});
        }}

        function goBack() {{
            if (currentView === 'state') {{
                // State → US country view
                drillToCountry('US', selectedCountry?.views || 0);
            }} else if (currentView === 'country') {{
                // Country → World view
                animateCameraToWorld();
            }}
        }}

        function updateDetailPanel(data) {{
            const panel = document.getElementById('detail-panel');
            const backBtn = document.getElementById('back-btn');
            if (!panel || !backBtn) return;

            if (data) {{
                let html = `
                    <h3 style="color: var(--accent); margin-bottom: 0.5rem;">${{data.name}}</h3>
                    <div style="font-size: 2rem; font-weight: 600;">${{data.views.toLocaleString()}}</div>
                    <div style="color: var(--muted); font-size: 0.875rem;">page views</div>
                `;

                // Show states for US country view
                if (data.isUS && data.regions && data.regions.length > 0) {{
                    html += `<div style="margin-top: 0.75rem; font-size: 0.75rem; color: var(--muted);">
                        <div style="color: #ff6b6b; margin-bottom: 0.25rem;">Click a state:</div>`;
                    data.regions.slice(0, 8).forEach(r => {{
                        const stateName = US_STATE_NAMES[r.region] || r.region;
                        html += `<div>${{stateName}}: ${{r.views}}</div>`;
                    }});
                    if (data.regions.length > 8) {{
                        html += `<div style="opacity: 0.6;">+ ${{data.regions.length - 8}} more</div>`;
                    }}
                    html += `</div>`;
                }}
                // Show cities for state view or international country view
                else if (data.cities && data.cities.length > 0) {{
                    html += `<div style="margin-top: 0.75rem; font-size: 0.75rem; color: var(--muted);">`;
                    if (currentView === 'state') {{
                        html += `<div style="color: #f39c12; margin-bottom: 0.25rem;">Top cities:</div>`;
                    }}
                    data.cities.slice(0, 8).forEach(c => {{
                        html += `<div>${{c.city}}: ${{c.views}}</div>`;
                    }});
                    if (data.cities.length > 8) {{
                        html += `<div style="opacity: 0.6;">+ ${{data.cities.length - 8}} more</div>`;
                    }}
                    html += `</div>`;
                }}

                panel.innerHTML = html;
                panel.style.display = 'block';
                backBtn.style.display = 'block';
            }} else {{
                panel.style.display = 'none';
                backBtn.style.display = 'none';
            }}
        }}

        function createStarfield() {{
            const geometry = new THREE.BufferGeometry();
            const positions = [];
            for (let i = 0; i < 2000; i++) {{
                const theta = Math.random() * Math.PI * 2;
                const phi = Math.acos(2 * Math.random() - 1);
                const r = 400 + Math.random() * 200;
                positions.push(
                    r * Math.sin(phi) * Math.cos(theta),
                    r * Math.sin(phi) * Math.sin(theta),
                    r * Math.cos(phi)
                );
            }}
            geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
            return new THREE.Points(geometry, new THREE.PointsMaterial({{
                color: 0x59b2cc, size: 0.5, transparent: true, opacity: 0.6
            }}));
        }}

        function handleGlobeClick(event, markers) {{
            if (isAnimating) return;

            const rect = renderer.domElement.getBoundingClientRect();
            const mouse = new THREE.Vector2(
                ((event.clientX - rect.left) / rect.width) * 2 - 1,
                -((event.clientY - rect.top) / rect.height) * 2 + 1
            );

            const raycaster = new THREE.Raycaster();
            raycaster.setFromCamera(mouse, camera);

            // Check state markers first (if we're in US view)
            if (currentView === 'country' && selectedCountry && selectedCountry.code === 'US') {{
                const stateMeshes = stateMarkers.map(m => m.mesh).filter(m => m.visible);
                const stateHits = raycaster.intersectObjects(stateMeshes);
                if (stateHits.length > 0) {{
                    const data = stateHits[0].object.userData;
                    if (data.isState && data.state && data.views) {{
                        drillToState(data.state, data.views, data.stateName);
                        return;
                    }}
                }}
            }}

            // Check country markers (filter out invisible ones)
            const visibleMarkers = markers.filter(m => m.visible);
            const hits = raycaster.intersectObjects(visibleMarkers);
            if (hits.length > 0) {{
                const data = hits[0].object.userData;
                if (data.country && data.views) {{
                    drillToCountry(data.country, data.views);
                }}
            }}
        }}

        async function initGlobe() {{
            const container = document.getElementById('globe-container');
            if (!container) return;

            // Create tooltip
            tooltip = document.getElementById('globe-tooltip');

            // Scene
            scene = new THREE.Scene();
            scene.background = new THREE.Color(CONFIG.backgroundColor);

            // Camera
            camera = new THREE.PerspectiveCamera(45, container.clientWidth / container.clientHeight, 1, 1000);
            camera.position.z = 280;

            // Renderer
            renderer = new THREE.WebGLRenderer({{ antialias: true }});
            renderer.setSize(container.clientWidth, container.clientHeight);
            renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
            container.appendChild(renderer.domElement);

            // Controls
            controls = new OrbitControls(camera, renderer.domElement);
            controls.enableDamping = true;
            controls.dampingFactor = 0.05;
            controls.minDistance = 150;
            controls.maxDistance = 400;
            controls.enablePan = false;
            controls.autoRotate = true;
            controls.autoRotateSpeed = 0.3;

            // Starfield
            scene.add(createStarfield());

            // Globe group (everything rotates together)
            globeGroup = new THREE.Group();

            // Ocean sphere
            const oceanGeo = new THREE.SphereGeometry(CONFIG.globeRadius - 0.5, 64, 64);
            globeGroup.add(new THREE.Mesh(oceanGeo, new THREE.MeshBasicMaterial({{
                color: CONFIG.oceanColor, transparent: true, opacity: 0.95
            }})));

            // Atmosphere
            const atmosphereGeo = new THREE.SphereGeometry(CONFIG.globeRadius + 2, 64, 64);
            globeGroup.add(new THREE.Mesh(atmosphereGeo, new THREE.MeshBasicMaterial({{
                color: CONFIG.atmosphereColor, transparent: true, opacity: 0.08, side: THREE.BackSide
            }})));

            scene.add(globeGroup);

            // Load country borders
            try {{
                const response = await fetch(CONFIG.countriesUrl);
                const topology = await response.json();
                const countries = topojson.feature(topology, topology.objects.countries);

                const borderMaterial = new THREE.LineBasicMaterial({{
                    color: CONFIG.borderColor, transparent: true, opacity: 0.3
                }});

                countries.features.forEach(feature => {{
                    const coords = feature.geometry.coordinates;
                    const type = feature.geometry.type;

                    const processRing = (ring) => {{
                        const points = ring.map(([lon, lat]) => latLonToVector3(lat, lon, CONFIG.globeRadius + 0.2));
                        if (points.length > 1) {{
                            globeGroup.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(points), borderMaterial));
                        }}
                    }};

                    if (type === 'Polygon') {{
                        coords.forEach(ring => processRing(ring));
                    }} else if (type === 'MultiPolygon') {{
                        coords.forEach(polygon => polygon.forEach(ring => processRing(ring)));
                    }}
                }});
            }} catch (e) {{
                console.warn('Failed to load country borders:', e);
            }}

            // Add visitor markers
            const glowTexture = createGlowTexture();
            const maxViews = Math.max(...globeData.map(d => d.views), 1);

            globeData.forEach(item => {{
                const coords = COUNTRY_COORDS[item.country];
                if (!coords) return;

                const [lat, lon] = coords;
                const position = latLonToVector3(lat, lon, CONFIG.globeRadius + 1);

                // Size based on views (log scale)
                const logViews = Math.log10(item.views + 1);
                const logMax = Math.log10(maxViews + 1);
                const size = 1 + (logViews / logMax) * 3;

                // Marker sphere
                const mesh = new THREE.Mesh(
                    new THREE.SphereGeometry(size, 16, 16),
                    new THREE.MeshBasicMaterial({{ color: CONFIG.pointColor, transparent: true, opacity: 0.9 }})
                );
                mesh.position.copy(position);
                mesh.userData = {{ country: item.country, views: item.views }};
                globeGroup.add(mesh);

                // Glow sprite
                const sprite = new THREE.Sprite(new THREE.SpriteMaterial({{
                    map: glowTexture, color: CONFIG.pointColor, transparent: true, opacity: 0.5, blending: THREE.AdditiveBlending
                }}));
                sprite.scale.set(size * 6, size * 6, 1);
                sprite.position.copy(position);
                globeGroup.add(sprite);

                // Track country markers for hide/show during drill-down
                countryMarkerMap[item.country] = {{ mesh, sprite }};
            }});

            // Raycaster for tooltips and clicks
            const raycaster = new THREE.Raycaster();
            const mouse = new THREE.Vector2();
            const markers = globeGroup.children.filter(c => c.userData && c.userData.country);

            renderer.domElement.addEventListener('mousemove', (e) => {{
                const rect = renderer.domElement.getBoundingClientRect();
                mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
                mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;

                raycaster.setFromCamera(mouse, camera);

                // Check state markers first when in US view
                if (currentView === 'country' && selectedCountry && selectedCountry.code === 'US' && stateMarkers.length > 0) {{
                    const stateIntersects = raycaster.intersectObjects(stateMarkers.map(m => m.mesh));
                    if (stateIntersects.length > 0 && tooltip) {{
                        const data = stateIntersects[0].object.userData;
                        const name = data.stateName || US_STATE_NAMES[data.state] || data.state;
                        tooltip.innerHTML = `<strong style="color:#ff6b6b">${{name}}</strong><br>${{data.views.toLocaleString()}} views<br><small style="color:var(--muted)">Click for cities</small>`;
                        tooltip.style.display = 'block';
                        tooltip.style.left = (e.clientX - rect.left + 15) + 'px';
                        tooltip.style.top = (e.clientY - rect.top + 15) + 'px';
                        renderer.domElement.style.cursor = 'pointer';
                        return;
                    }}
                }}

                // Check city markers when viewing a non-US country OR when in state view
                if (cityMarkers.length > 0 && (currentView === 'state' || (currentView === 'country' && selectedCountry && selectedCountry.code !== 'US'))) {{
                    const cityIntersects = raycaster.intersectObjects(cityMarkers.map(m => m.mesh));
                    if (cityIntersects.length > 0 && tooltip) {{
                        const data = cityIntersects[0].object.userData;
                        const cityName = data.city || 'Unknown';
                        const regionName = data.region ? ` (${{US_STATE_NAMES[data.region] || data.region}})` : '';
                        tooltip.innerHTML = `<strong style="color:#f39c12">${{cityName}}</strong>${{regionName}}<br>${{data.views.toLocaleString()}} views`;
                        tooltip.style.display = 'block';
                        tooltip.style.left = (e.clientX - rect.left + 15) + 'px';
                        tooltip.style.top = (e.clientY - rect.top + 15) + 'px';
                        renderer.domElement.style.cursor = 'pointer';
                        return;
                    }}
                }}

                // Check country markers
                const intersects = raycaster.intersectObjects(markers);
                if (intersects.length > 0 && tooltip) {{
                    const data = intersects[0].object.userData;
                    const name = COUNTRY_NAMES[data.country] || data.country;
                    const hint = data.country === 'US' ? 'Click for states' : 'Click to zoom';
                    tooltip.innerHTML = `<strong style="color:var(--accent)">${{name}}</strong><br>${{data.views.toLocaleString()}} views<br><small style="color:var(--muted)">${{hint}}</small>`;
                    tooltip.style.display = 'block';
                    tooltip.style.left = (e.clientX - rect.left + 15) + 'px';
                    tooltip.style.top = (e.clientY - rect.top + 15) + 'px';
                    renderer.domElement.style.cursor = 'pointer';
                }} else if (tooltip) {{
                    tooltip.style.display = 'none';
                    renderer.domElement.style.cursor = 'grab';
                }}
            }});

            // Click to drill down
            renderer.domElement.addEventListener('click', (e) => handleGlobeClick(e, markers));

            // Back button - uses hierarchical navigation
            const backBtn = document.getElementById('back-btn');
            if (backBtn) {{
                backBtn.addEventListener('click', goBack);
            }}

            // Escape key to go back or close modal
            document.addEventListener('keydown', (e) => {{
                if (e.key === 'Escape') {{
                    const modal = document.getElementById('globe-modal');
                    if (modal && modal.classList.contains('active')) {{
                        closeFullscreenModal();
                    }} else if (currentView !== 'world') {{
                        goBack();
                    }}
                }}
            }});

            // Fullscreen modal handlers - moves the actual globe instead of cloning
            const fullscreenBtn = document.getElementById('fullscreen-btn');
            const modal = document.getElementById('globe-modal');
            const modalCloseBtn = document.getElementById('modal-close-btn');
            const modalBackBtn = document.getElementById('modal-back-btn');
            const modalContainer = document.getElementById('modal-globe-container');
            const modalDetailPanel = document.getElementById('modal-detail-panel');
            const modalTooltip = document.getElementById('modal-tooltip');

            function openFullscreenModal() {{
                if (!modal || !modalContainer) return;

                isFullscreen = true;
                originalContainer = container;
                modal.classList.add('active');

                // Move the canvas to fullscreen container
                modalContainer.appendChild(renderer.domElement);

                // Update renderer size
                renderer.setSize(window.innerWidth, window.innerHeight);
                camera.aspect = window.innerWidth / window.innerHeight;
                camera.updateProjectionMatrix();

                // Update controls constraints for larger view
                controls.minDistance = 130;
                controls.maxDistance = 500;

                // Sync detail panel
                syncDetailPanel();
            }}

            function closeFullscreenModal() {{
                if (!modal || !originalContainer) return;

                isFullscreen = false;
                modal.classList.remove('active');

                // Move canvas back to original container
                originalContainer.appendChild(renderer.domElement);

                // Update renderer size
                renderer.setSize(originalContainer.clientWidth, originalContainer.clientHeight);
                camera.aspect = originalContainer.clientWidth / originalContainer.clientHeight;
                camera.updateProjectionMatrix();

                // Restore controls constraints
                controls.minDistance = 150;
                controls.maxDistance = 400;

                // Hide modal panels
                if (modalDetailPanel) modalDetailPanel.style.display = 'none';
                if (modalTooltip) modalTooltip.style.display = 'none';
            }}

            function syncDetailPanel() {{
                // Sync the detail panel content to modal
                const panel = document.getElementById('detail-panel');
                if (panel && modalDetailPanel) {{
                    modalDetailPanel.innerHTML = panel.innerHTML;
                    modalDetailPanel.style.display = panel.style.display;
                }}

                // Update modal back button visibility
                if (modalBackBtn) {{
                    modalBackBtn.style.display = currentView !== 'world' ? 'block' : 'none';
                }}
            }}

            // Override updateDetailPanel to sync both panels
            const originalUpdateDetailPanel = updateDetailPanel;
            updateDetailPanel = function(data) {{
                originalUpdateDetailPanel(data);
                if (isFullscreen) {{
                    syncDetailPanel();
                }}
            }};

            // Handle window resize for fullscreen
            window.addEventListener('resize', () => {{
                if (isFullscreen && modal.classList.contains('active')) {{
                    renderer.setSize(window.innerWidth, window.innerHeight);
                    camera.aspect = window.innerWidth / window.innerHeight;
                    camera.updateProjectionMatrix();
                }}
            }});

            // Update mousemove for fullscreen tooltip
            renderer.domElement.addEventListener('mousemove', (e) => {{
                if (!isFullscreen) return;

                const rect = renderer.domElement.getBoundingClientRect();
                mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
                mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;

                raycaster.setFromCamera(mouse, camera);

                // Check state markers first when in US view
                if (currentView === 'country' && selectedCountry && selectedCountry.code === 'US' && stateMarkers.length > 0) {{
                    const stateIntersects = raycaster.intersectObjects(stateMarkers.map(m => m.mesh));
                    if (stateIntersects.length > 0 && modalTooltip) {{
                        const data = stateIntersects[0].object.userData;
                        const name = data.stateName || US_STATE_NAMES[data.state] || data.state;
                        modalTooltip.innerHTML = `<strong style="color:#ff6b6b">${{name}}</strong><br>${{data.views.toLocaleString()}} views<br><small style="color:var(--muted)">Click for cities</small>`;
                        modalTooltip.style.display = 'block';
                        modalTooltip.style.left = (e.clientX + 15) + 'px';
                        modalTooltip.style.top = (e.clientY + 15) + 'px';
                        renderer.domElement.style.cursor = 'pointer';
                        return;
                    }}
                }}

                // Check city markers
                if (cityMarkers.length > 0 && (currentView === 'state' || (currentView === 'country' && selectedCountry && selectedCountry.code !== 'US'))) {{
                    const cityIntersects = raycaster.intersectObjects(cityMarkers.map(m => m.mesh));
                    if (cityIntersects.length > 0 && modalTooltip) {{
                        const data = cityIntersects[0].object.userData;
                        const cityName = data.city || 'Unknown';
                        const regionName = data.region ? ` (${{US_STATE_NAMES[data.region] || data.region}})` : '';
                        modalTooltip.innerHTML = `<strong style="color:#f39c12">${{cityName}}</strong>${{regionName}}<br>${{data.views.toLocaleString()}} views`;
                        modalTooltip.style.display = 'block';
                        modalTooltip.style.left = (e.clientX + 15) + 'px';
                        modalTooltip.style.top = (e.clientY + 15) + 'px';
                        renderer.domElement.style.cursor = 'pointer';
                        return;
                    }}
                }}

                // Check country markers
                const intersects = raycaster.intersectObjects(markers.filter(m => m.visible));
                if (intersects.length > 0 && modalTooltip) {{
                    const data = intersects[0].object.userData;
                    const name = COUNTRY_NAMES[data.country] || data.country;
                    const hint = data.country === 'US' ? 'Click for states' : 'Click to zoom';
                    modalTooltip.innerHTML = `<strong style="color:var(--accent)">${{name}}</strong><br>${{data.views.toLocaleString()}} views<br><small style="color:var(--muted)">${{hint}}</small>`;
                    modalTooltip.style.display = 'block';
                    modalTooltip.style.left = (e.clientX + 15) + 'px';
                    modalTooltip.style.top = (e.clientY + 15) + 'px';
                    renderer.domElement.style.cursor = 'pointer';
                }} else if (modalTooltip) {{
                    modalTooltip.style.display = 'none';
                    renderer.domElement.style.cursor = 'grab';
                }}
            }});

            if (fullscreenBtn) {{
                fullscreenBtn.addEventListener('click', openFullscreenModal);
            }}

            if (modalCloseBtn) {{
                modalCloseBtn.addEventListener('click', closeFullscreenModal);
            }}

            if (modalBackBtn) {{
                modalBackBtn.addEventListener('click', goBack);
            }}

            // Handle resize
            const resizeObserver = new ResizeObserver(() => {{
                camera.aspect = container.clientWidth / container.clientHeight;
                camera.updateProjectionMatrix();
                renderer.setSize(container.clientWidth, container.clientHeight);
            }});
            resizeObserver.observe(container);

            // Animation loop
            function animate() {{
                requestAnimationFrame(animate);
                controls.update();
                renderer.render(scene, camera);
            }}
            animate();
        }}

        // Initialize when DOM is ready
        if (document.readyState === 'loading') {{
            document.addEventListener('DOMContentLoaded', initGlobe);
        }} else {{
            initGlobe();
        }}
    </script>
</body>
</html>
"""
        return HTMLResponse(content=html)

    @router.get("/api/stats")
    async def api_stats(
        period: str = "7d",
        analytics_auth: Optional[str] = Cookie(None)
    ):
        """API endpoint for dashboard data."""
        if passkey and not _verify_auth(analytics_auth, expected_hash):
            return {"error": "unauthorized"}, 401

        data = await client.get_dashboard_data(period)
        return data.model_dump()

    @router.get("/api/realtime", response_class=HTMLResponse)
    async def api_realtime(analytics_auth: Optional[str] = Cookie(None)):
        """Get realtime visitor count (last 5 minutes) - returns HTML for HTMX."""
        if passkey and not _verify_auth(analytics_auth, expected_hash):
            return HTMLResponse(content="<span>-</span>", status_code=401)

        count = await client.get_realtime_count()
        # Return styled count with pulse indicator if visitors are present
        if count > 0:
            return HTMLResponse(content=f'<span style="display: flex; align-items: center; gap: 8px;">{count:,} <span class="loading-dot"></span></span>')
        return HTMLResponse(content=f'<span>{count:,}</span>')

    return router
