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
            return RedirectResponse(url="./login", status_code=302)

        data = await client.get_dashboard_data(period)

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

        # Simple HTML dashboard
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics - {site_name}</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <style>
        :root {{
            --bg: #0a0d12;
            --surface: #12161d;
            --border: #1e2530;
            --text: #e8edf3;
            --muted: #9ba3ad;
            --accent: #59b2cc;
            --green: #00F200;
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
                    <h2>Top Referrers</h2>
                    <table>
                        <thead><tr><th>Source</th><th>Views</th></tr></thead>
                        <tbody>
                            {''.join(f'<tr><td>{r["referrer"] or "Direct"}</td><td>{r["views"]:,}</td></tr>' for r in data.top_referrers) or '<tr><td colspan="2">No referrer data</td></tr>'}
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

            <div class="right-column">
                <div class="section" style="padding: 0; overflow: hidden;">
                    <div id="globe-container">
                        <span class="globe-title">Visitors by Country</span>
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

    <script>
        // Globe visualization
        const globeData = {str(globe_data).replace("'", '"')};

        // Country centroids (lat, lng)
        const countryCentroids = {{
            'US': [39.8, -98.5], 'CN': [35.0, 105.0], 'IN': [20.0, 77.0],
            'BR': [-14.2, -51.9], 'RU': [61.5, 105.3], 'JP': [36.2, 138.3],
            'DE': [51.2, 10.5], 'GB': [55.4, -3.4], 'FR': [46.2, 2.2],
            'IT': [41.9, 12.6], 'CA': [56.1, -106.3], 'AU': [-25.3, 133.8],
            'KR': [35.9, 127.8], 'ES': [40.5, -3.7], 'MX': [23.6, -102.6],
            'ID': [-0.8, 113.9], 'NL': [52.1, 5.3], 'SA': [23.9, 45.1],
            'TR': [39.0, 35.2], 'CH': [46.8, 8.2], 'PL': [51.9, 19.1],
            'SE': [60.1, 18.6], 'BE': [50.5, 4.5], 'TH': [15.9, 100.5],
            'AT': [47.5, 14.6], 'NO': [60.5, 8.5], 'AE': [23.4, 53.8],
            'SG': [1.4, 103.8], 'MY': [4.2, 101.9], 'PH': [12.9, 121.8],
            'DK': [56.3, 9.5], 'FI': [61.9, 25.7], 'IE': [53.1, -8.0],
            'IL': [31.0, 34.9], 'HK': [22.4, 114.1], 'NZ': [-40.9, 174.9],
            'CZ': [49.8, 15.5], 'PT': [39.4, -8.2], 'RO': [45.9, 25.0],
            'VN': [14.1, 108.3], 'ZA': [-30.6, 22.9], 'GR': [39.1, 21.8],
            'CL': [-35.7, -71.5], 'AR': [-38.4, -63.6], 'CO': [4.6, -74.3],
            'HU': [47.2, 19.5], 'UA': [48.4, 31.2], 'EG': [26.8, 30.8],
            'PK': [30.4, 69.3], 'NG': [9.1, 8.7], 'BD': [23.7, 90.4],
            'KE': [-0.0, 37.9], 'PE': [-9.2, -75.0], 'TW': [23.7, 121.0]
        }};

        function initGlobe() {{
            const container = document.getElementById('globe-container');
            if (!container) return;

            const width = container.clientWidth;
            const height = container.clientHeight;

            // Scene setup
            const scene = new THREE.Scene();
            const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 1000);
            camera.position.z = 2.5;

            const renderer = new THREE.WebGLRenderer({{ antialias: true, alpha: true }});
            renderer.setSize(width, height);
            renderer.setPixelRatio(window.devicePixelRatio);
            container.appendChild(renderer.domElement);

            // Earth sphere
            const geometry = new THREE.SphereGeometry(1, 64, 64);
            const material = new THREE.MeshBasicMaterial({{
                color: 0x12161d,
                transparent: true,
                opacity: 0.9
            }});
            const earth = new THREE.Mesh(geometry, material);
            scene.add(earth);

            // Wireframe overlay
            const wireGeometry = new THREE.SphereGeometry(1.002, 32, 32);
            const wireMaterial = new THREE.MeshBasicMaterial({{
                color: 0x1e2530,
                wireframe: true,
                transparent: true,
                opacity: 0.3
            }});
            const wireframe = new THREE.Mesh(wireGeometry, wireMaterial);
            scene.add(wireframe);

            // Atmosphere glow
            const glowGeometry = new THREE.SphereGeometry(1.1, 32, 32);
            const glowMaterial = new THREE.MeshBasicMaterial({{
                color: 0x59b2cc,
                transparent: true,
                opacity: 0.05,
                side: THREE.BackSide
            }});
            const glow = new THREE.Mesh(glowGeometry, glowMaterial);
            scene.add(glow);

            // Add visitor markers
            globeData.forEach(item => {{
                const coords = countryCentroids[item.country];
                if (!coords) return;

                const lat = coords[0] * Math.PI / 180;
                const lng = -coords[1] * Math.PI / 180;

                const radius = 1.02;
                const x = radius * Math.cos(lat) * Math.cos(lng);
                const y = radius * Math.sin(lat);
                const z = radius * Math.cos(lat) * Math.sin(lng);

                const size = 0.02 + (item.normalized * 0.06);
                const markerGeometry = new THREE.SphereGeometry(size, 16, 16);
                const markerMaterial = new THREE.MeshBasicMaterial({{
                    color: 0x59b2cc,
                    transparent: true,
                    opacity: 0.8
                }});
                const marker = new THREE.Mesh(markerGeometry, markerMaterial);
                marker.position.set(x, y, z);
                scene.add(marker);

                // Glow around marker
                const glowSize = size * 2;
                const markerGlowGeometry = new THREE.SphereGeometry(glowSize, 16, 16);
                const markerGlowMaterial = new THREE.MeshBasicMaterial({{
                    color: 0x59b2cc,
                    transparent: true,
                    opacity: 0.2
                }});
                const markerGlow = new THREE.Mesh(markerGlowGeometry, markerGlowMaterial);
                markerGlow.position.set(x, y, z);
                scene.add(markerGlow);
            }});

            // Animation
            let rotationSpeed = 0.001;
            function animate() {{
                requestAnimationFrame(animate);
                earth.rotation.y += rotationSpeed;
                wireframe.rotation.y += rotationSpeed;
                scene.children.forEach(child => {{
                    if (child !== earth && child !== wireframe && child !== glow) {{
                        // Rotate markers with earth
                    }}
                }});
                renderer.render(scene, camera);
            }}
            animate();

            // Handle resize
            window.addEventListener('resize', () => {{
                const newWidth = container.clientWidth;
                const newHeight = container.clientHeight;
                camera.aspect = newWidth / newHeight;
                camera.updateProjectionMatrix();
                renderer.setSize(newWidth, newHeight);
            }});
        }}

        // Initialize globe when DOM is ready
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

    @router.get("/api/realtime")
    async def api_realtime(analytics_auth: Optional[str] = Cookie(None)):
        """Get realtime visitor count (last 5 minutes)."""
        if passkey and not _verify_auth(analytics_auth, expected_hash):
            return {"error": "unauthorized"}, 401

        count = await client.get_realtime_count()
        return {"visitors": count}

    return router
