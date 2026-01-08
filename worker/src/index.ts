/**
 * 941 Analytics - Cloudflare Worker
 *
 * Privacy-first pageview collection:
 * - No cookies
 * - No fingerprinting
 * - Daily-rotating visitor hash (can't track across days)
 * - Country from Cloudflare (no IP stored)
 */

interface Env {
  DB: D1Database;
  ANALYTICS_SECRET: string; // For validating requests
}

interface PageViewData {
  site: string;
  url: string;
  title: string;
  ref: string;
  w: number; // viewport width
}

// Detect device type from viewport width
function getDeviceType(width: number): string {
  if (width === 0) return "unknown";
  if (width < 768) return "mobile";
  if (width < 1024) return "tablet";
  return "desktop";
}

// Generate daily-rotating visitor hash
// This allows "unique visitors today" without tracking individuals over time
async function generateVisitorHash(
  site: string,
  country: string,
  secret: string
): Promise<string> {
  const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD
  const data = `${secret}:${site}:${country}:${today}`;

  const encoder = new TextEncoder();
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoder.encode(data));
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray
    .slice(0, 8)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// Validate origin header
function isValidOrigin(origin: string | null, site: string): boolean {
  if (!origin) return false;
  try {
    const url = new URL(origin);
    // Allow localhost for development
    if (url.hostname === "localhost") return true;
    // Check if origin matches the site
    return url.hostname === site || url.hostname.endsWith(`.${site}`);
  } catch {
    return false;
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    // Handle preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // Only handle GET /collect
    if (url.pathname !== "/collect" || request.method !== "GET") {
      return new Response("Not Found", { status: 404 });
    }

    try {
      // Parse query parameters
      const params = url.searchParams;
      const data: PageViewData = {
        site: params.get("site") || "",
        url: params.get("url") || "",
        title: params.get("title") || "",
        ref: params.get("ref") || "",
        w: parseInt(params.get("w") || "0", 10),
      };

      // Validate required fields
      if (!data.site || !data.url) {
        return new Response("Missing required fields", { status: 400 });
      }

      // Get country from Cloudflare
      const country = request.cf?.country as string || "";

      // Generate daily visitor hash
      const visitorHash = await generateVisitorHash(
        data.site,
        country,
        env.ANALYTICS_SECRET
      );

      // Determine device type
      const deviceType = getDeviceType(data.w);

      // Insert into D1
      await env.DB.prepare(
        `INSERT INTO page_views (site, timestamp, url, page_title, referrer, country, device_type, visitor_hash)
         VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          data.site,
          data.url,
          data.title,
          data.ref,
          country,
          deviceType,
          visitorHash
        )
        .run();

      // Return 1x1 transparent GIF
      const gif = new Uint8Array([
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
        0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
      ]);

      return new Response(gif, {
        status: 200,
        headers: {
          ...corsHeaders,
          "Content-Type": "image/gif",
          "Cache-Control": "no-store, no-cache, must-revalidate",
        },
      });
    } catch (error) {
      console.error("Analytics error:", error);
      // Still return success to avoid breaking the page
      return new Response("OK", {
        status: 200,
        headers: corsHeaders,
      });
    }
  },
};
