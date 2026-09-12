import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const SITE_URL = "https://trymedia.ai";
const SUPABASE_URL = "https://uavbphkhomblzkjfuaot.supabase.co";
// This is the same public key shipped to the browser client. It cannot bypass RLS.
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVhdmJwaGtob21ibHpramZ1YW90Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzYyMjU0NDksImV4cCI6MjA1MTgwMTQ0OX0.BpHF9fxNgWWjMupXQ5GCJMj-n_iWJ27xAqm5fLXeudA";
const MINIMUM_QUALIFIED_CONTACTS = 50;
const output = resolve("public/journalist-sitemap.xml");

function escapeXml(value) {
  return String(value).replace(/[<>&'\"]/g, (char) => ({
    "<": "&lt;", ">": "&gt;", "&": "&amp;", "'": "&apos;", "\"": "&quot;",
  }[char]));
}

async function getEligibleBeats() {
  const params = new URLSearchParams({
    select: "beat_slug,qualified_contacts",
    qualified_contacts: `gte.${MINIMUM_QUALIFIED_CONTACTS}`,
    order: "qualified_contacts.desc",
  });
  const response = await fetch(`${SUPABASE_URL}/rest/v1/public_journalist_beats?${params}`, {
    headers: { apikey: SUPABASE_ANON_KEY, Authorization: `Bearer ${SUPABASE_ANON_KEY}` },
  });
  if (!response.ok) throw new Error(`Supabase sitemap query failed: ${response.status}`);
  const rows = await response.json();
  return rows.filter((row) => /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(row.beat_slug));
}

async function main() {
  try {
    const beats = await getEligibleBeats();
    const today = new Date().toISOString().slice(0, 10);
    const entries = [
      `<url><loc>${SITE_URL}/journalists</loc><lastmod>${today}</lastmod><changefreq>weekly</changefreq><priority>0.8</priority></url>`,
      ...beats.map((beat) => `<url><loc>${SITE_URL}/journalists/${escapeXml(beat.beat_slug)}</loc><lastmod>${today}</lastmod><changefreq>weekly</changefreq><priority>0.7</priority></url>`),
    ];
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n  ${entries.join("\n  ")}\n</urlset>\n`;
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, xml, "utf8");
    console.log(`Generated ${beats.length} eligible journalist beat URLs.`);
  } catch (error) {
    // Keep the checked-in sitemap during a transient build-time network outage.
    console.warn(`Journalist sitemap was not refreshed: ${error.message}`);
  }
}

await main();
