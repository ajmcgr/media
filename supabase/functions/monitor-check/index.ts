
// ===== Inlined branded email template =====
const _LOGO_URL = "https://trymedia.ai/email-logo-media.png";
const _BRAND_BLUE = "#1675e2";
const _PAGE_BG = "#f4f6f9";
const _CARD_BG = "#ffffff";
const _BORDER = "#e6e8ec";
const _DIVIDER = "#eef0f3";
const _HEADING = "#101214";
const _BODY_TEXT = "#4e5052";
const _MUTED = "#9aa0a6";

interface BrandedEmailOptions {
  preheader?: string;
  heading: string;
  body: string;
  cta?: { label: string; url: string };
  footerNote?: string;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function renderBrandedEmail(opts: BrandedEmailOptions): string {
  const { preheader = "", heading, body, cta, footerNote } = opts;
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /><title>${escapeHtml(heading)}</title></head>
<body style="margin:0;padding:0;background:${_PAGE_BG};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#22252a;-webkit-font-smoothing:antialiased;">
${preheader ? `<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">${preheader}</div>` : ""}
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${_PAGE_BG};padding:48px 16px;"><tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:${_CARD_BG};border-radius:14px;overflow:hidden;border:1px solid ${_BORDER};">
<tr><td align="center" style="padding:34px 32px 26px;border-bottom:1px solid ${_DIVIDER};"><img src="${_LOGO_URL}" alt="Media AI" width="180" style="width:180px;max-width:180px;height:auto;display:inline-block;border:0;outline:none;text-decoration:none;" /></td></tr>
<tr><td style="padding:36px 40px ${cta || footerNote ? "32px" : "40px"};">
<h1 style="margin:0 0 18px;font-size:22px;line-height:1.3;font-weight:700;color:${_HEADING};">${heading}</h1>
<div style="font-size:15px;line-height:1.6;color:${_BODY_TEXT};margin:0 0 ${cta ? "28px" : "0"};">${body}</div>
${cta ? `<div><a href="${escapeHtml(cta.url)}" style="display:inline-block;background:${_BRAND_BLUE};color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:13px 24px;border-radius:8px;">${escapeHtml(cta.label)}</a></div>` : ""}
</td></tr>
${footerNote ? `<tr><td align="center" style="padding:20px 32px 28px;border-top:1px solid ${_DIVIDER};"><p style="margin:0;font-size:13px;color:${_MUTED};line-height:1.5;">${footerNote}</p></td></tr>` : ""}
</table>
<p style="margin:18px 0 0;font-size:12px;color:${_MUTED};">© ${new Date().getFullYear()} Media AI · trymedia.ai</p>
</td></tr></table></body></html>`;
}
// ===== End inlined template =====

// Keyword Monitor — Google News based mention checker.
// Triggered manually from the UI or by the daily cron via monitor-run-all.
import { createClient } from "npm:@supabase/supabase-js@2.45.0";
import { requireUserOrInternal } from "../_shared/privileged-auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const APP_URL = "https://trymedia.ai/monitor";

interface Monitor {
  id: string;
  user_id: string;
  brand_name: string;
  website_url: string;
  competitor_urls: string[];
  keywords: string[];
  founder_names: string[];
  product_names: string[];
  email_alerts: boolean;
  alert_frequency: string;
}

type MentionType = "brand" | "competitor" | "founder" | "keyword" | "product";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function err(error: string, details?: unknown, status = 500) {
  return json({ error, details: details ? String(details) : undefined }, status);
}

function decodeEntities(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

function stripTags(s: string): string {
  return decodeEntities(s.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

function pickTag(xml: string, tag: string): string | null {
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const m = xml.match(re);
  if (!m) return null;
  let v = m[1].trim();
  v = v.replace(/^<!\[CDATA\[/, "").replace(/\]\]>$/, "");
  return v;
}

interface NewsItem {
  title: string;
  url: string;
  publisher: string;
  published_at: string | null;
  snippet: string;
  image_url: string | null;
}

function parseRss(xml: string): NewsItem[] {
  const items: NewsItem[] = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/gi;
  let m: RegExpExecArray | null;
  while ((m = itemRegex.exec(xml))) {
    const block = m[1];
    const title = stripTags(pickTag(block, "title") ?? "");
    const link = stripTags(pickTag(block, "link") ?? "");
    const pubDate = pickTag(block, "pubDate");
    const source = stripTags(pickTag(block, "source") ?? "");
    const desc = pickTag(block, "description") ?? "";
    const imgMatch = desc.match(/<img[^>]+src="([^"]+)"/i);
    items.push({
      title,
      url: link,
      publisher: source,
      published_at: pubDate ? new Date(pubDate).toISOString() : null,
      snippet: stripTags(desc).slice(0, 400),
      image_url: imgMatch ? imgMatch[1] : null,
    });
  }
  return items;
}

async function googleNews(term: string): Promise<NewsItem[]> {
  const q = encodeURIComponent(`"${term}"`);
  const url = `https://news.google.com/rss/search?q=${q}+when:7d&hl=en-US&gl=US&ceid=US:en`;
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "MediaAI-KeywordMonitor/1.0 (+https://trymedia.ai)" },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) {
      console.error("MONITOR_GNEWS_FAIL", term, res.status);
      return [];
    }
    const xml = await res.text();
    return parseRss(xml).slice(0, 15);
  } catch (e) {
    console.error("MONITOR_GNEWS_ERR", term, (e as Error).message);
    return [];
  }
}

const POSITIVE_WORDS = [
  "launch", "launches", "raises", "funding", "growth", "wins", "win",
  "award", "partnership", "expands", "milestone", "record", "success",
  "breakthrough", "acquires", "acquisition",
];
const NEGATIVE_WORDS = [
  "lawsuit", "sued", "fails", "fail", "decline", "down", "loss", "losses",
  "scandal", "fraud", "controversy", "fired", "lay off", "layoff", "layoffs",
  "breach", "hack", "leak", "criticism", "complaint", "outage",
];

function sentimentOf(text: string): "positive" | "neutral" | "negative" {
  const t = text.toLowerCase();
  let pos = 0, neg = 0;
  for (const w of POSITIVE_WORDS) if (t.includes(w)) pos++;
  for (const w of NEGATIVE_WORDS) if (t.includes(w)) neg++;
  if (neg > pos) return "negative";
  if (pos > neg) return "positive";
  return "neutral";
}

function hostFromUrl(u: string): string {
  try { return new URL(u.startsWith("http") ? u : `https://${u}`).hostname.replace(/^www\./, ""); }
  catch { return u; }
}

function buildTerms(m: Monitor): Array<{ term: string; type: MentionType }> {
  const terms: Array<{ term: string; type: MentionType }> = [];
  if (m.brand_name?.trim()) terms.push({ term: m.brand_name.trim(), type: "brand" });
  for (const f of m.founder_names ?? []) if (f?.trim()) terms.push({ term: f.trim(), type: "founder" });
  for (const c of m.competitor_urls ?? []) {
    const host = hostFromUrl(c);
    const name = host.split(".")[0];
    if (name) terms.push({ term: name, type: "competitor" });
  }
  for (const k of m.keywords ?? []) if (k?.trim()) terms.push({ term: k.trim(), type: "keyword" });
  for (const p of m.product_names ?? []) if (p?.trim()) terms.push({ term: p.trim(), type: "product" });
  // dedupe by lowercase term
  const seen = new Set<string>();
  return terms.filter((t) => {
    const k = t.term.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

async function sendInstantAlert(opts: {
  to: string;
  brand: string;
  mention: { title: string; url: string; publisher: string; mention_type: string; sentiment: string };
}) {
  const resendApiKey = Deno.env.get("RESEND_API_KEY")?.trim() ?? "";
  if (!resendApiKey) return;
  const html = renderBrandedEmail({
    preheader: `New mention for ${opts.brand}`,
    heading: `New mention: ${opts.brand}`,
    body: `
      <p style="margin:0 0 12px"><strong>${opts.mention.title}</strong></p>
      <p style="margin:0 0 8px;color:#4e5052">${opts.mention.publisher} · ${opts.mention.mention_type} · ${opts.mention.sentiment}</p>
      <p style="margin:0;font-size:13px;color:#9aa0a6"><a href="${opts.mention.url}">${opts.mention.url}</a></p>
    `,
    cta: { label: "Open Keyword Monitor", url: APP_URL },
  });

  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${resendApiKey}`,
    },
    body: JSON.stringify({
      from: "Media AI <hello@trymedia.ai>",
      to: [opts.to],
      subject: `New mention — ${opts.brand}`,
      html,
    }),
  });
  if (r.ok) console.log("MONITOR_EMAIL_SENT", opts.brand, opts.to);
  else console.error("MONITOR_EMAIL_FAIL", r.status, await r.text().catch(() => ""));
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return err("method_not_allowed", undefined, 405);

  try {
    const authorization = await requireUserOrInternal(req);
    if ("error" in authorization) return err(authorization.error, undefined, authorization.status);

    const body = await req.json().catch(() => ({}));
    const monitor_id = body?.monitor_id as string | undefined;
    if (!monitor_id) return err("monitor_id required", undefined, 400);

    console.log("MONITOR_RUN_CHECK_STARTED", monitor_id);

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRole) return err("missing_env", "SUPABASE envs not set");
    const supa = createClient(supabaseUrl, serviceRole);

    const { data: monitor, error: mErr } = await supa
      .from("brand_monitors")
      .select("*")
      .eq("id", monitor_id)
      .maybeSingle();
    if (mErr) return err("db_error", mErr.message);
    if (!monitor) return err("monitor_not_found", undefined, 404);

    const m = monitor as Monitor;
    if (authorization.caller.kind === "user" && m.user_id !== authorization.caller.userId) {
      return err("forbidden", undefined, 403);
    }
    const { data: userRow } = await supa.auth.admin.getUserById(m.user_id);
    const userEmail = userRow?.user?.email ?? null;

    const terms = buildTerms(m);
    let inserted = 0;
    let lastError: string | null = null;

    for (const { term, type } of terms) {
      const items = await googleNews(term);
      for (const it of items) {
        if (!it.url || !it.title) continue;
        const sentiment = sentimentOf(it.title + " " + it.snippet);
        const row = {
          monitor_id: m.id,
          user_id: m.user_id,
          url: it.url,
          url_kind: type === "competitor" ? "competitor" : "brand",
          mention_type: type,
          matched_keyword: term,
          source: "google_news",
          title: it.title,
          publisher: it.publisher,
          published_at: it.published_at,
          image_url: it.image_url,
          sentiment,
          summary: it.title,
          why_it_matters: null,
          pr_score: null,
          next_action: null,
          pitch_angle: null,
          email_sent: false,
        };
        const { error: insErr } = await supa
          .from("monitor_updates")
          .insert(row as never);
        if (insErr) {
          // duplicate (unique index) -> ignore
          if (!String(insErr.message).toLowerCase().includes("duplicate")) {
            lastError = insErr.message;
            console.error("MONITOR_INSERT_ERR", insErr.message);
          }
          continue;
        }
        inserted++;

        // instant priority alerts
        if (
          m.email_alerts &&
          m.alert_frequency === "instant" &&
          userEmail &&
          (type === "founder" || type === "competitor" || sentiment === "negative")
        ) {
          try {
            await sendInstantAlert({
              to: userEmail,
              brand: m.brand_name,
              mention: { title: it.title, url: it.url, publisher: it.publisher, mention_type: type, sentiment },
            });
            await supa.from("monitor_updates")
              .update({ email_sent: true } as never)
              .eq("monitor_id", m.id).eq("url", it.url);
          } catch (e) {
            console.error("alert send failed", e);
          }
        }
      }
    }

    const status = lastError ? "error" : (inserted > 0 ? "ok" : "no_results");
    await supa.from("brand_monitors").update({
      last_checked_at: new Date().toISOString(),
      last_status: status,
      last_error: lastError,
      last_mentions_found: inserted,
    } as never).eq("id", m.id);

    console.log("MONITOR_RUN_CHECK_FINISHED", m.id, { inserted, status });
    return json({ ok: true, inserted, status, terms_checked: terms.length });
  } catch (e) {
    console.error("monitor-check fatal", e);
    return err("internal_error", (e as Error).message);
  }
});
