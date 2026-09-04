// Enrich missing fields on a journalist or creator row using Exa + Lovable AI Gateway.
// POST { kind: "journalist" | "creator", id: number, fields?: string[] }
// If `fields` is omitted, enriches all empty/null fields the row supports.
import { createClient } from "npm:@supabase/supabase-js@2.45.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const enrichVersionHeaders = { ...corsHeaders, "X-Enrich-Version": "creator-fields-003" };
const jsonHeaders = { ...enrichVersionHeaders, "Content-Type": "application/json" };

const JOURNALIST_FIELDS = ["email", "category", "titles", "xhandle", "outlet", "country", "linkedin_url"] as const;
const CREATOR_FIELDS = ["email", "category", "bio", "ig_handle", "ig_followers", "ig_engagement_rate", "youtube_url", "youtube_subscribers", "linkedin_url", "country"] as const;

const EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const BAD_EMAIL_RE = /^(info|hello|contact|support|press|admin|noreply|no-reply|sales|hr|webmaster|privacy|legal|advertising|subscribe|newsletter|tips)@/i;

const FIELD_DESCRIPTIONS: Record<string, string> = {
  email: "Direct professional email address (avoid generic info@/press@).",
  category: "Primary beat or topic category (e.g. Technology, Finance, Health).",
  titles: "Current job title(s) at their outlet (e.g. Senior Reporter).",
  xhandle: "X/Twitter handle starting with @.",
  outlet: "Name of the publication or outlet they write for.",
  country: "Country they are based in.",
  bio: "Short professional bio (1-2 sentences).",
  ig_handle: "Instagram handle without the @ symbol (just the username).",
  ig_followers: "Instagram follower count as an integer (no commas).",
  ig_engagement_rate: "Instagram engagement rate as a decimal between 0 and 1 (e.g. 0.034 for 3.4%).",
  youtube_url: "Full YouTube channel URL (https://www.youtube.com/...).",
  youtube_subscribers: "YouTube subscriber count as an integer (no commas).",
  linkedin_url: "Full LinkedIn profile URL of the person (must contain linkedin.com/in/).",
};

function sanitizeQuery(query: string): string {
  return query.replace(/[\n\r\t]+/g, " ").replace(/\s+/g, " ").trim().slice(0, 350);
}

async function hunterFindEmail({ fullName, domain, company }: { fullName: string; domain?: string; company?: string }): Promise<string | null> {
  const key = Deno.env.get("HUNTER_API_KEY");
  if (!key || !fullName || (!domain && !company)) return null;
  try {
    const params = new URLSearchParams({ full_name: fullName, api_key: key });
    if (domain) params.set("domain", domain);
    if (company) params.set("company", company);
    const r = await fetch(`https://api.hunter.io/v2/email-finder?${params.toString()}`);
    if (!r.ok) return null;
    const j = await r.json();
    const email = j?.data?.email;
    return typeof email === "string" && email.includes("@") && !BAD_EMAIL_RE.test(email) ? email : null;
  } catch {
    return null;
  }
}

function scoreHunterEmail(entry: { value?: string; first_name?: string; last_name?: string; position?: string }, fullName: string): number {
  const email = (entry?.value ?? "").toLowerCase();
  if (!email || BAD_EMAIL_RE.test(email)) return -1;
  const parts = fullName.toLowerCase().split(/\s+/).filter((p) => p.length > 1);
  const first = (entry.first_name ?? "").toLowerCase();
  const last = (entry.last_name ?? "").toLowerCase();
  let score = 0;
  if (first && parts.includes(first)) score += 3;
  if (last && parts.includes(last)) score += 3;
  for (const p of parts) if (email.includes(p)) score += 1;
  if (entry.position) score += 0.5;
  return score;
}

async function hunterDomainSearch({ fullName, domain }: { fullName: string; domain: string }): Promise<{ email: string; role: boolean } | null> {
  const key = Deno.env.get("HUNTER_API_KEY");
  if (!key || !domain || !fullName) return null;
  try {
    const params = new URLSearchParams({ domain, api_key: key, limit: "100" });
    const r = await fetch(`https://api.hunter.io/v2/domain-search?${params.toString()}`);
    if (!r.ok) return null;
    const j = await r.json();
    const emails: Array<{ value?: string; first_name?: string; last_name?: string; position?: string; type?: string }> = j?.data?.emails ?? [];
    if (!emails.length) return null;
    let best: { email: string; score: number } | null = null;
    for (const e of emails) {
      const s = scoreHunterEmail(e, fullName);
      if (s > 0 && (!best || s > best.score)) best = { email: (e.value ?? "").toLowerCase(), score: s };
    }
    if (best && best.score >= 3) return { email: best.email, role: false };
    // role-email fallback
    const ROLE_RE = /^(newsroom|editorial|editor|tips|news|contact|press)@/i;
    for (const e of emails) {
      const v = (e.value ?? "").toLowerCase();
      if (v && ROLE_RE.test(v) && v.endsWith(`@${domain.toLowerCase()}`)) {
        return { email: v, role: true };
      }
    }
    return null;
  } catch {
    return null;
  }
}

let snovTokenCache: { token: string; expiresAt: number } | null = null;
async function snovGetToken(): Promise<string | null> {
  const id = Deno.env.get("SNOV_CLIENT_ID");
  const secret = Deno.env.get("SNOV_CLIENT_SECRET");
  if (!id || !secret) return null;
  if (snovTokenCache && snovTokenCache.expiresAt > Date.now() + 60_000) return snovTokenCache.token;
  try {
    const r = await fetch("https://api.snov.io/v1/oauth/access_token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grant_type: "client_credentials", client_id: id, client_secret: secret }),
    });
    if (!r.ok) return null;
    const j = await r.json();
    if (!j?.access_token) return null;
    snovTokenCache = { token: j.access_token, expiresAt: Date.now() + (Number(j.expires_in ?? 3600) * 1000) };
    return j.access_token;
  } catch {
    return null;
  }
}

async function snovFindEmail({ fullName, domain, company }: { fullName: string; domain?: string; company?: string }): Promise<{ email: string | null; status: string; error: string | null }> {
  if (!domain) return { email: null, status: "skipped_no_domain", error: null };
  const tokens = (fullName || "").trim().split(/\s+/).filter(Boolean);
  if (tokens.length < 2) return { email: null, status: "skipped_no_name", error: null };
  const firstName = tokens[0];
  const lastName = tokens[tokens.length - 1];
  const token = await snovGetToken();
  if (!token) return { email: null, status: "no_token", error: "snov_auth_failed" };
  try {
    const payload: Record<string, string> = { domain, firstName, lastName };
    if (company) payload.company = company;
    const r = await fetch("https://api.snov.io/v2/email-finder", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify(payload),
    });
    const text = await r.text();
    if (!r.ok) return { email: null, status: `http_${r.status}`, error: text.slice(0, 200) };
    let j: any = {};
    try { j = JSON.parse(text); } catch { return { email: null, status: "invalid_json", error: text.slice(0, 200) }; }
    const email: string | undefined = j?.data?.email ?? j?.email ?? j?.data?.emails?.[0]?.email;
    if (email && email.includes("@") && !BAD_EMAIL_RE.test(email)) {
      return { email, status: "ok", error: null };
    }
    return { email: null, status: "no_match", error: null };
  } catch (e) {
    return { email: null, status: "exception", error: e instanceof Error ? e.message : String(e) };
  }
}

async function exaSearch(query: string, numResults = 5, includeDomains: string[] = []): Promise<{ results: Array<{ url: string; title: string; text: string }>; error: string | null; providerResponseText: string | null }> {
  const key = Deno.env.get("EXA_API_KEY");
  if (!key) return { results: [], error: "EXA_API_KEY missing", providerResponseText: null };

  const sanitized = sanitizeQuery(query);
  if (!sanitized || sanitized.length < 3) return { results: [], error: "query_too_short", providerResponseText: null };

  const exaPayload: Record<string, unknown> = {
    query: sanitized,
    numResults,
    type: "auto",
    contents: { text: { maxCharacters: 4000 } },
  };
  if (includeDomains.length) exaPayload.includeDomains = includeDomains;

  const r = await fetch("https://api.exa.ai/search", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": key, "Authorization": `Bearer ${key}` },
    body: JSON.stringify(exaPayload),
  });

  const text = await r.text();
  if (!r.ok) return { results: [], error: text || `status ${r.status}`, providerResponseText: text };

  try {
    const data = JSON.parse(text);
    return {
      results: (data.results ?? []).map((x: { url: string; title?: string; text?: string }) => ({ url: x.url, title: x.title ?? "", text: x.text ?? "" })),
      error: null,
      providerResponseText: text,
    };
  } catch {
    return { results: [], error: "invalid provider JSON", providerResponseText: text };
  }
}

function deriveDomain(explicitDomain: string, outlet: string, sourceUrl: string): string {
  const fromDomain = hostFrom(explicitDomain);
  if (fromDomain) return fromDomain;
  const fromUrl = hostFrom(sourceUrl);
  if (fromUrl) return fromUrl;
  if (!outlet) return "";
  const fromOutlet = hostFrom(outlet);
  if (fromOutlet && fromOutlet.includes(".")) return fromOutlet;
  return "";
}

async function resolveOutletDomain(outlet: string): Promise<string | null> {
  const cleanOutlet = clean(outlet);
  if (!cleanOutlet || cleanOutlet.length < 3) return null;
  const direct = hostFrom(cleanOutlet);
  if (direct && direct.includes(".")) return direct;
  const q = sanitizeQuery(`official website ${cleanOutlet}`);
  if (!q) return null;
  const exa = await exaSearch(q, 3);
  if (!exa.results.length) return null;
  for (const result of exa.results) {
    const host = hostFrom(result.url);
    if (!host) continue;
    if (/linkedin\.com|x\.com|twitter\.com|facebook\.com|instagram\.com|youtube\.com|wikipedia\.org/.test(host)) continue;
    return host.replace(/^www\./, "");
  }
  return null;
}

function hostFrom(value: string): string | null {
  try { return new URL(value.startsWith("http") ? value : `https://${value}`).hostname.replace(/^www\./, ""); } catch { return null; }
}

function pickEmail(snippets: Array<{ url: string; text: string }>, name: string): { email: string; source_url: string } | null {
  const nameParts = name.toLowerCase().split(/\s+/).filter((p) => p.length > 2);
  const matches = snippets.flatMap((s) => (`${s.url}\n${s.text}`.match(EMAIL_RE) ?? []).map((raw) => ({
    email: raw.trim().replace(/[),.;:]+$/, ""),
    source_url: s.url,
  }))).filter((m) => !BAD_EMAIL_RE.test(m.email) && !/\.(png|jpg|jpeg|gif|webp|svg)$/i.test(m.email));
  if (!matches.length) return null;
  return matches.find((m) => nameParts.some((p) => m.email.toLowerCase().includes(p))) ?? matches[0];
}

function normalizeLinkedInUrl(value: string): string | null {
  const cleanUrl = value.trim().split(/[?#]/)[0].replace(/\/$/, "");
  if (!/linkedin\.com\/in\//i.test(cleanUrl)) return null;
  return cleanUrl.startsWith("http") ? cleanUrl : `https://${cleanUrl.replace(/^\/\//, "")}`;
}

function normalizeYouTubeUrl(value: string): string | null {
  let cleanUrl = value.trim().split(/[?#]/)[0].replace(/\/$/, "");
  if (!/(^|\.)youtube\.com\/(channel\/|c\/|user\/|@)/i.test(cleanUrl)) return null;
  cleanUrl = cleanUrl.replace(/(youtube\.com\/@[^/]+)\/.+$/i, "$1");
  return cleanUrl.startsWith("http") ? cleanUrl : `https://${cleanUrl.replace(/^\/\//, "")}`;
}

function youtubeUrlsFromHit(hit: { url: string; title?: string; text?: string }): string[] {
  const raw = `${hit.url}\n${hit.title ?? ""}\n${hit.text ?? ""}`;
  const matches = raw.match(/(?:https?:\/\/)?(?:www\.)?youtube\.com\/(?:channel\/[^\s"'<>),]+|c\/[^\s"'<>),]+|user\/[^\s"'<>),]+|@[^\s"'<>),/]+(?:\/[^\s"'<>),]+)?)/gi) ?? [];
  return matches.map((u) => normalizeYouTubeUrl(u)).filter((u): u is string => Boolean(u));
}

function scoreLinkedInHit(hit: { url: string; title?: string; text?: string }, name: string, context: string): number {
  const url = (hit.url || "").toLowerCase();
  const haystack = `${hit.title ?? ""} ${hit.text ?? ""} ${url}`.toLowerCase();
  const tokens = name.toLowerCase().split(/\s+/).filter((t) => t.length > 1);
  let score = 0;
  for (const token of tokens) if (haystack.includes(token) || url.includes(token)) score += 3;
  for (const token of context.toLowerCase().split(/\s+/).filter((t) => t.length > 3)) if (haystack.includes(token)) score += 1;
  if (/linkedin\.com\/in\//.test(url)) score += 2;
  return score;
}

async function findLinkedInUrl(name: string, outlet: string, title: string, country: string): Promise<{ url: string | null; error: string | null }> {
  const context = [outlet, title, country].filter(Boolean).join(" ");
  const queries = [
    `site:linkedin.com/in "${name}" ${context}`,
    `"${name}" ${context} LinkedIn`,
  ].map(sanitizeQuery).filter((q, i, arr) => q.length >= 3 && arr.indexOf(q) === i);

  let firstError: string | null = null;
  for (const query of queries) {
    const res = await exaSearch(query, 10, ["linkedin.com"]);
    if (res.error && !firstError) firstError = res.error;
    const hits = res.results
      .map((hit) => ({ hit, url: normalizeLinkedInUrl(hit.url) }))
      .filter((item): item is { hit: { url: string; title: string; text: string }; url: string } => Boolean(item.url))
      .map((item) => ({ ...item, score: scoreLinkedInHit(item.hit, name, context) }))
      .sort((a, b) => b.score - a.score);
    const best = hits.find((h) => h.score >= 5) ?? hits[0];
    if (best) return { url: best.url, error: null };
  }
  return { url: null, error: firstError };
}

async function findYouTubeUrl(name: string, igHandle: string, context: string): Promise<{ url: string | null; error: string | null }> {
  const cleanHandle = igHandle.replace(/^@/, "");
  const queries = [
    cleanHandle ? `site:youtube.com/@ "${cleanHandle}"` : "",
    cleanHandle ? `site:youtube.com "${cleanHandle}" YouTube channel` : "",
    `site:youtube.com/@ "${name}" ${context}`,
    `site:youtube.com/channel "${name}" ${context}`,
    `site:youtube.com "${name}" ${context} YouTube channel`,
    `"${name}" ${cleanHandle} YouTube channel`,
  ].map(sanitizeQuery).filter((q, i, arr) => q.length >= 3 && arr.indexOf(q) === i);

  let firstError: string | null = null;
  for (const query of queries) {
    const res = await exaSearch(query, 10);
    if (res.error && !firstError) firstError = res.error;
    const tokens = [cleanHandle, ...name.toLowerCase().split(/\s+/)].filter((t) => t.length > 1);
    const hits = res.results
      .flatMap((hit) => youtubeUrlsFromHit(hit).map((url) => ({ hit, url })))
      .filter((item): item is { hit: { url: string; title: string; text: string }; url: string } => Boolean(item.url))
      .map((item) => {
        const haystack = `${item.hit.title} ${item.hit.text} ${item.hit.url}`.toLowerCase();
        const score = tokens.reduce((sum, token) => sum + (haystack.includes(token.toLowerCase()) ? 2 : 0), 0) + (/youtube\.com\/@/i.test(item.url) ? 2 : 1);
        return { ...item, score };
      })
      .sort((a, b) => b.score - a.score);
    const best = hits.find((h) => h.score >= 3) ?? hits[0];
    if (best) return { url: best.url, error: null };
  }
  return { url: null, error: firstError };
}

function json(payload: Record<string, unknown>, status = 200): Response {
  return new Response(JSON.stringify(payload), { status, headers: jsonHeaders });
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function clean(value: unknown): string {
  return typeof value === "string" || typeof value === "number" ? String(value).trim() : "";
}

function numericId(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseCompactNumber(raw: string): number | null {
  const match = raw.replace(/,/g, "").match(/(\d+(?:\.\d+)?)\s*([kmb]|thousand|million|billion)?/i);
  if (!match) return null;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return null;
  const unit = (match[2] ?? "").toLowerCase();
  return Math.round(value * (/^b|billion/.test(unit) ? 1_000_000_000 : /^m|million/.test(unit) ? 1_000_000 : /^k|thousand/.test(unit) ? 1_000 : 1));
}

function findYouTubeSubscriberCount(snippets: Array<{ url: string; text: string; title?: string }>): number | null {
  for (const snippet of snippets) {
    const haystack = `${snippet.title ?? ""} ${snippet.text}`;
    const match = haystack.match(/(\d[\d,.]*(?:\.\d+)?\s*(?:k|m|b|thousand|million|billion)?)\s*(?:YouTube\s*)?(?:subscribers|subs)\b/i);
    const parsed = match ? parseCompactNumber(match[1]) : null;
    if (parsed) return parsed;
  }
  return null;
}

function deriveCountryFromText(snippets: Array<{ url: string; text: string; title?: string }>, countryContext: string): string | null {
  const knownCountries = ["United States", "United Kingdom", "Canada", "Australia", "Germany", "France", "Italy", "Spain", "Netherlands", "Sweden", "Norway", "Denmark", "Finland", "Ireland", "Portugal", "Brazil", "Mexico", "India", "Japan", "South Korea", "Singapore", "United Arab Emirates", "South Africa", "New Zealand"];
  const haystack = `${countryContext}\n${snippets.map((s) => `${s.title ?? ""} ${s.text}`).join("\n")}`;
  const based = haystack.match(/(?:based|located|lives|from)\s+in\s+([A-Z][A-Za-z .'-]{2,40})/);
  if (based) {
    const phrase = based[1].replace(/\s+(?:and|with|who|where|as|for|\|).*$/i, "").trim();
    const known = knownCountries.find((c) => new RegExp(`\\b${c.replace(/ /g, "\\s+")}\\b`, "i").test(phrase));
    if (known) return known;
  }
  return knownCountries.find((c) => new RegExp(`\\b${c.replace(/ /g, "\\s+")}\\b`, "i").test(haystack)) ?? null;
}

function sourceTable(value: unknown): "journalist" | "creators" | null {
  if (value === "journalist" || value === "journalists") return "journalist";
  if (value === "creator" || value === "creators") return "creators";
  return null;
}

async function isAdmin(admin: ReturnType<typeof createClient>, userId: string) {
  const { data, error } = await admin
    .from("user_roles")
    .select("role")
    .eq("user_id", userId)
    .eq("role", "admin")
    .maybeSingle();
  if (error) {
    console.error("enrich-contact admin role lookup failed", error);
    return false;
  }
  return Boolean(data);
}

async function extractFields(
  name: string,
  context: string,
  snippets: Array<{ url: string; text: string }>,
  fields: string[],
): Promise<Record<string, any>> {
  const key = Deno.env.get("LOVABLE_API_KEY");
  if (!key || !snippets.length) return {};

  const fieldList = fields.map((f) => `- ${f}: ${FIELD_DESCRIPTIONS[f] ?? f}`).join("\n");
  const corpus = snippets.slice(0, 6).map((s, i) => `[Source ${i + 1}] ${s.url}\n${s.text.slice(0, 1500)}`).join("\n\n");
  const prompt = `You are extracting verified contact details for "${name}"${context ? ` (${context})` : ""}.
Only return values explicitly supported by the sources. If unsure, omit the field.
Return JSON with these fields (omit any you cannot verify):
${fieldList}

Sources:
${corpus}`;

  try {
    const r = await fetch("https://ai.gateway.lovable.dev/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` },
      body: JSON.stringify({
        model: "google/gemini-2.5-flash",
        messages: [
          { role: "system", content: "Return only a single JSON object. No markdown." },
          { role: "user", content: prompt },
        ],
        max_completion_tokens: 8192,
      }),
    });
    if (!r.ok) {
      const errText = await r.text().catch(() => "");
      console.error("LOVABLE_AI_GATEWAY_ERROR", r.status, errText.slice(0, 500));
      return {};
    }
    const j = await r.json();
    const finish = j.choices?.[0]?.finish_reason;
    const txt = j.choices?.[0]?.message?.content ?? "";
    if (finish && finish !== "stop") console.warn("LOVABLE_AI_FINISH_REASON", finish, "len", txt.length);
    const m = txt.match(/\{[\s\S]*\}/);
    if (!m) {
      console.warn("LOVABLE_AI_NO_JSON", "finish", finish, "preview", txt.slice(0, 300));
      return {};
    }
    const parsed = JSON.parse(m[0]);
    const out: Record<string, unknown> = {};
    const numericFields = new Set(["ig_followers", "youtube_subscribers", "ig_engagement_rate"]);
    for (const f of fields) {
      const v = parsed[f];
      if (v === null || v === undefined || v === "") continue;
      if (numericFields.has(f)) {
        const n = typeof v === "number" ? v : Number(String(v).replace(/[, ]+/g, ""));
        if (Number.isFinite(n) && n > 0) {
          // Normalize engagement rate: if it came as a percent (>1), convert
          out[f] = f === "ig_engagement_rate" && n > 1 ? n / 100 : n;
        }
      } else if (typeof v === "string" && v.trim()) {
        out[f] = v.trim();
      }
    }
    return out as Record<string, string>;
  } catch {
    return {};
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: enrichVersionHeaders });

  try {
    const authHeader = req.headers.get("Authorization") ?? "";
    if (!authHeader) return json({ email: null, found: false, source: "none", confidence: null, error: "auth" }, 401);

    const userClient = createClient(
      Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_ANON_KEY")!,
      { global: { headers: { Authorization: authHeader } } },
    );
    const { data: { user } } = await userClient.auth.getUser();
    if (!user) return json({ email: null, found: false, source: "none", confidence: null, error: "auth" }, 401);

    const body = await req.json().catch(() => ({}));
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return json({ email: null, found: false, source: "none", confidence: null, error: "invalid_payload" });
    }

    const admin = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);
    const root = record(body);
    const clientContact = { ...record(root.contact), ...root };
    const table = sourceTable(clientContact.source_table) ?? (root.kind === "creator" ? "creators" : "journalist");
    const allFields = (table === "journalist" ? JOURNALIST_FIELDS : CREATOR_FIELDS) as readonly string[];
    const requestedFields = root.fields;
    const sourceId = numericId(clientContact.source_id ?? root.id);
    // A contact ID from the browser is never authority to update a shared row.
    // Regular users receive enrichment results only; verified admins may persist them.
    const shouldUpdateDb = await isAdmin(admin, user.id)
      && sourceId !== null
      && sourceTable(clientContact.source_table ?? root.kind) !== null;

    let row: Record<string, unknown> = {};
    if (shouldUpdateDb) {
      const { data: dbRow } = await admin.from(table).select("*").eq("id", sourceId).maybeSingle();
      if (dbRow) row = dbRow;
    }

    const contact = shouldUpdateDb ? row : clientContact;

    const targetFields = (Array.isArray(requestedFields) && requestedFields.length
      ? requestedFields.filter((f: string) => allFields.includes(f))
      : allFields.filter((f) => row[f] === null || row[f] === undefined || row[f] === ""));
    const fieldsToExtract = targetFields.length ? targetFields : ["email"];

    const name = clean(contact.name);
    const nameLetters = (name.match(/\p{L}/gu) ?? []).length;
    const nameTokens = name.split(/\s+/).filter((t) => (t.match(/\p{L}/gu) ?? []).length >= 2);
    if (!name || nameLetters < 2 || nameTokens.length < 1 || name === "—") {
      return json({ email: null, found: false, source: "none", confidence: null, error: "insufficient_identity", reason: "insufficient_identity" });
    }

    const outlet = clean(contact.outlet ?? row.outlet);
    const title = clean(contact.title ?? contact.titles ?? row.title ?? row.titles);
    const country = clean(contact.country ?? row.country);
    const categoryContext = clean(contact.category ?? row.category);
    const bioContext = clean(contact.bio ?? row.bio).slice(0, 180);
    const platformContext = clean(contact.platform ?? row.platform);
    const sourceUrl = clean(contact.url ?? contact.source_url ?? row.url ?? row.website);
    let outletDomain = deriveDomain(clean(contact.domain ?? root.domain), outlet, sourceUrl);
    if (!outletDomain && fieldsToExtract.includes("email")) {
      const resolved = await resolveOutletDomain(outlet);
      if (resolved) outletDomain = resolved;
    }
    const context = [outlet, title, country, categoryContext].filter(Boolean).join(" · ");

    const debug: Record<string, unknown> = { providersTried: [] as string[] };
    const tried = debug.providersTried as string[];

    // 1) Existing DB email
    const existingEmail = clean(row.email);
    if (fieldsToExtract.includes("email") && existingEmail && existingEmail.includes("@") && !BAD_EMAIL_RE.test(existingEmail)) {
      return json({ email: existingEmail, found: true, source: "database", confidence: 1, error: null, debug });
    }

    // 2/3) Hunter passes
    if (fieldsToExtract.includes("email") && (outletDomain || outlet)) {
      tried.push("hunter");
      const hunterEmail = await hunterFindEmail({ fullName: name, domain: outletDomain || undefined, company: outlet || undefined });
      if (hunterEmail) {
        if (shouldUpdateDb && sourceId !== null) {
          const update: Record<string, unknown> = { email: hunterEmail, enrichment_source_url: `hunter:${outletDomain || outlet}`, enriched_at: new Date().toISOString() };
          let { error: upErr } = await admin.from(table).update(update).eq("id", sourceId);
          if (upErr && /enrichment_source_url|enriched_at/.test(upErr.message ?? "")) {
            await admin.from(table).update({ email: hunterEmail }).eq("id", sourceId);
          }
        }
        return json({ email: hunterEmail, found: true, source: "hunter", confidence: 0.88, error: null, debug });
      }

      if (outletDomain) {
        const domainResult = await hunterDomainSearch({ fullName: name, domain: outletDomain });
        if (domainResult) {
          const { email: domainEmail, role } = domainResult;
          const src = role ? "hunter-domain-role" : "hunter-domain";
          const conf = role ? 0.45 : 0.78;
          if (shouldUpdateDb && sourceId !== null) {
            const update: Record<string, unknown> = { email: domainEmail, enrichment_source_url: `${src}:${outletDomain}`, enriched_at: new Date().toISOString() };
            let { error: upErr } = await admin.from(table).update(update).eq("id", sourceId);
            if (upErr && /enrichment_source_url|enriched_at/.test(upErr.message ?? "")) {
              await admin.from(table).update({ email: domainEmail }).eq("id", sourceId);
            }
          }
          return json({ email: domainEmail, found: true, source: src, confidence: conf, error: null, debug });
        }
      }
    }

    // 4) Snov fallback
    if (fieldsToExtract.includes("email") && outletDomain) {
      tried.push("snov");
      const snov = await snovFindEmail({ fullName: name, domain: outletDomain, company: outlet || undefined });
      debug.snovStatus = snov.status;
      debug.snovError = snov.error;
      if (snov.email) {
        if (shouldUpdateDb && sourceId !== null) {
          const update: Record<string, unknown> = { email: snov.email, enrichment_source_url: `snov:${outletDomain}`, enriched_at: new Date().toISOString() };
          let { error: upErr } = await admin.from(table).update(update).eq("id", sourceId);
          if (upErr && /enrichment_source_url|enriched_at/.test(upErr.message ?? "")) {
            await admin.from(table).update({ email: snov.email }).eq("id", sourceId);
          }
        }
        return json({ email: snov.email, found: true, source: "snov", confidence: 0.75, error: null, debug });
      }
    }

    // 5) LinkedIn URL via Exa (works without an extra API)
    if (fieldsToExtract.includes("linkedin_url")) {
      tried.push("linkedin-exa");
      const linkedIn = await findLinkedInUrl(name, outlet, title, country);
      if (linkedIn.error) debug.linkedin_error = linkedIn.error;
      if (linkedIn.url) {
        const liUrl = linkedIn.url;
        if (shouldUpdateDb && sourceId !== null) {
          await admin.from(table).update({ linkedin_url: liUrl }).eq("id", sourceId);
        }
        // If only linkedin was requested, return now.
        if (fieldsToExtract.length === 1) {
          return json({ email: null, linkedin_url: liUrl, found: true, source: "exa-linkedin", confidence: 0.7, error: null, debug });
        }
        debug.linkedin_url = liUrl;
      }
    }

    const existingIg = clean(contact.ig_handle ?? contact.handle ?? row.ig_handle);
    const existingYoutubeUrl = normalizeYouTubeUrl(clean(contact.youtube_url ?? row.youtube_url)) ?? "";
    const creatorContext = [outlet, title, country, categoryContext, platformContext, bioContext, existingIg ? `@${existingIg.replace(/^@/, "")}` : "", existingYoutubeUrl].filter(Boolean).join(" ");
    const wantsCreatorSocial = table === "creators" && fieldsToExtract.some((f) =>
      ["ig_handle", "ig_followers", "ig_engagement_rate", "youtube_url", "youtube_subscribers", "category", "bio", "country"].includes(f),
    );

    if (table === "creators" && (fieldsToExtract.includes("youtube_url") || fieldsToExtract.includes("youtube_subscribers"))) {
      tried.push("youtube-exa");
      const youtube = await findYouTubeUrl(name, existingIg, creatorContext);
      if (youtube.error) debug.youtube_error = youtube.error;
      if (youtube.url) {
        if (shouldUpdateDb && sourceId !== null) {
          await admin.from(table).update({ youtube_url: youtube.url }).eq("id", sourceId);
        }
        if (fieldsToExtract.length === 1 && fieldsToExtract.includes("youtube_url")) {
          return json({ email: null, youtube_url: youtube.url, found: true, source: "exa-youtube", confidence: 0.7, error: null, debug });
        }
        debug.youtube_url = youtube.url;
      }
    }

    const queries = [
      fieldsToExtract.includes("email") ? `"${name}" ${outlet} ${title} ${outletDomain ? `site:${outletDomain}` : ""} email contact` : "",
      fieldsToExtract.includes("email") ? `"${name}" ${outlet} email` : "",
      fieldsToExtract.includes("email") && table === "creators" && existingIg ? `"${name}" "${existingIg.replace(/^@/, "")}" email contact` : "",
      fieldsToExtract.includes("email") && table !== "creators" ? `"${name}" email` : "",
      table === "journalist" && outletDomain ? `"${name}" journalist contact site:${outletDomain}` : "",
      table === "journalist" && outlet ? `"${name}" ${outlet} journalist contact profile` : "",
      fieldsToExtract.includes("country") ? `"${name}" ${creatorContext} based in country location` : "",
      fieldsToExtract.includes("country") ? `"${name}" ${creatorContext} creator profile country` : "",
      wantsCreatorSocial ? `"${name}" ${creatorContext} instagram followers` : "",
      wantsCreatorSocial ? `"${name}" ${creatorContext} youtube channel subscribers` : "",
      wantsCreatorSocial && (existingYoutubeUrl || debug.youtube_url) ? `"${existingYoutubeUrl || String(debug.youtube_url)}" subscribers` : "",
      wantsCreatorSocial && existingIg ? `site:instagram.com "${existingIg.replace(/^@/, "")}"` : "",
      wantsCreatorSocial ? `"${name}" creator influencer profile bio` : "",
      wantsCreatorSocial ? `"${name}" socialblade ${existingIg ? existingIg.replace(/^@/, "") : ""}` : "",
    ].map(sanitizeQuery).filter((q, i, arr) => q.length >= 3 && arr.indexOf(q) === i);

    const settled = await Promise.all(queries.map((q) => exaSearch(q, 10)));
    const providerErrors = settled.map((s) => s.error).filter(Boolean) as string[];
    const allSnippets = settled.flatMap((s) => s.results)
      .filter((s, i, arr) => s.url && arr.findIndex((x) => x.url === s.url) === i)
      .slice(0, 30);

    if (!allSnippets.length) {
      if (debug.linkedin_url) {
        const linkedinUrl = String(debug.linkedin_url);
        return json({ email: null, linkedin_url: linkedinUrl, found: true, source: "exa-linkedin", confidence: 0.7, error: null, debug });
      }
      return json({ email: null, found: false, source: "none", confidence: null, error: "no_data_found", reason: outletDomain ? "no_results" : "no_domain", provider_error: providerErrors[0] ?? null, debug });
    }

    const extracted = await extractFields(name, context, allSnippets, fieldsToExtract);
    if (debug.youtube_url && fieldsToExtract.includes("youtube_url") && !extracted.youtube_url) {
      extracted.youtube_url = String(debug.youtube_url);
    }
    if (extracted.youtube_url) {
      const yt = normalizeYouTubeUrl(String(extracted.youtube_url));
      if (yt) extracted.youtube_url = yt;
      else delete extracted.youtube_url;
    }
    if (fieldsToExtract.includes("youtube_subscribers") && !extracted.youtube_subscribers) {
      const subs = findYouTubeSubscriberCount(allSnippets);
      if (subs) extracted.youtube_subscribers = subs;
    }
    if (fieldsToExtract.includes("country") && !extracted.country) {
      const derivedCountry = deriveCountryFromText(allSnippets, creatorContext);
      if (derivedCountry) extracted.country = derivedCountry;
    }
    if (extracted.email) {
      const cleaned = extracted.email.trim().replace(/[),.;:]+$/, "");
      if (!cleaned.includes("@") || BAD_EMAIL_RE.test(cleaned)) delete extracted.email;
      else extracted.email = cleaned;
    }

    let emailSourceUrl = allSnippets[0]?.url ?? null;
    if (fieldsToExtract.includes("email") && !extracted.email) {
      const directEmail = pickEmail(allSnippets, name);
      if (directEmail) {
        extracted.email = directEmail.email;
        emailSourceUrl = directEmail.source_url;
      }
    }

    if (!Object.keys(extracted).length && !debug.linkedin_url) {
      const error = fieldsToExtract.every((f) => f === "email") ? "no_email_found" : "no_data_found";
      return json({ email: null, found: false, source: "none", confidence: null, error, reason: outletDomain ? "no_match" : "no_domain", debug });
    }

    const email = extracted.email ?? null;
    const linkedinUrl = extracted.linkedin_url ?? debug.linkedin_url ?? null;
    const otherKeys = Object.keys(extracted).filter((k) => !["email", "linkedin_url"].includes(k));
    const found = Boolean(email || linkedinUrl || otherKeys.length);
    if (!shouldUpdateDb || sourceId === null) {
      return json({ ...extracted, email, linkedin_url: linkedinUrl, found, source: email ? "exa" : linkedinUrl ? "exa-linkedin" : otherKeys.length ? "exa" : "none", confidence: found ? 0.7 : null, error: found ? null : "no_data_found", debug });
    }

    const update: Record<string, unknown> = { ...extracted, enrichment_source_url: emailSourceUrl, enriched_at: new Date().toISOString() };
    let { error: upErr } = await admin.from(table).update(update).eq("id", sourceId);
    if (upErr && /enrichment_source_url|enriched_at/.test(upErr.message ?? "")) {
      const r = await admin.from(table).update(extracted).eq("id", sourceId);
      upErr = r.error;
    }

    return json({ ...extracted, email, linkedin_url: linkedinUrl, found, source: email ? "exa" : linkedinUrl ? "exa-linkedin" : otherKeys.length ? "exa" : "none", confidence: found ? 0.7 : null, error: found ? null : "no_data_found", debug });
  } catch (e) {
    return json({ email: null, found: false, source: "none", confidence: null, error: null, internal_error: e instanceof Error ? e.message : String(e) });
  }
});
