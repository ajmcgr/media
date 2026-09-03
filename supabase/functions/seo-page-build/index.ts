// Generates a programmatic SEO landing page (title/H1/meta/intro/faq + filter
// suggestions) from a topic prompt, then inserts into public.seo_pages.
// Reuses the OPENAI_API_KEY pattern already used by blog-generate.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { requireAdminOrInternal } from "../_shared/privileged-auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function slugify(s: string) {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "").slice(0, 80);
}

async function callAI(body: Record<string, unknown>) {
  const key = Deno.env.get("OPENAI_API_KEY");
  if (!key) throw new Error("OPENAI_API_KEY missing");
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`AI ${res.status}: ${await res.text()}`);
  return await res.json();
}

async function buildAndUpsert(
  supabase: ReturnType<typeof createClient>,
  topic: string,
  forcedSource: string | undefined,
  forcedSlug: string | undefined,
  publish: boolean,
) {
  const sys =
    "You are an SEO expert building landing pages for Media AI, a platform with a database of journalists and creators. " +
    "Given a topic phrase, return JSON describing a SEO landing page that lists matching contacts. " +
    "Be specific, useful, non-fluffy. No fluff filler. Use semantic HTML for intro_html (p, h2, h3, ul, strong). 250-400 words intro. " +
    "Output JSON only.";
  const user = `Topic: "${topic}"

Return JSON with these keys:
- source: "journalist" or "creator"
- slug: short URL slug, lowercase-kebab, max 60 chars
- title: SEO title under 60 chars including the topic keyword
- h1: page H1
- meta_description: under 155 chars
- intro_html: 250-400 word intro in semantic HTML (no <h1>)
- filters: object with optional keys: topics, category, country, outlet, ig_followers_min, youtube_subs_min, limit (max 100). Prefer 1-2 filters.
- faq: array of 3-5 {question, answer}`;

  const aiRes = await callAI({
    model: "gpt-4o-mini",
    messages: [{ role: "system", content: sys }, { role: "user", content: user }],
    response_format: { type: "json_object" },
    temperature: 0.7,
  });
  const p = JSON.parse(aiRes.choices?.[0]?.message?.content ?? "{}");
  const source: string = forcedSource || (p.source === "creator" ? "creator" : "journalist");
  const slug = slugify(forcedSlug || p.slug || topic);
  const row = {
    slug,
    source,
    title: String(p.title || topic).slice(0, 70).trim(),
    h1: String(p.h1 || p.title || topic).trim(),
    meta_description: String(p.meta_description || "").slice(0, 160).trim(),
    intro_html: String(p.intro_html || "").trim(),
    filters: (p.filters && typeof p.filters === "object") ? p.filters : {},
    faq: Array.isArray(p.faq) ? p.faq : [],
    published: publish,
  };
  const { data, error } = await supabase.from("seo_pages").upsert(row, { onConflict: "slug" }).select().single();
  if (error) throw error;
  return data;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const authorization = await requireAdminOrInternal(req);
    if ("error" in authorization) {
      return new Response(JSON.stringify({ error: authorization.error }), {
        status: authorization.status,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const body = await req.json().catch(() => ({}));
    const { topic, source: forcedSource, slug: forcedSlug, publish, auto, count } = body || {};

    const url = Deno.env.get("SUPABASE_URL")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(url, serviceKey);

    // ===== AUTO MODE: ask AI to brainstorm N topics, then generate each =====
    if (auto === true) {
      const n = Math.min(Math.max(Number(count) || 10, 1), 20);

      // Pull a small sample of real categories/topics/countries to ground the AI
      const [{ data: jSample }, { data: cSample }, { data: existing }] = await Promise.all([
        supabase.from("journalist").select("category,topics,country,outlet").limit(200),
        supabase.from("creators").select("category,country").limit(200),
        supabase.from("seo_pages").select("title,slug").limit(200),
      ]);
      const cats = Array.from(new Set([...(jSample ?? []).map((r: any) => r.category), ...(cSample ?? []).map((r: any) => r.category)].filter(Boolean))).slice(0, 40);
      const topics = Array.from(new Set((jSample ?? []).flatMap((r: any) => String(r.topics || "").split(/[,;|]/).map((s) => s.trim())).filter(Boolean))).slice(0, 60);
      const countries = Array.from(new Set([...(jSample ?? []).map((r: any) => r.country), ...(cSample ?? []).map((r: any) => r.country)].filter(Boolean))).slice(0, 30);
      const existingTitles = (existing ?? []).map((r: any) => r.title);

      const ideaRes = await callAI({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You generate high-intent, SEO-optimized list-page topic ideas for a media/PR platform. Output JSON only." },
          { role: "user", content: `Generate ${n} distinct SEO landing page topics (e.g. "Top AI journalists in San Francisco", "Best fintech podcasters", "Cybersecurity reporters at major outlets").

Ground them in this real data:
- Categories: ${cats.join(", ")}
- Topics/beats: ${topics.join(", ")}
- Countries: ${countries.join(", ")}

AVOID duplicates of these existing pages: ${existingTitles.join(" | ") || "(none yet)"}

Mix journalists and creators. Mix beat-only, beat+location, and outlet-type angles. Each topic should be a natural search query a PR person would Google.

Return JSON: { "topics": ["...", "..."] }` },
        ],
        response_format: { type: "json_object" },
        temperature: 0.9,
      });
      const ideas: string[] = (() => {
        try { return JSON.parse(ideaRes.choices?.[0]?.message?.content ?? "{}").topics ?? []; }
        catch { return []; }
      })().slice(0, n);

      const results: Array<{ topic: string; ok: boolean; slug?: string; error?: string }> = [];
      for (const t of ideas) {
        try {
          const page = await buildAndUpsert(supabase, t, undefined, undefined, publish === true);
          results.push({ topic: t, ok: true, slug: page.slug });
        } catch (e) {
          results.push({ topic: t, ok: false, error: e instanceof Error ? e.message : "unknown" });
        }
      }
      return new Response(JSON.stringify({ ok: true, generated: results }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    if (!topic || typeof topic !== "string") {
      return new Response(JSON.stringify({ error: "topic required" }), {
        status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const data = await buildAndUpsert(supabase, topic, forcedSource, forcedSlug, publish === true);
    return new Response(JSON.stringify({ ok: true, page: data }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("seo-page-build error:", e);
    return new Response(JSON.stringify({ error: e instanceof Error ? e.message : "Unknown" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
