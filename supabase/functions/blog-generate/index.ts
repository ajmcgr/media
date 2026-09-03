// supabase/functions/blog-generate/index.ts
// Queues a fresh blog post from pg_cron, inserts the post first, then attaches
// an optional cover image. This keeps cron from timing out before a post exists.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { generateAndStoreBlogImages } from "../_shared/blog-image.ts";
import { requireAdminOrInternal } from "../_shared/privileged-auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const TOPICS = [
  "AI tools for PR teams",
  "How to pitch tech journalists in 2026",
  "Building a media list that gets replies",
  "Influencer marketing benchmarks",
  "PR measurement beyond AVE",
  "Crisis communications playbook",
  "Newsjacking ethically",
  "How journalists actually use email",
  "Working with creators vs. journalists",
  "PR for early-stage startups",
  "Press release alternatives that work",
  "Embargoes, exclusives and how to choose",
  "Building relationships with reporters",
  "Podcast PR strategy for founders",
  "Local press vs national: when to pitch which",
];

type BlogSupabaseClient = ReturnType<typeof createClient<any, "public", any>>;

function slugify(s: string) {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 80);
}

function jsonResponse(body: Record<string, unknown>, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function requiredEnv(name: string) {
  const value = Deno.env.get(name);
  if (!value) throw new Error(`${name} missing`);
  return value;
}

async function callAI(body: Record<string, unknown>) {
  const key = requiredEnv("OPENAI_API_KEY");
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`AI ${res.status}: ${await res.text()}`);
  return await res.json();
}

async function buildPost(topic: string) {
  const writeRes = await callAI({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "You are a senior PR strategist writing for Media AI's blog. Write practical, specific, non-fluffy posts for PR pros, founders and comms leads. Use semantic HTML (h2, h3, p, ul, ol, blockquote, strong). 800-1100 words. No <h1>, no <html>/<body>, no markdown.",
      },
      {
        role: "user",
        content: `Write a blog post about: "${topic}". Return JSON only with keys: title (under 70 chars, compelling), description (under 160 chars, meta description), content (HTML body, no h1).`,
      },
    ],
    response_format: { type: "json_object" },
  });

  const raw = writeRes.choices?.[0]?.message?.content ?? "{}";
  const parsed = JSON.parse(raw);
  const title = String(parsed.title || topic).trim();
  const description = String(parsed.description || "").trim().slice(0, 160);
  const content = String(parsed.content || "").trim();
  if (!title || !content) throw new Error("AI returned empty post");

  return { title, description, content };
}

async function createUniqueSlug(supabase: BlogSupabaseClient, title: string) {
  const base = slugify(title) || `post-${Date.now().toString(36)}`;
  let slug = base;

  for (let i = 0; i < 5; i += 1) {
    const { data, error } = await supabase
      .from("blog_posts")
      .select("slug")
      .eq("slug", slug)
      .maybeSingle();

    if (error) throw error;
    if (!data) return slug;
    slug = `${base}-${Date.now().toString(36).slice(-4)}${i || ""}`;
  }

  return `${base}-${crypto.randomUUID().slice(0, 8)}`;
}

async function getLatestPostCreatedAt(supabase: BlogSupabaseClient) {
  const { data, error } = await supabase
    .from("blog_posts")
    .select("created_at")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  const createdAt = (data as { created_at?: string } | null)?.created_at;
  return createdAt || null;
}

async function attachCoverImage(supabase: BlogSupabaseClient, postId: string, post: {
  slug: string;
  title: string;
  description: string;
  content: string;
  topic: string;
  published?: string | null;
}) {
  try {
    const image = await generateAndStoreBlogImages(supabase as never, post as never);
    const { error: updateError } = await supabase
      .from("blog_posts")
      .update({ image })
      .eq("id", postId);
    if (updateError) console.error("blog-generate image update failed", updateError.message);
  } catch (err) {
    console.error("blog-generate image failed", err);
  }
}

async function generateAndInsert(topic: string, options: { skipIfRecent?: boolean } = {}) {
  console.log("blog-generate started", { topic });

  const supabase = createClient(
    requiredEnv("SUPABASE_URL"),
    requiredEnv("SUPABASE_SERVICE_ROLE_KEY"),
  );

  if (options.skipIfRecent) {
    const latestCreatedAt = await getLatestPostCreatedAt(supabase);
    const latestTime = latestCreatedAt ? new Date(latestCreatedAt).getTime() : 0;
    const threeDaysMs = 72 * 60 * 60 * 1000;

    if (latestTime && Date.now() - latestTime < threeDaysMs) {
      console.log("blog-generate skipped: recent post exists", { latestCreatedAt });
      return { skipped: true, reason: "recent_post", latestCreatedAt };
    }
  }

  const post = await buildPost(topic);
  const slug = await createUniqueSlug(supabase, post.title);
  const { data: inserted, error } = await supabase
    .from("blog_posts")
    .insert({
      slug,
      title: post.title,
      description: post.description,
      image: null,
      content: post.content,
      topic,
    })
    .select()
    .single();

  if (error) throw error;

  console.log("blog-generate inserted", { id: inserted.id, slug });
  await attachCoverImage(supabase, inserted.id, {
    slug,
    title: post.title,
    description: post.description,
    content: post.content,
    topic,
    published: (inserted as { published?: string }).published ?? null,
  });
  return inserted;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return jsonResponse({ ok: false, error: "Method not allowed" }, 405);

  try {
    const authorization = await requireAdminOrInternal(req);
    if ("error" in authorization) {
      return jsonResponse({ ok: false, error: authorization.error }, authorization.status);
    }

    const body = await req.json().catch(() => ({}));
    const topic = typeof body.topic === "string" && body.topic.trim()
      ? body.topic.trim().slice(0, 140)
      : TOPICS[Math.floor(Math.random() * TOPICS.length)];

    const skipIfRecent = body.source === "pg_cron" || body.source === "cron_install_probe";

    if (body.sync === true) {
      const result = await generateAndInsert(topic, { skipIfRecent });
      return jsonResponse({ ok: true, ...("skipped" in result ? result : { post: result }) });
    }

    const job = generateAndInsert(topic, { skipIfRecent }).catch((err) => {
      console.error("blog-generate error", err);
    });

    const edgeRuntime = (globalThis as typeof globalThis & {
      EdgeRuntime?: { waitUntil?: (promise: Promise<unknown>) => void };
    }).EdgeRuntime;

    if (edgeRuntime?.waitUntil) {
      edgeRuntime.waitUntil(job);
    } else {
      await job;
    }

    return jsonResponse({ ok: true, queued: true, topic });
  } catch (err) {
    console.error("blog-generate request error", err);
    return jsonResponse({ ok: false, error: err instanceof Error ? err.message : String(err) }, 500);
  }
});
