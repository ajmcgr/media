// supabase/functions/blog-generate/index.ts
// Queues a fresh blog post from pg_cron, inserts the post first, then attaches
// an optional cover image. This keeps cron from timing out before a post exists.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { Image } from "https://deno.land/x/imagescript@1.2.17/mod.ts";

// Generates one editorial image per post, then derives hero / card / og
// variants and stores them in Supabase Storage under a deterministic path so
// the URLs can be derived on the client without extra DB columns:
//   blog-images/<yyyy>/<mm>/<slug>/hero.jpg | card.jpg | og.jpg


const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models";
const IMAGE_MODELS: { model: string; aspect?: boolean }[] = [
  { model: "gemini-3.1-flash-image", aspect: true },
  { model: "gemini-3-pro-image", aspect: true },
  { model: "gemini-2.5-flash-image" },
];
// Text models are versioned aggressively; try newest first and fall back.
const TEXT_MODELS = ["gemini-3.5-flash", "gemini-flash-latest", "gemini-2.0-flash"];
export const BUCKET = "blog";

const STYLE = [
  "Flat two-colour vector illustration in a simple silhouette style, exactly like a brand icon.",
  "ONLY two colours: pure white background (#FFFFFF) and a single brand blue (#1675E2).",
  "No gradients, no shading, no textures, no photographic elements, no 3D, no glow, no noise.",
  "Bold clean geometry with smooth edges, generous white negative space, centred composition,",
  "solid blue shapes on white — the look of a crisp SVG pictogram scaled large.",
  "Absolutely no text, letters, numbers, logos, watermarks, UI screenshots, clipart,",
  "no stock-photo people, no robots, no glowing brains, no extra accent colours.",
].join(" ");

// Deterministic per-slug art direction so two posts never render the same image,
// even when the art-director model is unavailable and we fall back to the title.
const COMPOSITIONS = [
  "one large centred silhouette shape with small satellite shapes",
  "a simple repeating pattern of solid blue silhouettes across the frame",
  "a single bold silhouette off-centre with wide white space beside it",
  "overlapping flat blue silhouettes forming one combined shape",
  "a row of stacked flat blue bars and simple silhouette forms",
  "a circular blue field with a white silhouette knocked out of it",
  "a minimal line-and-shape pictogram, thick uniform blue strokes on white",
  "two mirrored blue silhouettes with a narrow white gap between them",
];
const ACCENTS = [
  "solid brand blue #1675E2 on white",
  "brand blue #1675E2 with a lighter blue tint (#8FBDF1) as the only secondary tone",
  "brand blue #1675E2 knocked out in white on a blue field",
  "brand blue #1675E2 with a deeper blue (#0F4F9E) for the second flat tone",
];
const MOODS = [
  "flat, graphic and iconographic",
  "clean, minimal and confident",
  "bold, poster-like and simple",
  "quiet, spacious and precise",
];


function hashSlug(slug: string) {
  let h = 2166136261;
  for (let i = 0; i < slug.length; i += 1) {
    h ^= slug.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return Math.abs(h);
}

function variationFor(slug: string) {
  const h = hashSlug(slug || String(Math.random()));
  return [
    `Composition: ${COMPOSITIONS[h % COMPOSITIONS.length]}.`,
    `Accent colour: ${ACCENTS[Math.floor(h / 7) % ACCENTS.length]}.`,
    `Lighting and mood: ${MOODS[Math.floor(h / 13) % MOODS.length]}.`,
    "Make this image visually distinct from other artwork in the same series.",
  ].join(" ");
}


type Post = {
  slug: string;
  title?: string | null;
  description?: string | null;
  content?: string | null;
  topic?: string | null;
  published?: string | null;
};

function key() {
  const k = Deno.env.get("GEMINI_API_KEY");
  if (!k) throw new Error("GEMINI_API_KEY missing");
  return k;
}

const strip = (html: string) =>
  html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();

async function gemini(model: string, body: Record<string, unknown>) {
  const res = await fetch(`${GEMINI_BASE}/${model}:generateContent?key=${key()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`gemini ${model} ${res.status}: ${text.slice(0, 400)}`);
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`gemini ${model} non-JSON response: ${text.slice(0, 200)}`);
  }
}

/** Derive a concise visual prompt from the whole article, not just the title. */
export async function buildImagePrompt(post: Post): Promise<string> {
  const excerpt = strip(post.content || "").slice(0, 2500);
  const variation = variationFor(post.slug || post.title || "");
  const brief = [
    `Title: ${post.title || ""}`,
    `Excerpt: ${post.description || ""}`,
    `Topic/category: ${post.topic || ""}`,
    `Body: ${excerpt}`,
  ].join("\n");

  for (const model of TEXT_MODELS) {
    try {
      const json = await gemini(model, {
        systemInstruction: {
          parts: [{
            text:
              "You are an icon designer for a SaaS media brand. Read the article and reply with ONE sentence (max 35 words) describing a SIMPLE flat two-colour silhouette illustration (blue on white) that represents the article's core idea. Only simple bold shapes and silhouettes — never text, faces, gradients, shading, photos, logos, robots or brains. Reply with the sentence only.",

          }],
        },
        contents: [{ role: "user", parts: [{ text: brief }] }],
        generationConfig: { temperature: 1, maxOutputTokens: 200 },
      });
      const idea = (json.candidates?.[0]?.content?.parts ?? [])
        .map((p: { text?: string }) => p.text || "")
        .join(" ")
        .replace(/\s+/g, " ")
        .trim();
      if (idea) return `${idea} ${STYLE} ${variation}`;
    } catch (err) {
      console.error("blog-image prompt model failed", model, err);
    }
  }

  return `A simple flat blue-on-white silhouette illustration representing "${post.title || post.topic || "modern public relations"}". ${STYLE} ${variation}`;
}


async function generateOnce(prompt: string): Promise<Uint8Array> {
  let lastErr: unknown;
  for (const { model, aspect } of IMAGE_MODELS) {
    try {
      const json = await gemini(model, {
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
          responseModalities: ["TEXT", "IMAGE"],
          ...(aspect ? { imageConfig: { aspectRatio: "16:9" } } : {}),
        },
      });
      const parts = json.candidates?.[0]?.content?.parts ?? [];
      const inline = parts.find((p: { inlineData?: { data?: string } }) => p.inlineData?.data);
      const b64 = inline?.inlineData?.data as string | undefined;
      if (!b64) throw new Error("no image part in response");
      return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
    } catch (err) {
      lastErr = err;
      console.error("blog-image model failed", model, err);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

/** Generate with automatic retries (exponential backoff). */
export async function generateImage(prompt: string, attempts = 3): Promise<Uint8Array> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i += 1) {
    try {
      return await generateOnce(prompt);
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1) await new Promise((r) => setTimeout(r, 1200 * (i + 1)));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

export const VARIANTS = {
  hero: { w: 1536, h: 864 },
  card: { w: 800, h: 450 },
  og: { w: 1200, h: 630 },
} as const;

export function imageBasePath(slug: string, published?: string | null) {
  const d = published ? new Date(published) : new Date();
  const date = Number.isNaN(d.getTime()) ? new Date() : d;
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  return `blog-images/${yyyy}/${mm}/${slug}`;
}

type SupabaseLike = {
  storage: {
    from: (b: string) => {
      upload: (
        path: string,
        body: Uint8Array,
        opts: { contentType: string; upsert: boolean; cacheControl: string },
      ) => Promise<{ error: { message: string } | null }>;
      getPublicUrl: (path: string) => { data: { publicUrl: string } };
    };
  };
};

/**
 * Full pipeline: prompt -> Gemini -> resize -> upload hero/card/og.
 * Returns the public hero URL (what `blog_posts.image` stores).
 */
export async function generateAndStoreBlogImages(
  supabase: SupabaseLike,
  post: Post,
): Promise<string> {
  const prompt = await buildImagePrompt(post);
  const raw = await generateImage(prompt);
  const decoded = await Image.decode(raw);
  const base = imageBasePath(post.slug, post.published);
  const storage = supabase.storage.from(BUCKET);

  let heroUrl = "";
  for (const [name, size] of Object.entries(VARIANTS)) {
    const variant = decoded.clone().cover(size.w, size.h);
    const bytes = await variant.encodeJPEG(84);
    const path = `${base}/${name}.jpg`;
    const { error } = await storage.upload(path, bytes, {
      contentType: "image/jpeg",
      upsert: true,
      cacheControl: "31536000",
    });
    if (error) throw new Error(`upload ${path}: ${error.message}`);
    if (name === "hero") heroUrl = storage.getPublicUrl(path).data.publicUrl;
  }

  console.log("blog-image generated", { slug: post.slug, prompt: prompt.slice(0, 120) });
  return heroUrl;
}
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

type AdminAuthorization =
  | { caller: { kind: "internal" } | { kind: "admin"; userId: string } }
  | { error: string; status: number };

// Kept local so this function can be deployed directly from the Supabase Dashboard.
async function requireAdminOrInternal(req: Request): Promise<AdminAuthorization> {
  try {
    const token = (req.headers.get("Authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
    if (!token) return { error: "missing_auth", status: 401 };
    const url = Deno.env.get("SUPABASE_URL")?.trim();
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")?.trim();
    if (!url || !serviceKey) throw new Error("Supabase authorization configuration missing");
    if (token === serviceKey) return { caller: { kind: "internal" } };

    const admin = createClient(url, serviceKey);
    const { data: authData, error: authError } = await admin.auth.getUser(token);
    if (authError || !authData.user) return { error: "invalid_auth", status: 401 };
    const { data: role, error: roleError } = await admin
      .from("user_roles")
      .select("role")
      .eq("user_id", authData.user.id)
      .eq("role", "admin")
      .maybeSingle();
    if (roleError || !role) return { error: "forbidden", status: 403 };
    return { caller: { kind: "admin", userId: authData.user.id } };
  } catch (error) {
    console.error("privileged authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}

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
