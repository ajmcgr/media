// supabase/functions/blog-image-generate/index.ts
// Generates Gemini artwork for blog posts. Two modes:
//   { slug } | { id }          -> one post (called after create/publish)
//   { backfill: true, limit }  -> one-time backfill of posts without an image
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
    console.error("blog image authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}

const json = (body: Record<string, unknown>, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });

function env(name: string) {
  const v = Deno.env.get(name);
  if (!v) throw new Error(`${name} missing`);
  return v;
}

const SELECT = "id,slug,title,description,content,topic,published,image";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ ok: false, error: "Method not allowed" }, 405);

  const authorization = await requireAdminOrInternal(req);
  if ("error" in authorization) return json({ ok: false, error: authorization.error }, authorization.status);

  try {
    const body = await req.json().catch(() => ({}));
    const supabase = createClient(env("SUPABASE_URL"), env("SUPABASE_SERVICE_ROLE_KEY"));

    const run = async (post: Record<string, unknown>) => {
      const image = await generateAndStoreBlogImages(supabase as never, post as never);
      const { error } = await supabase.from("blog_posts").update({ image }).eq("id", post.id);
      if (error) throw error;
      return image;
    };

    if (body.backfill === true) {
      const limit = Math.min(Number(body.limit) || 10, 25);
      let query = supabase.from("blog_posts").select(SELECT).order("published", { ascending: false }).limit(limit);
      if (body.force !== true) query = query.or("image.is.null,image.eq.");
      const { data, error } = await query;
      if (error) throw error;

      const results: { slug: string; ok: boolean; error?: string }[] = [];
      for (const post of data ?? []) {
        try {
          await run(post as Record<string, unknown>);
          results.push({ slug: (post as { slug: string }).slug, ok: true });
        } catch (err) {
          console.error("blog-image backfill failed", (post as { slug: string }).slug, err);
          results.push({
            slug: (post as { slug: string }).slug,
            ok: false,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }
      return json({ ok: true, processed: results.length, results });
    }

    const column = body.id ? "id" : "slug";
    const value = body.id || body.slug;
    if (!value) return json({ ok: false, error: "slug or id required" }, 400);

    const { data: post, error } = await supabase
      .from("blog_posts")
      .select(SELECT)
      .eq(column, value)
      .maybeSingle();
    if (error) throw error;
    if (!post) return json({ ok: false, error: "post not found" }, 404);

    const image = await run(post as Record<string, unknown>);
    return json({ ok: true, image });
  } catch (err) {
    console.error("blog-image-generate error", err);
    return json({ ok: false, error: err instanceof Error ? err.message : String(err) }, 500);
  }
});
