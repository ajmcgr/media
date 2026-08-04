// Shared blog artwork pipeline. Gemini only — no OpenAI image generation.
// Generates one editorial image per post, then derives hero / card / og
// variants and stores them in Supabase Storage under a deterministic path so
// the URLs can be derived on the client without extra DB columns:
//   blog-images/<yyyy>/<mm>/<slug>/hero.jpg | card.jpg | og.jpg

import { Image } from "https://deno.land/x/imagescript@1.2.17/mod.ts";

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
  "Premium editorial artwork for a modern SaaS technology brand.",
  "Abstract and conceptual rather than literal.",
  "Minimal composition, generous negative space, soft gradients,",
  "high contrast, refined light-blue / slate / off-white palette with a single accent,",
  "crisp geometry, subtle depth, magazine-quality art direction for a founder audience.",
  "Absolutely no text, letters, numbers, logos, watermarks, UI screenshots,",
  "no clipart, no stock-photo people, no robots, no glowing brains, no random icons.",
].join(" ");

// Deterministic per-slug art direction so two posts never render the same image,
// even when the art-director model is unavailable and we fall back to the title.
const COMPOSITIONS = [
  "a single large sweeping arc crossing the frame diagonally",
  "layered translucent panes overlapping off-centre",
  "a loose grid of thin lines dissolving toward one corner",
  "concentric rings drifting out of alignment",
  "a tall column of stacked soft ribbons",
  "scattered geometric shards orbiting an empty centre",
  "a smooth folded surface catching raking light",
  "two intersecting planes with a narrow bright seam",
];
const ACCENTS = [
  "electric blue",
  "warm amber",
  "deep indigo",
  "soft coral",
  "teal",
  "violet",
  "graphite with a pale mint accent",
  "cobalt with cream highlights",
];
const MOODS = [
  "cool daylight, airy and calm",
  "low-key with dramatic side light",
  "bright high-key and optimistic",
  "dusk gradient, quiet and reflective",
  "matte, flat and graphic",
  "glossy with soft specular highlights",
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
              "You are an art director for a premium SaaS media brand. Read the article and reply with ONE sentence (max 45 words) describing an abstract, conceptual image that represents the article's core idea. Describe shapes, composition, motion and colour — never text, people, logos or literal objects like robots or brains. Reply with the sentence only.",
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

  return `An abstract editorial composition representing "${post.title || post.topic || "modern public relations"}". ${STYLE} ${variation}`;
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
