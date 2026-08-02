// Blog artwork helpers. The image pipeline (Gemini, server-side) stores three
// variants per post at a deterministic path, so the card/OG URLs are derived
// from the stored hero URL without extra database columns.
//   blog-images/<yyyy>/<mm>/<slug>/hero.jpg | card.jpg | og.jpg

export const BLOG_IMAGE_FALLBACK = "/blog-fallback.svg";

type Variant = "hero" | "card" | "og";

const isPipelineUrl = (url: string) => /\/blog-images\/.+\/hero\.jpe?g(\?.*)?$/i.test(url);

export function blogImage(url: string | null | undefined, variant: Variant = "hero"): string {
  if (!url) return BLOG_IMAGE_FALLBACK;
  if (variant === "hero" || !isPipelineUrl(url)) return url;
  return url.replace(/hero\.(jpe?g)(\?.*)?$/i, `${variant}.$1$2`);
}

/** Absolute URL for social previews (X, LinkedIn, Facebook, Discord). */
export function blogOgImage(url: string | null | undefined): string {
  const og = blogImage(url, "og");
  if (og.startsWith("http")) return og;
  const origin = typeof window !== "undefined" ? window.location.origin : "https://trymedia.ai";
  return `${origin}${og}`;
}
