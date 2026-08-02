// supabase/functions/blog-image-generate/index.ts
// Generates Gemini artwork for blog posts. Two modes:
//   { slug } | { id }          -> one post (called after create/publish)
//   { backfill: true, limit }  -> one-time backfill of posts without an image
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { generateAndStoreBlogImages } from "../_shared/blog-image.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

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
