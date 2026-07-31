import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

/**
 * View counts power Trending / Most read. Fully automatic: every article view
 * calls the increment RPC, and the homepage reads the aggregate.
 * If the table/RPC hasn't been created yet the hooks fail silently and the
 * UI falls back to recency-based ordering.
 */

export const useBlogViewCounts = () =>
  useQuery({
    queryKey: ["blog-view-counts"],
    staleTime: 10 * 60 * 1000,
    queryFn: async (): Promise<Record<string, number>> => {
      const { data, error } = await supabase
        .from("blog_post_views" as never)
        .select("slug,views");
      if (error || !Array.isArray(data)) return {};
      const map: Record<string, number> = {};
      (data as Array<{ slug: string; views: number }>).forEach((r) => {
        map[r.slug] = Number(r.views) || 0;
      });
      return map;
    },
  });

export const useRecordBlogView = (slug?: string) => {
  useEffect(() => {
    if (!slug) return;
    const key = `blog-view:${slug}`;
    try {
      const last = Number(sessionStorage.getItem(key) || 0);
      if (Date.now() - last < 30 * 60 * 1000) return;
      sessionStorage.setItem(key, String(Date.now()));
    } catch {
      /* storage unavailable */
    }
    supabase.rpc("increment_blog_view" as never, { p_slug: slug } as never).then(
      () => undefined,
      () => undefined,
    );
  }, [slug]);
};
