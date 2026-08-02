import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import postsData from "@/data/blog-posts.json";

export type BlogPost = {
  slug: string;
  title: string;
  description: string;
  image: string;
  published: string;
  content: string;
};

const STATIC_POSTS = postsData as BlogPost[];

export const useBlogPosts = () => {
  return useQuery({
    queryKey: ["blog-posts"],
    queryFn: async (): Promise<BlogPost[]> => {
      const { data, error } = await supabase
        .from("blog_posts" as never)
        .select("slug,title,description,image,content,published")
        .order("published", { ascending: false });
      const dynamic = (!error && Array.isArray(data) ? (data as BlogPost[]) : []).map((p) => ({
        ...p,
        image: p.image || "",
      }));
      const seen = new Set(dynamic.map((p) => p.slug));
      const merged = [...dynamic, ...STATIC_POSTS.filter((p) => !seen.has(p.slug))];
      return merged.sort((a, b) => (b.published || "").localeCompare(a.published || ""));
    },
    staleTime: 5 * 60 * 1000,
  });
};

export const useBlogPost = (slug: string | undefined) => {
  return useQuery({
    queryKey: ["blog-post", slug],
    enabled: !!slug,
    queryFn: async (): Promise<BlogPost | null> => {
      if (!slug) return null;
      const { data } = await supabase
        .from("blog_posts" as never)
        .select("slug,title,description,image,content,published")
        .eq("slug", slug)
        .maybeSingle();
      if (data) return data as BlogPost;
      return STATIC_POSTS.find((p) => p.slug === slug) ?? null;
    },
  });
};
