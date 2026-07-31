// Render-time enrichment for blog posts. Everything here is derived from the
// columns the daily generator already writes (title, description, content,
// topic, published), so any newly published article gets the full treatment
// automatically with no manual configuration.

import { BLOG_CATEGORIES, BlogCategory, detectCategory } from "./categories";
import type { BlogPost } from "@/hooks/useBlog";

export const AUTHOR = {
  name: "Media AI Editorial",
  role: "PR & media research team",
  bio: "The Media AI editorial team researches how journalists, creators and comms teams actually work, drawing on a database of 50,000+ journalists and 30,000+ creators. Every article is reviewed against our editorial standards before publishing.",
  url: "https://trymedia.ai/about",
  avatarInitials: "MA",
};

export type Heading = { id: string; text: string; level: 2 | 3 };
export type Faq = { question: string; answer: string };

export type EnrichedPost = BlogPost & {
  category: BlogCategory;
  html: string;
  headings: Heading[];
  readingMinutes: number;
  wordCount: number;
  takeaways: string[];
  tldr: string;
  faq: Faq[];
  difficulty: "Beginner" | "Intermediate" | "Advanced";
  updated: string;
};

const slugifyHeading = (text: string, index: number) =>
  (text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 60) || `section-${index}`) + (index ? `` : ``);

const stripTags = (html: string) => html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();

const parse = (html: string): Document | null => {
  if (typeof DOMParser === "undefined") return null;
  try {
    return new DOMParser().parseFromString(`<div id="__root">${html}</div>`, "text/html");
  } catch {
    return null;
  }
};

export function enrichPost(post: BlogPost): EnrichedPost {
  const category = detectCategory(post);
  const rawText = stripTags(post.content || "");
  const wordCount = rawText ? rawText.split(" ").length : 0;
  const readingMinutes = Math.max(1, Math.round(wordCount / 220));

  const doc = parse(post.content || "");
  const headings: Heading[] = [];
  const faq: Faq[] = [];
  let takeaways: string[] = [];
  let tldr = post.description || "";
  let html = post.content || "";

  if (doc) {
    const root = doc.getElementById("__root");
    if (root) {
      // 1. Stable anchors on every h2/h3 for the table of contents.
      const nodes = Array.from(root.querySelectorAll("h2, h3"));
      nodes.forEach((node, i) => {
        const text = (node.textContent || "").trim();
        if (!text) return;
        const id = node.id || slugifyHeading(text, i);
        node.id = id;
        headings.push({ id, text, level: node.tagName === "H3" ? 3 : 2 });

        // 2. Question-style headings become FAQ entries (+ FAQ schema).
        if (/\?\s*$/.test(text)) {
          let answer = "";
          let sibling = node.nextElementSibling;
          while (sibling && !/^H[1-3]$/.test(sibling.tagName)) {
            answer += ` ${sibling.textContent || ""}`;
            if (answer.length > 400) break;
            sibling = sibling.nextElementSibling;
          }
          answer = answer.replace(/\s+/g, " ").trim();
          if (answer.length > 40) faq.push({ question: text, answer: answer.slice(0, 600) });
        }
      });

      // 3. Key takeaways from the first meaningful list in the article.
      const list = root.querySelector("ul, ol");
      if (list) {
        takeaways = Array.from(list.querySelectorAll(":scope > li"))
          .map((li) => (li.textContent || "").replace(/\s+/g, " ").trim())
          .filter((t) => t.length > 12)
          .slice(0, 5);
      }

      // 4. TL;DR from the first substantial paragraph.
      const firstPara = Array.from(root.querySelectorAll("p")).find(
        (p) => (p.textContent || "").trim().length > 120,
      );
      if (firstPara) tldr = (firstPara.textContent || "").replace(/\s+/g, " ").trim().slice(0, 320);

      html = root.innerHTML;
    }
  }

  const difficulty: EnrichedPost["difficulty"] =
    wordCount > 1600 ? "Advanced" : wordCount > 900 ? "Intermediate" : "Beginner";

  return {
    ...post,
    category,
    html,
    headings,
    readingMinutes,
    wordCount,
    takeaways,
    tldr,
    faq: faq.slice(0, 6),
    difficulty,
    updated: post.published,
  };
}

/** Lightweight version for list pages — skips DOM parsing of the full body. */
export type PostSummary = BlogPost & {
  category: BlogCategory;
  readingMinutes: number;
  wordCount: number;
};

export function summarise(post: BlogPost): PostSummary {
  const category = detectCategory(post);
  const text = stripTags((post.content || "").slice(0, 40000));
  const wordCount = text ? text.split(" ").length : 0;
  return {
    ...post,
    category,
    wordCount,
    readingMinutes: Math.max(1, Math.round(wordCount / 220)),
  };
}

const STOPWORDS = new Set([
  "the", "and", "for", "with", "that", "your", "you", "how", "why", "what", "from", "this",
  "into", "are", "not", "但", "when", "their", "them", "than", "then", "have", "has", "will",
  "can", "get", "use", "using", "guide", "best", "top", "way", "ways",
]);

const terms = (s: string) =>
  new Set(
    (s || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 3 && !STOPWORDS.has(w)),
  );

/** Related articles: same category first, then title/topic term overlap. */
export function relatedPosts(current: BlogPost, all: BlogPost[], limit = 4): PostSummary[] {
  const currentCat = detectCategory(current);
  const currentTerms = terms(`${current.title} ${current.description}`);

  return all
    .filter((p) => p.slug !== current.slug)
    .map((p) => {
      const s = summarise(p);
      let score = s.category.slug === currentCat.slug ? 10 : 0;
      const t = terms(`${p.title} ${p.description}`);
      t.forEach((w) => {
        if (currentTerms.has(w)) score += 2;
      });
      return { post: s, score };
    })
    .sort((a, b) => b.score - a.score || (b.post.published || "").localeCompare(a.post.published || ""))
    .slice(0, limit)
    .map((r) => r.post);
}

export function neighbours(current: BlogPost, all: BlogPost[]) {
  const sorted = [...all].sort((a, b) => (b.published || "").localeCompare(a.published || ""));
  const i = sorted.findIndex((p) => p.slug === current.slug);
  return {
    newer: i > 0 ? sorted[i - 1] : null,
    older: i >= 0 && i < sorted.length - 1 ? sorted[i + 1] : null,
  };
}

export function categoryCounts(posts: BlogPost[]) {
  const counts = new Map<string, number>();
  posts.forEach((p) => {
    const c = detectCategory(p);
    counts.set(c.slug, (counts.get(c.slug) || 0) + 1);
  });
  return BLOG_CATEGORIES.map((c) => ({ category: c, count: counts.get(c.slug) || 0 }));
}

export const formatDate = (iso?: string) => {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
};
