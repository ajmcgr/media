import { useMemo, useState } from "react";
import Layout from "@/components/Layout";
import { Helmet } from "react-helmet-async";
import { Link } from "react-router-dom";
import { Search as SearchIcon, ArrowRight, Clock } from "lucide-react";
import { useBlogPosts } from "@/hooks/useBlog";
import { useBlogViewCounts } from "@/hooks/useBlogViews";
import { categoryCounts, formatDate, summarise } from "@/lib/blog/derive";
import { PILLARS } from "@/lib/blog/pillars";
import PostCard from "@/components/blog/PostCard";

const SITE = "https://trymedia.ai";

const SectionHeading = ({ title, action }: { title: string; action?: React.ReactNode }) => (
  <div className="flex items-end justify-between mb-8">
    <h2 className="text-xs uppercase tracking-[0.18em] text-muted-foreground">{title}</h2>
    {action}
  </div>
);

const Blog = () => {
  const { data: rawPosts = [], isLoading } = useBlogPosts();
  const { data: views = {} } = useBlogViewCounts();
  const [query, setQuery] = useState("");

  const posts = useMemo(() => rawPosts.map(summarise), [rawPosts]);

  const results = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    return posts
      .filter(
        (p) =>
          p.title.toLowerCase().includes(q) ||
          (p.description || "").toLowerCase().includes(q) ||
          p.category.name.toLowerCase().includes(q),
      )
      .slice(0, 12);
  }, [posts, query]);

  const [featured, ...rest] = posts;
  const latest = rest.slice(0, 6);

  const mostRead = useMemo(() => {
    const scored = posts.filter((p) => views[p.slug]);
    if (scored.length >= 4) {
      return [...scored].sort((a, b) => (views[b.slug] || 0) - (views[a.slug] || 0)).slice(0, 5);
    }
    // Fallback while view data accumulates: depth + recency.
    return [...posts].sort((a, b) => b.wordCount - a.wordCount).slice(0, 5);
  }, [posts, views]);

  const trending = useMemo(() => {
    const cutoff = Date.now() - 45 * 24 * 60 * 60 * 1000;
    const recent = posts.filter((p) => new Date(p.published).getTime() > cutoff);
    const pool = recent.length >= 4 ? recent : posts;
    return [...pool]
      .sort((a, b) => (views[b.slug] || 0) - (views[a.slug] || 0) || (b.published || "").localeCompare(a.published || ""))
      .slice(0, 5);
  }, [posts, views]);

  // Editor's picks: substantial, structured, still recent.
  const picks = useMemo(
    () =>
      [...posts]
        .map((p) => ({
          p,
          score:
            Math.min(p.wordCount, 2200) / 100 +
            (new Date(p.published).getTime() > Date.now() - 120 * 24 * 60 * 60 * 1000 ? 8 : 0),
        }))
        .sort((a, b) => b.score - a.score)
        .slice(0, 3)
        .map((x) => x.p),
    [posts],
  );

  const recentlyUpdated = useMemo(() => posts.slice(0, 5), [posts]);
  const categories = useMemo(() => categoryCounts(rawPosts).filter((c) => c.count > 0), [rawPosts]);

  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "Blog",
    name: "Media AI Blog",
    url: `${SITE}/blog`,
    description:
      "Playbooks on PR, journalist outreach, influencer marketing and media strategy from the Media AI team.",
    publisher: { "@type": "Organization", name: "Media AI", url: SITE },
  };

  return (
    <Layout>
      <Helmet>
        <title>PR & Journalist Outreach Blog | Media AI</title>
        <meta
          name="description"
          content="Playbooks on PR, journalist outreach, press releases and influencer marketing — published daily by the Media AI editorial team."
        />
        <link rel="canonical" href={`${SITE}/blog`} />
        <meta property="og:type" content="website" />
        <meta property="og:title" content="PR & Journalist Outreach Blog | Media AI" />
        <meta
          property="og:description"
          content="Playbooks on PR, journalist outreach, press releases and influencer marketing."
        />
        <meta property="og:url" content={`${SITE}/blog`} />
        <meta name="twitter:card" content="summary_large_image" />
        <link rel="alternate" type="application/rss+xml" title="Media AI Blog" href="/rss.xml" />
        <script type="application/ld+json">{JSON.stringify(jsonLd)}</script>
      </Helmet>

      <div className="container mx-auto px-6 py-16 md:py-20 max-w-6xl">
        {/* Masthead */}
        <header className="mb-14 max-w-3xl">
          <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-5">
            The Media AI Blog
          </p>
          <h1 className="text-4xl md:text-6xl font-medium text-foreground leading-[1.05] tracking-tight mb-5">
            Playbooks for modern PR and media teams.
          </h1>
          <p className="text-lg text-muted-foreground mb-8">
            Field notes on PR, journalist outreach, influencer marketing and earned media — new
            articles published daily.
          </p>

          <div className="relative max-w-md">
            <SearchIcon
              className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"
              aria-hidden="true"
            />
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search articles"
              aria-label="Search articles"
              className="w-full rounded-md border border-border bg-background py-2.5 pl-9 pr-3 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
        </header>

        {query.trim() ? (
          <section aria-label="Search results" className="mb-20">
            <SectionHeading title={`${results.length} result${results.length === 1 ? "" : "s"}`} />
            {results.length === 0 ? (
              <p className="text-muted-foreground">Nothing matched “{query}”.</p>
            ) : (
              <ul className="divide-y divide-border border-y border-border">
                {results.map((p) => (
                  <li key={p.slug}>
                    <Link to={`/blog/${p.slug}`} className="group flex flex-col gap-1 py-4">
                      <span className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">
                        {p.category.name}
                      </span>
                      <span className="text-lg font-medium text-foreground group-hover:text-primary transition-colors">
                        {p.title}
                      </span>
                      <span className="text-sm text-muted-foreground line-clamp-1">{p.description}</span>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </section>
        ) : (
          <>
            {isLoading && <p className="text-muted-foreground">Loading articles…</p>}

            {featured && (
              <section className="mb-20">
                <PostCard post={featured} variant="featured" />
              </section>
            )}

            <div className="grid gap-16 lg:grid-cols-[minmax(0,1fr)_300px]">
              <div>
                {/* Latest */}
                <section className="mb-20">
                  <SectionHeading title="Latest articles" />
                  <div className="grid gap-x-8 gap-y-12 sm:grid-cols-2">
                    {latest.map((p) => (
                      <PostCard key={p.slug} post={p} />
                    ))}
                  </div>
                </section>

                {/* Editor's picks */}
                {picks.length > 0 && (
                  <section className="mb-20">
                    <SectionHeading title="Editor’s picks" />
                    <ul className="divide-y divide-border border-y border-border">
                      {picks.map((p) => (
                        <li key={p.slug}>
                          <Link to={`/blog/${p.slug}`} className="group flex items-baseline justify-between gap-6 py-5">
                            <span>
                              <span className="block text-[11px] uppercase tracking-[0.14em] text-muted-foreground mb-1">
                                {p.category.name}
                              </span>
                              <span className="text-lg font-medium text-foreground group-hover:text-primary transition-colors">
                                {p.title}
                              </span>
                            </span>
                            <span className="shrink-0 text-xs text-muted-foreground whitespace-nowrap">
                              {p.readingMinutes} min
                            </span>
                          </Link>
                        </li>
                      ))}
                    </ul>
                  </section>
                )}

                {/* Pillars */}
                <section className="mb-20">
                  <SectionHeading title="Start with a complete guide" />
                  <div className="grid gap-4 sm:grid-cols-2">
                    {PILLARS.map((p) => (
                      <Link
                        key={p.slug}
                        to={`/blog/guide/${p.slug}`}
                        className="group rounded-xl border border-border bg-card p-5 hover:border-foreground/20 transition-colors"
                      >
                        <p className="font-medium text-foreground group-hover:text-primary transition-colors mb-1">
                          {p.title}
                        </p>
                        <p className="text-sm text-muted-foreground line-clamp-2">{p.seoDescription}</p>
                      </Link>
                    ))}
                  </div>
                </section>

                {/* Categories */}
                <section>
                  <SectionHeading title="Popular categories" />
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    {categories.map(({ category, count }) => (
                      <Link
                        key={category.slug}
                        to={`/blog/category/${category.slug}`}
                        className="group rounded-lg border border-border bg-card px-4 py-3 hover:border-foreground/20 transition-colors"
                      >
                        <p className="text-sm font-medium text-foreground group-hover:text-primary transition-colors">
                          {category.name}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {count} article{count === 1 ? "" : "s"}
                        </p>
                      </Link>
                    ))}
                  </div>
                </section>
              </div>

              {/* Sidebar */}
              <aside className="space-y-12 lg:sticky lg:top-8 lg:self-start">
                <section>
                  <SectionHeading title="Trending" />
                  <ol className="space-y-1">
                    {trending.map((p, i) => (
                      <li key={p.slug} className="flex gap-3">
                        <span className="pt-3 text-xs tabular-nums text-muted-foreground">{i + 1}</span>
                        <PostCard post={p} variant="compact" />
                      </li>
                    ))}
                  </ol>
                </section>

                <section>
                  <SectionHeading title="Most read" />
                  <ol className="space-y-1">
                    {mostRead.map((p) => (
                      <li key={p.slug}>
                        <PostCard post={p} variant="compact" />
                      </li>
                    ))}
                  </ol>
                </section>

                <section>
                  <SectionHeading title="Recently updated" />
                  <ul className="space-y-3">
                    {recentlyUpdated.map((p) => (
                      <li key={p.slug} className="text-sm">
                        <Link to={`/blog/${p.slug}`} className="text-foreground hover:text-primary transition-colors">
                          {p.title}
                        </Link>
                        <p className="flex items-center gap-1.5 text-xs text-muted-foreground mt-0.5">
                          <Clock className="h-3 w-3" aria-hidden="true" />
                          {formatDate(p.published)}
                        </p>
                      </li>
                    ))}
                  </ul>
                </section>

                <section className="rounded-xl border border-border bg-muted/30 p-5">
                  <p className="text-sm font-medium text-foreground mb-2">
                    Find the journalists covering your story
                  </p>
                  <p className="text-sm text-muted-foreground mb-4">
                    Search 50,000+ journalists and 30,000+ creators in plain English.
                  </p>
                  <Link
                    to="/search"
                    className="inline-flex items-center gap-2 text-sm font-medium text-primary hover:underline"
                  >
                    Try Media AI <ArrowRight className="h-3.5 w-3.5" />
                  </Link>
                </section>
              </aside>
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default Blog;
