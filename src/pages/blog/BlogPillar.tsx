import { useMemo } from "react";
import Layout from "@/components/Layout";
import { Helmet } from "react-helmet-async";
import { Link, useParams } from "react-router-dom";
import { useBlogPosts } from "@/hooks/useBlog";
import { summarise } from "@/lib/blog/derive";
import { getCategory } from "@/lib/blog/categories";
import { getPillar, PILLARS } from "@/lib/blog/pillars";
import PostCard from "@/components/blog/PostCard";
import ContextualCTA from "@/components/blog/ContextualCTA";
import NotFound from "../NotFound";

const SITE = "https://trymedia.ai";

const BlogPillar = () => {
  const { slug } = useParams();
  const pillar = getPillar(slug);
  const { data: rawPosts = [] } = useBlogPosts();

  const grouped = useMemo(() => {
    if (!pillar) return [];
    const posts = rawPosts.map(summarise);
    return pillar.categories
      .map((catSlug) => ({
        category: getCategory(catSlug)!,
        posts: posts.filter((p) => p.category.slug === catSlug).slice(0, 6),
      }))
      .filter((g) => g.category && g.posts.length > 0);
  }, [rawPosts, pillar]);

  if (!pillar) return <NotFound />;

  const url = `${SITE}/blog/guide/${pillar.slug}`;
  const primaryCategory = getCategory(pillar.categories[0]);

  const schemas = [
    {
      "@context": "https://schema.org",
      "@type": "Article",
      headline: pillar.seoTitle,
      description: pillar.seoDescription,
      mainEntityOfPage: { "@type": "WebPage", "@id": url },
      author: { "@type": "Organization", name: "Media AI Editorial", url: `${SITE}/about` },
      publisher: { "@type": "Organization", name: "Media AI", url: SITE },
    },
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Blog", item: `${SITE}/blog` },
        { "@type": "ListItem", position: 2, name: pillar.title, item: url },
      ],
    },
  ];

  return (
    <Layout>
      <Helmet>
        <title>{`${pillar.seoTitle} | Media AI`}</title>
        <meta name="description" content={pillar.seoDescription} />
        <link rel="canonical" href={url} />
        <meta property="og:type" content="article" />
        <meta property="og:title" content={pillar.seoTitle} />
        <meta property="og:description" content={pillar.seoDescription} />
        <meta property="og:url" content={url} />
        <meta name="twitter:card" content="summary_large_image" />
        {schemas.map((s, i) => (
          <script key={i} type="application/ld+json">
            {JSON.stringify(s)}
          </script>
        ))}
      </Helmet>

      <div className="container mx-auto px-6 py-16 max-w-4xl">
        <nav aria-label="Breadcrumb" className="mb-8 text-sm text-muted-foreground">
          <Link to="/blog" className="hover:text-foreground transition-colors">
            Blog
          </Link>
          <span className="mx-2" aria-hidden="true">/</span>
          <span className="text-foreground">Guides</span>
        </nav>

        <header className="mb-14">
          <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-5">
            Complete guide
          </p>
          <h1 className="text-4xl md:text-5xl font-medium tracking-tight leading-[1.08] text-foreground mb-5">
            {pillar.title}
          </h1>
          <p className="text-lg text-muted-foreground">{pillar.intro}</p>
        </header>

        <div className="space-y-10 mb-20">
          {pillar.chapters.map((c) => (
            <section key={c.heading}>
              <h2 className="text-2xl font-medium tracking-tight text-foreground mb-3">{c.heading}</h2>
              <p className="text-muted-foreground leading-relaxed">{c.body}</p>
            </section>
          ))}
        </div>

        {grouped.map((g) => (
          <section key={g.category.slug} className="mb-16">
            <div className="flex items-end justify-between mb-8">
              <h2 className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                {g.category.name}
              </h2>
              <Link
                to={`/blog/category/${g.category.slug}`}
                className="text-sm text-muted-foreground hover:text-foreground transition-colors"
              >
                View all
              </Link>
            </div>
            <div className="grid gap-x-8 gap-y-12 sm:grid-cols-2">
              {g.posts.map((p) => (
                <PostCard key={p.slug} post={p} />
              ))}
            </div>
          </section>
        ))}

        {primaryCategory && <ContextualCTA kind={primaryCategory.cta} />}

        <section className="mt-16">
          <h2 className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-6">
            Other complete guides
          </h2>
          <ul className="space-y-2">
            {PILLARS.filter((p) => p.slug !== pillar.slug).map((p) => (
              <li key={p.slug}>
                <Link to={`/blog/guide/${p.slug}`} className="text-foreground hover:text-primary transition-colors">
                  {p.title}
                </Link>
              </li>
            ))}
          </ul>
        </section>
      </div>
    </Layout>
  );
};

export default BlogPillar;
