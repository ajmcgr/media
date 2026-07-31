import { useMemo } from "react";
import Layout from "@/components/Layout";
import { Helmet } from "react-helmet-async";
import { Link, useParams } from "react-router-dom";
import { useBlogPosts } from "@/hooks/useBlog";
import { summarise } from "@/lib/blog/derive";
import { BLOG_CATEGORIES, getCategory } from "@/lib/blog/categories";
import PostCard from "@/components/blog/PostCard";
import ContextualCTA from "@/components/blog/ContextualCTA";
import NotFound from "../NotFound";

const SITE = "https://trymedia.ai";

const BlogCategory = () => {
  const { slug } = useParams();
  const category = getCategory(slug);
  const { data: rawPosts = [] } = useBlogPosts();

  const posts = useMemo(
    () => rawPosts.map(summarise).filter((p) => p.category.slug === slug),
    [rawPosts, slug],
  );

  if (!category) return <NotFound />;

  const url = `${SITE}/blog/category/${category.slug}`;
  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name: category.seoTitle,
    description: category.seoDescription,
    url,
    isPartOf: { "@type": "Blog", name: "Media AI Blog", url: `${SITE}/blog` },
  };
  const breadcrumb = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      { "@type": "ListItem", position: 1, name: "Blog", item: `${SITE}/blog` },
      { "@type": "ListItem", position: 2, name: category.name, item: url },
    ],
  };

  return (
    <Layout>
      <Helmet>
        <title>{`${category.seoTitle} | Media AI`}</title>
        <meta name="description" content={category.seoDescription} />
        <link rel="canonical" href={url} />
        <meta property="og:type" content="website" />
        <meta property="og:title" content={category.seoTitle} />
        <meta property="og:description" content={category.seoDescription} />
        <meta property="og:url" content={url} />
        <meta name="twitter:card" content="summary_large_image" />
        <script type="application/ld+json">{JSON.stringify(jsonLd)}</script>
        <script type="application/ld+json">{JSON.stringify(breadcrumb)}</script>
      </Helmet>

      <div className="container mx-auto px-6 py-16 max-w-6xl">
        <nav aria-label="Breadcrumb" className="mb-8 text-sm text-muted-foreground">
          <Link to="/blog" className="hover:text-foreground transition-colors">
            Blog
          </Link>
          <span className="mx-2" aria-hidden="true">/</span>
          <span className="text-foreground">{category.name}</span>
        </nav>

        <header className="mb-14 max-w-3xl">
          <h1 className="text-4xl md:text-5xl font-medium tracking-tight leading-[1.08] text-foreground mb-5">
            {category.name}
          </h1>
          <p className="text-lg text-muted-foreground">{category.intro}</p>
        </header>

        {posts.length === 0 ? (
          <p className="text-muted-foreground mb-16">
            No articles in this category yet — new posts publish daily.
          </p>
        ) : (
          <div className="grid gap-x-8 gap-y-12 sm:grid-cols-2 lg:grid-cols-3 mb-20">
            {posts.map((p) => (
              <PostCard key={p.slug} post={p} />
            ))}
          </div>
        )}

        <ContextualCTA kind={category.cta} />

        <section className="mt-20">
          <h2 className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-6">
            Other categories
          </h2>
          <div className="flex flex-wrap gap-2">
            {BLOG_CATEGORIES.filter((c) => c.slug !== category.slug).map((c) => (
              <Link
                key={c.slug}
                to={`/blog/category/${c.slug}`}
                className="rounded-md border border-border px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:border-foreground/20 transition-colors"
              >
                {c.name}
              </Link>
            ))}
          </div>
        </section>
      </div>
    </Layout>
  );
};

export default BlogCategory;
