import { useMemo } from "react";
import Layout from "@/components/Layout";
import { Helmet } from "react-helmet-async";
import { Link, useParams } from "react-router-dom";
import { ArrowLeft, ArrowRight, Clock, CalendarDays, Gauge } from "lucide-react";
import { useBlogPost, useBlogPosts } from "@/hooks/useBlog";
import { useRecordBlogView } from "@/hooks/useBlogViews";
import { AUTHOR, enrichPost, formatDate, neighbours, relatedPosts } from "@/lib/blog/derive";
import { insightsFor } from "@/lib/blog/insights";
import ReadingProgress from "@/components/blog/ReadingProgress";
import TableOfContents from "@/components/blog/TableOfContents";
import ShareBar from "@/components/blog/ShareBar";
import AuthorCard from "@/components/blog/AuthorCard";
import ContextualCTA from "@/components/blog/ContextualCTA";
import InsightCard from "@/components/blog/InsightCard";
import PostCard from "@/components/blog/PostCard";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import NotFound from "./NotFound";

const SITE = "https://trymedia.ai";

const BlogPost = () => {
  const { slug } = useParams();
  const { data: post, isLoading } = useBlogPost(slug);
  const { data: allPosts = [] } = useBlogPosts();
  useRecordBlogView(slug);

  const enriched = useMemo(() => (post ? enrichPost(post) : null), [post]);
  const related = useMemo(() => (post ? relatedPosts(post, allPosts) : []), [post, allPosts]);
  const nav = useMemo(() => (post ? neighbours(post, allPosts) : { newer: null, older: null }), [post, allPosts]);
  const insights = useMemo(
    () => (enriched ? insightsFor(enriched.category.slug, enriched.slug, 2) : []),
    [enriched],
  );

  if (isLoading) {
    return (
      <Layout>
        <div className="container mx-auto px-6 py-16 max-w-3xl text-muted-foreground">Loading…</div>
      </Layout>
    );
  }

  if (!enriched) return <NotFound />;

  const url = `${SITE}/blog/${enriched.slug}`;
  const cat = enriched.category;

  const schemas: object[] = [
    {
      "@context": "https://schema.org",
      "@type": "Article",
      headline: enriched.title,
      description: enriched.description,
      image: enriched.image ? [enriched.image] : undefined,
      datePublished: enriched.published,
      dateModified: enriched.updated,
      articleSection: cat.name,
      wordCount: enriched.wordCount,
      inLanguage: "en",
      author: { "@type": "Organization", name: AUTHOR.name, url: AUTHOR.url },
      publisher: {
        "@type": "Organization",
        name: "Media AI",
        url: SITE,
      },
      mainEntityOfPage: { "@type": "WebPage", "@id": url },
    },
    {
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Blog", item: `${SITE}/blog` },
        { "@type": "ListItem", position: 2, name: cat.name, item: `${SITE}/blog/category/${cat.slug}` },
        { "@type": "ListItem", position: 3, name: enriched.title, item: url },
      ],
    },
  ];

  if (enriched.faq.length) {
    schemas.push({
      "@context": "https://schema.org",
      "@type": "FAQPage",
      mainEntity: enriched.faq.map((f) => ({
        "@type": "Question",
        name: f.question,
        acceptedAnswer: { "@type": "Answer", text: f.answer },
      })),
    });
  }

  return (
    <Layout>
      <ReadingProgress />
      <Helmet>
        <title>{`${enriched.title} | Media AI`}</title>
        <meta name="description" content={enriched.description} />
        <link rel="canonical" href={url} />
        <meta property="og:type" content="article" />
        <meta property="og:title" content={enriched.title} />
        <meta property="og:description" content={enriched.description} />
        {enriched.image && <meta property="og:image" content={enriched.image} />}
        <meta property="og:url" content={url} />
        <meta property="article:published_time" content={enriched.published} />
        <meta property="article:section" content={cat.name} />
        <meta name="twitter:card" content="summary_large_image" />
        {schemas.map((s, i) => (
          <script key={i} type="application/ld+json">
            {JSON.stringify(s)}
          </script>
        ))}
      </Helmet>

      <div className="container mx-auto px-6 py-12 max-w-6xl">
        <nav aria-label="Breadcrumb" className="mb-8 text-sm text-muted-foreground">
          <ol className="flex flex-wrap items-center gap-2">
            <li>
              <Link to="/blog" className="hover:text-foreground transition-colors">
                Blog
              </Link>
            </li>
            <li aria-hidden="true">/</li>
            <li>
              <Link to={`/blog/category/${cat.slug}`} className="hover:text-foreground transition-colors">
                {cat.name}
              </Link>
            </li>
          </ol>
        </nav>

        <div className="grid gap-12 lg:grid-cols-[minmax(0,1fr)_240px]">
          <article>
            <header className="mb-10">
              <Link
                to={`/blog/category/${cat.slug}`}
                className="text-xs uppercase tracking-[0.18em] text-muted-foreground hover:text-foreground transition-colors"
              >
                {cat.name}
              </Link>
              <h1 className="mt-4 text-3xl md:text-5xl font-medium tracking-tight leading-[1.1] text-foreground">
                {enriched.title}
              </h1>
              <p className="mt-5 text-lg text-muted-foreground max-w-2xl">{enriched.description}</p>

              <div className="mt-6 flex flex-wrap items-center gap-x-5 gap-y-2 text-sm text-muted-foreground">
                <span className="flex items-center gap-1.5">
                  <CalendarDays className="h-3.5 w-3.5" aria-hidden="true" />
                  {formatDate(enriched.published)}
                </span>
                <span className="flex items-center gap-1.5">
                  <Clock className="h-3.5 w-3.5" aria-hidden="true" />
                  {enriched.readingMinutes} min read
                </span>
                <span className="flex items-center gap-1.5">
                  <Gauge className="h-3.5 w-3.5" aria-hidden="true" />
                  {enriched.difficulty}
                </span>
                <span>By {AUTHOR.name}</span>
              </div>

              <div className="mt-6">
                <ShareBar url={url} title={enriched.title} />
              </div>
            </header>

            {enriched.image && (
              <div className="mb-10 overflow-hidden rounded-2xl bg-muted">
                <img
                  src={enriched.image}
                  alt={`Illustration for “${enriched.title}”`}
                  width={1600}
                  height={900}
                  fetchPriority="high"
                  className="h-auto w-full object-cover"
                />
              </div>
            )}

            {/* TL;DR */}
            {enriched.tldr && (
              <section className="mb-8 rounded-xl border border-border bg-muted/30 p-6">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground mb-2">TL;DR</p>
                <p className="text-foreground leading-relaxed">{enriched.tldr}</p>
              </section>
            )}

            {/* Key takeaways */}
            {enriched.takeaways.length > 2 && (
              <section className="mb-10 rounded-xl border border-border p-6">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground mb-3">
                  Key takeaways
                </p>
                <ul className="space-y-2">
                  {enriched.takeaways.map((t, i) => (
                    <li key={i} className="flex gap-3 text-sm text-foreground leading-relaxed">
                      <span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-primary" aria-hidden="true" />
                      <span className="line-clamp-3">{t}</span>
                    </li>
                  ))}
                </ul>
              </section>
            )}

            {insights[0] && <InsightCard insight={insights[0]} />}

            <div
              className="blog-content prose prose-neutral dark:prose-invert max-w-none prose-headings:font-medium prose-headings:tracking-tight prose-headings:scroll-mt-24 prose-p:text-foreground/90 prose-a:text-primary prose-img:rounded-xl prose-table:text-sm"
              dangerouslySetInnerHTML={{ __html: enriched.html }}
            />

            {insights[1] && <InsightCard insight={insights[1]} />}

            {/* FAQ */}
            {enriched.faq.length > 0 && (
              <section className="mt-14" aria-labelledby="faq-heading">
                <h2 id="faq-heading" className="text-2xl font-medium tracking-tight mb-5">
                  Frequently asked questions
                </h2>
                <Accordion type="single" collapsible className="border-t border-border">
                  {enriched.faq.map((f, i) => (
                    <AccordionItem key={i} value={`faq-${i}`}>
                      <AccordionTrigger className="text-left text-base">{f.question}</AccordionTrigger>
                      <AccordionContent className="text-muted-foreground leading-relaxed">
                        {f.answer}
                      </AccordionContent>
                    </AccordionItem>
                  ))}
                </Accordion>
              </section>
            )}

            <div className="mt-14">
              <ContextualCTA kind={cat.cta} />
            </div>

            <div className="mt-10">
              <AuthorCard />
            </div>

            <div className="mt-8 flex flex-wrap items-center justify-between gap-4 text-sm">
              <ShareBar url={url} title={enriched.title} />
              <Link to="/about#editorial-standards" className="text-muted-foreground hover:text-foreground transition-colors">
                Editorial standards
              </Link>
            </div>

            {/* Prev / next */}
            <nav className="mt-12 grid gap-4 sm:grid-cols-2" aria-label="More articles">
              {nav.older && (
                <Link
                  to={`/blog/${nav.older.slug}`}
                  className="group rounded-xl border border-border p-5 hover:border-foreground/20 transition-colors"
                >
                  <span className="flex items-center gap-2 text-xs text-muted-foreground mb-2">
                    <ArrowLeft className="h-3.5 w-3.5" /> Previous
                  </span>
                  <span className="font-medium text-foreground group-hover:text-primary transition-colors">
                    {nav.older.title}
                  </span>
                </Link>
              )}
              {nav.newer && (
                <Link
                  to={`/blog/${nav.newer.slug}`}
                  className="group rounded-xl border border-border p-5 text-right hover:border-foreground/20 transition-colors sm:col-start-2"
                >
                  <span className="flex items-center justify-end gap-2 text-xs text-muted-foreground mb-2">
                    Next <ArrowRight className="h-3.5 w-3.5" />
                  </span>
                  <span className="font-medium text-foreground group-hover:text-primary transition-colors">
                    {nav.newer.title}
                  </span>
                </Link>
              )}
            </nav>

            {/* Related */}
            {related.length > 0 && (
              <section className="mt-16">
                <h2 className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-8">
                  Related articles
                </h2>
                <div className="grid gap-x-8 gap-y-12 sm:grid-cols-2">
                  {related.map((p) => (
                    <PostCard key={p.slug} post={p} />
                  ))}
                </div>
              </section>
            )}
          </article>

          {/* Sticky rail */}
          <aside className="hidden lg:block">
            <div className="sticky top-8 space-y-10">
              <TableOfContents headings={enriched.headings} />

              <div>
                <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-4">
                  Related tools
                </p>
                <ul className="space-y-2 text-sm">
                  {cat.tools.map((t) => (
                    <li key={t.href}>
                      <Link to={t.href} className="text-muted-foreground hover:text-foreground transition-colors">
                        {t.label}
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>

              <div>
                <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-4">Compare</p>
                <ul className="space-y-2 text-sm">
                  <li>
                    <Link to="/compare/muck-rack" className="text-muted-foreground hover:text-foreground transition-colors">
                      Media AI vs Muck Rack
                    </Link>
                  </li>
                  <li>
                    <Link to="/compare/cision" className="text-muted-foreground hover:text-foreground transition-colors">
                      Media AI vs Cision
                    </Link>
                  </li>
                  <li>
                    <Link to="/compare/meltwater" className="text-muted-foreground hover:text-foreground transition-colors">
                      Media AI vs Meltwater
                    </Link>
                  </li>
                  <li>
                    <Link to="/discover" className="text-muted-foreground hover:text-foreground transition-colors">
                      Browse journalist lists
                    </Link>
                  </li>
                </ul>
              </div>
            </div>
          </aside>
        </div>
      </div>
    </Layout>
  );
};

export default BlogPost;
