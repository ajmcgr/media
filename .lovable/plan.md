## Constraint that shapes everything

The daily pipeline is: `pg_cron` → `blog-generate` edge function → insert into `blog_posts` (slug, title, description, image, content, topic, published) → `useBlog` reads it → `/blog` and `/blog/:slug` render it.

Every feature below is **derived at render time from those existing columns** (or added as nullable columns the generator fills in automatically). No manual tagging, no CMS step, no change to cron. If a feature can't be derived automatically, it's dropped — noted at the bottom.

## Phase 1 — Derivation layer (no visible change, everything else depends on it)

A single `src/lib/blog/derive.ts` that turns a raw post into an enriched post:

- **Reading time** — word count from HTML.
- **Category** — keyword-match the title/topic/content against the 14 category definitions; falls back to "PR Strategy". Zero config for new posts.
- **Headings / table of contents** — parse `h2`/`h3` out of content, inject stable `id` anchors.
- **Key takeaways / TL;DR** — first list or first paragraph pulled from the article body.
- **FAQ** — detect `h2/h3` ending in "?" plus their answer, emit as FAQ block + FAQ schema.
- **Related articles** — score by shared category, topic and title-term overlap.
- **Contextual CTA** — map category → the right product CTA (journalist search, media list builder, creator search, pitch generator, press release generator).
- **Related tools / comparison pages / product features** — same category map, wired to existing `/tools`, `/compare`, `/guides`, `/discover` routes.
- **Author** — single editorial author record (Media AI editorial team) with bio + expertise, applied to every post.

## Phase 2 — Article page rebuild (`/blog/:slug`)

Premium docs-style layout (Stripe/Linear feel): reading-progress bar, sticky right-hand TOC, wider type scale, better tables/lists/quotes/callouts, mobile + dark mode + a11y pass.

Auto-inserted blocks: TL;DR card, key takeaways, reading time, published + last-updated dates, author bio card, FAQ accordion, copy-link + social share, previous/next article, related-articles grid, contextual CTA block, related tools/comparisons rail.

Schema: Article, FAQPage, BreadcrumbList, Person (author), Organization. Canonical, OG, Twitter cards per post.

## Phase 3 — Blog homepage rebuild (`/blog`)

Sections all computed from the live post list: Featured (newest), Latest, Trending + Most Read (view counts, see below), Editor's Picks (highest-signal heuristic: has FAQ + longest + recent), Recently Updated, Popular categories grid, client-side search over titles/descriptions/categories. Cards show reading time, category, author, dates.

## Phase 4 — Category landing pages

`/blog/category/:slug` for all 14 categories, each with its own SEO title/description/H1/intro copy and an auto-filtered post list. New posts land in a category automatically via Phase 1.

## Phase 5 — Pillar hubs

Six pillar pages (`/blog/guide/:slug`: PR, Journalist Outreach, Influencer Marketing, Product Launch PR, Media Lists, Press Releases) with hand-written evergreen framing plus an auto-updating list of every matching article.

## Phase 6 — SEO plumbing

- Dynamic `sitemap.xml` (edge function) so every new post is listed the moment it publishes; keep existing static entries and all current URLs.
- RSS feed edge function at `/rss.xml`.
- robots.txt updated with the sitemap index.
- Lazy images, `width`/`height` to kill CLS, LCP preload on the featured image, route-level code splitting for the blog bundle.

## Phase 7 — Generator upgrade (still fully automatic)

Extend `blog-generate`'s prompt so every future article ships with: question-style H2s, a definitions block, at least one comparison table, statistics with citations, a bullet summary, an FAQ section, and callout/checklist blocks — i.e. the visual variety and AI-search quotability requirements are produced at write time. Add nullable `category`, `takeaways`, `faq`, `updated_at`, `views` columns; the function fills them, and the render layer falls back to Phase 1 derivation for the ~existing back catalogue. Backfill migration is additive only.

## Phase 8 — Signature "Media Intelligence" cards

Insight cards (Journalist Insight / Trend / Pitch Tip / Media Stat) generated per article by the same edge function and stored on the post, rendered inline. Only real, database-derived or model-generated-with-source numbers — no invented stats presented as research.

## Technical notes

- New columns are nullable with defaults; the running cron keeps working unchanged even before the migration is applied.
- All existing URLs (`/blog`, `/blog/:slug`, `/resources/blog` redirect) are preserved.
- View counts for Trending/Most Read need a `blog_views` table + an increment RPC — small, automatic, no manual work.
- Programmatic SEO (section 18) already exists via `seo_pages` + `/discover`; I'll link it into the blog rather than rebuild it.

## Deliberately out of scope (can't be automated, per your rule)

- **Interactive tools** (subject-line analyzer, ROI calculator, etc.) — these are separate products, not blog automation; worth a dedicated follow-up.
- **Multilingual / hreflang** — needs a translation pipeline; would add a manual step per article today.
- **Expert quotes and real research citations** — an LLM can't source these reliably; I'll render citations when the generator produces linkable sources and omit the block otherwise.
- **New competitor comparison pages** — `/compare` already exists; I'll extend the list there if you want, but it's hand-written content, not automated.

Want me to build all phases in order, or start with 1–3 (the highest-impact visible change) and review before continuing?