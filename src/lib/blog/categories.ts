// Blog taxonomy. Categories are matched automatically from a post's
// title / topic / content, so newly published articles are categorised
// with zero manual work.

export type CtaKind =
  | "journalists"
  | "creators"
  | "media-list"
  | "pitch"
  | "press-release"
  | "monitoring"
  | "analytics";

export type BlogCategory = {
  slug: string;
  name: string;
  blurb: string;
  seoTitle: string;
  seoDescription: string;
  intro: string;
  keywords: string[];
  cta: CtaKind;
  tools: Array<{ label: string; href: string }>;
};

export const CTAS: Record<
  CtaKind,
  { eyebrow: string; headline: string; body: string; action: string; href: string }
> = {
  journalists: {
    eyebrow: "Looking for the right reporters?",
    headline: "Search 50,000+ journalists by beat",
    body: "Describe the story in plain English and Media AI returns the reporters who actually cover it — with verified contact details.",
    action: "Search journalists",
    href: "/search",
  },
  creators: {
    eyebrow: "Looking for creator contacts?",
    headline: "Find creators who match your brand",
    body: "Filter 30,000+ creators by category, country, audience size and platform, then export a ready-to-pitch list.",
    action: "Search creators",
    href: "/search",
  },
  "media-list": {
    eyebrow: "Need a media list?",
    headline: "Build a targeted media list instantly",
    body: "Turn one prompt into a segmented, deduplicated media list you can share with your team or export to CSV.",
    action: "Build a media list",
    href: "/search",
  },
  pitch: {
    eyebrow: "Pitching a journalist?",
    headline: "Generate a pitch tailored to their beat",
    body: "Media AI reads a reporter's recent coverage and drafts a personalised opening line you can actually send.",
    action: "Generate an AI pitch",
    href: "/tools/pitch-personalization-helper",
  },
  "press-release": {
    eyebrow: "Writing a press release?",
    headline: "Draft a press release in minutes",
    body: "Start from a proven structure, then distribute it to the reporters who cover your category.",
    action: "Generate a press release",
    href: "/tools/press-release-structure-builder",
  },
  monitoring: {
    eyebrow: "Tracking your coverage?",
    headline: "Monitor mentions as they publish",
    body: "Set up keyword monitors and get alerted the moment your brand, competitors or category get written about.",
    action: "Start monitoring",
    href: "/monitor",
  },
  analytics: {
    eyebrow: "Measuring PR impact?",
    headline: "Prove the value of earned media",
    body: "Track coverage, outlet authority and share of voice in one place instead of a spreadsheet.",
    action: "See PR analytics",
    href: "/tools/pr-roi-snapshot-calculator",
  },
};

export const BLOG_CATEGORIES: BlogCategory[] = [
  {
    slug: "ai-for-pr",
    name: "AI for PR",
    blurb: "How comms teams use AI without losing the human touch.",
    seoTitle: "AI for PR: Tools, Workflows and Playbooks",
    seoDescription:
      "Practical guides on using AI for public relations — research, pitching, media lists, monitoring and measurement.",
    intro:
      "AI is now part of the everyday PR workflow: finding reporters, drafting pitches, summarising coverage and spotting trends. These articles cover what works, what to automate, and where a human still has to sign off.",
    keywords: ["ai", "artificial intelligence", "automation", "llm", "chatgpt", "gpt", "machine learning", "ai tools"],
    cta: "journalists",
    tools: [
      { label: "AI journalist search", href: "/search" },
      { label: "Beat & outlet matcher", href: "/tools/beat-outlet-matcher" },
      { label: "Media AI query builder", href: "/tools/media-ai-query-builder" },
    ],
  },
  {
    slug: "media-relations",
    name: "Media Relations",
    blurb: "Building relationships that outlast a single campaign.",
    seoTitle: "Media Relations Strategy Guides for PR Teams",
    seoDescription:
      "How to build and maintain relationships with reporters, editors and producers — outreach, follow-ups and long-term trust.",
    intro:
      "Media relations is a long game. These guides cover how to research a reporter properly, when to follow up, and how to stay useful between stories.",
    keywords: ["media relations", "relationship", "reporters", "editors", "newsroom", "press office", "follow up"],
    cta: "journalists",
    tools: [
      { label: "Journalist database", href: "/search" },
      { label: "Follow-up cadence builder", href: "/tools/follow-up-cadence-builder" },
      { label: "Timezone converter", href: "/tools/journalist-timezone-converter" },
    ],
  },
  {
    slug: "journalist-outreach",
    name: "Journalist Outreach",
    blurb: "Pitches that get opened, read and replied to.",
    seoTitle: "Journalist Outreach: How to Pitch and Get Replies",
    seoDescription:
      "Tactics for pitching journalists — subject lines, personalisation, timing, follow-ups and the mistakes that kill a pitch.",
    intro:
      "Most pitches fail for the same handful of reasons. These articles break down the mechanics of outreach that reporters actually respond to.",
    keywords: ["outreach", "pitch", "pitching", "cold email", "subject line", "email", "reply rate", "journalist"],
    cta: "pitch",
    tools: [
      { label: "Pitch personalisation generator", href: "/tools/pitch-personalization-helper" },
      { label: "Subject line split-tester", href: "/tools/subject-line-split-tester" },
      { label: "Pitch fit score calculator", href: "/tools/pitch-fit-score-calculator" },
    ],
  },
  {
    slug: "press-releases",
    name: "Press Releases",
    blurb: "When they still work, and how to write one worth reading.",
    seoTitle: "Press Release Writing and Distribution Guides",
    seoDescription:
      "How to write, structure and distribute a press release in 2026 — plus the alternatives that often work better.",
    intro:
      "The press release isn't dead, but the wire-blast approach mostly is. Here's how to structure, write and place one so it earns coverage.",
    keywords: ["press release", "newswire", "wire", "boilerplate", "announcement", "distribution"],
    cta: "press-release",
    tools: [
      { label: "Press release structure builder", href: "/tools/press-release-structure-builder" },
      { label: "Quote polisher", href: "/tools/quote-polisher-pr" },
      { label: "Boilerplate refinery", href: "/tools/boilerplate-refinery" },
    ],
  },
  {
    slug: "startup-pr",
    name: "Startup PR",
    blurb: "Earned media for teams without a comms budget.",
    seoTitle: "Startup PR: Getting Press Without an Agency",
    seoDescription:
      "PR playbooks for founders and early-stage teams — funding announcements, founder stories and first coverage.",
    intro:
      "Early-stage PR is about picking the few moments that are genuinely newsworthy and executing them well. These guides are written for founders doing it themselves.",
    keywords: ["startup", "founder", "seed", "series a", "funding", "early-stage", "yc", "venture"],
    cta: "journalists",
    tools: [
      { label: "Search tech reporters", href: "/search" },
      { label: "Beat & outlet matcher", href: "/tools/beat-outlet-matcher" },
      { label: "Media kit builder", href: "/tools/media-kit-builder-lite" },
    ],
  },
  {
    slug: "crisis-communications",
    name: "Crisis Communications",
    blurb: "Holding statements, escalation and getting ahead of it.",
    seoTitle: "Crisis Communications Playbooks and Templates",
    seoDescription:
      "How to prepare for and respond to a communications crisis — holding statements, escalation paths and media handling.",
    intro:
      "The first hour decides how the story is written. These articles cover preparation, holding statements and how to communicate while the facts are still moving.",
    keywords: ["crisis", "holding statement", "reputation", "damage control", "apology", "incident", "escalation"],
    cta: "monitoring",
    tools: [
      { label: "Crisis holding statement generator", href: "/tools/crisis-holding-statement-generator" },
      { label: "Media monitoring", href: "/monitor" },
      { label: "Quote polisher", href: "/tools/quote-polisher-pr" },
    ],
  },
  {
    slug: "influencer-marketing",
    name: "Influencer Marketing",
    blurb: "Creator partnerships that survive contact with a CFO.",
    seoTitle: "Influencer Marketing Strategy, Benchmarks and Tools",
    seoDescription:
      "Creator discovery, briefs, rates, disclosure and measurement — practical influencer marketing guides for brand teams.",
    intro:
      "Working with creators is closer to media relations than to advertising. These articles cover discovery, briefs, rates, compliance and measurement.",
    keywords: ["influencer", "creator", "instagram", "tiktok", "youtube", "ugc", "sponsorship", "brand deal"],
    cta: "creators",
    tools: [
      { label: "Creator search", href: "/search" },
      { label: "Influencer brief builder", href: "/tools/influencer-brief-builder" },
      { label: "Rate card estimator", href: "/tools/rate-card-estimator-lite" },
    ],
  },
  {
    slug: "thought-leadership",
    name: "Thought Leadership",
    blurb: "Turning executive opinion into earned coverage.",
    seoTitle: "Thought Leadership PR: Bylines, Op-Eds and Commentary",
    seoDescription:
      "How to build an executive profile through bylines, expert commentary, LinkedIn and speaking — without the fluff.",
    intro:
      "Thought leadership works when there's an actual point of view. These guides cover bylines, op-eds, expert commentary and building an executive's media presence.",
    keywords: ["thought leadership", "byline", "op-ed", "executive", "linkedin", "commentary", "expert", "personal brand"],
    cta: "journalists",
    tools: [
      { label: "Find commentary opportunities", href: "/search" },
      { label: "Quote polisher", href: "/tools/quote-polisher-pr" },
      { label: "Hashtag & angle finder", href: "/tools/hashtag-angle-finder" },
    ],
  },
  {
    slug: "product-launches",
    name: "Product Launches",
    blurb: "Launch narratives, embargoes and coordinated coverage.",
    seoTitle: "Product Launch PR: Embargoes, Exclusives and Timing",
    seoDescription:
      "How to run a product launch with the press — narrative, embargoes, exclusives, briefing decks and launch-day timing.",
    intro:
      "A launch is a coordination problem as much as a story problem. These articles cover narrative, embargo mechanics, briefings and launch-day sequencing.",
    keywords: ["launch", "embargo", "exclusive", "announcement", "go-to-market", "briefing", "product"],
    cta: "media-list",
    tools: [
      { label: "Embargo & timing planner", href: "/tools/embargo-timing-planner" },
      { label: "Build a launch media list", href: "/search" },
      { label: "Outreach sequence generator", href: "/tools/outreach-sequence-generator" },
    ],
  },
  {
    slug: "media-monitoring",
    name: "Media Monitoring",
    blurb: "Knowing what's being said before your CEO does.",
    seoTitle: "Media Monitoring: Tools, Alerts and Workflows",
    seoDescription:
      "How to monitor brand, competitor and category coverage — alerting, share of voice and turning mentions into action.",
    intro:
      "Monitoring is only useful if it changes what you do next. These guides cover alert setup, noise reduction and reporting.",
    keywords: ["monitoring", "alerts", "mentions", "clipping", "share of voice", "tracking", "coverage report"],
    cta: "monitoring",
    tools: [
      { label: "Keyword monitors", href: "/monitor" },
      { label: "Coverage tracker template", href: "/tools/coverage-tracker-template" },
      { label: "Link health checker", href: "/tools/link-health-checker" },
    ],
  },
  {
    slug: "podcast-outreach",
    name: "Podcast Outreach",
    blurb: "Booking the shows your buyers actually listen to.",
    seoTitle: "Podcast Outreach: How to Get Booked as a Guest",
    seoDescription:
      "How to research, pitch and prepare for podcast appearances — guest pitches, show selection and follow-through.",
    intro:
      "Podcasts convert better than most earned media and are far easier to book. Here's how to pick shows, pitch hosts and show up prepared.",
    keywords: ["podcast", "guest", "host", "show", "audio", "interview", "booking"],
    cta: "creators",
    tools: [
      { label: "Find podcasts and hosts", href: "/search" },
      { label: "Pitch personalisation generator", href: "/tools/pitch-personalization-helper" },
      { label: "Follow-up cadence builder", href: "/tools/follow-up-cadence-builder" },
    ],
  },
  {
    slug: "brand-communications",
    name: "Brand Communications",
    blurb: "Messaging, positioning and staying consistent everywhere.",
    seoTitle: "Brand Communications: Messaging and Positioning Guides",
    seoDescription:
      "Message houses, positioning, tone of voice and keeping brand communications consistent across every channel.",
    intro:
      "Good comms starts with a message that survives being repeated. These articles cover positioning, message architecture and internal alignment.",
    keywords: ["brand", "messaging", "positioning", "tone of voice", "narrative", "internal comms", "reputation"],
    cta: "analytics",
    tools: [
      { label: "Boilerplate refinery", href: "/tools/boilerplate-refinery" },
      { label: "Media kit builder", href: "/tools/media-kit-builder-lite" },
      { label: "Quote polisher", href: "/tools/quote-polisher-pr" },
    ],
  },
  {
    slug: "earned-media",
    name: "Earned Media",
    blurb: "Coverage you didn't pay for, and how to get more of it.",
    seoTitle: "Earned Media Strategy: How to Get More Coverage",
    seoDescription:
      "Earned media tactics — newsjacking, data stories, exclusives and the angles reporters actually pick up.",
    intro:
      "Earned media is won with angles, timing and data. These guides cover the story types that consistently get picked up.",
    keywords: ["earned media", "coverage", "newsjacking", "data story", "trend story", "features", "backlink"],
    cta: "media-list",
    tools: [
      { label: "Search journalists by beat", href: "/search" },
      { label: "Hashtag & angle finder", href: "/tools/hashtag-angle-finder" },
      { label: "Coverage tracker template", href: "/tools/coverage-tracker-template" },
    ],
  },
  {
    slug: "pr-analytics",
    name: "PR Analytics",
    blurb: "Measurement that a CFO will accept.",
    seoTitle: "PR Analytics and Measurement Frameworks",
    seoDescription:
      "How to measure PR beyond AVE — coverage quality, share of voice, referral traffic and pipeline attribution.",
    intro:
      "AVE is dead but the reporting requirement isn't. These articles cover the metrics that hold up in a board deck.",
    keywords: ["measurement", "analytics", "ave", "roi", "metrics", "reporting", "attribution", "kpi", "benchmark"],
    cta: "analytics",
    tools: [
      { label: "PR ROI snapshot calculator", href: "/tools/pr-roi-snapshot-calculator" },
      { label: "UTM builder for PR links", href: "/tools/utm-builder-pr-links" },
      { label: "Coverage analytics dashboard", href: "/tools/coverage-analytics-dashboard" },
    ],
  },
];

export const DEFAULT_CATEGORY = BLOG_CATEGORIES[1]; // Media Relations

export const getCategory = (slug?: string) =>
  BLOG_CATEGORIES.find((c) => c.slug === slug);

/** Deterministic keyword scoring — no manual tagging needed for new posts. */
export function detectCategory(input: {
  title?: string | null;
  topic?: string | null;
  description?: string | null;
  content?: string | null;
}): BlogCategory {
  const strong = `${input.title || ""} ${input.topic || ""} ${input.description || ""}`.toLowerCase();
  const body = (input.content || "").toLowerCase().slice(0, 6000);

  let best = DEFAULT_CATEGORY;
  let bestScore = 0;

  for (const cat of BLOG_CATEGORIES) {
    let score = 0;
    for (const kw of cat.keywords) {
      if (strong.includes(kw)) score += 5;
      const occurrences = body.split(kw).length - 1;
      score += Math.min(occurrences, 4);
    }
    if (score > bestScore) {
      bestScore = score;
      best = cat;
    }
  }

  return bestScore > 0 ? best : DEFAULT_CATEGORY;
}
