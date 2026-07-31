// Pillar / content-hub pages. Evergreen framing plus an automatically
// updating list of every matching article (matched by category, so new daily
// posts join the right hub with no manual work).

export type Pillar = {
  slug: string;
  title: string;
  seoTitle: string;
  seoDescription: string;
  intro: string;
  categories: string[];
  chapters: Array<{ heading: string; body: string }>;
};

export const PILLARS: Pillar[] = [
  {
    slug: "public-relations",
    title: "The Ultimate Guide to PR",
    seoTitle: "The Ultimate Guide to PR (2026)",
    seoDescription:
      "A complete guide to modern public relations: strategy, media relations, pitching, measurement and the tools comms teams actually use.",
    intro:
      "Public relations is the work of earning attention you didn't pay for. This hub collects everything we've published on doing that well — from building the target list to proving the result.",
    categories: ["media-relations", "earned-media", "brand-communications", "pr-analytics", "ai-for-pr"],
    chapters: [
      {
        heading: "What PR actually is",
        body: "PR is the practice of shaping how an organisation is understood by the people who write and talk about it. Unlike advertising, the placement is decided by a third party — which is why relevance, timing and credibility matter more than budget.",
      },
      {
        heading: "The four building blocks",
        body: "A working PR programme has four parts: a narrative worth repeating, a target list of the right journalists and creators, a repeatable outreach process, and measurement that connects coverage to business outcomes.",
      },
      {
        heading: "How to start from zero",
        body: "Pick one genuinely newsworthy moment, build a list of 20–40 reporters who have written about that exact subject in the last 90 days, and pitch each of them something specific. That beats a 500-contact wire blast every time.",
      },
    ],
  },
  {
    slug: "journalist-outreach",
    title: "The Ultimate Guide to Journalist Outreach",
    seoTitle: "The Ultimate Guide to Journalist Outreach",
    seoDescription:
      "How to find the right journalists, write a pitch they'll open, time it correctly and follow up without burning the relationship.",
    intro:
      "Outreach is where most PR programmes succeed or quietly fail. This hub covers research, personalisation, timing, follow-ups and the mistakes that cost you the reply.",
    categories: ["journalist-outreach", "media-relations"],
    chapters: [
      {
        heading: "Research before you write",
        body: "Read the reporter's last three pieces. If you can't name the angle they'd take on your story, you aren't ready to pitch them.",
      },
      {
        heading: "The anatomy of a pitch that works",
        body: "A subject line under 50 characters, a first line that references their work, two sentences of news, one line of proof, and a single clear ask. Nothing else.",
      },
      {
        heading: "Timing and follow-up",
        body: "Send Tuesday to Thursday morning in the reporter's timezone. Follow up once, in the same thread, three to five working days later. Then stop.",
      },
    ],
  },
  {
    slug: "influencer-marketing",
    title: "The Ultimate Guide to Influencer Marketing",
    seoTitle: "The Ultimate Guide to Influencer Marketing",
    seoDescription:
      "Creator discovery, briefs, rates, contracts, disclosure and measurement — a complete influencer marketing guide for brand teams.",
    intro:
      "Creator partnerships work like media relations with a contract attached. This hub covers discovery, negotiation, compliance and measurement.",
    categories: ["influencer-marketing", "podcast-outreach"],
    chapters: [
      {
        heading: "Finding the right creators",
        body: "Filter on audience fit, not follower count. Category, country, engagement rate and past brand work predict performance far better than reach.",
      },
      {
        heading: "Briefs and rates",
        body: "A good brief states deliverables, usage rights, exclusivity, timeline and budget range up front. Vague briefs produce inflated quotes and weak content.",
      },
      {
        heading: "Disclosure and measurement",
        body: "Get disclosure right by default (FTC/ASA), and measure with tracked links and creator-specific codes so you can compare partners honestly.",
      },
    ],
  },
  {
    slug: "product-launch-pr",
    title: "The Ultimate Guide to Product Launch PR",
    seoTitle: "The Ultimate Guide to Product Launch PR",
    seoDescription:
      "Launch narrative, embargoes, exclusives, briefing decks and launch-day sequencing — how to run a product launch with the press.",
    intro:
      "A launch is a coordination problem. This hub covers the narrative, the embargo mechanics and the day-of sequencing that turns an announcement into coverage.",
    categories: ["product-launches", "press-releases", "startup-pr"],
    chapters: [
      {
        heading: "Build the narrative first",
        body: "Reporters cover change, not features. State what is now possible that wasn't before, and who it matters to.",
      },
      {
        heading: "Embargo or exclusive?",
        body: "An exclusive buys depth with one outlet; an embargo buys breadth across many. Choose based on whether you need one great story or twenty adequate ones.",
      },
      {
        heading: "Launch-day sequencing",
        body: "Brief five working days out, confirm the embargo in writing, send assets early, and keep a spokesperson genuinely available on the day.",
      },
    ],
  },
  {
    slug: "media-lists",
    title: "The Ultimate Guide to Media Lists",
    seoTitle: "The Ultimate Guide to Building a Media List",
    seoDescription:
      "How to build, segment, verify and maintain a media list that actually gets replies — including how to keep it from going stale.",
    intro:
      "The media list is the single highest-leverage asset in a PR programme. This hub covers how to build one, segment it and keep it accurate.",
    categories: ["media-relations", "earned-media", "ai-for-pr"],
    chapters: [
      {
        heading: "Start from coverage, not from a directory",
        body: "Find the stories closest to yours from the last 90 days and list who wrote them. That list beats any pre-packaged category export.",
      },
      {
        heading: "Segment before you send",
        body: "Split by beat, region and seniority so each segment gets a genuinely different angle. One message to one list is the definition of a blast.",
      },
      {
        heading: "Keep it alive",
        body: "Newsroom moves make lists decay fast. Re-verify before every campaign, and remove anyone who hasn't published in your space in six months.",
      },
    ],
  },
  {
    slug: "press-releases",
    title: "The Ultimate Guide to Press Releases",
    seoTitle: "The Ultimate Guide to Press Releases",
    seoDescription:
      "How to write, structure and distribute a press release in 2026 — plus when to skip it entirely and pitch directly instead.",
    intro:
      "The press release still has a job: making the facts easy to verify. This hub covers structure, writing and distribution — and when not to bother.",
    categories: ["press-releases", "brand-communications"],
    chapters: [
      {
        heading: "Structure",
        body: "Headline, dateline, the news in one sentence, the proof, one quote that says something, the boilerplate, and a real contact who answers email.",
      },
      {
        heading: "Writing",
        body: "Cut every adjective that can't be sourced. If a claim can't be checked in under a minute, it will be removed by the reporter anyway.",
      },
      {
        heading: "Distribution",
        body: "A release supports outreach, it doesn't replace it. Send it directly to a targeted list; use the wire only when you need the compliance record.",
      },
    ],
  },
];

export const getPillar = (slug?: string) => PILLARS.find((p) => p.slug === slug);
