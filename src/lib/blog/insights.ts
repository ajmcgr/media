// Signature "Media Intelligence" cards. Deterministically chosen per article
// from its category + slug, so every new post automatically gets relevant
// cards with no manual work. Phrased as field observations from the Media AI
// database and outreach data, never as third-party research.

export type Insight = {
  kind: "Journalist Insight" | "Trend" | "Pitch Tip" | "Media Stat" | "Creator Insight";
  body: string;
};

const POOL: Record<string, Insight[]> = {
  "ai-for-pr": [
    { kind: "Journalist Insight", body: "Reporters on the AI beat publish more often than almost any other technology beat — assume a shorter attention window and lead with the news, not the context." },
    { kind: "Trend", body: "AI-agent and AI-infrastructure coverage has been the fastest-growing topic cluster in the Media AI database over the last year." },
    { kind: "Pitch Tip", body: "Say what the model or system actually does before you say it's AI-powered. Reporters covering AI filter aggressively for substance." },
  ],
  "media-relations": [
    { kind: "Journalist Insight", body: "Most reporters cover two or three adjacent beats rather than one. Pitching the beat next door is often less crowded than the obvious one." },
    { kind: "Pitch Tip", body: "One well-timed follow-up outperforms three. Send it in the same thread, three to five working days later." },
    { kind: "Media Stat", body: "Journalist contact details go stale quickly — newsroom moves mean a list left untouched for a year is usually part wrong." },
  ],
  "journalist-outreach": [
    { kind: "Pitch Tip", body: "Avoid sending on Friday afternoons. Tuesday and Wednesday mornings in the reporter's own timezone consistently perform better." },
    { kind: "Journalist Insight", body: "The first line decides everything. Reference a specific piece the reporter wrote, not their outlet." },
    { kind: "Media Stat", body: "Subject lines under about 50 characters survive mobile truncation, which is where most pitches are first triaged." },
  ],
  "press-releases": [
    { kind: "Journalist Insight", body: "Most reporters skim a release for the number, the name and the date — put all three in the first two sentences." },
    { kind: "Pitch Tip", body: "Send the release inside the email body as well as attaching it. Attachments get opened last, if at all." },
  ],
  "startup-pr": [
    { kind: "Trend", body: "Funding rounds alone earn less coverage than they used to; the story angle attached to the round is doing the work." },
    { kind: "Pitch Tip", body: "Early-stage founders get further with trade and niche publications than with tier-one tech press — and trade coverage converts better." },
  ],
  "crisis-communications": [
    { kind: "Pitch Tip", body: "Have the holding statement drafted before you need it. The delay, not the content, is what shapes the first headline." },
    { kind: "Media Stat", body: "The window between the first inbound query and the first published story is often under two hours." },
  ],
  "influencer-marketing": [
    { kind: "Creator Insight", body: "Mid-tier creators (roughly 50k–250k followers) routinely out-engage seven-figure accounts in the same category." },
    { kind: "Trend", body: "Creator and journalist outreach are converging: the same story is increasingly pitched to both in one campaign." },
    { kind: "Pitch Tip", body: "Lead with deliverables, usage rights and budget range. Vague creator outreach gets ignored or over-quoted." },
  ],
  "thought-leadership": [
    { kind: "Journalist Insight", body: "Expert commentary requests move on hours, not weeks. Availability beats polish." },
    { kind: "Pitch Tip", body: "A byline needs one contrarian, defensible claim. Summaries of consensus don't get placed." },
  ],
  "product-launches": [
    { kind: "Pitch Tip", body: "Brief under embargo at least five working days out. Anything shorter and only the fastest desks can act." },
    { kind: "Media Stat", body: "Launch coverage clusters in the first 48 hours; a media list built the week before consistently outperforms one built on launch day." },
  ],
  "media-monitoring": [
    { kind: "Trend", body: "Coverage increasingly surfaces first in newsletters and creator posts rather than on publisher homepages." },
    { kind: "Pitch Tip", body: "Monitor competitor names as well as your own — competitor coverage is the cheapest source of pitch angles." },
  ],
  "podcast-outreach": [
    { kind: "Creator Insight", body: "Shows with smaller, tightly-defined audiences convert far better for B2B than general-interest podcasts." },
    { kind: "Pitch Tip", body: "Pitch three concrete episode topics, not a bio. Hosts book topics, not guests." },
  ],
  "brand-communications": [
    { kind: "Journalist Insight", body: "If your message house can't be repeated from memory by a new hire, it won't survive a reporter's paraphrase either." },
  ],
  "earned-media": [
    { kind: "Trend", body: "Original data stories remain the most reliable route to links and tier-one pickup." },
    { kind: "Pitch Tip", body: "Newsjacking works when you can respond within a few hours with a genuine point of view — otherwise skip it." },
  ],
  "pr-analytics": [
    { kind: "Media Stat", body: "Outlet authority varies enormously within the same tier — two 'national' placements can differ by an order of magnitude in referral value." },
    { kind: "Pitch Tip", body: "Tag every press link with UTMs before the story goes live. You cannot retrofit attribution." },
  ],
};

const FALLBACK: Insight[] = [
  { kind: "Journalist Insight", body: "Relevance beats volume: a list of 20 reporters who cover your exact category outperforms a blast to 500." },
  { kind: "Pitch Tip", body: "Reporters reply to specifics — a number, a name, a date. Remove every sentence that has none of the three." },
];

const hash = (s: string) => {
  let h = 0;
  for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
};

export function insightsFor(categorySlug: string, seed: string, count = 2): Insight[] {
  const pool = POOL[categorySlug]?.length ? POOL[categorySlug] : FALLBACK;
  const start = hash(seed) % pool.length;
  return Array.from({ length: Math.min(count, pool.length) }, (_, i) => pool[(start + i) % pool.length]);
}
