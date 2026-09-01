import { useEffect, useRef, useState } from "react";
import { Helmet } from "react-helmet-async";
import { Link, useNavigate } from "react-router-dom";
import { Check, Plus, Minus, ArrowRight, Star, X as XIcon, CornerDownRight, Linkedin } from "lucide-react";

const XLogo = ({ className }: { className?: string }) => (
  <svg viewBox="0 0 24 24" className={className} fill="currentColor" aria-hidden="true">
    <path d="M18.244 2H21.5l-7.5 8.57L23 22h-6.844l-5.36-6.99L4.6 22H1.34l8.02-9.16L1 2h7.02l4.84 6.39L18.244 2zm-1.2 18h1.86L7.04 4H5.06l11.984 16z" />
  </svg>
);
import Header from "@/components/Header";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/contexts/AuthContext";
import { getPlanPrice, startCheckout } from "@/lib/billing";
import { toast } from "sonner";
import { trackEvent } from "@/lib/analytics";

import heroProductMain from "@/assets/home/hero-product-main.png";
import heroProductDiscover from "@/assets/home/feature-database.jpg";
import featureAiChat from "@/assets/home/feature-ai-chat.jpg";
import audiencesImg from "@/assets/home/audiences-v2.jpg";
import featureSearch from "@/assets/home/feature-search-v2.jpg";
import featureBrandMonitor from "@/assets/home/feature-brand-monitor.jpg";
import featureMediaLists from "@/assets/home/feature-media-lists.jpg";
import featureContactInbox from "@/assets/home/feature-contact-inbox.jpg";
import featureTeams from "@/assets/home/feature-teams.jpg";
import featureExport from "@/assets/home/feature-export-v2.jpg";
import logoMuckrack from "@/assets/home/logo-muckrack-new.png";
import logoCision from "@/assets/home/logo-cision-new.png";
import logoMeltwater from "@/assets/home/logo-meltwater-new.png";
import logoMedia from "@/assets/home/logo-media-new.png";
import brand1 from "@/assets/home/brand-1-v3.png";
import brand2 from "@/assets/home/brand-2.png";
import brand3 from "@/assets/home/brand-3-new-v2.png";
import brand4 from "@/assets/home/brand-4-v2.png";
import newBadge from "@/assets/home/new-badge.png";
import avatar1 from "@/assets/home/avatar-1.png";
import avatar2 from "@/assets/home/avatar-2-new-v2.jpg";
import avatar3 from "@/assets/home/avatar-3.png";
import avatar4 from "@/assets/home/avatar-4-v2.png";
import jacksonAvatar from "@/assets/home/testimonial-jackson-v2.jpg";

type Interval = "monthly" | "yearly";
function HeroVideo({ src, poster }: { src: string; poster: string }) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [playing, setPlaying] = useState(false);
  const [ready, setReady] = useState(false);
  const [userPaused, setUserPaused] = useState(false);

  useEffect(() => {
    const el = containerRef.current;
    const vid = videoRef.current;
    if (!el || !vid) return;
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduce) return;
    const io = new IntersectionObserver(
      ([entry]) => {
        if (!videoRef.current) return;
        if (entry.isIntersecting && entry.intersectionRatio >= 0.4) {
          if (!userPaused) videoRef.current.play().catch(() => {});
        } else {
          videoRef.current.pause();
        }
      },
      { threshold: [0, 0.4, 0.75] }
    );
    io.observe(el);
    return () => io.disconnect();
  }, [userPaused]);

  const toggle = () => {
    const v = videoRef.current;
    if (!v) return;
    if (v.paused) {
      setUserPaused(false);
      v.play().catch(() => {});
    } else {
      setUserPaused(true);
      v.pause();
    }
  };

  return (
    <div
      className="rounded-2xl overflow-hidden bg-white ring-1 ring-black/10"
      style={{ boxShadow: "0 30px 60px -20px rgba(0,0,0,0.18), 0 10px 20px -10px rgba(0,0,0,0.08)" }}
    >
      {/* Faux browser chrome */}
      <div className="flex items-center gap-3 px-4 h-9 md:h-10 bg-[#f3f4f6] border-b border-black/5">
        <div className="flex items-center gap-1.5">
          <span className="h-3 w-3 rounded-full bg-[#ff5f57]" />
          <span className="h-3 w-3 rounded-full bg-[#febc2e]" />
          <span className="h-3 w-3 rounded-full bg-[#28c840]" />
        </div>
        <div className="flex-1" />

      </div>

      <div
        ref={containerRef}
        className="group relative w-full cursor-pointer"
        onClick={toggle}
        role="button"
        aria-label={playing ? "Pause demo video" : "Play demo video"}
      >
        <img src={poster} alt="" aria-hidden="true" className="w-full h-auto block" />
        <video
          ref={videoRef}
          src={src}
          poster={poster}
          loop
          muted
          playsInline
          preload="metadata"
          onCanPlay={() => setReady(true)}
          onPlay={() => setPlaying(true)}
          onPause={() => setPlaying(false)}
          aria-label="Media AI search finding a tech journalist in the United Kingdom and saving the search"
          className={`absolute inset-0 w-full h-full transition-opacity duration-700 ease-out ${ready && playing ? "opacity-100" : "opacity-0"}`}
        />
        <div
          className={`absolute inset-0 flex items-center justify-center transition-opacity duration-300 ${playing ? "opacity-0 group-hover:opacity-100" : "opacity-100"}`}
        >
          <div className="h-16 w-16 md:h-20 md:w-20 rounded-full bg-white/90 backdrop-blur shadow-lg flex items-center justify-center ring-1 ring-black/5">
            {playing ? (
              <svg viewBox="0 0 24 24" className="h-7 w-7 text-foreground" fill="currentColor"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>
            ) : (
              <svg viewBox="0 0 24 24" className="h-7 w-7 text-foreground translate-x-0.5" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

type PlanId = "starter" | "growth" | "enterprise";

type Tier = {
  id: PlanId;
  name: string;
  tagline: string;
  monthly: number | null;
  yearly: number | null;
  features: string[];
  highlight?: boolean;
  badge?: string;
  cta: string;
};

const TIERS: Tier[] = [
  {
    id: "starter",
    name: "Starter",
    tagline: "Media AI search.",
    monthly: 29,
    yearly: 290,
    cta: "Start Free Trial",
    features: [
      "~500 AI searches / month",
      "Verified contact emails where available, plus on-demand enrichment",
      "Capped at 100 media contacts per query",
      "Top-up credits any time",
      "Email support",
      "1-month free trial",
    ],
  },
  {
    id: "growth",
    name: "Growth",
    tagline: "Media AI search + full journalist & creator database + keyword monitoring + inbox + teams.",
    monthly: 99,
    yearly: 990,
    highlight: true,
    badge: "Most popular",
    cta: "Start Free Trial",
    features: [
      "~3,000 AI searches / month",
      "Verified contact emails where available, plus on-demand enrichment",
      "Unlimited media contacts per query",
      "Top-up credits any time",
      "100% database access — no row limits",
      "Sort, filter, save views, export",
      "Share contacts via link, email, or CSV",
      "Keyword Monitor — daily Google News alerts",
      "Outreach Inbox — pitch & reply tracking",
      "Team workspaces — invite teammates, shared lists & roles",
      "Email support",
      "1-month free trial",
    ],
  },
  {
    id: "enterprise",
    name: "Enterprise",
    tagline: "Custom API, volume credits, dedicated support.",
    monthly: null,
    yearly: null,
    cta: "Contact us",
    features: [
      "Verified contact emails where available, plus on-demand enrichment",
      "Everything in Growth",
      "Custom API access",
      "Volume credit pricing",
      "SSO + dedicated support",
      "Custom contracts",
    ],
  },
];


const FAQS = [
  { q: "What is Media AI?", a: "Media AI is an AI-powered database of journalists and creators. PR and social media pros use Search to find the right contacts, save searches, build lists, and monitor brand mentions — all in one place." },
  { q: "How does AI search work?", a: "Just ask in plain English — e.g. 'I'm looking for a tech journalist in the United Kingdom'. Search queries the database plus live web sources and returns a curated list of journalists or creators with available contact details, beats, outlets, and social handles. You can refine with follow-up questions." },
  { q: "Can I find creators and influencers, not just journalists?", a: "Yes. The Creators view lets you filter by Instagram followers, engagement rate, YouTube subscribers, category, and country, and Search works across both journalists and creators." },
  { q: "How many journalists and creators are in the database?", a: "Media AI maintains a growing global database of journalists and creators, with new contacts added continuously and records refreshed as public information changes." },
  { q: "Do you offer verified email addresses?", a: "Many profiles include a verified email alongside available social handles, outlet, country, topic, and beat information. Use 'Find email' on profiles without an address to enrich the contact on demand." },
  { q: "Can I save lists, monitor coverage, and send pitches?", a: "Yes. Save any search, build custom lists, get brand mention alerts in Monitor, connect your inbox to send and track pitches, and export contacts to CSV any time." },
  { q: "How accurate is the contact data?", a: "Records are assembled from public sources and refreshed as changes are detected. Contact details can still change, so email enrichment is available when an address is missing." },
  { q: "Is there a free trial? Can I cancel anytime?", a: "Every paid plan includes a 1-month free trial of the features included in that plan. You can change or cancel your plan at any time from your account — no contracts, no cancellation fees." },
  { q: "Where does the contact data come from?", a: "Our proprietary AI crawler aggregates publicly available information from outlets, bylines, social platforms, and creator profiles across the web." },
];

const AUDIENCES = [
  { label: "Startups", body: "Find the right journalists fast and get more media coverage, effortlessly." },
  { label: "SMB's", body: "Scale outreach without scaling headcount — pitch smarter with verified contacts." },
  { label: "In-house", body: "Centralize your media list and keep your team aligned on every campaign." },
  { label: "PR/Social Media Agencies", body: "Manage multiple clients with one source of truth for journalist and creator contacts." },
  { label: "Freelancers", body: "Punch above your weight with the same database the big agencies use." },
];

const VALUE_PROPS = [
  { title: "AI Crawler", body: "Our AI crawls the web to find the contact info of the world's leading journalists and creators.", icon: "🤖" },
  { title: "Proprietary data", body: "Our proprietary database is updated with all key information you need including email and socials.", icon: "📊" },
  { title: "Sync and share", body: "Export and download all the contacts you need in whatever format you need including CSV, .xls & more.", icon: "📎" },
  { title: "No contracts", body: "We don't tie you down to a contract or require a sales pitch.", icon: "🚫" },
  { title: "Always updated", body: "Our AI is constantly double checking journalist and creator contact info while always adding new contacts.", icon: "⚡\n" },
  { title: "Media experts", body: "Our team is comprised of real PR and Social media experts - not financial or sales types.", icon: "🤝" },
];

const FEATURE_BLOCKS = [
  {
    eyebrow: "AI Search",
    body: "Ask in plain English and get curated journalist and creator contacts in seconds. Our AI assistant turns natural-language questions into precise media lists — no filters, no spreadsheets, no guesswork.",
    image: featureAiChat,
    bullets: ["Conversational search across the full database", "Refine results with follow-up questions", "Export the contacts you need with one click"],
  },
  {
    eyebrow: "Full Database",
    body: "Effortlessly explore a rich database of creators and journalists, handpicked for their expertise and credibility. Whether you're seeking creative talent for a campaign or a journalist to amplify your story.",
    image: heroProductDiscover,
    bullets: ["Access to a growing network of professionals", "Streamlined interface for intuitive browsing", "Discover fresh talent for topics or industries"],
  },
  {
    eyebrow: "Media Lists",
    body: "Build curated media lists straight from Search results. Save shortlists for each launch, keep them updated as contacts change, and share them with teammates, clients, or collaborators with a single link.",
    image: featureMediaLists,
    bullets: ["Save journalists and creators into named lists in one click", "Share any list via public link, email, or X / LinkedIn / WhatsApp", "Recipients get a clean, branded read-only view — no login required"],
  },
  {
    eyebrow: "Search and Filter",
    body: "Refine your search and save time with advanced filtering options. Our search tool lets you pinpoint profiles based on topics, categories, or job titles, ensuring you find exactly what you're looking for.",
    image: featureSearch,
    bullets: ["Filter by beat, outlet, country, or title", "Save views and reuse your favorite filters", "Search by name, email, or social handle"],
  },
  {
    eyebrow: "Seamless Export",
    body: "Whether you need a shortlist of potential candidates or a comprehensive database, our export feature helps you stay organized and productive.",
    image: featureExport,
    bullets: ["Export to CSV, Excel, or .xls", "Share contacts with your team via link or email", "One-click sync to keep lists fresh"],
  },
  {
    eyebrow: "Keyword Monitor",
    body: "Track brands, founders, competitors, products, and keywords across Google News. Media AI surfaces fresh mentions, scores sentiment, and pings you with re-engagement emails so you never miss a signal.",
    image: featureBrandMonitor,
    bullets: ["Google News mentions for brands, founders, competitors, products & keywords", "Sentiment + source breakdown with trend charts", "Instant priority alerts and daily / weekly digests"],
  },
  {
    eyebrow: "Contact Inbox",
    body: "Keep every journalist and creator reply in one place. Media AI threads conversations against the contacts in your database so you always know who replied, what they said, and what to send next.",
    image: featureContactInbox,
    bullets: ["Unified inbox across all your outreach", "Replies linked to journalist and outlet records", "See unread, scheduled, and follow-up status at a glance"],
  },
  {
    eyebrow: "Teams",
    body: "Bring your whole PR and comms team into one shared workspace. Invite teammates, assign roles, and collaborate on the same lists, contacts, and outreach without stepping on each other's toes.",
    image: featureTeams,
    bullets: ["Shared workspace with owner, admin, and member roles", "Invite teammates by email with seat-based billing", "Centralized audit trail of exports, reveals, and invites"],
  },
];

const RESOURCES_LINKS = [
  ["Build a Media List", "/resources/build-a-media-list-that-gets-replies"],
  ["Pitching Framework", "/resources/exclusive-vs-wide-pitching"],
  ["Press Release Templates", "/resources/press-release-templates-by-announcement-type"],
  ["30 Day PR Plan", "/resources/30-day-pr-plan-for-product-launches"],
  ["PR For Funding", "/resources/pr-for-funding-announcements"],
  ["Build a Press Kit", "/resources/press-kit-that-journalists-use"],
  ["Influencer Briefs", "/resources/influencer-briefs-that-drive-results"],
  ["FTC Disclosures", "/resources/ftc-asa-disclosure-for-campaigns"],
  ["Social Proof in PR", "/resources/using-reviews-and-social-proof-in-pr"],
  ["Social Media ROI", "/resources/social-media-roi-measurement-guide"],
  ["PR Attribution", "/resources/pr-attribution-with-utms"],
  ["Digital PR", "/resources/digital-pr-link-building"],
  ["Pitching Podcasts", "/resources/pitching-podcasts-for-brand-story"],
  ["PR Positioning", "/resources/messaging-and-positioning-for-pr"],
  ["Crisis Communications", "/resources/crisis-communications-playbook"],
  ["Thought Leadership", "/resources/thought-leadership-to-op-ed"],
] as const;

const TOOLS_LINKS: ReadonlyArray<readonly [string, string]> = [
  ["Beat & Outlet Matcher", "/tools/beat-outlet-matcher"],
  ["Pitch Personalization Generator", "/tools/pitch-personalization-helper"],
  ["Subject Line Split-Tester", "/tools/subject-line-split-tester"],
  ["Pitch Fit Score Calculator", "/tools/pitch-fit-score-calculator"],
  ["Embargo & Timing Planner", "/tools/embargo-timing-planner"],
  ["List Segmenter", "/tools/list-segmenter-lite"],
  ["Outreach Sequence Generator", "/tools/outreach-sequence-generator"],
  ["Press Release Structure Generator", "/tools/press-release-structure-builder"],
  ["Quote Polisher for PR", "/tools/quote-polisher-pr"],
  ["UTM Generator for PR Links", "/tools/utm-builder-pr-links"],
  ["Link Health & No-Follow Checker", "/tools/link-health-checker"],
  ["Media Kit Generator", "/tools/media-kit-builder-lite"],
  ["Influencer Brief Generator", "/tools/influencer-brief-builder"],
  ["Rate Card Estimator", "/tools/rate-card-estimator-lite"],
];

const COMPARE_LINKS: ReadonlyArray<readonly [string, string]> = [
  ["Media AI vs Muck Rack", "/compare/muck-rack"],
  ["Media AI vs Cision", "/compare/cision"],
  ["Media AI vs Meltwater", "/compare/meltwater"],
  ["Media AI vs GRIN", "/compare/grin"],
  ["Media AI vs HypeAuditor", "/compare/hypeauditor"],
  ["Media AI vs Later", "/compare/later"],
  ["Media AI vs Impact.com", "/compare/impact-com"],
];

const Index = () => {
  const [interval, setIntervalVal] = useState<Interval>("monthly");
  const [openFaq, setOpenFaq] = useState<number | null>(null);
  const { user } = useAuth();
  const navigate = useNavigate();
  const [pendingPlan, setPendingPlan] = useState<PlanId | null>(null);

  const goCheckout = async (plan: PlanId) => {
    if (plan === "enterprise") {
      navigate("/request-demo");
      return;
    }
    if (!user) {
      const next = `/pricing?plan=${plan}&interval=${interval}&checkout=1`;
      trackEvent("plan_selected", { plan, interval, source: "homepage", authenticated: false });
      navigate(`/signup?next=${encodeURIComponent(next)}`);
      return;
    }
    try {
      setPendingPlan(plan);
      trackEvent("checkout_started", { plan, interval, source: "homepage" });
      const value = getPlanPrice(plan, interval);
      trackEvent("begin_checkout", {
        currency: "USD",
        value,
        items: [{ item_id: plan, item_name: `Media AI ${plan}`, price: value, quantity: 1 }],
      });
      await startCheckout(plan, interval);
    } catch (e) {
      toast.error((e as Error).message ?? "Could not start checkout");
      setPendingPlan(null);
    }
  };

  const startTrial = () => {
    trackEvent("trial_cta_clicked", { source: "homepage", authenticated: Boolean(user) });
    navigate(user ? "/pricing" : `/signup?next=${encodeURIComponent("/pricing")}`);
  };

  const PrimaryCTA = ({ className = "" }: { className?: string }) => (
    <Button
      onClick={startTrial}
      className={`bg-primary hover:bg-primary-hover text-primary-foreground font-medium rounded-lg px-6 h-12 text-sm ${className}`}
    >
      Start Free Trial <ArrowRight className="ml-2 h-4 w-4" />
    </Button>
  );

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Helmet>
        <title>Media — Search Journalists and Creators Using AI</title>
        <meta
          name="description"
          content="Find the right journalists and creators for your stories, campaigns, and projects in seconds. AI-powered media database with verified emails and socials."
        />
        <link rel="canonical" href="https://trymedia.ai/" />
        <meta property="og:url" content="https://trymedia.ai/" />
      </Helmet>

      <Header />

      {/* HERO */}
      <section className="px-6 pt-16 pb-10 md:pt-24">
        <div className="max-w-5xl mx-auto text-center">
          <h1
            className="text-[34px] md:text-[56px] lg:text-[68px] font-medium tracking-tight leading-[1.04] mb-8"
            style={{ fontFamily: "var(--font-heading)", color: "#282c34" }}
          >
            Search <span style={{ color: "#282c34" }}>Journalists and Creators</span> Using AI
          </h1>
          <p className="text-lg md:text-xl text-muted-foreground max-w-3xl mx-auto mb-10">
            Find the right voices for your stories, campaigns, and projects in just a few clicks from our database of top journalists and creators — powered by AI.
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-6 mb-6">
            <PrimaryCTA />
            <div
              className="senja-embed"
              data-id="20a2f52c-c242-49a6-a8e6-38e737f40524"
              data-mode="shadow"
              data-lazyload="false"
              style={{ display: "block" }}
            />

          </div>

          

          <div className="flex flex-wrap items-center justify-center gap-x-8 gap-y-2 text-sm text-muted-foreground">
            <span className="flex items-center gap-2"><Check className="h-4 w-4 text-primary" /> 1-month free trial</span>
            <span className="flex items-center gap-2"><Check className="h-4 w-4 text-primary" /> Secure payment</span>
            <span className="flex items-center gap-2"><Check className="h-4 w-4 text-primary" /> Cancel any-time</span>
          </div>
        </div>

        <div className="max-w-6xl mx-auto mt-16">
          <HeroVideo poster={heroProductMain} src="/video/chat-hero.mp4" />
        </div>

        <div className="max-w-6xl mx-auto mt-20 text-center">
          <p className="text-sm text-muted-foreground mb-8">Our users secure coverage in</p>
          <div className="overflow-hidden relative [mask-image:linear-gradient(to_right,transparent,black_10%,black_90%,transparent)]">
            <div className="flex items-center gap-12 md:gap-16 animate-[scroll_40s_linear_infinite] whitespace-nowrap w-max">
              {(() => {
                const logos = [
                  { name: "TechCrunch", src: "/press-logos/techcrunch.png" },
                  { name: "Forbes", src: "/press-logos/forbes.png" },
                  { name: "Fast Company", src: "/press-logos/fastcompany.png" },
                  { name: "TIME", src: "/press-logos/time.png" },
                  { name: "Fortune", src: "/press-logos/fortune.png" },
                  { name: "Business Insider", src: "/press-logos/businessinsider.jpg" },
                  { name: "The Economist", src: "/press-logos/economist.png" },
                  { name: "Daily Mail", src: "/press-logos/dailymail.png" },
                  { name: "CNN", src: "/press-logos/cnn.svg" },
                  { name: "MarketWatch", src: "/press-logos/marketwatch.png" },
                  { name: "USA Today", src: "/press-logos/usatoday.png" },
                  { name: "CNET", src: "/press-logos/cnet.png" },
                  { name: "The Verge", src: "/press-logos/theverge.png" },
                ];
                return [...logos, ...logos].map((logo, i) => (
                  <div
                    key={i}
                    className="flex items-center justify-center w-32 md:w-40 h-10 md:h-12 flex-shrink-0"
                  >
                    <img
                      src={logo.src}
                      alt={logo.name}
                      className="max-h-full max-w-full object-contain opacity-70 hover:opacity-100 transition-opacity"
                    />
                  </div>
                ));
              })()}
            </div>
          </div>
        </div>
      </section>

      {/* FEATURES */}
      <section className="px-6 py-24">
        <div className="max-w-5xl mx-auto text-center mb-16">
          <h2 className="text-3xl md:text-5xl lg:text-6xl font-medium tracking-tight leading-[1.1]" style={{ fontFamily: "var(--font-heading)" }}>
            Powerful Features Built to <br className="hidden md:block" />
            <span className="text-primary">Simplify Discovery</span>
          </h2>
        </div>

        <div className="max-w-6xl mx-auto space-y-6">
          {FEATURE_BLOCKS.map((f, i) => (
            <div key={f.eyebrow} className="rounded-3xl border border-border bg-white p-8 md:p-12">
              <div className={`grid md:grid-cols-2 gap-10 items-center ${i % 2 ? "md:[&>*:first-child]:order-2" : ""}`}>
                <div>
                  <h3 className="text-3xl md:text-4xl font-medium mb-5" style={{ fontFamily: "var(--font-heading)" }}>
                    {f.eyebrow}
                  </h3>
                  <p className="text-muted-foreground mb-6 leading-relaxed">{f.body}</p>
                  <PrimaryCTA className="mb-6" />
                  <ul className="space-y-3">
                    {f.bullets.map((b) => (
                      <li key={b} className="flex items-start gap-2 text-sm">
                        <CornerDownRight className="h-4 w-4 mt-0.5 text-primary flex-shrink-0" />
                        <span>{b}</span>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="rounded-2xl bg-secondary/40 p-4 min-h-[260px] flex items-center justify-center overflow-hidden">
                  <img src={f.image} alt={f.eyebrow} className="w-full h-auto rounded-lg" />
                </div>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* BRANDS */}
      <section className="px-6 py-16">
        <div className="max-w-5xl mx-auto text-center">
          <p className="text-sm text-muted-foreground mb-10">
            Innovative brands that have secured media coverage with us
          </p>
          <div className="flex flex-wrap items-center justify-center gap-x-12 gap-y-8 opacity-70">
            {[brand1, brand2, brand3, brand4].map((b, i) => (
              <img key={i} src={b} alt="" className="h-8 md:h-10 w-auto object-contain" />
            ))}
          </div>
        </div>
      </section>

      {/* AUDIENCES */}
      <section className="px-6 py-20">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-5xl font-medium tracking-tight mb-4" style={{ fontFamily: "var(--font-heading)" }}>
              For any PR or Social Media pro
            </h2>
            <p className="text-muted-foreground max-w-xl mx-auto">
              People who use Media AI include PR and Social Media professionals everywhere.
            </p>
          </div>
          <div className="rounded-3xl bg-accent/40 p-8 md:p-12">
            <div className="grid md:grid-cols-2 gap-10 items-center">
              <div className="space-y-1">
                {AUDIENCES.map((a, idx) => (
                  <div key={a.label} className={`py-5 ${idx !== AUDIENCES.length - 1 ? "border-b border-border" : ""}`}>
                    <h3 className="text-2xl md:text-3xl font-medium mb-1" style={{ fontFamily: "var(--font-heading)" }}>
                      {a.label}
                    </h3>
                    <p className="text-sm text-muted-foreground">{a.body}</p>
                  </div>
                ))}
              </div>
              <div className="rounded-full overflow-hidden aspect-square max-w-md mx-auto">
                <img src={audiencesImg} alt="PR and social media professionals at work" className="w-full h-full object-cover" />
              </div>
            </div>
          </div>
        </div>
      </section>

      <Testimonial
        quote="Media AI made it so easy to "
        highlight="find the right collaborators"
        tail=" for our campaign. It's a game-changer."
        name="Sarah Jones"
        role="Marketing Manager, Remote3"
        avatar={avatar4}
      />

      {/* VALUE PROPS — blue band */}
      <section className="px-6 py-24 bg-primary text-primary-foreground">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-5xl font-medium tracking-tight mb-4" style={{ fontFamily: "var(--font-heading)" }}>
              The media database built by <span className="text-[hsl(160_70%_75%)]">PR's, for PR's</span>
            </h2>
            <p className="text-primary-foreground/80 max-w-xl mx-auto">
              People who use Media AI include PR and Social Media professionals everywhere.
            </p>
          </div>
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {VALUE_PROPS.map((v) => (
              <div key={v.title} className="rounded-2xl bg-primary-foreground/10 p-8 backdrop-blur-sm">
                <div className="h-10 w-10 rounded-lg bg-primary-foreground/15 flex items-center justify-center mb-6">
                  <span className="text-xl whitespace-pre-line">{v.icon}</span>
                </div>
                <h3 className="text-2xl font-medium mb-3" style={{ fontFamily: "var(--font-heading)" }}>{v.title}</h3>
                <p className="text-sm text-primary-foreground/80 leading-relaxed">{v.body}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <Testimonial
        quote="I used Media AI to get the best "
        highlight="find journalists and influencers"
        tail=" for our event. Their team are so helpful."
        name="Medet Serik"
        role="Executive Solutions Consultant, Huawei"
        avatar={avatar2}
      />

      {/* PRICING */}
      <section id="pricing" className="px-6 py-20">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto mb-12">
            <h2 className="text-3xl md:text-5xl font-medium tracking-tight mb-4" style={{ fontFamily: "var(--font-heading)" }}>
              Flexible pricing plans to suit your needs
            </h2>
            <p className="text-muted-foreground text-lg">
              Choose between monthly and yearly subscriptions. Cancel any time.
            </p>

            <div className="inline-flex items-center mt-8 p-1 rounded-full border border-border bg-secondary">
              {(["monthly", "yearly"] as Interval[]).map((opt) => (
                <button
                  key={opt}
                  type="button"
                  onClick={() => setIntervalVal(opt)}
                  className={`px-5 py-2 rounded-full text-sm font-medium transition-colors ${
                    interval === opt
                      ? "bg-white text-foreground shadow-sm"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                >
                  {opt === "monthly" ? "Monthly" : "Yearly"}
                  {opt === "yearly" && (
                    <span className="ml-2 text-xs text-primary">Save ~17%</span>
                  )}
                </button>
              ))}
            </div>
          </div>

          <div className="grid md:grid-cols-3 gap-6">
            {TIERS.map((tier) => {
              const price = interval === "monthly" ? tier.monthly : tier.yearly;
              const period = interval === "monthly" ? "/mo" : "/yr";
              return (
                <div
                  key={tier.id}
                  className={`relative rounded-2xl p-8 flex flex-col bg-white ${
                    tier.highlight
                      ? "border-2 border-primary shadow-lg"
                      : "border border-border"
                  }`}
                >
                  {tier.badge && (
                    <div className="absolute -top-3 left-1/2 -translate-x-1/2">
                      <span className="px-3 py-1 rounded-full bg-primary text-primary-foreground text-xs font-medium">
                        {tier.badge}
                      </span>
                    </div>
                  )}
                  <h3 className="text-xl font-medium mb-1">{tier.name}</h3>
                  <p className="text-sm mb-6 text-muted-foreground">
                    {tier.tagline}
                  </p>
                  <div className="mb-6">
                    {price !== null ? (
                      <>
                        <span className="text-4xl font-medium">${price}</span>
                        <span className="text-sm ml-1 text-muted-foreground">
                          {period}
                        </span>
                      </>
                    ) : (
                      <span className="text-2xl font-medium">Custom</span>
                    )}
                  </div>
                  <Button
                    onClick={() => goCheckout(tier.id)}
                    disabled={pendingPlan !== null}
                    className="w-full mb-6"
                    variant={tier.highlight ? "default" : "outline"}
                  >
                    {pendingPlan === tier.id ? "Redirecting…" : tier.cta}
                  </Button>
                  <ul className="space-y-3 text-sm">
                    {tier.features.map((f) => (
                      <li key={f} className="flex items-start gap-2">
                        <Check className="h-4 w-4 mt-0.5 flex-shrink-0 text-primary" />
                        <span>{f}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              );
            })}
          </div>

          <p className="text-center text-xs text-muted-foreground mt-10">
            All prices in USD. Subscriptions renew automatically until canceled.
          </p>
        </div>
      </section>

      {/* COMPARISON */}
      <section className="px-6 py-16">
        <div className="max-w-5xl mx-auto">
          <p className="text-center text-muted-foreground text-lg mb-10">Compared to other PR & Influencer Marketing platforms</p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th></th>
                  <th className="px-4 py-4 w-[18%]"><img src={logoMedia} alt="Media AI" className="h-6 mx-auto object-contain" /></th>
                  <th className="px-4 py-4 w-[18%]"><img src={logoMuckrack} alt="Muck Rack" className="h-6 mx-auto opacity-70 object-contain" /></th>
                  <th className="px-4 py-4 w-[18%]"><img src={logoCision} alt="Cision" className="h-6 mx-auto opacity-70 object-contain" /></th>
                  <th className="px-4 py-4 w-[18%]"><img src={logoMeltwater} alt="Meltwater" className="h-6 mx-auto opacity-70 object-contain" /></th>
                </tr>
              </thead>
              <tbody className="[&>tr]:border-t [&>tr]:border-border">
                {[
                  { label: "AI search", row: [true, false, false, false] },
                  { label: "Journalist database", row: [true, true, true, true] },
                  { label: "Creator database", row: [true, false, false, true] },
                  { label: "Export to CSV", row: [true, true, true, true] },
                  { label: "Inbox", row: [true, true, true, true] },
                  { label: "Monitor", row: [true, true, true, true] },
                  { label: "Updated", row: ["AI", "?", "?", "?"] },
                  { label: "Monthly cost***", row: ["$99", "$400+", "$700+", "$1000+"] },
                  { label: "Annual cost****", row: ["$990", "$5,000+", "$8,000+", "$12,000+"] },
                  { label: "Payment", row: ["Credit Card", "Contract", "Contract", "Contract"] },
                  { label: "Free Trial?", row: [true, false, false, false] },
                ].map((r) => (
                  <tr key={r.label}>
                    <td className="py-5 pr-4 text-muted-foreground">{r.label}</td>
                    {r.row.map((v, i) => (
                      <td key={i} className="py-5 px-4 text-center">
                        {typeof v === "boolean" ? (
                          v ? <Check className="h-5 w-5 text-emerald-500 mx-auto" strokeWidth={2.5} /> : <XIcon className="h-5 w-5 text-red-500 mx-auto" strokeWidth={2.5} />
                        ) : (
                          <span className={i === 0 ? "text-primary font-medium" : "text-muted-foreground"}>{v}</span>
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="text-center mt-10">
            <PrimaryCTA />
          </div>
          <p className="text-xs text-muted-foreground mt-8 text-center max-w-3xl mx-auto leading-relaxed">
            *Trademarks owned by Muck Rack, LLC, Cision US Inc., Meltwater News US Inc / **Estimated, companies do not publicly provide database size / *** ****Estimated, companies do not publicly provide pricing on their websites
          </p>
        </div>
      </section>

      <Testimonial
        quote="I work across several markets in APAC. Media AI "
        highlight="helped me find journalists"
        tail=" across every one of them."
        name="Jackson Nemeth"
        role="Business Development, Cibes Lift Group"
        avatar={jacksonAvatar}
      />

      {/* FAQ */}
      <section className="px-6 py-20">
        <div className="max-w-3xl mx-auto">
          <h2 className="text-4xl md:text-6xl font-medium tracking-tight mb-12" style={{ fontFamily: "var(--font-heading)" }}>
            FAQ's
          </h2>
          <div className="divide-y divide-border border-y border-border">
            {FAQS.map((f, i) => {
              const open = openFaq === i;
              return (
                <div key={f.q}>
                  <button
                    type="button"
                    onClick={() => setOpenFaq(open ? null : i)}
                    className="w-full flex items-center gap-4 py-6 text-left"
                  >
                    {open ? <Minus className="h-5 w-5 text-primary flex-shrink-0" /> : <Plus className="h-5 w-5 text-primary flex-shrink-0" />}
                    <span className="text-lg md:text-xl font-medium" style={{ fontFamily: "var(--font-heading)" }}>{f.q}</span>
                  </button>
                  {open && <div className="pl-9 pb-6 text-sm text-muted-foreground leading-relaxed">{f.a}</div>}
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* FINAL CTA */}
      <section className="px-6 py-12">
        <div className="max-w-5xl mx-auto rounded-3xl bg-primary text-primary-foreground p-10 md:p-16 relative overflow-hidden">
          <div className="grid md:grid-cols-2 gap-8 items-center">
            <div>
              <h2 className="text-3xl md:text-5xl font-medium tracking-tight mb-8 leading-tight" style={{ fontFamily: "var(--font-heading)" }}>
                Find journalists and creators. Start exploring Media AI.
              </h2>
              <Button
                onClick={startTrial}
                className="bg-white text-foreground hover:bg-white/90 font-medium rounded-lg px-6 h-12"
              >
                Start Free Trial <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </div>
            <div className="hidden md:block">
              <img src={heroProductMain} alt="" className="rounded-xl shadow-2xl" />
            </div>
          </div>
        </div>
      </section>

      {/* FOOTER */}
      <footer className="px-6 py-16 mt-12">
        <div className="max-w-6xl mx-auto grid md:grid-cols-6 gap-8 text-sm">
          <FooterCol title="Company" links={[
            ["About", "/about"],
            ["Blog", "/blog"],
            ["Media Kit", "/media-kit"],
            ["Community", "https://chat.whatsapp.com/KKjLvfjPY2ND11cexE0Tae?mode=gi_t"],
          ]} />
          <FooterCol title="Support" links={[
            ["Support", "mailto:support@trymedia.ai"],
            ["Privacy Policy", "/privacy"],
            ["Terms of Service", "/terms"],
          ]} />
          <div>
            <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Resources</h4>
            <ul className="space-y-2.5 text-muted-foreground">
              {RESOURCES_LINKS.slice(0, 9).map(([label, href]) => (
                <li key={label}>
                  {href.startsWith("http") ? (
                    <a href={href} className="hover:text-foreground" target="_blank" rel="noopener noreferrer">{label}</a>
                  ) : (
                    <Link to={href} className="hover:text-foreground">{label}</Link>
                  )}
                </li>
              ))}
              <li><Link to="/resources" className="hover:text-foreground font-medium">View All Resources →</Link></li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Free Tools</h4>
            <ul className="space-y-2.5 text-muted-foreground">
              {TOOLS_LINKS.slice(0, 9).map(([label, href]) => (
                <li key={label}>
                  <Link to={href} className="hover:text-foreground">{label}</Link>
                </li>
              ))}
              <li><Link to="/tools" className="hover:text-foreground font-medium">View All Tools →</Link></li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Compare</h4>
            <ul className="space-y-2.5 text-muted-foreground">
              {COMPARE_LINKS.map(([label, href]) => (
                <li key={label}>
                  <Link to={href} className="hover:text-foreground">{label}</Link>
                </li>
              ))}
              <li><Link to="/compare" className="hover:text-foreground font-medium">View All →</Link></li>
            </ul>
          </div>
          <div>
            <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>Connect</h4>
            <div className="flex items-center gap-3">
              <a href="https://x.com/trymediaai" target="_blank" rel="noopener noreferrer" className="h-9 w-9 rounded-full border border-border flex items-center justify-center hover:bg-secondary"><XLogo className="h-4 w-4" /></a>
              <a href="https://www.linkedin.com/company/trymediaai" target="_blank" rel="noopener noreferrer" className="h-9 w-9 rounded-full border border-border flex items-center justify-center hover:bg-secondary"><Linkedin className="h-4 w-4" /></a>
            </div>
          </div>
        </div>
        <p className="text-xs text-muted-foreground text-center mt-12">
                Copyright © {new Date().getFullYear()} Works App, Inc. Built with 🫶🏻 by{" "}
                <a href="http://x.com/alexmacgregor__/" target="_blank" rel="noopener noreferrer" className="hover:text-foreground">Alex</a>.
        </p>
      </footer>
    </div>
  );
};

const FooterCol = ({ title, links }: { title: string; links: readonly (readonly [string, string])[] }) => (
  <div>
    <h4 className="font-medium mb-4" style={{ fontFamily: "var(--font-heading)" }}>{title}</h4>
    <ul className="space-y-2.5 text-muted-foreground">
      {links.map(([label, href]) => (
        <li key={label}>
          <a href={href} className="hover:text-foreground" target={href.startsWith("http") ? "_blank" : undefined} rel="noopener noreferrer">{label}</a>
        </li>
      ))}
    </ul>
  </div>
);

const Testimonial = ({
  quote, highlight, tail, name, role, avatar,
}: { quote: string; highlight: string; tail: string; name: string; role: string; avatar: string }) => (
  <section className="px-6 py-20">
    <div className="max-w-4xl mx-auto">
      <div className="flex items-center gap-1 mb-8">
        {[...Array(5)].map((_, i) => (
          <Star key={i} className="h-6 w-6 fill-yellow-400 text-yellow-400" />
        ))}
      </div>
      <p className="text-2xl md:text-4xl font-medium leading-snug mb-10" style={{ fontFamily: "var(--font-heading)" }}>
        “{quote}<span className="text-primary">{highlight}</span>{tail}”
      </p>
      <div className="flex items-center gap-4">
        <img src={avatar} alt={name} className="h-12 w-12 rounded-full object-cover" />
        <div>
          <div className="text-base font-medium">{name}</div>
          <div className="text-sm text-muted-foreground">{role}</div>
        </div>
      </div>
    </div>
  </section>
);

export default Index;
