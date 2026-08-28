import { useEffect, useRef, useState } from "react";
import { Helmet } from "react-helmet-async";
import { Check } from "lucide-react";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/contexts/AuthContext";
import { useNavigate, useSearchParams } from "react-router-dom";
import { startCheckout, startTopup, TOPUP_PACKS, type TopupPack } from "@/lib/billing";
import { toast } from "sonner";
import { trackEvent } from "@/lib/analytics";

type PlanId = "starter" | "growth" | "enterprise";
type Interval = "monthly" | "yearly";

interface Tier {
  id: PlanId;
  name: string;
  tagline: string;
  monthly: number | null;
  yearly: number | null;
  features: string[];
  highlight?: boolean;
  badge?: string;
  cta: string;
}

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
      "Verified journalist and creator contact emails",
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
      "Verified journalist and creator contact emails",
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
      "Verified journalist and creator contact emails",
      "Everything in Growth",
      "Custom API access",
      "Volume credit pricing",
      "SSO + dedicated support",
      "Custom contracts",
    ],
  },
];

const Pricing = () => {
  const [interval, setInterval] = useState<Interval>("monthly");
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const resumedCheckout = useRef(false);
  const [pendingPlan, setPendingPlan] = useState<PlanId | null>(null);
  const [pendingPack, setPendingPack] = useState<TopupPack | null>(null);

  const handleSubscribe = async (plan: PlanId) => {
    if (plan === "enterprise") {
      navigate("/request-demo");
      return;
    }
    const nextPath = `/pricing?plan=${plan}&interval=${interval}&checkout=1`;
    const next = encodeURIComponent(nextPath);
    if (!user) {
      trackEvent("plan_selected", { plan, interval, source: "pricing", authenticated: false });
      navigate(`/signup?next=${next}`);
      return;
    }
    try {
      setPendingPlan(plan);
      trackEvent("checkout_started", { plan, interval, source: "pricing" });
      await startCheckout(plan, interval);
    } catch (e) {
      const msg = (e as Error).message;
      if (msg === "NOT_AUTHENTICATED") {
        toast.error("Please sign in to continue");
        navigate(`/login?next=${next}`);
      } else {
        toast.error(msg ?? "Could not start checkout");
      }
      setPendingPlan(null);
    }
  };

  useEffect(() => {
    if (authLoading || !user || resumedCheckout.current || searchParams.get("checkout") !== "1") return;
    const plan = searchParams.get("plan");
    if (plan !== "starter" && plan !== "growth") return;
    const requestedInterval = searchParams.get("interval") === "yearly" ? "yearly" : "monthly";
    resumedCheckout.current = true;
    setInterval(requestedInterval);
    setPendingPlan(plan);
    trackEvent("checkout_resumed", { plan, interval: requestedInterval, source: "post_auth" });
    startCheckout(plan, requestedInterval).catch((error) => {
      setPendingPlan(null);
      toast.error((error as Error).message || "Could not start checkout");
    });
  }, [authLoading, searchParams, user]);

  const handleTopup = async (pack: TopupPack) => {
    if (!user) {
      navigate("/login?next=" + encodeURIComponent("/pricing"));
      return;
    }
    try {
      setPendingPack(pack);
      await startTopup(pack);
    } catch (e) {
      toast.error((e as Error).message ?? "Could not start checkout");
      setPendingPack(null);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <Helmet>
        <title>Pricing — Media AI</title>
        <meta
          name="description"
          content="Simple plans for Media AI search and the journalist/creator database. Monthly or yearly. 1-month free trial."
        />
        <link rel="canonical" href="https://trymedia.ai/pricing" />
        <meta property="og:url" content="https://trymedia.ai/pricing" />
      </Helmet>

      <Header />

      <main className="max-w-6xl mx-auto px-6 py-16">
        <div className="text-center max-w-2xl mx-auto mb-12">
          <h1 className="text-4xl md:text-5xl font-medium tracking-tight mb-4">
            Flexible pricing plans to suit your needs
          </h1>
          <p className="text-muted-foreground text-lg">
            Choose between monthly and yearly subscriptions. Cancel any time.
          </p>

          <div className="inline-flex items-center mt-8 p-1 rounded-md border border-border bg-secondary">
            {(["monthly", "yearly"] as Interval[]).map((opt) => (
              <button
                key={opt}
                type="button"
                onClick={() => setInterval(opt)}
                className={`px-5 py-2 rounded-md text-sm font-medium transition-colors ${
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
                  onClick={() => handleSubscribe(tier.id)}
                  disabled={authLoading || pendingPlan !== null}
                  className="w-full mb-6"
                  variant={tier.highlight ? "default" : "outline"}
                >
                  {pendingPlan === tier.id
                    ? "Redirecting…"
                    : tier.id === "enterprise"
                      ? tier.cta
                      : user
                        ? tier.cta
                        : "Start Free Trial"}
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
      </main>
      <Footer />
    </div>
  );
};

export default Pricing;
