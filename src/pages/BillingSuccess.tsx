import { useEffect, useRef } from "react";
import { Helmet } from "react-helmet-async";
import { useNavigate, useSearchParams } from "react-router-dom";
import Header from "@/components/Header";
import { Button } from "@/components/ui/button";
import { useSubscription } from "@/hooks/useSubscription";
import { CheckCircle2 } from "lucide-react";
import { confirmCheckout } from "@/lib/billing";
import { trackEvent } from "@/lib/analytics";

const BillingSuccess = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const sessionId = searchParams.get("session_id");
  const sub = useSubscription();
  const confirmedSession = useRef<string | null>(null);

  useEffect(() => {
    if (sessionId && confirmedSession.current !== sessionId) {
      confirmedSession.current = sessionId;
      confirmCheckout(sessionId)
        .then(() => {
          trackEvent("purchase", { transaction_id: sessionId, plan: sub.planIdentifier || "unknown" });
          return sub.refresh();
        })
        .catch((error) => console.error("confirm-checkout failed", error));
    }

    if (sub.active) {
      navigate("/search", { replace: true });
      return;
    }
    const interval = window.setInterval(() => { sub.refresh(); }, 1500);
    const fallback = window.setTimeout(() => {
      navigate("/search", { replace: true });
    }, 8000);
    return () => {
      window.clearInterval(interval);
      window.clearTimeout(fallback);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sub.active, sessionId]);

  return (
    <div className="min-h-screen bg-background">
      <Helmet>
        <title>Welcome — Media AI</title>
      </Helmet>
      <Header />
      <main className="max-w-xl mx-auto px-6 py-24 text-center">
        <CheckCircle2 className="h-12 w-12 text-primary mx-auto mb-6" />
        <h1 className="text-3xl font-medium mb-3">You're in.</h1>
        <p className="text-muted-foreground mb-8">
          {sub.active
            ? "Your subscription is active. Redirecting you to Search."
            : "Payment received. We're activating your subscription — this usually takes a few seconds."}
        </p>
        <div className="flex gap-3 justify-center">
          <Button onClick={() => navigate("/search")} disabled={!sub.active}>
            {sub.active ? "Open Search" : "Activating…"}
          </Button>
          <Button variant="outline" onClick={() => navigate("/account")}>
            Account
          </Button>
        </div>
      </main>
    </div>
  );
};

export default BillingSuccess;
