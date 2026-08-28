// create-checkout — Stripe Checkout session for subscription plans.
// Body: { user_id, user_email, plan_identifier: "starter"|"growth"|..., interval: "monthly"|"yearly" }
// Resolves price_id from public.plans, attaches user_id + plan_identifier metadata
// to BOTH the session and the subscription so stripe-webhook can link it back.

import Stripe from "https://esm.sh/stripe@17.5.0?target=denonext";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

console.log("CHECKOUT_V7_RUNNING - real session 2026-05-04");

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY");
    const SITE_URL = Deno.env.get("SITE_URL") ?? "https://trymedia.ai";
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    if (!STRIPE_SECRET_KEY) return json({ error: "missing_stripe_key" }, 500);

    const body = await req.json().catch(() => ({} as Record<string, unknown>));
    const user_id = String(body.user_id ?? "").trim();
    const user_email = String(body.user_email ?? "").trim();
    const plan_identifier = String(body.plan_identifier ?? "").toLowerCase().trim();
    const interval = (String(body.interval ?? "monthly").toLowerCase().trim() === "yearly")
      ? "yearly" : "monthly";

    console.log("CHECKOUT_BODY", { user_id, user_email, plan_identifier, interval });

    if (!user_id || !user_email) return json({ error: "missing_user" }, 400);
    if (!plan_identifier) return json({ error: "plan_identifier required" }, 400);

    const admin = createClient(SUPABASE_URL, SERVICE_KEY);
    const { data: plan, error: planErr } = await admin
      .from("plans")
      .select("identifier, monthly_price_id, yearly_price_id")
      .eq("identifier", plan_identifier)
      .maybeSingle();
    if (planErr) return json({ error: "plan_lookup_failed", detail: planErr.message }, 500);
    if (!plan) return json({ error: "plan_not_found", plan_identifier }, 400);

    const price_id = interval === "yearly"
      ? (plan.yearly_price_id ?? plan.monthly_price_id)
      : plan.monthly_price_id;
    if (!price_id) return json({ error: "price_id_missing_for_plan", plan_identifier, interval }, 400);

    // Reuse existing Stripe customer if we have one
    const { data: profile } = await admin
      .from("profiles")
      .select("stripe_customer_id")
      .eq("id", user_id)
      .maybeSingle();

    const stripe = new Stripe(STRIPE_SECRET_KEY);
    const sessionParams: Stripe.Checkout.SessionCreateParams = {
      mode: "subscription",
      line_items: [{ price: price_id, quantity: 1 }],
      allow_promotion_codes: true,
      metadata: {
        supabase_user_id: user_id,
        plan_identifier,
        interval,
      },
      subscription_data: {
        metadata: {
          supabase_user_id: user_id,
          plan_identifier,
          interval,
        },
      },
      success_url: `${SITE_URL}/billing/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${SITE_URL}/pricing`,
    };
    if (profile?.stripe_customer_id) {
      sessionParams.customer = profile.stripe_customer_id;
    } else {
      sessionParams.customer_email = user_email;
      sessionParams.subscription_data = {
        ...sessionParams.subscription_data,
        trial_period_days: 30,
      };
    }

    const session = await stripe.checkout.sessions.create(sessionParams);
    return json({ url: session.url });
  } catch (e) {
    console.error("create-checkout error", e);
    return json({ error: (e as Error).message }, 500);
  }
});

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}
