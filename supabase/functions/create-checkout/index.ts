// create-checkout — Stripe Checkout session for subscription plans.
// Body: { plan_identifier: "starter"|"growth"|..., interval: "monthly"|"yearly" }
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

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY");
    const SITE_URL = Deno.env.get("SITE_URL") ?? "https://trymedia.ai";
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    if (!STRIPE_SECRET_KEY) return json({ error: "missing_stripe_key" }, 500);

    const token = (req.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
    if (!token) return json({ error: "missing_auth" }, 401);

    const admin = createClient(SUPABASE_URL, SERVICE_KEY);
    const { data: authData, error: authError } = await admin.auth.getUser(token);
    if (authError || !authData.user?.id || !authData.user.email) {
      return json({ error: "invalid_auth" }, 401);
    }

    const user_id = authData.user.id;
    const user_email = authData.user.email.trim().toLowerCase();
    const body = await req.json().catch(() => ({} as Record<string, unknown>));
    const plan_identifier = String(body.plan_identifier ?? "").toLowerCase().trim();
    const interval = (String(body.interval ?? "monthly").toLowerCase().trim() === "yearly")
      ? "yearly" : "monthly";

    if (!plan_identifier) return json({ error: "plan_identifier required" }, 400);

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

    // Reuse an existing customer and inspect both local and Stripe history before granting a trial.
    const { data: profile, error: profileError } = await admin
      .from("profiles")
      .select("stripe_customer_id, trial_used_at")
      .eq("id", user_id)
      .maybeSingle();
    if (profileError) throw profileError;

    const { data: localSubscription, error: subscriptionHistoryError } = await admin
      .from("subscriptions")
      .select("id")
      .eq("user_id", user_id)
      .limit(1)
      .maybeSingle();
    if (subscriptionHistoryError) throw subscriptionHistoryError;

    const stripe = new Stripe(STRIPE_SECRET_KEY);
    let customerId = profile?.stripe_customer_id ?? null;

    if (!customerId) {
      const matchingCustomers = await stripe.customers.list({ email: user_email, limit: 10 });
      customerId = matchingCustomers.data[0]?.id ?? null;
      if (customerId) {
        const { error: customerSyncError } = await admin
          .from("profiles")
          .update({ stripe_customer_id: customerId })
          .eq("id", user_id);
        if (customerSyncError) throw customerSyncError;
      }
    }

    let hasStripeSubscriptionHistory = false;
    if (customerId) {
      const subscriptions = await stripe.subscriptions.list({ customer: customerId, status: "all", limit: 1 });
      hasStripeSubscriptionHistory = subscriptions.data.length > 0;
    }

    const trialEligible = !profile?.trial_used_at && !localSubscription && !hasStripeSubscriptionHistory;
    const sessionParams: Stripe.Checkout.SessionCreateParams = {
      mode: "subscription",
      line_items: [{ price: price_id, quantity: 1 }],
      allow_promotion_codes: true,
      metadata: {
        supabase_user_id: user_id,
        plan_identifier,
        interval,
        trial_granted: String(trialEligible),
      },
      subscription_data: {
        metadata: {
          supabase_user_id: user_id,
          plan_identifier,
          interval,
          trial_granted: String(trialEligible),
        },
      },
      success_url: `${SITE_URL}/billing/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${SITE_URL}/pricing`,
    };
    if (customerId) {
      sessionParams.customer = customerId;
    } else {
      sessionParams.customer_email = user_email;
    }
    if (trialEligible) {
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
