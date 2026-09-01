// confirm-checkout — called by /billing/success as a synchronous backup to the Stripe webhook.
// It verifies the logged-in Supabase user owns the Checkout Session, then mirrors the
// Stripe subscription into public.subscriptions and public.profiles.

import Stripe from "https://esm.sh/stripe@17.5.0?target=denonext";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const STRIPE_SECRET_KEY = Deno.env.get("STRIPE_SECRET_KEY")!;
    if (!STRIPE_SECRET_KEY) return json({ error: "missing_stripe_key" }, 500);

    const token = (req.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
    if (!token) return json({ error: "missing_auth" }, 401);

    const admin = createClient(SUPABASE_URL, SERVICE_KEY);
    const { data: authData, error: authErr } = await admin.auth.getUser(token);
    if (authErr || !authData.user) return json({ error: "invalid_auth" }, 401);

    const { session_id } = await req.json().catch(() => ({} as Record<string, unknown>));
    const sessionId = String(session_id ?? "").trim();
    if (!sessionId) return json({ error: "missing_session_id" }, 400);

    const stripe = new Stripe(STRIPE_SECRET_KEY, { apiVersion: "2024-11-20.acacia" });
    const session = await stripe.checkout.sessions.retrieve(sessionId, { expand: ["subscription"] });
    if (session.mode !== "subscription" || !session.subscription) {
      return json({ error: "not_subscription_checkout" }, 400);
    }

    const sessionUserId = session.metadata?.supabase_user_id;
    const sessionEmail = (session.customer_details?.email ?? session.customer_email ?? "").toLowerCase();
    const authEmail = (authData.user.email ?? "").toLowerCase();
    if (sessionUserId && sessionUserId !== authData.user.id) return json({ error: "session_user_mismatch" }, 403);
    if (!sessionUserId && (!sessionEmail || !authEmail || sessionEmail !== authEmail)) {
      return json({ error: "session_email_mismatch" }, 403);
    }

    const sub = typeof session.subscription === "string"
      ? await stripe.subscriptions.retrieve(session.subscription)
      : session.subscription as Stripe.Subscription;

    const planIdentifier = await upsertSubscription(
      admin,
      stripe,
      sub,
      authData.user.id,
      session.metadata?.plan_identifier,
    );
    return json({
      ok: true,
      plan_identifier: planIdentifier,
      interval: session.metadata?.interval ?? sub.metadata?.interval ?? "monthly",
      amount_total: session.amount_total ?? 0,
      currency: session.currency ?? "usd",
    });
  } catch (e) {
    console.error("confirm-checkout error", e);
    return json({ error: (e as Error).message }, 500);
  }
});

async function upsertSubscription(admin: ReturnType<typeof createClient>, stripe: Stripe, sub: Stripe.Subscription, fallbackUserId: string, fallbackPlan?: string) {
  const priceId = sub.items.data[0]?.price.id;
  const userId = sub.metadata?.supabase_user_id || fallbackUserId;
  let planIdentifier = sub.metadata?.plan_identifier || fallbackPlan;

  if (!planIdentifier && priceId) {
    const { data: monthlyPlan } = await admin
      .from("plans")
      .select("identifier")
      .eq("monthly_price_id", priceId)
      .maybeSingle();
    const { data: yearlyPlan } = monthlyPlan ? { data: monthlyPlan } : await admin
      .from("plans")
      .select("identifier")
      .eq("yearly_price_id", priceId)
      .maybeSingle();
    planIdentifier = (monthlyPlan ?? yearlyPlan)?.identifier;
  }

  if (!userId || !planIdentifier || !priceId) throw new Error("missing_subscription_mapping");

  const row = {
    user_id: userId,
    plan_identifier: planIdentifier,
    stripe_customer_id: sub.customer as string,
    stripe_subscription_id: sub.id,
    stripe_price_id: priceId,
    status: sub.status,
    current_period_start: toIso(sub.current_period_start),
    current_period_end: toIso(sub.current_period_end),
    cancel_at_period_end: sub.cancel_at_period_end,
    canceled_at: toIso(sub.canceled_at),
  };

  const { error } = await admin.from("subscriptions").upsert(row, { onConflict: "stripe_subscription_id" });
  if (error) throw error;

  let subscriptionForProfile = sub;
  if (!["active", "trialing", "past_due"].includes(sub.status)) {
    const subscriptions = await stripe.subscriptions.list({ customer: sub.customer as string, status: "all", limit: 10 });
    subscriptionForProfile = subscriptions.data.find((candidate) =>
      candidate.id !== sub.id && ["active", "trialing", "past_due"].includes(candidate.status)
    ) ?? sub;
  }

  const isActive = ["active", "trialing", "past_due"].includes(subscriptionForProfile.status);
  const profileUpdate: Record<string, unknown> = {
    sub_active: isActive,
    plan_identifier: subscriptionForProfile.metadata?.plan_identifier || planIdentifier,
    sub_period_end: toIso(subscriptionForProfile.current_period_end),
    stripe_customer_id: subscriptionForProfile.customer as string,
  };
  if (subscriptionForProfile.trial_start) {
    profileUpdate.trial_used_at = toIso(subscriptionForProfile.trial_start);
  }

  const { error: profileError } = await admin
    .from("profiles")
    .update(profileUpdate)
    .eq("id", userId);
  if (profileError) throw profileError;

  return planIdentifier;
}

function toIso(unix: number | null | undefined) {
  return unix ? new Date(unix * 1000).toISOString() : null;
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}
