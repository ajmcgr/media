// Stripe webhook → upserts public.subscriptions (source of truth). Public endpoint; Stripe auth is via signature.
// IMPORTANT: Supabase JWT verification must be disabled for this function; Stripe does not send Supabase auth headers.
// Deploy from the project root so supabase/config.toml applies [functions.stripe-webhook] verify_jwt = false.
//
// Required secrets: STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET,
//                   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import Stripe from "https://esm.sh/stripe@17.5.0?target=denonext";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const stripe = new Stripe(Deno.env.get("STRIPE_SECRET_KEY")!, {
  apiVersion: "2024-11-20.acacia",
});
const admin = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

const ACTIVE_STATUSES = new Set(["active", "trialing", "past_due"]);

Deno.serve(async (req) => {
  const webhookSecrets = [
    Deno.env.get("STRIPE_WEBHOOK_SECRET"),
    Deno.env.get("STRIPE_WEBHOOK_SECRET_SECONDARY"),
  ].map((secret) => secret?.trim()).filter(Boolean) as string[];
  if (webhookSecrets.length === 0) {
    console.error("stripe webhook misconfigured: no webhook secret is configured");
    return new Response("missing webhook secret", { status: 500 });
  }

  const sig = req.headers.get("stripe-signature");
  if (!sig) return new Response("missing signature", { status: 400 });

  const raw = await req.text();
  let event: Stripe.Event;
  let verificationError: unknown;
  for (const webhookSecret of webhookSecrets) {
    try {
      event = await stripe.webhooks.constructEventAsync(raw, sig, webhookSecret);
      break;
    } catch (e) {
      verificationError = e;
    }
  }

  if (!event!) {
    const e = verificationError as Error;
    console.error("signature verification failed", {
      name: e?.name,
      message: e?.message,
      hasWebhookSecret: true,
      configuredSecretCount: webhookSecrets.length,
      rawBodyLength: raw.length,
    });
    return new Response("invalid signature", { status: 400 });
  }

  try {
    switch (event.type) {
      case "checkout.session.completed": {
        const session = event.data.object as Stripe.Checkout.Session;
        if (session.mode === "subscription" && session.subscription) {
          const sub = await stripe.subscriptions.retrieve(session.subscription as string);
          await upsertSubscription(sub, {
            userId: session.metadata?.supabase_user_id,
            planIdentifier: session.metadata?.plan_identifier,
            customerEmail: session.customer_details?.email ?? session.customer_email ?? undefined,
          });
        } else if (session.mode === "payment" && session.metadata?.kind === "topup") {
          const userId = session.metadata.supabase_user_id;
          const tokens = Number(session.metadata.tokens || 0);
          if (userId && tokens > 0) {
            await grantTopupOnce(session.id, userId, tokens);
          }
        }
        break;
      }
      case "customer.subscription.created":
      case "customer.subscription.updated":
      case "customer.subscription.deleted":
      case "customer.subscription.paused":
      case "customer.subscription.resumed": {
        const sub = event.data.object as Stripe.Subscription;
        await upsertSubscription(sub);
        break;
      }
      case "invoice.payment_failed": {
        const invoice = event.data.object as Stripe.Invoice;
        if (invoice.subscription) {
          const sub = await stripe.subscriptions.retrieve(invoice.subscription as string);
          await upsertSubscription(sub);
        }
        break;
      }
      case "invoice.paid": {
        const invoice = event.data.object as Stripe.Invoice;
        if (invoice.subscription) {
          const sub = await stripe.subscriptions.retrieve(invoice.subscription as string);
          await upsertSubscription(sub, { customerEmail: invoice.customer_email ?? undefined });
        }
        break;
      }
      default:
        // ignore other events
        break;
    }
    return new Response(JSON.stringify({ received: true }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("webhook handler error", e);
    return new Response((e as Error).message, { status: 500 });
  }
});

async function grantTopupOnce(sessionId: string, userId: string, tokens: number) {
  const { data: granted, error } = await admin.rpc("grant_topup_credits_once", {
    _user: userId,
    _stripe_session_id: sessionId,
    _tokens: tokens,
  });
  if (error) throw error;
  if (granted) console.log("granted topup credits", { userId, tokens });
}

async function upsertSubscription(
  sub: Stripe.Subscription,
  fallback: { userId?: string; planIdentifier?: string; customerEmail?: string } = {},
) {
  const priceId = sub.items.data[0]?.price.id;
  let userId = sub.metadata?.supabase_user_id || fallback.userId;
  let planIdentifier = sub.metadata?.plan_identifier || fallback.planIdentifier;

  // Resolve plan from the price id if metadata is missing
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
    planIdentifier = (monthlyPlan ?? yearlyPlan)?.identifier ?? undefined;
  }

  // Resolve user from stripe_customer_id on profiles when metadata absent
  if (!userId) {
    const { data: profile } = await admin
      .from("profiles")
      .select("id")
      .eq("stripe_customer_id", sub.customer as string)
      .maybeSingle();
    userId = profile?.id ?? undefined;
  }

  // Final fallback for legacy sessions/customers that have no stored customer id yet.
  if (!userId && fallback.customerEmail) {
    const { data: profile } = await admin
      .from("profiles")
      .select("id")
      .ilike("email", fallback.customerEmail)
      .maybeSingle();
    userId = profile?.id ?? undefined;
  }

  if (!userId || !planIdentifier || !priceId) {
    console.warn("skipping sub upsert — missing userId/plan/price", {
      sub: sub.id, userId, planIdentifier, priceId,
    });
    return;
  }

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

  const { error } = await admin
    .from("subscriptions")
    .upsert(row, { onConflict: "stripe_subscription_id" });
  if (error) throw error;

  const isActive = ACTIVE_STATUSES.has(sub.status);
  if (!isActive) {
    const replacement = await findActiveSubscriptionForCustomer(sub.customer as string, sub.id);
    if (replacement) {
      console.log("inactive subscription event ignored because customer has another active subscription", {
        inactiveSub: sub.id,
        activeSub: replacement.id,
        customer: sub.customer,
        userId,
      });
      await upsertSubscription(replacement, {
        userId,
        planIdentifier,
        customerEmail: fallback.customerEmail,
      });
      return;
    }
  }

  // Mirror to profiles so useSubscription (single source of truth) stays in sync
  const profileUpdate: Record<string, unknown> = {
    sub_active: isActive,
    plan_identifier: planIdentifier,
    sub_period_end: toIso(sub.current_period_end),
    stripe_customer_id: sub.customer as string,
  };
  if (sub.trial_start) {
    profileUpdate.trial_used_at = toIso(sub.trial_start);
  }

  const { error: profErr } = await admin
    .from("profiles")
    .update(profileUpdate)
    .eq("id", userId);
  if (profErr) console.error("profile sync error", profErr);
}

async function findActiveSubscriptionForCustomer(customerId: string, ignoredSubscriptionId: string) {
  const subscriptions = await stripe.subscriptions.list({
    customer: customerId,
    status: "all",
    limit: 10,
  });

  return subscriptions.data.find((candidate) =>
    candidate.id !== ignoredSubscriptionId && ACTIVE_STATUSES.has(candidate.status)
  );
}

function toIso(unix: number | null | undefined) {
  return unix ? new Date(unix * 1000).toISOString() : null;
}
