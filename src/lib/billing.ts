import { supabase } from "@/integrations/supabase/client";
import { trackEvent } from "@/lib/analytics";

export type PlanId = "starter" | "growth";
export type BillingInterval = "monthly" | "yearly";
export type TopupPack = "small" | "medium" | "large";

export const TOPUP_PACKS: Record<TopupPack, { tokens: number; priceUsd: number; label: string }> = {
  small: { tokens: 100_000, priceUsd: 10, label: "100k credits" },
  medium: { tokens: 500_000, priceUsd: 40, label: "500k credits" },
  large: { tokens: 2_000_000, priceUsd: 120, label: "2M credits" },
};

export const PLAN_PRICES: Record<PlanId, Record<BillingInterval, number>> = {
  starter: { monthly: 29, yearly: 290 },
  growth: { monthly: 99, yearly: 990 },
};

export function getPlanPrice(plan: PlanId, interval: BillingInterval) {
  return PLAN_PRICES[plan][interval];
}

type InvokeResponse = {
  url?: string;
  ok?: boolean;
  active?: boolean;
  tokens?: number;
  granted?: number;
  already?: boolean;
  plan_identifier?: PlanId;
  interval?: BillingInterval;
  amount_total?: number;
  currency?: string;
  pack?: TopupPack;
};

async function authedInvoke(path: string, body: unknown) {
  const {
    data: { session },
  } = await supabase.auth.getSession();
  if (!session) throw new Error("NOT_AUTHENTICATED");

  const { data, error } = await supabase.functions.invoke(path, {
    body,
    headers: {
      Authorization: `Bearer ${session.access_token}`,
    },
  });

  if (error) {
    const ctx = (error as { context?: Response }).context;
    let detail = "";
    try {
      detail = ctx ? await ctx.clone().text() : "";
    } catch {
      /* ignore */
    }
    throw new Error(detail || error.message || `Request failed: ${path}`);
  }

  return (data ?? {}) as InvokeResponse;
}

export async function startCheckout(plan: PlanId, interval: BillingInterval = "monthly") {
  const {
    data: { session },
  } = await supabase.auth.getSession();
  if (!session?.user?.id || !session.user.email) throw new Error("NOT_AUTHENTICATED");
  const payload = { plan_identifier: plan.toLowerCase() as PlanId, interval };
  const { url } = await authedInvoke("create-checkout", payload);
  if (!url) throw new Error("Checkout URL missing.");
  window.location.href = url;
}

export async function confirmCheckout(sessionId: string) {
  const data = await authedInvoke("confirm-checkout", { session_id: sessionId });
  return {
    ok: Boolean(data.ok),
    plan: data.plan_identifier,
    interval: data.interval,
    amountTotal: Number(data.amount_total ?? 0),
    currency: data.currency ?? "usd",
  };
}

export async function confirmTopup(sessionId: string | null) {
  const {
    data: { session },
  } = await supabase.auth.getSession();
  if (!session?.user?.id || !session.user.email) throw new Error("NOT_AUTHENTICATED");

  // Confirm only through dedicated endpoint.
  // If session_id is missing, don't hard-fail UI; webhook can still grant credits.
  if (!sessionId) {
    return { ok: true, tokens: 0, already: true, amountTotal: 0, currency: "usd", pack: undefined };
  }

  const data = await authedInvoke("confirm-topup", { session_id: sessionId });

  return {
    ok: Boolean(data.ok),
    tokens: Number(data.tokens ?? data.granted ?? 0),
    already: Boolean(data.already),
    amountTotal: Number(data.amount_total ?? 0),
    currency: data.currency ?? "usd",
    pack: data.pack,
  };
}

export async function syncSubscription() {
  const data = await authedInvoke("sync-subscription", {});
  return { ok: Boolean(data.ok), active: Boolean(data.active) };
}

export async function startTopup(pack: TopupPack) {
  const {
    data: { session },
  } = await supabase.auth.getSession();
  if (!session?.user?.id || !session.user.email) throw new Error("NOT_AUTHENTICATED");
  const selectedPack = TOPUP_PACKS[pack];
  trackEvent("begin_checkout", {
    currency: "USD",
    value: selectedPack.priceUsd,
    items: [{ item_id: `topup_${pack}`, item_name: selectedPack.label, price: selectedPack.priceUsd, quantity: 1 }],
  });
  const { url } = await authedInvoke("create-topup", { pack });
  if (!url) throw new Error("Checkout URL missing.");
  window.location.href = url;
}

export async function openBillingPortal() {
  const { url } = await authedInvoke("customer-portal", {});
  if (!url) throw new Error("Billing portal URL missing.");
  window.location.href = url;
}
