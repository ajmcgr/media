// One-time Stripe Checkout for token top-up packs + synchronous confirmation.
// Checkout body: { pack: "small"|"medium"|"large" }
// Confirm body:  { action: "confirm", session_id?: string, user_id?: string, user_email?: string }
// Uses inline price_data — no Stripe dashboard setup required.

import Stripe from "npm:stripe@17.5.0";
import { createClient } from "npm:@supabase/supabase-js@2.45.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

export type Pack = "small" | "medium" | "large";

export const PACKS: Record<Pack, { tokens: number; amount: number; name: string }> = {
  small:  { tokens:  100_000, amount:  1000, name: "100k chat tokens" },
  medium: { tokens:  500_000, amount:  4000, name: "500k chat tokens" },
  large:  { tokens: 2_000_000, amount: 12000, name: "2M chat tokens" },
};

const fallbackSiteUrl = "https://trymedia.ai";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      status: 200,
      headers: corsHeaders,
    });
  }

  if (req.method !== "POST") {
    return json({ error: "method_not_allowed" }, 405);
  }

  try {
    const stripeSecretKey = Deno.env.get("STRIPE_SECRET_KEY") ?? "";
    if (!stripeSecretKey) return json({ error: "missing_stripe_key" }, 500);

    const body = await req.json().catch(() => ({} as Record<string, unknown>));
    const stripe = new Stripe(stripeSecretKey, { apiVersion: "2024-11-20.acacia" });

    if (String(body.action ?? "").toLowerCase() === "confirm" || body.session_id) {
      return await confirmTopup(req, body, stripe);
    }

    const token = (req.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
    if (!token) return json({ error: "missing_auth" }, 401);
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const admin = createClient(SUPABASE_URL, SERVICE_KEY);
    const { data: authData, error: authError } = await admin.auth.getUser(token);
    if (authError || !authData.user?.id || !authData.user.email) {
      return json({ error: "invalid_auth" }, 401);
    }

    const user_id = authData.user.id;
    const user_email = authData.user.email.trim().toLowerCase();
    const packKey = String(body.pack ?? "").toLowerCase().trim() as Pack;
    const pack = PACKS[packKey];

    if (!pack) return json({ error: "invalid_pack", receivedPack: packKey }, 400);

    const siteUrl = siteOrigin(req);
    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      customer_email: user_email,
      line_items: [{
        quantity: 1,
        price_data: {
          currency: "usd",
          unit_amount: pack.amount,
          product_data: { name: `Media AI — ${pack.name}` },
        },
      }],
      allow_promotion_codes: true,
      metadata: {
        supabase_user_id: user_id,
        kind: "topup",
        pack: packKey,
        tokens: String(pack.tokens),
      },
      success_url: `${siteUrl}/search?topup=success&session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${siteUrl}/search?topup=cancelled`,
    });

    return json({ url: session.url });
  } catch (e) {
    console.error("create-topup error", e);
    return json({ error: (e as Error).message }, 500);
  }
});

async function confirmTopup(
  req: Request,
  body: Record<string, unknown>,
  stripe: Stripe,
) {
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  if (!SUPABASE_URL || !SERVICE_KEY) return json({ error: "missing_supabase_config" }, 500);

  const token = (req.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
  if (!token) return json({ error: "missing_auth" }, 401);

  const admin = createClient(SUPABASE_URL, SERVICE_KEY);
  const { data: authData, error: authErr } = await admin.auth.getUser(token);
  if (authErr || !authData.user) return json({ error: "invalid_auth" }, 401);

  const sessionId = String(body.session_id ?? "").trim();
  if (sessionId) {
    const session = await stripe.checkout.sessions.retrieve(sessionId);
    return await grantTopupSession(admin, stripe, session, authData.user.id);
  }

  const requestedUserId = String(body.user_id ?? authData.user.id).trim();
  const requestedEmail = String(body.user_email ?? authData.user.email ?? "").trim().toLowerCase();
  if (requestedUserId !== authData.user.id) return json({ error: "user_mismatch" }, 403);

  // Recovery path for old success URLs that did not include {CHECKOUT_SESSION_ID}.
  const recent = await stripe.checkout.sessions.list({ limit: 30 });
  const candidates = recent.data.filter((session) => {
    const sessionUserId = session.metadata?.supabase_user_id;
    const sessionEmail = (session.customer_details?.email ?? session.customer_email ?? "").toLowerCase();
    const isRecent = (Date.now() / 1000) - session.created < 60 * 60 * 24 * 7;
    return session.mode === "payment"
      && session.metadata?.kind === "topup"
      && session.payment_status === "paid"
      && isRecent
      && (sessionUserId === authData.user.id || (!!requestedEmail && sessionEmail === requestedEmail));
  });

  if (!candidates.length) return json({ ok: false, error: "no_paid_topup_found" }, 404);

  let granted = 0;
  let already = 0;
  for (const session of candidates) {
    const result = await processTopupGrant(admin, stripe, session, authData.user.id);
    if (result.granted) granted += result.tokens;
    if (result.already) already += 1;
  }
  return json({ ok: true, recovered: true, granted, already });
}

async function grantTopupSession(
  admin: ReturnType<typeof createClient>,
  stripe: Stripe,
  session: Stripe.Checkout.Session,
  authUserId: string,
) {
  if (session.mode !== "payment" || session.metadata?.kind !== "topup") {
    return json({ error: "not_topup" }, 400);
  }
  if (session.payment_status !== "paid") {
    return json({ error: "not_paid", status: session.payment_status }, 400);
  }
  const result = await processTopupGrant(admin, stripe, session, authUserId);
  return json({ ok: true, ...result });
}

async function processTopupGrant(
  admin: ReturnType<typeof createClient>,
  stripe: Stripe,
  session: Stripe.Checkout.Session,
  authUserId: string,
) {
  const sessionUserId = session.metadata?.supabase_user_id;
  if (sessionUserId && sessionUserId !== authUserId) throw new Error("user_mismatch");

  if (session.metadata?.granted === "1") {
    return { already: true, granted: false, tokens: 0, session_id: session.id };
  }

  const userId = sessionUserId || authUserId;
  const tokens = Number(session.metadata?.tokens || 0);
  if (!tokens) throw new Error("missing_tokens");

  const { data: granted, error: grantError } = await admin.rpc("grant_topup_credits_once", {
    _user: userId,
    _stripe_session_id: session.id,
    _tokens: tokens,
  });
  if (grantError) throw grantError;

  await stripe.checkout.sessions.update(session.id, {
    metadata: { ...(session.metadata || {}), granted: "1" },
  });

  return { already: !granted, granted: !!granted, tokens: granted ? tokens : 0, session_id: session.id };
}

function siteOrigin(req: Request) {
  const candidates = [req.headers.get("origin"), Deno.env.get("SITE_URL"), fallbackSiteUrl];
  for (const candidate of candidates) {
    try {
      if (candidate) return new URL(candidate).origin;
    } catch {
      // Try next candidate.
    }
  }
  return fallbackSiteUrl;
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}
