// confirm-topup — synchronous fallback for top-up purchases.
// Verifies the user owns the Checkout Session and grants chat credits if not yet granted.
import Stripe from "npm:stripe@17.5.0";
import { createClient } from "npm:@supabase/supabase-js@2.45.4";

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
    const session = await stripe.checkout.sessions.retrieve(sessionId);
    if (session.mode !== "payment" || session.metadata?.kind !== "topup") {
      return json({ error: "not_topup" }, 400);
    }
    if (session.payment_status !== "paid") return json({ error: "not_paid", status: session.payment_status }, 400);

    const sessionUserId = session.metadata?.supabase_user_id;
    const sessionEmail = (session.customer_details?.email ?? session.customer_email ?? "").toLowerCase();
    const authEmail = (authData.user.email ?? "").toLowerCase();
    if (sessionUserId && sessionUserId !== authData.user.id) return json({ error: "user_mismatch" }, 403);
    if (!sessionUserId && (!sessionEmail || !authEmail || sessionEmail !== authEmail)) {
      return json({ error: "session_email_mismatch" }, 403);
    }

    const userId = sessionUserId || authData.user.id;
    const tokens = Number(session.metadata?.tokens || 0);
    if (!tokens) return json({ error: "missing_tokens" }, 400);

    const { data: granted, error: grantError } = await admin.rpc("grant_topup_credits_once", {
      _user: userId,
      _stripe_session_id: sessionId,
      _tokens: tokens,
    });
    if (grantError) throw grantError;

    // Mark session as granted to prevent double-grant if webhook also fires.
    try {
      await stripe.checkout.sessions.update(sessionId, {
        metadata: { ...(session.metadata || {}), granted: "1" },
      });
    } catch (e) {
      console.warn("could not mark session granted", e);
    }

    return json({
      ok: true,
      already: !granted,
      tokens,
      amount_total: session.amount_total ?? 0,
      currency: session.currency ?? "usd",
      pack: session.metadata?.pack,
    });
  } catch (e) {
    console.error("confirm-topup error", e);
    return json({ error: (e as Error).message }, 500);
  }
});

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}
