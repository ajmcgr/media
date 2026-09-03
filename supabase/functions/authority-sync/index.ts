// authority-sync — cached Ahrefs Domain Rating refresh
// Uses Ahrefs' public, no-unit DR endpoint. The optional API key is retained
// server-side because Ahrefs will require API-key authentication from Sep 2026.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { requireAdminOrInternal } from "../_shared/privileged-auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function normalizeDomain(input: string | null | undefined): string | null {
  if (!input) return null;
  let s = String(input).trim().toLowerCase();
  if (!s) return null;
  // Outlet fields often contain composites such as "example.com, News Desk".
  // Extract a hostname rather than passing the whole label to Ahrefs.
  const host = s.match(/(?:https?:\/\/)?(?:www\.)?([a-z0-9-]+(?:\.[a-z0-9-]+)+)/i)?.[1];
  if (!host || host.includes("@")) return null;
  return host.replace(/^www\./, "");
}

type AhrefsResult = { dr: number | null; status: number | "exception" };

async function ahrefsDR(domain: string, token: string): Promise<AhrefsResult> {
  try {
    // This endpoint is explicitly free and returns the current DR. Do not use
    // /site-explorer/domain-rating here: it consumes paid API units per call.
    const url = `https://api.ahrefs.com/v3/public/domain-rating-free?target=${encodeURIComponent(domain)}`;
    const headers: Record<string, string> = { Accept: "application/json" };
    if (token) headers.Authorization = `Bearer ${token}`;
    for (let attempt = 0; attempt < 3; attempt++) {
      const res = await fetch(url, { headers });
      if (res.ok) {
        const j = await res.json();
        const dr = j?.domain_rating?.domain_rating ?? j?.domain_rating ?? null;
        const numeric = typeof dr === "number" ? dr : Number(dr);
        return { dr: Number.isFinite(numeric) ? Math.max(0, Math.min(100, Math.round(numeric))) : null, status: res.status };
      }
      if (res.status !== 429 || attempt === 2) {
        console.warn("AUTHORITY_SYNC_AHREFS_ERROR", { domain, status: res.status });
        return { dr: null, status: res.status };
      }
      const retryAfter = Number(res.headers.get("retry-after"));
      await new Promise((resolve) => setTimeout(resolve, Number.isFinite(retryAfter) ? retryAfter * 1000 : 500 * (attempt + 1)));
    }
    return { dr: null, status: "exception" };
  } catch {
    return { dr: null, status: "exception" };
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const authorization = await requireAdminOrInternal(req);
  if ("error" in authorization) {
    return new Response(JSON.stringify({ error: authorization.error }), {
      status: authorization.status,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const AHREFS = Deno.env.get("AHREFS_API_TOKEN") || "";
  const admin = createClient(SUPABASE_URL, SERVICE_KEY);

  console.log("AUTHORITY_SYNC_STARTED", { hasAhrefs: !!AHREFS });

  // 1. Pull distinct outlets
  const domainSet = new Set<string>();
  let from = 0;
  const PAGE = 1000;
  for (;;) {
    const { data, error } = await admin
      .from("journalist")
      .select("outlet")
      .not("outlet", "is", null)
      .range(from, from + PAGE - 1);
    if (error) {
      console.error("AUTHORITY_SYNC_QUERY_FAILED", error);
      break;
    }
    if (!data?.length) break;
    for (const r of data) {
      const d = normalizeDomain((r as { outlet: string | null }).outlet);
      if (d) domainSet.add(d);
    }
    if (data.length < PAGE) break;
    from += PAGE;
  }
  const allDomains = Array.from(domainSet);
  console.log("AUTHORITY_SYNC_DOMAINS", { count: allDomains.length });

  // Skip domains synced in the last 25 days to avoid needless API calls.
  const cutoff = new Date(Date.now() - 25 * 24 * 60 * 60 * 1000).toISOString();
  const fresh = new Set<string>();
  {
    let f = 0;
    for (;;) {
      const { data, error } = await admin
        .from("outlet_authority")
        .select("domain")
        .gte("updated_at", cutoff)
        .in("source", ["ahrefs", "ahrefs_free"])
        .range(f, f + PAGE - 1);
      if (error) { console.error("AUTHORITY_SYNC_FRESH_QUERY_FAILED", error); break; }
      if (!data?.length) break;
      for (const r of data) if (r.domain) fresh.add(r.domain);
      if (data.length < PAGE) break;
      f += PAGE;
    }
  }
  const requestedLimit = Number((await req.json().catch(() => ({}))).limit);
  const batchLimit = Number.isFinite(requestedLimit) ? Math.min(500, Math.max(1, Math.floor(requestedLimit))) : 250;
  const pendingDomains = allDomains.filter((d) => !fresh.has(d));
  const domains = pendingDomains.slice(0, batchLimit);
  console.log("AUTHORITY_SYNC_TO_FETCH", { total: allDomains.length, fresh: fresh.size, pending: pendingDomains.length, toFetch: domains.length });

  let updated = 0;
  let failed = 0;
  const failureStatuses: Record<string, number> = {};

  if (AHREFS) {
    // The public endpoint is free, but controlled parallelism keeps a large
    // first refresh within Edge Function runtime and API rate limits.
    const CONCURRENCY = 8;
    for (let i = 0; i < domains.length; i += CONCURRENCY) {
      const batch = domains.slice(i, i + CONCURRENCY);
      const outcomes = await Promise.all(batch.map(async (domain) => {
        const result = await ahrefsDR(domain, AHREFS);
        if (result.dr == null) {
          console.warn("AUTHORITY_SYNC_MISS", { domain, status: result.status });
          return { ok: false, status: result.status };
        }
        const { error } = await admin.from("outlet_authority").upsert(
          { domain, authority_score: result.dr, source: "ahrefs_free", updated_at: new Date().toISOString() },
          { onConflict: "domain" }
        );
        if (error) {
          console.warn("AUTHORITY_SYNC_UPSERT_FAILED", { domain, error: error.message });
          return { ok: false, status: "upsert" };
        }
        return { ok: true, status: "ok" };
      }));
      updated += outcomes.filter((outcome) => outcome.ok).length;
      for (const outcome of outcomes.filter((outcome) => !outcome.ok)) {
        failed++;
        const status = String(outcome.status);
        failureStatuses[status] = (failureStatuses[status] ?? 0) + 1;
      }
    }
  } else {
    console.log("AUTHORITY_SYNC_SKIPPED_NO_TOKEN");
  }

  console.log("AUTHORITY_SYNC_FINISHED", { domains: domains.length, updated, failed, failureStatuses });

  return new Response(
    JSON.stringify({ ok: true, domains: domains.length, updated, failed, failure_statuses: failureStatuses, remaining: Math.max(0, pendingDomains.length - domains.length), hasAhrefs: !!AHREFS }),
    { headers: { ...corsHeaders, "Content-Type": "application/json" } }
  );
});
