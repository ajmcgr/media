import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "https://trymedia.ai",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

type AdminAuthorization =
  | { caller: { kind: "internal" } | { kind: "admin"; userId: string } }
  | { error: string; status: number };

// Kept local so this function can be deployed directly from the Supabase Dashboard.
async function requireAdminOrInternal(req: Request): Promise<AdminAuthorization> {
  try {
    const token = (req.headers.get("Authorization") ?? "").replace(/^Bearer\s+/i, "").trim();
    if (!token) return { error: "missing_auth", status: 401 };
    const url = Deno.env.get("SUPABASE_URL")?.trim();
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")?.trim();
    if (!url || !serviceKey) throw new Error("Supabase authorization configuration missing");
    if (token === serviceKey) return { caller: { kind: "internal" } };

    const admin = createClient(url, serviceKey);
    const { data: authData, error: authError } = await admin.auth.getUser(token);
    if (authError || !authData.user) return { error: "invalid_auth", status: 401 };
    const { data: role, error: roleError } = await admin
      .from("user_roles")
      .select("role")
      .eq("user_id", authData.user.id)
      .eq("role", "admin")
      .maybeSingle();
    if (roleError || !role) return { error: "forbidden", status: 403 };
    return { caller: { kind: "admin", userId: authData.user.id } };
  } catch (error) {
    console.error("Exa authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  const json = (body: Record<string, unknown>, status = 200) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });

  const authorization = await requireAdminOrInternal(req);
  if ("error" in authorization) return json({ error: authorization.error }, authorization.status);

  try {
    const EXA_API_KEY = Deno.env.get("EXA_API_KEY");
    console.log("EXA_API_KEY exists:", !!EXA_API_KEY);
    if (!EXA_API_KEY) {
      return json({ results: [], error: "EXA_API_KEY not configured", status: 500 });
    }

    const body = await req.json().catch(() => ({}));
    const query = typeof body?.query === "string" ? body.query.trim().slice(0, 500) : "";
    console.log("EXA_QUERY", query);
    if (!query) {
      return json({ results: [], error: "query is required", status: 400 });
    }

    const exaQuery = `
      site:linkedin.com/in OR site:twitter.com
      ("journalist" OR "reporter" OR "editor")
      ${query}
    `.trim();

    const res = await fetch("https://api.exa.ai/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${EXA_API_KEY}`,
      },
      body: JSON.stringify({
        query: exaQuery,
        numResults: 20,
        contents: { text: { maxCharacters: 400 } },
      }),
    });

    console.log("EXA_STATUS_CODE", res.status);
    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      console.error("EXA_API_ERROR", res.status, data);
      return json({
        results: [],
        error: typeof data?.error === "string" ? data.error : `Exa API returned status ${res.status}`,
        status: res.status,
        details: data,
      });
    }

    const rawCount = Array.isArray(data?.results) ? data.results.length : 0;
    console.log("EXA_RAW_RESULTS", rawCount);

    const results = (data.results || []).map((r: { title?: string; url?: string; text?: string }) => ({
      name: r.title || "",
      url: r.url,
      snippet: (r.text || "").slice(0, 200),
      source: "web",
    }));

    return json({ results, error: null, status: 200 });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    console.error("exa-search error:", message);
    return json({ results: [], error: message, status: 500 });
  }
});
