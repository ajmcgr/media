
// ===== Inlined branded email template =====
const _LOGO_URL = "https://trymedia.ai/email-logo-media.png";
const _BRAND_BLUE = "#1675e2";
const _PAGE_BG = "#f4f6f9";
const _CARD_BG = "#ffffff";
const _BORDER = "#e6e8ec";
const _DIVIDER = "#eef0f3";
const _HEADING = "#101214";
const _BODY_TEXT = "#4e5052";
const _MUTED = "#9aa0a6";

interface BrandedEmailOptions {
  preheader?: string;
  heading: string;
  body: string;
  cta?: { label: string; url: string };
  footerNote?: string;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function renderBrandedEmail(opts: BrandedEmailOptions): string {
  const { preheader = "", heading, body, cta, footerNote } = opts;
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /><title>${escapeHtml(heading)}</title></head>
<body style="margin:0;padding:0;background:${_PAGE_BG};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#22252a;-webkit-font-smoothing:antialiased;">
${preheader ? `<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">${preheader}</div>` : ""}
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${_PAGE_BG};padding:48px 16px;"><tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:${_CARD_BG};border-radius:14px;overflow:hidden;border:1px solid ${_BORDER};">
<tr><td align="center" style="padding:34px 32px 26px;border-bottom:1px solid ${_DIVIDER};"><img src="${_LOGO_URL}" alt="Media AI" width="180" style="width:180px;max-width:180px;height:auto;display:inline-block;border:0;outline:none;text-decoration:none;" /></td></tr>
<tr><td style="padding:36px 40px ${cta || footerNote ? "32px" : "40px"};">
<h1 style="margin:0 0 18px;font-size:22px;line-height:1.3;font-weight:700;color:${_HEADING};">${heading}</h1>
<div style="font-size:15px;line-height:1.6;color:${_BODY_TEXT};margin:0 0 ${cta ? "28px" : "0"};">${body}</div>
${cta ? `<div><a href="${escapeHtml(cta.url)}" style="display:inline-block;background:${_BRAND_BLUE};color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:13px 24px;border-radius:8px;">${escapeHtml(cta.label)}</a></div>` : ""}
</td></tr>
${footerNote ? `<tr><td align="center" style="padding:20px 32px 28px;border-top:1px solid ${_DIVIDER};"><p style="margin:0;font-size:13px;color:${_MUTED};line-height:1.5;">${footerNote}</p></td></tr>` : ""}
</table>
<p style="margin:18px 0 0;font-size:12px;color:${_MUTED};">© ${new Date().getFullYear()} Media AI · trymedia.ai</p>
</td></tr></table></body></html>`;
}
// ===== End inlined template =====

// Iterate all active monitors and invoke monitor-check for each.
// Triggered by pg_cron daily. Also sends daily/weekly digest emails.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
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
    console.error("privileged authorization failed", error);
    return { error: "authorization_unavailable", status: 503 };
  }
}

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APP_URL = "https://trymedia.ai/monitor";

async function sendDigest(opts: {
  to: string;
  brand: string;
  frequency: "daily" | "weekly";
  mentions: Array<{ title: string; url: string; publisher: string | null; mention_type: string | null; sentiment: string | null }>;
}) {
  const resendApiKey = Deno.env.get("RESEND_API_KEY")?.trim() ?? "";
  if (!resendApiKey) return false;
  const items = opts.mentions.slice(0, 8).map((m) => `
    <p style="margin:0 0 10px;font-size:14px;line-height:1.45">
      <a href="${m.url}" style="color:#1675e2;text-decoration:none;font-weight:600">${m.title}</a><br/>
      <span style="color:#9aa0a6;font-size:12px">${m.publisher ?? ""} · ${m.mention_type ?? ""} · ${m.sentiment ?? ""}</span>
    </p>
  `).join("");

  const html = renderBrandedEmail({
    preheader: `${opts.frequency === "daily" ? "Daily" : "Weekly"} digest for ${opts.brand}`,
    heading: `${opts.frequency === "daily" ? "Today's" : "This week's"} mentions for ${opts.brand}`,
    body: `
      <p style="margin:0 0 16px">${opts.mentions.length} new mention${opts.mentions.length === 1 ? "" : "s"} from Google News.</p>
      ${items}
    `,
    cta: { label: "Open Keyword Monitor", url: APP_URL },
  });

  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${resendApiKey}`,
    },
    body: JSON.stringify({
      from: "Media AI <hello@trymedia.ai>",
      to: [opts.to],
      subject: `${opts.frequency === "daily" ? "Daily" : "Weekly"} mentions — ${opts.brand}`,
      html,
    }),
  });
  if (r.ok) {
    console.log("MONITOR_EMAIL_SENT", opts.brand, opts.frequency, opts.to);
    return true;
  }
  console.error("digest send failed", r.status, await r.text().catch(() => ""));
  return false;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  try {
    const authorization = await requireAdminOrInternal(req);
    if ("error" in authorization) {
      return new Response(JSON.stringify({ error: authorization.error }), {
        status: authorization.status,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    console.log("MONITOR_SCHEDULED_CHECK_STARTED");
    const supa = createClient(SUPABASE_URL, SERVICE_ROLE);
    const cutoff = new Date(Date.now() - 23 * 60 * 60 * 1000).toISOString();
    const { data: monitors, error } = await supa
      .from("brand_monitors")
      .select("*")
      .eq("is_active", true)
      .or(`last_checked_at.is.null,last_checked_at.lt.${cutoff}`)
      .limit(500);
    if (error) throw error;

    const results: Array<{ id: string; ok: boolean; digest?: boolean }> = [];

    for (const m of monitors ?? []) {
      let ok = false;
      try {
        const r = await fetch(`${SUPABASE_URL}/functions/v1/monitor-check`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${SERVICE_ROLE}`,
          },
          body: JSON.stringify({ monitor_id: m.id }),
        });
        ok = r.ok;
      } catch (e) {
        console.error("invoke failed", m.id, e);
      }

      // digest emails (daily/weekly)
      let digestSent = false;
      if (ok && m.email_alerts && (m.alert_frequency === "daily" || m.alert_frequency === "weekly")) {
        const windowH = m.alert_frequency === "daily" ? 24 : 24 * 7;
        const lastSent = m.digest_last_sent_at ? new Date(m.digest_last_sent_at).getTime() : 0;
        const dueAfter = Date.now() - windowH * 60 * 60 * 1000;
        if (lastSent < dueAfter) {
          const since = new Date(Math.max(lastSent, dueAfter)).toISOString();
          const { data: mentions } = await supa
            .from("monitor_updates")
            .select("title, url, publisher, mention_type, sentiment")
            .eq("monitor_id", m.id)
            .gte("detected_at", since)
            .order("detected_at", { ascending: false })
            .limit(20);
          if ((mentions?.length ?? 0) > 0) {
            const { data: userRow } = await supa.auth.admin.getUserById(m.user_id);
            const userEmail = userRow?.user?.email;
            if (userEmail) {
              digestSent = await sendDigest({
                to: userEmail,
                brand: m.brand_name,
                frequency: m.alert_frequency as "daily" | "weekly",
                mentions: (mentions as Array<{ title: string; url: string; publisher: string | null; mention_type: string | null; sentiment: string | null }>).map((x) => ({ ...x, title: x.title ?? "Mention" })),
              });
              if (digestSent) {
                await supa.from("brand_monitors")
                  .update({ digest_last_sent_at: new Date().toISOString() } as never)
                  .eq("id", m.id);
              }
            }
          }
        }
      }

      results.push({ id: m.id, ok, digest: digestSent });
    }

    console.log("MONITOR_SCHEDULED_CHECK_FINISHED", results.length);
    return new Response(JSON.stringify({ ok: true, count: results.length, results }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("monitor-run-all error", e);
    return new Response(JSON.stringify({ error: "internal_error", details: (e as Error).message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
