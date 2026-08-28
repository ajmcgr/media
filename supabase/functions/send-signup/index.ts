import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

// ===== Inlined branded email template (kept in-file so dashboard deploys work) =====
const LOGO_URL = "https://trymedia.ai/email-logo-media.png";
const BRAND_BLUE = "#1675e2";
const PAGE_BG = "#f4f6f9";
const CARD_BG = "#ffffff";
const BORDER = "#e6e8ec";
const DIVIDER = "#eef0f3";
const HEADING = "#101214";
const BODY_TEXT = "#4e5052";
const MUTED = "#9aa0a6";

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
const escapeAttr = escapeHtml;

function renderBrandedEmail(opts: BrandedEmailOptions): string {
  const { preheader = "", heading, body, cta, footerNote } = opts;
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /><title>${escapeAttr(heading)}</title></head>
<body style="margin:0;padding:0;background:${PAGE_BG};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#22252a;-webkit-font-smoothing:antialiased;">
${preheader ? `<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">${preheader}</div>` : ""}
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${PAGE_BG};padding:48px 16px;"><tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:${CARD_BG};border-radius:14px;overflow:hidden;border:1px solid ${BORDER};">
<tr><td align="center" style="padding:34px 32px 26px;border-bottom:1px solid ${DIVIDER};"><img src="${LOGO_URL}" alt="Media AI" width="180" style="width:180px;max-width:180px;height:auto;display:inline-block;border:0;outline:none;text-decoration:none;" /></td></tr>
<tr><td style="padding:36px 40px ${cta || footerNote ? "32px" : "40px"};">
<h1 style="margin:0 0 18px;font-size:22px;line-height:1.3;font-weight:700;color:${HEADING};">${heading}</h1>
<div style="font-size:15px;line-height:1.6;color:${BODY_TEXT};margin:0 0 ${cta ? "28px" : "0"};">${body}</div>
${cta ? `<div><a href="${escapeAttr(cta.url)}" style="display:inline-block;background:${BRAND_BLUE};color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:13px 24px;border-radius:8px;">${escapeHtml(cta.label)}</a></div>` : ""}
</td></tr>
${footerNote ? `<tr><td align="center" style="padding:20px 32px 28px;border-top:1px solid ${DIVIDER};"><p style="margin:0;font-size:13px;color:${MUTED};line-height:1.5;">${footerNote}</p></td></tr>` : ""}
</table>
<p style="margin:18px 0 0;font-size:12px;color:${MUTED};">© ${new Date().getFullYear()} Media AI · trymedia.ai</p>
</td></tr></table></body></html>`;
}
// ===== End inlined template =====

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
  "Access-Control-Max-Age": "86400",
};

const FROM_ADDRESS = "Media <hello@trymedia.ai>";
const CONFIRM_REDIRECT_URL = "https://trymedia.ai/auth/confirm";

function safeNext(value: unknown): string {
  if (typeof value !== "string" || !value.startsWith("/") || value.startsWith("//")) return "/search";
  return value;
}

Deno.serve(async (req) => {
  console.log("send-signup invoked", req.method, new Date().toISOString());
  if (req.method === "OPTIONS") {
    return new Response("ok", { status: 200, headers: corsHeaders });
  }

  try {
    const { email, password, displayName, company, next: requestedNext } = await req.json();
    const next = safeNext(requestedNext);

    if (!email || typeof email !== "string") {
      return response({ error: "email is required" }, 400);
    }
    if (!password || typeof password !== "string" || password.length < 8) {
      return response({ error: "password must be at least 8 characters" }, 400);
    }

    const resendApiKey = Deno.env.get("RESEND_API_KEY");
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

    const missingConfig = [
      ["RESEND_API_KEY", resendApiKey],
      ["SUPABASE_URL", supabaseUrl],
      ["SUPABASE_SERVICE_ROLE_KEY", serviceRole],
    ]
      .filter(([, value]) => !value)
      .map(([name]) => name);

    if (missingConfig.length > 0) {
      console.error("send-signup missing configuration:", missingConfig);
      return response({ error: "Server not configured", missing: missingConfig }, 500);
    }

    const admin = createClient(supabaseUrl, serviceRole, {
      auth: { autoRefreshToken: false, persistSession: false },
    });

    const normalizedEmail = email.trim().toLowerCase();

    const { data, error } = await admin.auth.admin.generateLink({
      type: "signup",
      email: normalizedEmail,
      password,
      options: {
        redirectTo: CONFIRM_REDIRECT_URL,
        data: {
          display_name: typeof displayName === "string" ? displayName : "",
          company: typeof company === "string" ? company : "",
        },
      },
    });

    const tokenHash = data?.properties?.hashed_token;

    if (error || !data?.properties?.action_link || !tokenHash) {
      const message = error?.message ?? "Could not create signup link";
      const status = /already registered|already been registered|user already/i.test(message) ? 409 : 400;
      return response({ error: message }, status);
    }

    const actionLink = `https://trymedia.ai/auth/confirm?token_hash=${encodeURIComponent(tokenHash)}&type=signup&next=${encodeURIComponent(next)}`;
    const firstName = typeof displayName === "string" && displayName.trim() ? displayName.trim().split(/\s+/)[0] : "";

    const html = renderBrandedEmail({
      preheader: "Confirm your Media AI account",
      heading: firstName ? `Welcome to Media AI, ${escapeHtml(firstName)}` : "Confirm your account",
      body: `Confirm your email to activate your account and start finding journalists and creators.<br/><br/><span style="font-size:12px;color:#9aa0a6;word-break:break-all;">Or paste this link: ${escapeHtml(actionLink)}</span>`,
      cta: { label: "Confirm email", url: actionLink },
      footerNote: "If you didn't create an account, you can safely ignore this email.",
    });

    const sendRes = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${resendApiKey}`,
      },
      body: JSON.stringify({
        from: FROM_ADDRESS,
        to: [normalizedEmail],
        subject: "Confirm your Media AI account",
        html,
      }),
    });

    if (!sendRes.ok) {
      const bodyText = await sendRes.text();
      console.error(`signup email send failed [${sendRes.status}]: ${bodyText}`);
      return response({ error: "Could not send confirmation email" }, 502);
    }

    return response({ ok: true });
  } catch (e) {
    console.error("send-signup error:", e);
    return response({ error: "Unexpected error" }, 500);
  }
});

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}
