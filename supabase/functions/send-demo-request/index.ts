
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

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};



interface DemoRequest {
  name: string;
  email: string;
  company: string;
  role?: string;
  teamSize?: string;
  message?: string;
}

const validate = (body: any): { ok: true; data: DemoRequest } | { ok: false; error: string } => {
  if (!body || typeof body !== "object") return { ok: false, error: "Invalid body" };
  const required = ["name", "email", "company"] as const;
  for (const k of required) {
    if (typeof body[k] !== "string" || !body[k].trim()) {
      return { ok: false, error: `Missing field: ${k}` };
    }
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(body.email)) {
    return { ok: false, error: "Invalid email" };
  }
  const data: DemoRequest = {
    name: String(body.name).slice(0, 200),
    email: String(body.email).slice(0, 200),
    company: String(body.company).slice(0, 200),
    role: body.role ? String(body.role).slice(0, 200) : "",
    teamSize: body.teamSize ? String(body.teamSize).slice(0, 50) : "",
    message: body.message ? String(body.message).slice(0, 5000) : "",
  };
  return { ok: true, data };
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const RESEND_API_KEY = Deno.env.get("RESEND_API_KEY");
    if (!RESEND_API_KEY) throw new Error("RESEND_API_KEY is not configured");

    const reqBody = await req.json().catch(() => null);
    const parsed = validate(reqBody);
    if (parsed.ok === false) {
      return new Response(JSON.stringify({ error: parsed.error }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }
    const { name, email, company, role, teamSize, message } = parsed.data;

    const htmlBody = `
      <p><strong>Name:</strong> ${escapeHtml(name)}</p>
      <p><strong>Email:</strong> ${escapeHtml(email)}</p>
      <p><strong>Company:</strong> ${escapeHtml(company)}</p>
      <p><strong>Role:</strong> ${escapeHtml(role || "—")}</p>
      <p><strong>Team size:</strong> ${escapeHtml(teamSize || "—")}</p>
      <p><strong>Message:</strong></p>
      <p style="white-space:pre-wrap">${escapeHtml(message || "—")}</p>
    `;
    const html = renderBrandedEmail({
      preheader: `Demo request from ${company}`,
      heading: "New demo request",
      body: htmlBody,
    });

    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${RESEND_API_KEY}`,
      },
      body: JSON.stringify({
        from: "Media AI <hello@trymedia.ai>",
        to: ["alex@trylaunch.ai"],
        reply_to: email,
        subject: `Demo request — ${company}`,
        html,
      }),
    });

    const data = await response.json();
    if (!response.ok) {
      console.error("Resend error", response.status, data);
      throw new Error(`Resend API failed [${response.status}]: ${JSON.stringify(data)}`);
    }

    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    console.error("send-demo-request error:", errorMessage);
    return new Response(JSON.stringify({ success: false, error: errorMessage }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
