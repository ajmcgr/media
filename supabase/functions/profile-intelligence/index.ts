import { createClient } from "npm:@supabase/supabase-js@2.45.4";
export type ContactKind = "journalist" | "creator";

export type CoverageFact = {
  id: string;
  headline: string;
  canonical_url: string;
  outlet?: string | null;
  published_at?: string | null;
  summary?: string | null;
  topics?: string[] | null;
};

export type IntelligenceStatus = "ready" | "generating" | "insufficient_evidence" | "failed";
export type Confidence = "high" | "medium" | "low";

export type GeneratedIntelligence = {
  profile_summary?: string;
  primary_topics?: unknown;
  secondary_topics?: unknown;
  recent_coverage_focus?: unknown;
  typical_content_formats?: unknown;
  audience_signals?: unknown;
  writing_signals?: unknown;
  geographic_relevance?: unknown;
  publication_focus?: unknown;
  topic_trends?: unknown;
  pitch_guidance?: unknown;
};

const GENERATION_VERSION = "profile-intelligence-v1";
const MAX_LIST_ITEMS = 8;

function compact(value: unknown) {
  return typeof value === "string" ? value.trim().replace(/\s+/g, " ") : "";
}

export function sanitizeText(value: unknown, maxLength = 1200) {
  const text = compact(value);
  return text.length > maxLength ? `${text.slice(0, maxLength - 1).trim()}...` : text;
}

export function sanitizeStringArray(value: unknown, maxItems = MAX_LIST_ITEMS, maxLength = 96) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => sanitizeText(item, maxLength)).filter(Boolean))].slice(0, maxItems);
}

export function confidenceFromCoverage(coverage: CoverageFact[]): Confidence {
  const summarized = coverage.filter((item) => sanitizeText(item.summary, 160).length > 0).length;
  if (coverage.length >= 5 && summarized >= 3) return "high";
  if (coverage.length >= 2 || summarized >= 1) return "medium";
  return "low";
}

export function evidenceFromCoverage(coverage: CoverageFact[]) {
  return coverage.slice(0, 8).map((item) => ({
    source_type: "coverage",
    coverage_id: item.id,
    headline: sanitizeText(item.headline, 180),
    canonical_url: item.canonical_url,
    outlet: sanitizeText(item.outlet, 120) || null,
    published_at: item.published_at ?? null,
    has_summary: Boolean(sanitizeText(item.summary, 80)),
  }));
}

export function normalizeGeneratedIntelligence(input: GeneratedIntelligence) {
  const pitch = typeof input.pitch_guidance === "object" && input.pitch_guidance !== null
    ? input.pitch_guidance as Record<string, unknown>
    : {};

  return {
    profile_summary: sanitizeText(input.profile_summary, 900),
    primary_topics: sanitizeStringArray(input.primary_topics, 6),
    secondary_topics: sanitizeStringArray(input.secondary_topics, 8),
    recent_coverage_focus: sanitizeStringArray(input.recent_coverage_focus, 8),
    typical_content_formats: sanitizeStringArray(input.typical_content_formats, 6),
    audience_signals: sanitizeStringArray(input.audience_signals, 6),
    writing_signals: sanitizeStringArray(input.writing_signals, 6),
    geographic_relevance: sanitizeStringArray(input.geographic_relevance, 5),
    publication_focus: sanitizeStringArray(input.publication_focus, 6),
    topic_trends: Array.isArray(input.topic_trends)
      ? input.topic_trends.slice(0, 6).map((trend) => {
        const row = typeof trend === "object" && trend !== null ? trend as Record<string, unknown> : {};
        return {
          topic: sanitizeText(row.topic, 96),
          direction: sanitizeText(row.direction, 80),
          evidence: sanitizeText(row.evidence, 180),
        };
      }).filter((trend) => trend.topic)
      : [],
    pitch_guidance: {
      recommended_angle: sanitizeText(pitch.recommended_angle, 260),
      likely_interest: sanitizeText(pitch.likely_interest, 260),
      suggested_tone: sanitizeText(pitch.suggested_tone, 180),
      evidence_to_include: sanitizeStringArray(pitch.evidence_to_include, 5, 140),
      avoid: sanitizeStringArray(pitch.avoid, 5, 140),
    },
  };
}

export function hasEnoughEvidence(coverage: CoverageFact[]) {
  return coverage.some((item) => sanitizeText(item.headline, 20) && item.canonical_url?.startsWith("http"));
}

export function fingerprintSource(contactKind: ContactKind, contact: Record<string, unknown>, coverage: CoverageFact[]) {
  return JSON.stringify({
    generation_version: GENERATION_VERSION,
    contact_kind: contactKind,
    contact: {
      id: contact.id,
      name: contact.name,
      outlet: contact.outlet,
      titles: contact.titles,
      category: contact.category,
      topics: contact.topics,
      bio: contact.bio,
      country: contact.country,
      type: contact.type,
    },
    coverage: coverage.map((item) => ({
      id: item.id,
      headline: item.headline,
      canonical_url: item.canonical_url,
      outlet: item.outlet,
      published_at: item.published_at,
      summary: item.summary,
      topics: item.topics,
    })),
  });
}

export { GENERATION_VERSION };

type AdminClient = ReturnType<typeof createClient>;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const MODEL = "gpt-4o-mini";
const DAILY_GENERATION_LIMIT = 25;

class HttpError extends Error {
  constructor(message: string, readonly status: number) {
    super(message);
  }
}

function json(body: unknown, init: ResponseInit = {}) {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: { "Content-Type": "application/json", ...corsHeaders, ...(init.headers ?? {}) },
  });
}

async function sha256(value: string) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function parseBody(value: unknown): { contact_kind: ContactKind; contact_id: number; refresh: boolean } {
  const body = typeof value === "object" && value !== null ? value as Record<string, unknown> : {};
  const contactKind = body.contact_kind === "journalist" || body.contact_kind === "creator" ? body.contact_kind : null;
  const contactId = Number(body.contact_id);
  if (!contactKind || !Number.isInteger(contactId) || contactId < 1) {
    throw new Error("Invalid contact_kind or contact_id.");
  }
  return { contact_kind: contactKind, contact_id: contactId, refresh: body.refresh === true };
}

async function getAuthenticatedUser(req: Request) {
  const authHeader = req.headers.get("Authorization") ?? "";
  if (!authHeader.startsWith("Bearer ")) return null;

  const userClient = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_ANON_KEY")!,
    { global: { headers: { Authorization: authHeader } } },
  );
  const { data, error } = await userClient.auth.getUser();
  if (error || !data.user) return null;
  return data.user;
}

async function requireGenerationAccess(admin: AdminClient, userId: string) {
  const [{ data: profile, error: profileError }, { data: role, error: roleError }] = await Promise.all([
    admin.from("profiles").select("sub_active").eq("id", userId).maybeSingle(),
    admin.from("user_roles").select("role").eq("user_id", userId).eq("role", "admin").maybeSingle(),
  ]);
  if (profileError || roleError) throw new HttpError("Unable to verify generation access.", 503);
  if (role) return;
  if (!profile?.sub_active) throw new HttpError("An active plan is required to generate AI profiles.", 403);

  const { data: allowed, error: limitError } = await admin.rpc("consume_profile_intelligence_generation", {
    _user: userId,
    _daily_limit: DAILY_GENERATION_LIMIT,
  });
  if (limitError) throw new HttpError("Profile generation is temporarily unavailable.", 503);
  if (!allowed) throw new HttpError("Daily AI profile generation limit reached.", 429);
}

async function loadContact(admin: AdminClient, contactKind: ContactKind, contactId: number) {
  const table = contactKind === "journalist" ? "journalist" : "creators";
  const { data, error } = await admin
    .from(table)
    .select("*")
    .eq("id", contactId)
    .maybeSingle();

  if (error) throw new Error(`Could not load ${contactKind}.`);
  if (!data) throw new Error("Contact not found.");
  return data as Record<string, unknown>;
}

async function loadCoverage(admin: AdminClient, contactKind: ContactKind, contactId: number) {
  const { data, error } = await admin
    .from("contact_coverage")
    .select("id,headline,canonical_url,outlet,published_at,summary,topics")
    .eq("contact_kind", contactKind)
    .eq("contact_id", contactId)
    .order("published_at", { ascending: false, nullsFirst: false })
    .limit(8);

  if (error) throw new Error("Could not load contact coverage.");
  return (data ?? []) as CoverageFact[];
}

function termSet(...values: unknown[]) {
  const terms = new Set<string>();
  for (const value of values) {
    const text = Array.isArray(value) ? value.join(",") : String(value ?? "");
    for (const part of text.toLowerCase().split(/[^a-z0-9]+/)) {
      if (part.length >= 3) terms.add(part);
    }
  }
  return terms;
}

function overlapScore(base: Set<string>, candidate: Record<string, unknown>) {
  const candidateTerms = termSet(candidate.topics, candidate.category, candidate.titles, candidate.bio, candidate.type, candidate.outlet);
  let shared = 0;
  for (const term of base) if (candidateTerms.has(term)) shared += 1;
  return shared;
}

async function findSimilarContacts(admin: AdminClient, contactKind: ContactKind, contact: Record<string, unknown>) {
  const table = contactKind === "journalist" ? "journalist" : "creators";
  const baseTerms = termSet(contact.topics, contact.category, contact.titles, contact.bio, contact.type, contact.outlet);
  const category = sanitizeText(contact.category, 80);
  const outlet = sanitizeText(contact.outlet, 100);
  const country = sanitizeText(contact.country, 80);

  let query = admin.from(table).select("id,name,outlet,titles,topics,category,country,type,bio").neq("id", contact.id).limit(120);
  if (category) query = query.ilike("category", `%${category.replace(/[%_]/g, "")}%`);

  const { data, error } = await query;
  if (error) return [];

  return ((data ?? []) as Array<Record<string, unknown>>)
    .map((candidate) => {
      const shared = overlapScore(baseTerms, candidate);
      const sameOutlet = outlet && sanitizeText(candidate.outlet, 100).toLowerCase() === outlet.toLowerCase();
      const sameCountry = country && sanitizeText(candidate.country, 80).toLowerCase() === country.toLowerCase();
      const score = shared + (sameOutlet ? 3 : 0) + (sameCountry ? 1 : 0);
      const reasons = [
        shared > 0 ? `Shares ${shared} topic signal${shared === 1 ? "" : "s"}` : "",
        sameOutlet ? "same outlet" : "",
        sameCountry ? "same country" : "",
      ].filter(Boolean);
      return {
        kind: contactKind,
        id: candidate.id,
        name: sanitizeText(candidate.name, 120) || "Unnamed contact",
        outlet: sanitizeText(candidate.outlet ?? candidate.category, 140) || null,
        reason: reasons.join(", "),
        score,
      };
    })
    .filter((candidate) => candidate.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5)
    .map(({ score: _score, ...candidate }) => candidate);
}

function buildPrompt(contactKind: ContactKind, contact: Record<string, unknown>, coverage: CoverageFact[]) {
  const facts = {
    contact_kind: contactKind,
    contact: {
      name: contact.name,
      outlet: contact.outlet,
      titles: contact.titles,
      category: contact.category,
      topics: contact.topics,
      bio: contact.bio,
      country: contact.country,
      type: contact.type,
    },
    verified_coverage: coverage.map((item) => ({
      id: item.id,
      headline: item.headline,
      outlet: item.outlet,
      published_at: item.published_at,
      summary: item.summary,
      topics: item.topics,
      canonical_url: item.canonical_url,
    })),
  };

  return [
    {
      role: "system",
      content:
        "You create evidence-grounded media outreach intelligence. Use only the provided contact facts and verified coverage. Do not infer private beliefs, demographics, motives, or unprovided facts. If evidence is weak, use cautious language. Return only valid JSON.",
    },
    {
      role: "user",
      content: `Analyze this media contact for earned-media outreach. Return JSON with keys: profile_summary, primary_topics, secondary_topics, recent_coverage_focus, typical_content_formats, audience_signals, writing_signals, geographic_relevance, publication_focus, topic_trends, pitch_guidance. pitch_guidance must include recommended_angle, likely_interest, suggested_tone, evidence_to_include, avoid. topic_trends must be an array of {topic,direction,evidence}. Facts:\n${JSON.stringify(facts, null, 2)}`,
    },
  ];
}

async function generateWithOpenAI(contactKind: ContactKind, contact: Record<string, unknown>, coverage: CoverageFact[]) {
  const apiKey = Deno.env.get("OPENAI_API_KEY");
  if (!apiKey) throw new Error("OPENAI_API_KEY is not configured.");

  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: MODEL,
      temperature: 0.2,
      response_format: { type: "json_object" },
      messages: buildPrompt(contactKind, contact, coverage),
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`OpenAI request failed: ${text.slice(0, 180)}`);
  }

  const payload = await response.json();
  const content = payload?.choices?.[0]?.message?.content;
  if (typeof content !== "string") throw new Error("OpenAI returned an empty response.");
  return normalizeGeneratedIntelligence(JSON.parse(content));
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ error: "Method not allowed" }, { status: 405 });

  try {
    const user = await getAuthenticatedUser(req);
    if (!user) return json({ error: "Unauthorized" }, { status: 401 });

    const { contact_kind, contact_id, refresh } = parseBody(await req.json().catch(() => null));
    const admin = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);
    const [contact, coverage] = await Promise.all([
      loadContact(admin, contact_kind, contact_id),
      loadCoverage(admin, contact_kind, contact_id),
    ]);

    const sourceFingerprint = await sha256(fingerprintSource(contact_kind, contact, coverage));
    const { data: cached } = await admin
      .from("contact_ai_profiles")
      .select("*")
      .eq("contact_kind", contact_kind)
      .eq("contact_id", contact_id)
      .maybeSingle();

    const isFresh = cached?.source_fingerprint === sourceFingerprint
      && cached?.expires_at
      && new Date(cached.expires_at).getTime() > Date.now();

    if (!refresh && isFresh && ["ready", "insufficient_evidence"].includes(cached.status)) {
      return json({ profile: cached, cached: true });
    }

    const now = new Date();
    if (!hasEnoughEvidence(coverage)) {
      const row = {
        contact_kind,
        contact_id,
        status: "insufficient_evidence",
        source_fingerprint: sourceFingerprint,
        confidence: null,
        profile_summary: null,
        primary_topics: [],
        secondary_topics: [],
        recent_coverage_focus: [],
        typical_content_formats: [],
        audience_signals: [],
        writing_signals: [],
        geographic_relevance: [],
        publication_focus: [],
        topic_trends: [],
        pitch_guidance: {},
        evidence: [],
        similar_contacts: [],
        model: null,
        generation_version: GENERATION_VERSION,
        generated_at: now.toISOString(),
        expires_at: new Date(now.getTime() + 24 * 60 * 60 * 1000).toISOString(),
        error_code: null,
      };
      const { data, error } = await admin.from("contact_ai_profiles").upsert(row, { onConflict: "contact_kind,contact_id" }).select("*").single();
      if (error) throw error;
      return json({ profile: data, cached: false });
    }

    // Only paid users can start a fresh generation; cached profiles remain readable.
    await requireGenerationAccess(admin, user.id);

    await admin.from("contact_ai_profiles").upsert({
      contact_kind,
      contact_id,
      status: "generating",
      source_fingerprint: sourceFingerprint,
      generation_version: GENERATION_VERSION,
    }, { onConflict: "contact_kind,contact_id" });

    const [generated, similarContacts] = await Promise.all([
      generateWithOpenAI(contact_kind, contact, coverage),
      findSimilarContacts(admin, contact_kind, contact),
    ]);

    const row = {
      contact_kind,
      contact_id,
      status: "ready",
      source_fingerprint: sourceFingerprint,
      confidence: confidenceFromCoverage(coverage),
      ...generated,
      evidence: evidenceFromCoverage(coverage),
      similar_contacts: similarContacts,
      model: MODEL,
      generation_version: GENERATION_VERSION,
      generated_at: now.toISOString(),
      expires_at: new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString(),
      error_code: null,
    };
    const { data, error } = await admin.from("contact_ai_profiles").upsert(row, { onConflict: "contact_kind,contact_id" }).select("*").single();
    if (error) throw error;

    return json({ profile: data, cached: false });
  } catch (error) {
    console.error("profile-intelligence error", error);
    return json(
      { error: error instanceof Error ? error.message : "Unable to generate profile intelligence." },
      { status: error instanceof HttpError ? error.status : 400 },
    );
  }
});
