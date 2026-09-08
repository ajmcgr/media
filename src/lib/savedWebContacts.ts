import { supabase } from "@/integrations/supabase/client";

export type WebContactInput = {
  name?: string | null;
  outlet?: string | null;
  title?: string | null;
  category?: string | null;
  country?: string | null;
  email?: string | null;
  ig_handle?: string | null;
  youtube_url?: string | null;
  linkedin_url?: string | null;
  xhandle?: string | null;
  source_url?: string | null;
};

export type SavedWebContact = WebContactInput & {
  id: string;
  user_id: string;
  kind: "journalist" | "creator";
  source_key: string;
  created_at: string;
};

const text = (value: string | null | undefined) => String(value ?? "").trim();

export function webContactSourceKey(row: WebContactInput) {
  return [text(row.source_url), text(row.email), text(row.name), text(row.outlet), text(row.title)]
    .filter(Boolean)
    .join("|")
    .toLowerCase();
}

export async function saveWebContact(
  userId: string,
  kind: "journalists" | "creators",
  row: WebContactInput,
) {
  const name = text(row.name);
  const sourceKey = webContactSourceKey(row);
  if (!name || !sourceKey) throw new Error("This result is missing the details needed to save it.");

  const payload = {
    user_id: userId,
    kind: kind === "journalists" ? "journalist" : "creator",
    name,
    outlet: text(row.outlet) || null,
    title: text(row.title) || null,
    category: text(row.category) || null,
    country: text(row.country) || null,
    email: text(row.email) || null,
    ig_handle: text(row.ig_handle) || null,
    youtube_url: text(row.youtube_url) || null,
    linkedin_url: text(row.linkedin_url) || null,
    xhandle: text(row.xhandle) || null,
    source_url: text(row.source_url) || null,
    source_key: sourceKey,
  };

  const { data, error } = await supabase
    .from("saved_web_contacts")
    .upsert(payload, { onConflict: "user_id,kind,source_key" })
    .select("id")
    .single();
  if (error) throw error;
  return data.id as string;
}
