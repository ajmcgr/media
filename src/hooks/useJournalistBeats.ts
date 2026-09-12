import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export type JournalistBeatRecord = {
  beat_key: string;
  beat_slug: string;
  total_contacts: number;
  qualified_contacts: number;
};

export type PublicJournalist = {
  id: number;
  name: string | null;
  outlet: string | null;
  titles: string | null;
  topics: string | null;
  country: string | null;
  linkedin_url: string | null;
  xhandle: string | null;
};

export const MINIMUM_QUALIFIED_CONTACTS = 50;

export function useJournalistBeats() {
  return useQuery({
    queryKey: ["public-journalist-beats"],
    queryFn: async (): Promise<JournalistBeatRecord[]> => {
      const { data, error } = await supabase
        .from("public_journalist_beats" as never)
        .select("beat_key,beat_slug,total_contacts,qualified_contacts")
        .gte("qualified_contacts", MINIMUM_QUALIFIED_CONTACTS)
        .order("qualified_contacts", { ascending: false });
      if (error) throw error;
      return (data ?? []) as JournalistBeatRecord[];
    },
  });
}

export function useJournalistBeat(slug?: string) {
  return useQuery({
    queryKey: ["public-journalist-beat", slug],
    enabled: !!slug,
    queryFn: async (): Promise<JournalistBeatRecord | null> => {
      const { data, error } = await supabase
        .from("public_journalist_beats" as never)
        .select("beat_key,beat_slug,total_contacts,qualified_contacts")
        .eq("beat_slug", slug)
        .gte("qualified_contacts", MINIMUM_QUALIFIED_CONTACTS)
        .maybeSingle();
      if (error) throw error;
      return (data as JournalistBeatRecord | null) ?? null;
    },
  });
}

export function usePublicJournalistsForBeat(beatName?: string) {
  return useQuery({
    queryKey: ["public-journalist-beat-contacts", beatName],
    enabled: !!beatName,
    queryFn: async (): Promise<PublicJournalist[]> => {
      const { data, error } = await supabase
        .from("journalist")
        .select("id,name,outlet,titles,topics,country,linkedin_url,xhandle")
        .ilike("category", beatName!)
        .not("name", "is", null)
        .not("outlet", "is", null)
        .order("outlet", { ascending: true })
        .limit(50);
      if (error) throw error;
      return (data ?? []) as PublicJournalist[];
    },
  });
}
