import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import type { SavedWebContact } from "@/lib/savedWebContacts";

export type JournalistList = {
  id: string;
  name: string;
  created_at: string;
  is_disabled: boolean | null;
  user_id: string | null;
};

export type ListItem = {
  id: number;
  connected_journalist_list: string;
  connected_journalist: number | null;
  connected_creator: number | null;
  created_at: string;
};

export type SavedWebListItem = {
  id: number;
  list_id: string;
  created_at: string;
  saved_web_contacts: SavedWebContact | null;
};

export const useLists = (userId: string | undefined) =>
  useQuery({
    queryKey: ["lists", userId],
    enabled: !!userId,
    queryFn: async () => {
      const { data, error } = await supabase
        .from("journalist_list")
        .select("id,name,created_at,is_disabled,user_id")
        .eq("user_id", userId!)
        .or("is_disabled.is.null,is_disabled.eq.false")
        .order("created_at", { ascending: false });
      if (error) throw error;
      return (data ?? []) as JournalistList[];
    },
  });

export const useListItems = (listId: string | undefined) =>
  useQuery({
    queryKey: ["list-items", listId],
    enabled: !!listId,
    queryFn: async () => {
      const { data, error } = await supabase
        .from("journalist_list_items")
        .select("id,connected_journalist_list,connected_journalist,connected_creator,created_at")
        .eq("connected_journalist_list", listId!)
        .order("created_at", { ascending: false });
      if (error) throw error;
      return (data ?? []) as ListItem[];
    },
  });

export const useSavedWebListItems = (listId: string | undefined) =>
  useQuery({
    queryKey: ["saved-web-list-items", listId],
    enabled: !!listId,
    queryFn: async () => {
      const { data, error } = await supabase
        .from("saved_web_list_items")
        .select("id,list_id,created_at,saved_web_contacts(*)")
        .eq("list_id", listId!)
        .order("created_at", { ascending: false });
      if (error) throw error;
      return (data ?? []) as SavedWebListItem[];
    },
  });

export const useCreateList = (userId: string | undefined) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) => {
      if (!userId) throw new Error("Not signed in");
      const { data, error } = await supabase
        .from("journalist_list")
        .insert({ name, user_id: userId, is_disabled: false })
        .select()
        .single();
      if (error) throw error;
      return data as JournalistList;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["lists", userId] }),
  });
};

export const useAddToList = (userId: string | undefined) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { listId: string; journalistId?: number; creatorId?: number }) => {
      const row: Record<string, unknown> = { connected_journalist_list: args.listId };
      if (args.journalistId) row.connected_journalist = args.journalistId;
      if (args.creatorId) row.connected_creator = args.creatorId;
      const { error } = await supabase.from("journalist_list_items").insert(row);
      if (error) throw error;

      // Auto-trigger email enrichment if the journalist has no email yet.
      if (args.journalistId) {
        try {
          const { data: j } = await supabase
            .from("journalist")
            .select("id,name,outlet,email")
            .eq("id", args.journalistId)
            .maybeSingle();
          if (j && !j.email && j.name && j.outlet) {
            // Fire and forget — don't block list-add UX on enrichment latency.
            supabase.functions.invoke("enrich-contact", {
              body: {
                source_table: "journalist",
                source_id: j.id,
                name: j.name,
                outlet: j.outlet,
                fields: ["email"],
              },
            }).catch(() => {});
          }
        } catch {
          // Non-fatal — list membership already succeeded.
        }
      }
    },
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: ["list-items", vars.listId] });
      qc.invalidateQueries({ queryKey: ["lists", userId] });
    },
  });
};

export const useBulkAddToList = (userId: string | undefined) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { listId: string; journalistIds?: number[]; creatorIds?: number[]; savedWebContactIds?: string[] }) => {
      const rows: Record<string, unknown>[] = [];
      for (const jid of args.journalistIds ?? []) {
        rows.push({ connected_journalist_list: args.listId, connected_journalist: jid });
      }
      for (const cid of args.creatorIds ?? []) {
        rows.push({ connected_journalist_list: args.listId, connected_creator: cid });
      }
      if (rows.length) {
        const { error } = await supabase.from("journalist_list_items").insert(rows);
        if (error) throw error;
      }

      const savedWebRows = (args.savedWebContactIds ?? []).map((saved_web_contact_id) => ({
        list_id: args.listId,
        saved_web_contact_id,
      }));
      if (savedWebRows.length) {
        const { error } = await supabase.from("saved_web_list_items").upsert(savedWebRows, {
          onConflict: "list_id,saved_web_contact_id",
          ignoreDuplicates: true,
        });
        if (error) throw error;
      }
      if (!rows.length && !savedWebRows.length) return { added: 0 };

      // Fire-and-forget enrichment for journalists missing emails.
      if (args.journalistIds?.length) {
        try {
          const { data: js } = await supabase
            .from("journalist")
            .select("id,name,outlet,email")
            .in("id", args.journalistIds);
          for (const j of js ?? []) {
            if (j && !j.email && j.name && j.outlet) {
              supabase.functions.invoke("enrich-contact", {
                body: {
                  source_table: "journalist",
                  source_id: j.id,
                  name: j.name,
                  outlet: j.outlet,
                  fields: ["email"],
                },
              }).catch(() => {});
            }
          }
        } catch {
          // non-fatal
        }
      }
      return { added: rows.length + savedWebRows.length };
    },
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: ["list-items", vars.listId] });
      qc.invalidateQueries({ queryKey: ["saved-web-list-items", vars.listId] });
      qc.invalidateQueries({ queryKey: ["lists", userId] });
    },
  });
};

export const useRemoveFromList = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (itemId: number) => {
      const { error } = await supabase.from("journalist_list_items").delete().eq("id", itemId);
      if (error) throw error;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["list-items"] }),
  });
};

export const useRemoveSavedWebFromList = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (itemId: number) => {
      const { error } = await supabase.from("saved_web_list_items").delete().eq("id", itemId);
      if (error) throw error;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["saved-web-list-items"] }),
  });
};

export const useDeleteList = (userId: string | undefined) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (listId: string) => {
      const { error } = await supabase
        .from("journalist_list")
        .update({ is_disabled: true })
        .eq("id", listId);
      if (error) throw error;
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["lists", userId] }),
  });
};
