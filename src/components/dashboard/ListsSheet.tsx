import { useState } from "react";
import type { ReactNode } from "react";
import { useAuth } from "@/contexts/AuthContext";
import {
  Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger,
} from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ListChecks, Plus, Trash2, Download, Loader2 } from "lucide-react";
import {
  useLists, useListItems, useCreateList, useDeleteList, useRemoveFromList,
} from "@/hooks/useLists";
import { supabase } from "@/integrations/supabase/client";
import { toCsv, downloadCsv } from "@/lib/csv";
import { toast } from "@/hooks/use-toast";
import { ShareListDialog } from "./ShareListDialog";
import { trackEvent } from "@/lib/analytics";

type ListsSheetProps = {
  triggerNode?: ReactNode;
  triggerClassName?: string;
  triggerChildren?: ReactNode;
};

export const ListsSheet = ({ triggerNode, triggerClassName, triggerChildren }: ListsSheetProps = {}) => {
  const { user } = useAuth();
  const [open, setOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [activeId, setActiveId] = useState<string | undefined>();

  const lists = useLists(user?.id);
  const items = useListItems(activeId);
  const createList = useCreateList(user?.id);
  const deleteList = useDeleteList(user?.id);
  const removeItem = useRemoveFromList();

  const handleCreate = async () => {
    const name = newName.trim();
    if (!name) return;
    try {
      const list = await createList.mutateAsync(name);
      trackEvent("list_created", { source: "lists_sheet" });
      setNewName("");
      setActiveId(list.id);
    } catch (e) {
      toast({ title: "Couldn't create list", description: (e as Error).message, variant: "destructive" });
    }
  };

  const handleExport = async () => {
    if (!activeId || !items.data) return;
    const journalistIds = items.data.map((i) => i.connected_journalist).filter(Boolean) as number[];
    const creatorIds = items.data.map((i) => i.connected_creator).filter(Boolean) as number[];

    const rows: Record<string, string | number | null>[] = [];
    if (journalistIds.length) {
      const { data } = await supabase.from("journalist")
        .select("name,email,category,titles,topics,xhandle,outlet,country")
        .in("id", journalistIds);
      data?.forEach((d) => rows.push({ type: "journalist", ...d }));
    }
    if (creatorIds.length) {
      const { data } = await supabase.from("creators")
        .select("name,email,category,ig_handle,ig_followers,ig_engagement_rate,youtube_url,youtube_subscribers,type")
        .in("id", creatorIds);
      data?.forEach((d) => rows.push({ type: "creator", ...d }));
    }
    if (!rows.length) {
      toast({ title: "Nothing to export", description: "This list is empty." });
      return;
    }
    const headers = Array.from(new Set(rows.flatMap((r) => Object.keys(r))));
    const filename = `list-${Date.now()}.csv`;
    downloadCsv(filename, toCsv(rows, headers));
    supabase.functions.invoke("send-export-notification", {
      body: { filename, rowCount: rows.length, source: "lists" },
    }).catch(() => {});
  };

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        {triggerNode ?? (
          <Button
            variant="ghost"
            size="sm"
            className={triggerClassName ?? "font-medium text-sm px-3 py-2 h-auto rounded-md text-gray-700 hover:text-gray-900 hover:bg-gray-100"}
          >
            {triggerChildren ?? "Lists"}
          </Button>
        )}
      </SheetTrigger>
      <SheetContent className="w-full sm:max-w-lg flex flex-col">
        <SheetHeader>
          <SheetTitle>Your lists</SheetTitle>
        </SheetHeader>

        <div className="flex gap-2 py-3">
          <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="New list name" />
          <Button onClick={handleCreate} disabled={createList.isPending || !newName.trim()}>
            {createList.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
          </Button>
        </div>

        <div className="border-t border-border -mx-6" />

        <div className="flex-1 overflow-auto">
          {lists.isLoading ? (
            <div className="p-4 text-sm text-muted-foreground">Loading…</div>
          ) : (lists.data?.length ?? 0) === 0 ? (
            <div className="p-4 text-sm text-muted-foreground">No lists yet. Create one above.</div>
          ) : (
            <div className="divide-y divide-border">
              {lists.data!.map((l) => (
                <div key={l.id}>
                  <button
                    type="button"
                    onClick={() => setActiveId(activeId === l.id ? undefined : l.id)}
                    className={`w-full text-left px-3 py-3 flex items-center justify-between hover:bg-secondary/40 ${activeId === l.id ? "bg-secondary/60" : ""}`}
                  >
                    <span className="text-sm font-medium truncate">{l.name}</span>
                    <span className="text-xs text-muted-foreground">{new Date(l.created_at).toLocaleDateString()}</span>
                  </button>
                  {activeId === l.id && (
                    <div className="px-3 pb-3 space-y-2">
                      <div className="flex flex-wrap gap-2">
                        <Button size="sm" variant="outline" className="gap-1.5" onClick={handleExport}>
                          <Download className="h-3.5 w-3.5" />Export CSV
                        </Button>
                        <ShareListDialog listId={l.id} listName={l.name} />
                        <Button size="sm" variant="ghost" className="gap-1.5 text-destructive hover:text-destructive"
                          onClick={() => deleteList.mutate(l.id)}>
                          <Trash2 className="h-3.5 w-3.5" />Delete
                        </Button>
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {items.isLoading ? "Loading…" : `${items.data?.length ?? 0} items`}
                      </div>
                      {items.data?.map((it) => (
                        <div key={it.id} className="flex items-center justify-between text-xs px-2 py-1.5 rounded bg-secondary/40">
                          <span className="truncate">
                            {it.connected_journalist ? `Journalist #${it.connected_journalist}` : `Creator #${it.connected_creator}`}
                          </span>
                          <button onClick={() => removeItem.mutate(it.id)} className="text-muted-foreground hover:text-destructive">
                            <Trash2 className="h-3 w-3" />
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
};
