import { useState } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useLists, useBulkAddToList, useCreateList } from "@/hooks/useLists";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuTrigger, DropdownMenuLabel, DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { Plus, Loader2, X, ListPlus } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "@/hooks/use-toast";
import { trackEvent, trackOncePerSession } from "@/lib/analytics";

type Props = {
  count: number;
  journalistIds?: number[];
  creatorIds?: number[];
  onClear: () => void;
  /** Resolve private web discoveries before attaching them to a list. */
  resolveExtraIds?: () => Promise<{ journalistIds?: number[]; creatorIds?: number[]; savedWebContactIds?: string[] }>;
};

export const BulkAddToListBar = ({ count, journalistIds, creatorIds, onClear, resolveExtraIds }: Props) => {
  const { user } = useAuth();
  const lists = useLists(user?.id);
  const bulk = useBulkAddToList(user?.id);
  const createList = useCreateList(user?.id);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [resolving, setResolving] = useState(false);

  if (count === 0) return null;

  const collectIds = async () => {
    let jIds = [...(journalistIds ?? [])];
    let cIds = [...(creatorIds ?? [])];
    if (resolveExtraIds) {
      setResolving(true);
      try {
        const extra = await resolveExtraIds();
      if (extra.journalistIds?.length) jIds = jIds.concat(extra.journalistIds);
      if (extra.creatorIds?.length) cIds = cIds.concat(extra.creatorIds);
      return { journalistIds: jIds, creatorIds: cIds, savedWebContactIds: extra.savedWebContactIds ?? [] };
      } finally { setResolving(false); }
    }
    return { journalistIds: jIds, creatorIds: cIds, savedWebContactIds: [] };
  };

  const handleAdd = async (listId: string, listName: string) => {
    try {
      const ids = await collectIds();
      const res = await bulk.mutateAsync({ listId, ...ids });
      trackEvent("contacts_added_to_list", { count: res.added, source: "bulk_selection" });
      if (res.added > 0) {
        trackOncePerSession("activation_completed", {
          milestone: "contacts_saved_to_list",
          count: res.added,
          source: "bulk_selection",
        }, `activation_completed.${user?.id ?? "anonymous"}`);
      }
      toast({ title: `Added ${res.added} to ${listName}` });
      onClear();
    } catch (e) {
      toast({ title: "Couldn't add", description: (e as Error).message, variant: "destructive" });
    }
  };

  const handleCreate = async () => {
    const n = name.trim();
    if (!n) return;
    try {
      const list = await createList.mutateAsync(n);
      const ids = await collectIds();
      const res = await bulk.mutateAsync({ listId: list.id, ...ids });
      trackEvent("contacts_added_to_list", { count: res.added, source: "new_list" });
      if (res.added > 0) {
        trackOncePerSession("activation_completed", {
          milestone: "contacts_saved_to_list",
          count: res.added,
          source: "new_list",
        }, `activation_completed.${user?.id ?? "anonymous"}`);
      }
      toast({ title: `Added ${res.added} to ${n}` });
      setName(""); setCreating(false);
      onClear();
    } catch (e) {
      toast({ title: "Couldn't create", description: (e as Error).message, variant: "destructive" });
    }
  };

  return (
    <div className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50 bg-foreground text-background rounded-full shadow-lg pl-4 pr-1.5 py-1.5 flex items-center gap-3 text-sm">
      <span className="font-medium">{count} selected</span>
      <DropdownMenu onOpenChange={(o) => !o && setCreating(false)}>
        <DropdownMenuTrigger asChild>
          <Button size="sm" variant="secondary" className="h-8 gap-1.5 rounded-full" disabled={bulk.isPending || resolving}>
            {(bulk.isPending || resolving) ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ListPlus className="h-3.5 w-3.5" />}
            Add to list
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="center" side="top" className="w-64">
          <DropdownMenuLabel className="text-xs">Add {count} to list</DropdownMenuLabel>
          <DropdownMenuSeparator />
          {lists.isLoading ? (
            <div className="px-2 py-2 text-xs text-muted-foreground">Loading…</div>
          ) : (lists.data?.length ?? 0) === 0 && !creating ? (
            <div className="px-2 py-2 text-xs text-muted-foreground">No lists yet.</div>
          ) : (
            lists.data?.map((l) => (
              <DropdownMenuItem key={l.id} onSelect={() => handleAdd(l.id, l.name)} className="text-sm">
                {l.name}
              </DropdownMenuItem>
            ))
          )}
          <DropdownMenuSeparator />
          {creating ? (
            <div className="p-2 flex gap-1.5">
              <Input autoFocus value={name} onChange={(e) => setName(e.target.value)}
                placeholder="List name" className="h-8 text-sm"
                onKeyDown={(e) => e.key === "Enter" && handleCreate()} />
              <Button size="sm" className="h-8 px-2" onClick={handleCreate} disabled={!name.trim() || createList.isPending}>
                {createList.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : "Add"}
              </Button>
            </div>
          ) : (
            <DropdownMenuItem onSelect={(e) => { e.preventDefault(); setCreating(true); }} className="text-sm gap-1.5">
              <Plus className="h-3.5 w-3.5" />New list…
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
      <button
        type="button"
        onClick={onClear}
        className="h-8 w-8 inline-flex items-center justify-center rounded-full hover:bg-background/10"
        aria-label="Clear selection"
      >
        <X className="h-4 w-4" />
      </button>
    </div>
  );
};
