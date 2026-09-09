import { useState } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { useLists, useAddToList, useCreateList } from "@/hooks/useLists";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuTrigger, DropdownMenuLabel, DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { Plus, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "@/hooks/use-toast";
import { trackEvent, trackOncePerSession } from "@/lib/analytics";

type Props = { journalistId?: number; creatorId?: number };

export const AddToListMenu = ({ journalistId, creatorId }: Props) => {
  const { user } = useAuth();
  const lists = useLists(user?.id);
  const addTo = useAddToList(user?.id);
  const createList = useCreateList(user?.id);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");

  const handleAdd = async (listId: string) => {
    try {
      await addTo.mutateAsync({ listId, journalistId, creatorId });
      trackEvent("contacts_added_to_list", { count: 1, source: "single_contact" });
      trackOncePerSession("activation_completed", {
        milestone: "contacts_saved_to_list",
        count: 1,
        source: "single_contact",
      }, `activation_completed.${user?.id ?? "anonymous"}`);
      toast({ title: "Added to list" });
    } catch (e) {
      toast({ title: "Couldn't add", description: (e as Error).message, variant: "destructive" });
    }
  };

  const handleCreate = async () => {
    const n = name.trim();
    if (!n) return;
    try {
      const list = await createList.mutateAsync(n);
      await addTo.mutateAsync({ listId: list.id, journalistId, creatorId });
      trackEvent("contacts_added_to_list", { count: 1, source: "new_list_single_contact" });
      trackOncePerSession("activation_completed", {
        milestone: "contacts_saved_to_list",
        count: 1,
        source: "new_list_single_contact",
      }, `activation_completed.${user?.id ?? "anonymous"}`);
      setName(""); setCreating(false);
      toast({ title: `Added to ${n}` });
    } catch (e) {
      toast({ title: "Couldn't create", description: (e as Error).message, variant: "destructive" });
    }
  };

  return (
    <DropdownMenu onOpenChange={(o) => !o && setCreating(false)}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="opacity-0 group-hover:opacity-100 focus:opacity-100 transition-opacity inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
          aria-label="Add to list"
        >
          <Plus className="h-3.5 w-3.5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-60">
        <DropdownMenuLabel className="text-xs">Add to list</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {lists.isLoading ? (
          <div className="px-2 py-2 text-xs text-muted-foreground">Loading…</div>
        ) : (lists.data?.length ?? 0) === 0 && !creating ? (
          <div className="px-2 py-2 text-xs text-muted-foreground">No lists yet.</div>
        ) : (
          lists.data?.map((l) => (
            <DropdownMenuItem key={l.id} onSelect={() => handleAdd(l.id)} className="text-sm">
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
  );
};
