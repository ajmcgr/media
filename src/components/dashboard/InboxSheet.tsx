import { useState } from "react";
import type { ReactNode } from "react";
import { sanitizeEmailHtml } from "@/lib/sanitizeHtml";
import { Inbox as InboxIcon, Mail, Send, Loader2, Plug, X } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Spinner } from "@/components/ui/spinner";
import { toast } from "sonner";
import {
  useNylasStatus, useConnectNylas, useDisconnectNylas,
  useNylasThreads, useNylasThread, useSendEmail,
} from "@/hooks/useNylas";

type View =
  | { kind: "list" }
  | { kind: "thread"; id: string }
  | { kind: "compose"; replyTo?: { messageId: string; to: { email: string; name?: string }[]; subject: string } };

type InboxSheetProps = {
  triggerNode?: ReactNode;
  triggerClassName?: string;
  triggerChildren?: ReactNode;
};

export function InboxSheet({ triggerNode, triggerClassName, triggerChildren }: InboxSheetProps = {}) {
  const [open, setOpen] = useState(false);
  const [view, setView] = useState<View>({ kind: "list" });
  const [search, setSearch] = useState("");

  const status = useNylasStatus();
  const { connect, connecting } = useConnectNylas();
  const disconnect = useDisconnectNylas();

  const connected = !!status.data?.email;
  const threads = useNylasThreads(search || undefined, open && connected);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        {triggerNode ?? (
          <Button
            variant="ghost"
            size="sm"
            className={triggerClassName ?? "font-medium text-sm px-3 py-2 h-auto rounded-md text-gray-700 hover:text-gray-900 hover:bg-gray-100"}
          >
            {triggerChildren ?? "Inbox"}
          </Button>
        )}
      </SheetTrigger>
      <SheetContent side="right" className="w-full sm:max-w-2xl p-0 flex flex-col">
        <SheetHeader className="px-5 py-4 border-b border-border">
          <div className="flex items-center justify-between gap-3">
            <SheetTitle className="text-base font-medium">Inbox</SheetTitle>
            {connected && (
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span className="truncate max-w-[200px]">{status.data?.email}</span>
                <Button variant="ghost" size="sm" className="h-7 text-xs"
                  onClick={() => disconnect.mutate(undefined, { onSuccess: () => toast.success("Disconnected") })}>
                  Disconnect
                </Button>
              </div>
            )}
          </div>
        </SheetHeader>

        {!connected ? (
          <div className="flex-1 flex flex-col items-center justify-center p-10 text-center gap-4">
            <Plug className="h-10 w-10 text-muted-foreground" />
            <div>
              <h3 className="text-base font-medium">Connect your inbox</h3>
              <p className="text-sm text-muted-foreground mt-1 max-w-xs">
                Securely link Gmail, Outlook or any IMAP account via Nylas to read and send from inside Media AI.
              </p>
            </div>
            <Button onClick={() => connect().catch((e) => toast.error(e.message))} disabled={connecting}>
              {connecting ? <><Loader2 className="h-4 w-4 mr-2 animate-spin" />Connecting…</> : "Connect inbox"}
            </Button>
          </div>
        ) : view.kind === "list" ? (
          <>
            <div className="p-3 border-b border-border flex items-center gap-2">
              <Input
                placeholder="Search mail…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="h-9"
              />
              <Button size="sm" className="gap-1.5" onClick={() => setView({ kind: "compose" })}>
                <Mail className="h-3.5 w-3.5" />New
              </Button>
            </div>
            <div className="flex-1 overflow-auto">
              {threads.isLoading ? (
                <div className="p-8 flex justify-center"><Spinner /></div>
              ) : threads.error ? (
                <div className="p-8 text-center text-sm text-destructive">{(threads.error as Error).message}</div>
              ) : (threads.data?.threads.length ?? 0) === 0 ? (
                <div className="p-12 text-center text-sm text-muted-foreground">No threads.</div>
              ) : (
                threads.data!.threads.map((t) => {
                  const from = t.participants?.[0];
                  return (
                    <button key={t.id} type="button" onClick={() => setView({ kind: "thread", id: t.id })}
                      className="w-full text-left px-5 py-3 border-b border-border hover:bg-secondary/40 transition-colors">
                      <div className="flex items-baseline justify-between gap-2">
                        <div className={`text-sm truncate ${t.unread ? "font-semibold" : "font-medium"}`}>
                          {from?.name || from?.email || "Unknown"}
                        </div>
                        {t.latest_message_received_date && (
                          <div className="text-xs text-muted-foreground flex-shrink-0">
                            {formatDistanceToNow(new Date(t.latest_message_received_date * 1000), { addSuffix: true })}
                          </div>
                        )}
                      </div>
                      <div className="text-sm truncate text-foreground/90">{t.subject || "(no subject)"}</div>
                      {t.snippet && <div className="text-xs text-muted-foreground truncate mt-0.5">{t.snippet}</div>}
                    </button>
                  );
                })
              )}
            </div>
          </>
        ) : view.kind === "thread" ? (
          <ThreadView threadId={view.id} onBack={() => setView({ kind: "list" })}
            onReply={(m) => setView({ kind: "compose", replyTo: m })} />
        ) : (
          <Compose replyTo={view.replyTo} onClose={() => setView({ kind: "list" })} />
        )}
      </SheetContent>
    </Sheet>
  );
}

function ThreadView({ threadId, onBack, onReply }: {
  threadId: string;
  onBack: () => void;
  onReply: (m: { messageId: string; to: { email: string; name?: string }[]; subject: string }) => void;
}) {
  const { data, isLoading, error } = useNylasThread(threadId);
  return (
    <div className="flex-1 overflow-auto">
      <div className="px-5 py-3 border-b border-border flex items-center justify-between">
        <Button variant="ghost" size="sm" onClick={onBack}><X className="h-4 w-4 mr-1" />Close</Button>
        {data && (
          <Button size="sm" variant="outline" onClick={() => {
            const last = data.messages[data.messages.length - 1];
            if (!last) return;
            onReply({
              messageId: last.id,
              to: last.from,
              subject: last.subject?.startsWith("Re:") ? last.subject : `Re: ${last.subject ?? ""}`,
            });
          }}>Reply</Button>
        )}
      </div>
      {isLoading ? <div className="p-8 flex justify-center"><Spinner /></div> :
        error ? <div className="p-8 text-center text-sm text-destructive">{(error as Error).message}</div> :
          data && (
            <div className="p-5 space-y-4">
              <h2 className="text-lg font-medium">{data.thread.subject || "(no subject)"}</h2>
              {data.messages.map((m) => (
                <div key={m.id} className="border border-border rounded-lg p-4 bg-white">
                  <div className="flex items-baseline justify-between text-xs text-muted-foreground mb-2">
                    <div className="text-sm text-foreground font-medium">{m.from[0]?.name || m.from[0]?.email}</div>
                    <div>{new Date(m.date * 1000).toLocaleString()}</div>
                  </div>
                  <div className="text-xs text-muted-foreground mb-3">to {m.to.map((t) => t.email).join(", ")}</div>
                  <div className="prose prose-sm max-w-none text-sm" dangerouslySetInnerHTML={{ __html: sanitizeEmailHtml(m.body) }} />
                </div>
              ))}
            </div>
          )}
    </div>
  );
}

function Compose({ replyTo, onClose }: {
  replyTo?: { messageId: string; to: { email: string; name?: string }[]; subject: string };
  onClose: () => void;
}) {
  const [to, setTo] = useState(replyTo?.to.map((t) => t.email).join(", ") ?? "");
  const [subject, setSubject] = useState(replyTo?.subject ?? "");
  const [body, setBody] = useState("");
  const send = useSendEmail();

  const submit = () => {
    const recipients = to.split(",").map((s) => s.trim()).filter(Boolean).map((email) => ({ email }));
    if (!recipients.length) return toast.error("Add at least one recipient");
    send.mutate(
      { to: recipients, subject, body, reply_to_message_id: replyTo?.messageId },
      {
        onSuccess: () => { toast.success("Sent"); onClose(); },
        onError: (e) => toast.error((e as Error).message),
      }
    );
  };

  return (
    <div className="flex-1 flex flex-col">
      <div className="px-5 py-3 border-b border-border flex items-center justify-between">
        <div className="text-sm font-medium">{replyTo ? "Reply" : "New message"}</div>
        <Button variant="ghost" size="sm" onClick={onClose}><X className="h-4 w-4" /></Button>
      </div>
      <div className="p-5 space-y-3 flex-1 flex flex-col">
        <Input placeholder="To (comma-separated)" value={to} onChange={(e) => setTo(e.target.value)} />
        <Input placeholder="Subject" value={subject} onChange={(e) => setSubject(e.target.value)} />
        <Textarea placeholder="Write your message…" value={body} onChange={(e) => setBody(e.target.value)} className="flex-1 min-h-[240px]" />
        <div className="flex justify-end">
          <Button onClick={submit} disabled={send.isPending} className="gap-1.5">
            {send.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Send className="h-3.5 w-3.5" />}
            Send
          </Button>
        </div>
      </div>
    </div>
  );
}
