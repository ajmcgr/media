import React, { useCallback, useEffect, useRef, useState } from "react";
import { Helmet } from "react-helmet-async";
import { NavLink, useNavigate, useParams } from "react-router-dom";
import { ArrowUp, Database, Download, HelpCircle, Inbox as InboxIcon, ListChecks, Loader2, MessageSquare, MoreHorizontal, PanelLeftClose, PanelLeftOpen, Pencil, Pin, PinOff, Plus, Radar, Search as SearchIcon, Settings, Sparkles, ThumbsDown, ThumbsUp, Trash2, Zap } from "lucide-react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useAuth } from "@/contexts/AuthContext";
import { supabase } from "@/integrations/supabase/client";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { InboxSheet } from "@/components/dashboard/InboxSheet";
import { ListsSheet } from "@/components/dashboard/ListsSheet";
import { AddToListMenu } from "@/components/dashboard/AddToListMenu";
import { BulkAddToListBar } from "@/components/dashboard/BulkAddToListBar";
import { Checkbox } from "@/components/ui/checkbox";
import {
  useSavedSearches, useUpsertSavedSearch,
  useTogglePinSavedSearch, useDeleteSavedSearch, useRenameSavedSearch,
} from "@/hooks/useSavedSearches";
import { toCsv, downloadCsv } from "@/lib/csv";

import { useChatUsage } from "@/hooks/useChatUsage";
import { Link } from "react-router-dom";
import { toast } from "sonner";
import { confirmTopup, startTopup, type TopupPack } from "@/lib/billing";
import { useOutletAuthorities, resolveAuthority } from "@/hooks/useOutletAuthority";
import { AuthorityBadge } from "@/components/dashboard/AuthorityBadge";
import OnboardingChecklist, { markFirstSearchComplete } from "@/components/OnboardingChecklist";
import {
  useChatThreads, useCreateChatThread, useUpdateChatThread, useDeleteChatThread,
  fetchChatThread, deriveThreadTitle,
} from "@/hooks/useChatThreads";
import AppHeader from "@/components/AppHeader";
import { ContactProfileSheet } from "@/components/search/ContactProfileSheet";
import { MatchExplanationPopover } from "@/components/search/MatchExplanation";
import { useSubscription } from "@/hooks/useSubscription";
import { isGrowthPlanIdentifier } from "@/lib/plans";
import { cn } from "@/lib/utils";
import { trackEvent } from "@/lib/analytics";

type Msg = { role: "user" | "assistant"; content: string; ts?: string };

function formatMsgTime(ts?: string) {
  const d = ts ? new Date(ts) : new Date();
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  const time = d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  if (sameDay) return time;
  return `${d.toLocaleDateString([], { month: "short", day: "numeric" })} · ${time}`;
}

type Row = {
  source: "database" | "exa";
  source_id?: number | string;
  source_url?: string;
  source_table?: "journalist" | "creators";
  name: string | null;
  outlet: string | null;
  title: string | null;
  category: string | null;
  topics?: string | null;
  display_topic?: string | null;
  country: string | null;
  email: string | null;
  ig_handle?: string | null;
  ig_followers?: number | null;
  youtube_url?: string | null;
  linkedin_url?: string | null;
  xhandle?: string | null;
  reason?: string;
  match_score?: number;
};
type Pagination = { limit: number; offset: number; total_estimated: number; has_more: boolean; returned?: number; next_offset?: number | null };
type Results =
  | { kind: "journalists" | "creators"; rows: Row[]; query?: string; intent?: { count?: number } | null; debug?: Record<string, unknown> | null; pagination?: Pagination | null; sources?: { database: number; web: number } | null }
  | null;

const JOURNALIST_COLS: { key: keyof Row | "authority"; label: string }[] = [
  { key: "name", label: "Name" },
  { key: "title", label: "Title" },
  { key: "outlet", label: "Outlet" },
  { key: "authority", label: "Authority" },
  { key: "category", label: "Topic" },
  { key: "country", label: "Country" },
  { key: "email", label: "Email" },
  { key: "linkedin_url", label: "LinkedIn" },
  { key: "xhandle", label: "X" },
];
const CREATOR_COLS: { key: keyof Row | "authority"; label: string }[] = [
  { key: "name", label: "Name" },
  { key: "ig_handle", label: "Instagram" },
  { key: "youtube_url", label: "YouTube" },
  { key: "ig_followers", label: "Followers" },
  { key: "category", label: "Topic" },
  { key: "email", label: "Email" },
  { key: "linkedin_url", label: "LinkedIn" },
];

const MIN_CHAT_RESULTS = 25;
const AUTO_PERSIST_WEB_ROWS = 12;
const SEARCH_PAGE_SIZE = 50;
const CHAT_STOPWORDS = new Set([
  "a", "an", "and", "are", "at", "based", "best", "by", "find", "for", "from",
  "get", "give", "help", "i", "i'm", "im", "in", "is", "journalist", "journalists",
  "list", "looking", "me", "need", "of", "on", "or", "please", "reporter", "reporters",
  "search", "show", "that", "the", "to", "want", "who", "with",
  "about", "cover", "covers", "covering", "write", "writes", "writing",
]);

const COUNTRY_FALLBACK_ALIASES: Array<{ canonical: string; terms: string[] }> = [
  { canonical: "united kingdom", terms: ["united kingdom", "uk", "britain", "british", "england", "london"] },
  { canonical: "united states", terms: ["united states", "usa", "us", "america", "american"] },
  { canonical: "singapore", terms: ["singapore", "sg"] },
  { canonical: "united arab emirates", terms: ["united arab emirates", "uae", "dubai", "abu dhabi"] },
  { canonical: "canada", terms: ["canada", "canadian"] },
  { canonical: "australia", terms: ["australia", "australian"] },
  { canonical: "india", terms: ["india", "indian"] },
];

const TOPIC_FALLBACK_ALIASES: Array<{ trigger: string; terms: string[] }> = [
  { trigger: "tech", terms: ["tech", "technology", "software", "saas", "startup", "innovation"] },
  { trigger: "ai", terms: ["ai", "artificial intelligence", "machine learning", "technology", "tech"] },
  { trigger: "fintech", terms: ["fintech", "finance", "financial", "banking", "payments"] },
  { trigger: "health", terms: ["health", "healthcare", "medical", "wellness", "fitness"] },
  { trigger: "real estate", terms: ["real estate", "property", "realty", "housing"] },
  { trigger: "beauty", terms: ["beauty", "makeup", "skincare"] },
  { trigger: "gaming", terms: ["gaming", "games", "esports"] },
  { trigger: "football", terms: ["football", "soccer", "premier league", "fifa", "world cup"] },
  { trigger: "sports", terms: ["sports", "sport", "football", "soccer", "athletics"] },
  { trigger: "travel", terms: ["travel", "tourism", "destination"] },
];

function normalizeQuery(text: string) {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function safeSearchFragment(text: string) {
  return text
    .toLowerCase()
    .replace(/[,%()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildFallbackTerms(text: string) {
  const normalized = normalizeQuery(text)
    .replace(/\bi am looking for\b/g, " ")
    .replace(/\bi m looking for\b/g, " ")
    .replace(/\blooking for\b/g, " ")
    .replace(/\bcan you find\b/g, " ")
    .replace(/\bfind me\b/g, " ")
    .replace(/\bfind\b/g, " ")
    .replace(/\bshow me\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const rawTokens = normalized
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean);

  const tokens = rawTokens.filter((token) => token.length > 1 && !CHAT_STOPWORDS.has(token));
  const collected = new Set<string>();
  const push = (value: string) => {
    const cleaned = safeSearchFragment(value);
    if (!cleaned) return;
    collected.add(cleaned);
  };

  push(normalized);
  push(normalized.replace(/\bjournalists?\b/g, " ").replace(/\breporters?\b/g, " ").trim());
  push(tokens.join(" "));

  for (const token of tokens) push(token);
  for (let i = 0; i < tokens.length - 1; i += 1) push(`${tokens[i]} ${tokens[i + 1]}`);
  for (let i = 0; i < tokens.length - 2; i += 1) push(`${tokens[i]} ${tokens[i + 1]} ${tokens[i + 2]}`);

  const normalizedBlob = ` ${normalized} `;
  for (const { canonical, terms } of COUNTRY_FALLBACK_ALIASES) {
    if (normalizedBlob.includes(` ${canonical} `) || terms.some((term) => normalizedBlob.includes(` ${term} `))) {
      for (const term of terms) push(term);
    }
  }

  for (const { trigger, terms } of TOPIC_FALLBACK_ALIASES) {
    if (normalizedBlob.includes(` ${trigger} `)) {
      for (const term of terms) push(term);
    }
  }

  return [...collected].slice(0, 16);
}

function dedupeRows(rows: Row[]) {
  const seen = new Set<string>();
  const out: Row[] = [];
  for (const row of rows) {
    const key = row.source === "database"
      ? `db:${row.source_table ?? "unknown"}:${String(row.source_id ?? row.email ?? `${row.name}|${row.outlet}`)}`
      : `exa:${String(row.source_url ?? row.email ?? `${row.name}|${row.outlet}`)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function buildOrExpression(fields: string[], terms: string[]) {
  return terms
    .flatMap((term) => {
      const fragment = safeSearchFragment(term);
      if (!fragment) return [] as string[];
      return fields.map((field) => `${field}.ilike.%${fragment}%`);
    })
    .join(",");
}

function matchesAnyWholeTerm(haystack: string, terms: string[]) {
  const hay = normalizeQuery(haystack);
  return terms.some((term) => {
    const cleaned = normalizeQuery(term);
    if (!cleaned) return false;
    return new RegExp(`(?:^|[^a-z0-9])${cleaned.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?:$|[^a-z0-9])`, "i").test(hay);
  });
}

function matchesFallbackSubject(row: Row, query: string) {
  const normalized = normalizeQuery(query);
  const terms = buildFallbackTerms(query);
  const looksSpecific =
    /\b(about|cover|covers|covering|write|writes|writing|report|reports|reporting)\b/.test(normalized) &&
    terms.length >= 2;
  const isMessi = normalized.includes("lionel messi") || normalized.includes("leo messi");
  if (!looksSpecific && !isMessi) return true;

  const hay = [
    row.category,
    row.display_topic,
    row.topics,
    row.title,
    row.outlet,
    row.reason,
    row.source_url,
  ].filter(Boolean).join(" ");

  const direct = isMessi ? ["lionel messi", "leo messi", "messi"] : [terms.join(" "), terms[terms.length - 1]];
  if (matchesAnyWholeTerm(hay, direct)) return true;

  if (isMessi) {
    const sportsTerms = [
      "football",
      "soccer",
      "inter miami",
      "argentina",
      "barcelona",
      "psg",
      "mls",
      "fifa",
      "world cup",
      "champions league",
      "uefa",
      "la liga",
      "sports",
      "sport",
    ];
    const antiTerms = ["automotive", "cars", "auto", "electric vehicles", "car repair", "trucks", "suvs"];
    return matchesAnyWholeTerm(hay, sportsTerms) && !matchesAnyWholeTerm(hay, antiTerms);
  }

  return false;
}

function rowPersistenceKey(row: Row) {
  return String(row.source_url ?? row.email ?? `${row.name}|${row.outlet}|${row.title}`);
}

function inferredTopicFromQuery(query: string): string | null {
  const normalized = ` ${normalizeQuery(query)} `;
  for (const { trigger, terms } of TOPIC_FALLBACK_ALIASES) {
    if (normalized.includes(` ${trigger} `) || terms.some((term) => normalized.includes(` ${term} `))) return trigger;
  }
  return null;
}

function titleCaseWords(value: string): string {
  return value.replace(/\b\w/g, (c) => c.toUpperCase());
}

function detectCountryFromQuery(query: string): string | null {
  const norm = ` ${normalizeQuery(query)} `;
  for (const { canonical, terms } of COUNTRY_FALLBACK_ALIASES) {
    if (norm.includes(` ${canonical} `) || terms.some((t) => norm.includes(` ${t} `))) {
      return titleCaseWords(canonical);
    }
  }
  return null;
}

const EXAMPLE_SEARCHES = [
  "Find AI journalists in San Francisco",
  "Find fintech creators in Singapore",
  "Find startup podcasts in London",
];

function formatTopicLabel(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return trimmed;
  if (trimmed.length <= 4) return trimmed.toUpperCase(); // AI, NFL, UK, etc.
  return trimmed.replace(/\b([a-z])/g, (_, c) => c.toUpperCase());
}

function topicValue(row: Row, query = ""): string | null {
  const display = String(row.display_topic ?? "").trim();
  if (display) return formatTopicLabel(display);
  const topics = String(row.topics ?? "").trim();
  if (topics) return formatTopicLabel(topics);
  const category = String(row.category ?? "").trim();
  const inferred = inferredTopicFromQuery(query);
  if (inferred) {
    const hay = normalizeQuery([row.title, row.outlet, row.reason, category].filter(Boolean).join(" "));
    if (!category || TOPIC_FALLBACK_ALIASES.some(({ trigger, terms }) => trigger === inferred && terms.some((term) => hay.includes(term)))) return formatTopicLabel(inferred);
  }
  return category ? formatTopicLabel(category) : null;
}

function shouldAutoPersistRow(kind: "journalists" | "creators", row: Row) {
  if (row.source !== "exa" || !row.name) return false;
  if (kind === "journalists") return !!(row.email || row.outlet || row.title);
  return !!(row.email || row.ig_handle || row.youtube_url || row.outlet);
}

async function fetchJournalistFallback(query: string): Promise<Row[]> {
  const terms = buildFallbackTerms(query);
  if (!terms.length) return [];

  const collected: Row[] = [];
  const expression = buildOrExpression(
    ["outlet", "titles", "topics", "country", "category"],
    terms,
  );

  if (expression) {
    const { data, error } = await supabase
      .from("journalist")
      .select("id,name,email,category,titles,topics,xhandle,outlet,country,linkedin_url")
      .or(expression)
      .limit(75);

    if (!error) {
      for (const row of data ?? []) {
        collected.push({
          source: "database",
          source_id: row.id,
          source_table: "journalist",
          name: row.name,
          outlet: row.outlet,
          title: row.titles,
          category: row.category,
          display_topic: row.topics ?? row.category,
          topics: row.topics ?? null,
          country: row.country,
          email: row.email,
          linkedin_url: row.linkedin_url,
          xhandle: row.xhandle,
        });
      }
    }
  }

  if (collected.length < 12) {
    const simpleTokens = terms.filter((term) => !term.includes(" ")).slice(0, 6);
    for (const term of simpleTokens) {
      const pattern = `%${safeSearchFragment(term)}%`;
      const { data, error } = await supabase
        .from("journalist")
        .select("id,name,email,category,titles,topics,xhandle,outlet,country,linkedin_url")
        .or(`outlet.ilike.${pattern},titles.ilike.${pattern},topics.ilike.${pattern},country.ilike.${pattern},category.ilike.${pattern}`)
        .limit(25);

      if (error) continue;
      for (const row of data ?? []) {
        collected.push({
          source: "database",
          source_id: row.id,
          source_table: "journalist",
          name: row.name,
          outlet: row.outlet,
          title: row.titles,
          category: row.category,
          display_topic: row.topics ?? row.category,
          topics: row.topics ?? null,
          country: row.country,
          email: row.email,
          linkedin_url: row.linkedin_url,
          xhandle: row.xhandle,
        });
      }
      if (collected.length >= 75) break;
    }
  }

  return dedupeRows(collected).filter((row) => matchesFallbackSubject(row, query)).slice(0, 75);
}

async function fetchCreatorFallback(query: string): Promise<Row[]> {
  const terms = buildFallbackTerms(query);
  if (!terms.length) return [];

  const collected: Row[] = [];
  const expression = buildOrExpression(
    ["category", "bio", "type"],
    terms,
  );

  if (expression) {
    const { data, error } = await supabase
      .from("creators")
      .select("id,name,category,email,bio,ig_handle,ig_followers,youtube_url,type,linkedin_url")
      .or(expression)
      .limit(75);

    if (!error) {
      for (const row of data ?? []) {
        collected.push({
          source: "database",
          source_id: row.id,
          source_table: "creators",
          name: row.name,
          outlet: row.type,
          title: row.bio,
          category: row.category,
          display_topic: row.category,
          country: null,
          email: row.email,
          ig_handle: row.ig_handle,
          ig_followers: row.ig_followers,
          youtube_url: row.youtube_url,
          linkedin_url: row.linkedin_url,
        });
      }
    }
  }

  if (collected.length < 12) {
    const simpleTokens = terms.filter((term) => !term.includes(" ")).slice(0, 6);
    for (const term of simpleTokens) {
      const pattern = `%${safeSearchFragment(term)}%`;
      const { data, error } = await supabase
        .from("creators")
        .select("id,name,category,email,bio,ig_handle,ig_followers,youtube_url,type,linkedin_url")
        .or(`category.ilike.${pattern},bio.ilike.${pattern},type.ilike.${pattern}`)
        .limit(25);

      if (error) continue;
      for (const row of data ?? []) {
        collected.push({
          source: "database",
          source_id: row.id,
          source_table: "creators",
          name: row.name,
          outlet: row.type,
          title: row.bio,
          category: row.category,
          display_topic: row.category,
          country: null,
          email: row.email,
          ig_handle: row.ig_handle,
          ig_followers: row.ig_followers,
          youtube_url: row.youtube_url,
          linkedin_url: row.linkedin_url,
        });
      }
      if (collected.length >= 75) break;
    }
  }

  return dedupeRows(collected).filter((row) => matchesFallbackSubject(row, query)).slice(0, 75);
}

async function expandChatResults(base: Exclude<Results, null>, query: string): Promise<Exclude<Results, null>> {
  // The edge function owns retrieval, filtering, and ranking. Padding a thin
  // result set with a second browser-side keyword search can bypass those
  // safeguards and surface weak matches, so preserve its ranked response.
  void query;
  return base;
}

type IconType = React.ComponentType<{ className?: string }>;

const SidebarNavItem = ({
  icon: Icon,
  label,
  active,
  disabled,
  collapsed,
  onClick,
}: {
  icon: IconType;
  label: string;
  active?: boolean;
  disabled?: boolean;
  collapsed?: boolean;
  onClick?: () => void;
}) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    title={collapsed ? label : undefined}
    className={cn(
      "flex items-center rounded-lg text-sm transition-colors",
      collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
      active
        ? "bg-gray-900 text-white hover:bg-gray-900"
        : "text-gray-700 hover:bg-gray-100",
      disabled && "opacity-40 cursor-not-allowed hover:bg-transparent",
    )}
  >
    <Icon className={cn("h-[18px] w-[18px]", active ? "text-white" : "text-gray-500")} />
    {!collapsed && <span className="font-semibold">{label}</span>}
  </button>
);

const SidebarNavButton = React.forwardRef<
  HTMLButtonElement,
  { icon: IconType; label: string; collapsed?: boolean } & React.ButtonHTMLAttributes<HTMLButtonElement>
>(({ icon: Icon, label, collapsed, ...props }, ref) => (
  <button
    ref={ref}
    type="button"
    title={collapsed ? label : undefined}
    {...props}
    className={cn(
      "flex items-center rounded-lg text-sm text-gray-700 hover:bg-gray-100",
      collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
    )}
  >
    <Icon className="h-[18px] w-[18px] text-gray-500" />
    {!collapsed && <span className="font-semibold">{label}</span>}
  </button>
));
SidebarNavButton.displayName = "SidebarNavButton";

const Chat = () => {
  const { user, signOut } = useAuth();
  const navigate = useNavigate();
  const { planIdentifier } = useSubscription();
  const hasGrowth = isGrowthPlanIdentifier(planIdentifier);
  const { threadId } = useParams<{ threadId?: string }>();
  const initials = (user?.email ?? "?").slice(0, 2).toUpperCase();

  const [messages, setMessages] = useState<Msg[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [results, setResults] = useState<Results>(null);
  const [lastQuery, setLastQuery] = useState<string>("");
  const [savingIdx, setSavingIdx] = useState<Record<number, "saving" | "saved">>({});
  const [enrichingIdx, setEnrichingIdx] = useState<Record<number, boolean>>({});
  const [relevanceFeedback, setRelevanceFeedback] = useState<Record<string, "relevant" | "not_relevant">>({});
  const [openProfile, setOpenProfile] = useState<{ kind: "journalist" | "creator"; id: number; query?: string; matchScore?: number } | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem("chat.sidebarCollapsed") === "1";
  });
  useEffect(() => {
    try { localStorage.setItem("chat.sidebarCollapsed", sidebarCollapsed ? "1" : "0"); } catch {
      // Sidebar state persistence is optional.
    }
  }, [sidebarCollapsed]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const loadMoreSentinelRef = useRef<HTMLDivElement>(null);
  const autoPersistedWebRows = useRef<Set<string>>(new Set());

  // Compute a stable rowKey for each row (db: source_table:source_id, web: web:url|domain|name+outlet)
  const computeRowKey = (r: Row): string | null => {
    if (r.source === "database" && r.source_table && r.source_id != null) {
      return `${r.source_table}:${r.source_id}`;
    }
    let host = "";
    try { host = r.source_url ? new URL(r.source_url).hostname.replace(/^www\./, "") : ""; } catch { host = ""; }
    const ident = r.source_url || host || ((r.name && (r.outlet || r.email)) ? `${r.name}|${r.outlet ?? r.email}` : "");
    if (!ident) return null;
    return `web:${ident}`;
  };
  const rowsWithKeys = (results?.rows ?? []).map((r) => ({ row: r, rowKey: computeRowKey(r) }));
  useEffect(() => {
    // no-op
  }, [results?.rows]);


  // Bulk selection of rows by stable rowKey
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());
  useEffect(() => { setSelectedRows(new Set()); }, [results?.kind, lastQuery]);
  const toggleRow = (key: string) => {
    setSelectedRows((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  };
  const selectableKeys = rowsWithKeys.map((x) => x.rowKey).filter((k): k is string => !!k);
  const allRowsSelected = selectableKeys.length > 0 && selectableKeys.every((k) => selectedRows.has(k));
  const someRowsSelected = selectableKeys.some((k) => selectedRows.has(k));
  const toggleAllRows = () => {
    setSelectedRows((prev) => {
      if (allRowsSelected) {
        const next = new Set(prev);
        for (const k of selectableKeys) next.delete(k);
        return next;
      }
      const next = new Set(prev);
      for (const k of selectableKeys) next.add(k);
      return next;
    });
  };
  const selectedDbIds = rowsWithKeys
    .filter(({ row, rowKey }) => rowKey && selectedRows.has(rowKey) && row.source === "database" && typeof row.source_id === "number")
    .map(({ row }) => row.source_id as number);
  const selectedWebRows = rowsWithKeys
    .filter(({ row, rowKey }) => rowKey && selectedRows.has(rowKey) && row.source !== "database")
    .map(({ row }) => row);
  const selectedCount = selectedDbIds.length + selectedWebRows.length;

  const submitRelevanceFeedback = (row: Row, rowKey: string, feedback: "relevant" | "not_relevant") => {
    if (!user || !lastQuery.trim()) return;
    const feedbackKey = `${lastQuery}:${rowKey}`;
    const previous = relevanceFeedback[feedbackKey];
    setRelevanceFeedback((current) => ({ ...current, [feedbackKey]: feedback }));
    trackEvent("search_result_feedback", { feedback, result_source: row.source || "unknown" });

    void (async () => {
      // The generated Supabase type file can lag this recently-added feedback table.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { error } = await (supabase as any)
        .from("search_result_feedback")
        .upsert(
          {
            user_id: user.id,
            search_query: lastQuery.trim(),
            result_key: rowKey,
            feedback,
            source: row.source,
            source_table: row.source_table ?? null,
            source_id: row.source_id == null ? null : String(row.source_id),
            updated_at: new Date().toISOString(),
          },
          { onConflict: "user_id,search_query,result_key" },
        );
      if (!error) return;
      setRelevanceFeedback((current) => {
        const next = { ...current };
        if (previous) next[feedbackKey] = previous;
        else delete next[feedbackKey];
        return next;
      });
      toast.error("Could not save relevance feedback. Please try again.");
    })();
  };

  const savedSearches = useSavedSearches(!!user);
  const chatThreads = useChatThreads(!!user);
  const createThread = useCreateChatThread();
  const updateThread = useUpdateChatThread();
  const deleteThread = useDeleteChatThread();
  const activeThreadIdRef = useRef<string | null>(threadId ?? null);
  useEffect(() => { activeThreadIdRef.current = threadId ?? null; }, [threadId]);

  // Load thread from route param
  useEffect(() => {
    let cancelled = false;
    if (!user) return;
    if (!threadId) {
      setMessages([]); setResults(null); setSavingIdx({}); setEnrichingIdx({}); setInput("");
      return;
    }
    (async () => {
      try {
        const t = await fetchChatThread(threadId);
        if (cancelled) return;
        if (!t) { navigate("/search", { replace: true }); return; }
        const msgs = (t.messages ?? []) as Msg[];
        setMessages(msgs);
        setResults(null);
        setSavingIdx({});
        setEnrichingIdx({});
        // Re-fetch results panel for the most recent user query in this thread.
        const lastUser = [...msgs].reverse().find((m) => m.role === "user")?.content?.trim();
        if (lastUser) {
          setLastQuery(lastUser);
          setLoading(true);
          try {
            const { data, error } = await supabase.functions.invoke("chat", {
              body: { messages: [{ role: "user", content: lastUser }], limit: SEARCH_PAGE_SIZE, offset: 0, results_only: true },
            });
            if (cancelled) return;
            if (!error && data?.results) {
              const expanded = await expandChatResults(data.results, lastUser);
              if (cancelled) return;
              setResults({ ...expanded, pagination: data?.pagination ?? null, sources: data?.sources ?? null });
            }
            if (data?.usage) applyServerUsage(data.usage);
          } catch {
            // silent — thread still loads even if results re-fetch fails
          } finally {
            if (!cancelled) setLoading(false);
          }
        }
      } catch (e) {
        if (!cancelled) toast.error((e as Error).message || "Could not load search");
      }
    })();
    return () => { cancelled = true; };

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threadId, user?.id]);

  // Persist messages whenever they change for the active thread
  const lastPersistedRef = useRef<string>("");
  useEffect(() => {
    const id = activeThreadIdRef.current;
    if (!id || !user) return;
    if (messages.length === 0) return;
    const serialized = JSON.stringify(messages);
    if (serialized === lastPersistedRef.current) return;
    lastPersistedRef.current = serialized;
    const t = setTimeout(() => {
      updateThread.mutate({
        id,
        messages,
        title: deriveThreadTitle(messages),
      });
    }, 400);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages, user?.id]);

  const upsertSearch = useUpsertSavedSearch();
  const togglePin = useTogglePinSavedSearch();
  const deleteSearch = useDeleteSavedSearch();
  const renameSearch = useRenameSavedSearch();

  const { usage, applyServerUsage, refresh: refreshUsage } = useChatUsage();

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [messages, loading]);

  // Infinite-scroll: when sentinel below the table enters viewport, fetch next page.
  const loadMoreRef = useRef<() => void>(() => {});
  useEffect(() => {
    const el = loadMoreSentinelRef.current;
    if (!el) return;
    if (!results?.pagination?.has_more) return;
    const observer = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) loadMoreRef.current();
      }
    }, { root: null, rootMargin: "200px", threshold: 0 });
    observer.observe(el);
    return () => observer.disconnect();
  }, [results?.pagination?.has_more, results?.rows.length]);

  // Confirm Stripe top-up on return from checkout (synchronous backup to webhook)
  useEffect(() => {
    const url = new URL(document.URL);
    const topup = url.searchParams.get("topup");
    const sessionId = url.searchParams.get("session_id");
    if (topup === "success") {
      (async () => {
        try {
          const result = await confirmTopup(sessionId);
          if (result.ok && result.amountTotal > 0 && sessionId) {
            const paidValue = result.amountTotal / 100;
            trackEvent("purchase", {
              transaction_id: sessionId,
              currency: result.currency.toUpperCase(),
              value: paidValue,
              items: [{
                item_id: `topup_${result.pack || "credits"}`,
                item_name: "Media AI search credits",
                price: paidValue,
                quantity: 1,
              }],
            });
          }
          toast.success("Credits added to your account");
        } catch (e) {
          console.error("confirm top-up failed", e);
          toast.error((e as Error).message || "Could not confirm top-up");
        } finally {
          await refreshUsage();
          url.searchParams.delete("topup");
          url.searchParams.delete("session_id");
          window.history.replaceState({}, "", url.pathname + (url.search ? url.search : ""));
        }
      })();
    } else if (topup === "cancelled") {
      toast.info("Top-up cancelled");
      url.searchParams.delete("topup");
      window.history.replaceState({}, "", url.pathname + (url.search ? url.search : ""));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const preventChatFormSubmit = (event: SubmitEvent) => {
      event.preventDefault();
      event.stopPropagation();
    };

    document.addEventListener("submit", preventChatFormSubmit, true);
    return () => document.removeEventListener("submit", preventChatFormSubmit, true);
  }, []);

  useEffect(() => {
    if (!results) return;

    const persistable = results.rows
      .map((row, index) => ({ row, index }))
      .filter(({ row }) => shouldAutoPersistRow(results.kind, row))
      .filter(({ row }) => !autoPersistedWebRows.current.has(rowPersistenceKey(row)))
      .slice(0, AUTO_PERSIST_WEB_ROWS);

    if (!persistable.length) return;

    let cancelled = false;
    (async () => {
      for (const { row } of persistable) {
        if (cancelled) break;
        const key = rowPersistenceKey(row);
        autoPersistedWebRows.current.add(key);
        try {
          const { data, error } = await supabase.functions.invoke("save-contact", {
            body: {
              kind: results.kind,
              row: {
                name: row.name,
                outlet: row.outlet,
                title: row.title,
                category: row.category,
                country: row.country,
                email: row.email,
                ig_handle: row.ig_handle,
                youtube_url: row.youtube_url,
                source_url: row.source_url,
              },
            },
          });
          if (cancelled || error || !data?.ok) continue;
          setResults((prev) => {
            if (!prev || prev.kind !== results.kind) return prev;
            const sourceTable: Row["source_table"] = prev.kind === "journalists" ? "journalist" : "creators";
            const rows = prev.rows.map((candidate) => {
              if (rowPersistenceKey(candidate) !== key) return candidate;
              return {
                ...candidate,
                source: "database" as const,
                source_id: data.id,
                source_table: sourceTable,
              };
            });
            return { ...prev, rows };
          });
        } catch {
          // Silent on background persistence.
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [results]);

  const handleSend = async (inputValue = input.trim(), reset = false) => {
    
    if (!inputValue.trim() || loading) return;
    const allowance = Number(usage?.allowance ?? 0);
    const used = Number(usage?.used ?? 0);
    const topup_credits = Number(usage?.credits ?? 0);
    const monthly_remaining = Math.max(0, allowance - used);
    const total_available = monthly_remaining + topup_credits;
    
    if (total_available <= 0) {
      setMessages((m) => [
        ...m,
        { role: "assistant", content: "You've used all your search credits for this month. Click the **Buy credits** button in the lower-left sidebar to buy a top-up pack, or [upgrade your plan](/pricing).", ts: new Date().toISOString() },
      ]);
      return;
    }
    const base = reset ? [] : messages;
    setMessages([...base, { role: "user", content: inputValue, ts: new Date().toISOString() }]);
    setInput("");
    setLoading(true);
    setLastQuery(inputValue);
    markFirstSearchComplete();
    trackEvent("search_submitted", {
      query_length: inputValue.trim().length,
      is_new_search: reset,
      plan: planIdentifier || "unknown",
    });

    // Ensure thread exists; create lazily on first message and reflect in URL.
    if (!activeThreadIdRef.current && user) {
      try {
        const initialMsgs: Msg[] = reset
          ? [{ role: "user", content: inputValue, ts: new Date().toISOString() }]
          : [...base, { role: "user", content: inputValue, ts: new Date().toISOString() }];
        const created = await createThread.mutateAsync({
          title: deriveThreadTitle(initialMsgs),
          messages: initialMsgs,
        });
        activeThreadIdRef.current = created.id;
        lastPersistedRef.current = JSON.stringify(initialMsgs);
        navigate(`/search/${created.id}`, { replace: true });
      } catch (e) {
        console.error("create thread failed", e);
      }
    }

    try {
      const chatRes = await supabase.functions.invoke("chat", {
        body: { messages: [...base, { role: "user", content: inputValue }], limit: SEARCH_PAGE_SIZE, offset: 0 },
      });
      const { data, error } = chatRes;
      if (error) {
        const ctx = (error as { context?: Response }).context;
        let detail = "";
        try { detail = ctx ? await ctx.clone().text() : ""; } catch { /* ignore */ }
        let parsed: { error?: string; message?: string; usage?: Partial<import("@/hooks/useChatUsage").ChatUsage>; debug?: { remaining?: number; credits?: number; sub_active?: boolean } } | null = null;
        try { parsed = detail ? JSON.parse(detail) : null; } catch { /* ignore */ }
        if (parsed?.usage) applyServerUsage(parsed.usage);
        if (detail.includes("quota_exhausted")) {
          const dbgRemaining = Number(parsed?.debug?.remaining ?? parsed?.usage?.remaining ?? 0);
          const dbgCredits = Number(parsed?.debug?.credits ?? parsed?.usage?.credits ?? 0);
          if (dbgRemaining > 0 || dbgCredits > 0) {
            applyServerUsage({
              remaining: dbgRemaining,
              credits: dbgCredits,
              ...(parsed?.usage ?? {}),
            });
            setMessages((m) => m.slice(0, -1));
            setInput(inputValue);
            setMessages((m) => [...m, { role: "assistant", content: `Your balance shows ${dbgRemaining.toLocaleString()} remaining + ${dbgCredits.toLocaleString()} credits, but the server reported quota exhausted. Please click **Send** again to retry.`, ts: new Date().toISOString() }]);
            await refreshUsage();
            return;
          }
          setMessages((m) => [...m, { role: "assistant", content: "You've used all your search credits for this month. Click the **Buy credits** button in the lower-left sidebar to buy a top-up pack, or [upgrade your plan](/pricing).", ts: new Date().toISOString() }]);
          await refreshUsage();
          return;
        }
        if (parsed?.error === "model_provider_error") {
          setMessages((m) => [...m, { role: "assistant", content: "Your Media AI credits are available, but the AI model provider rejected the request. Please check the configured OpenAI billing/API key.", ts: new Date().toISOString() }]);
          return;
        }
        throw error;
      }
      setMessages((m) => [
        ...m,
        { role: "assistant", content: data?.content || "(no response)", ts: new Date().toISOString() },
      ]);
      if (data?.usage) applyServerUsage(data.usage);
      if (data?.results) {
        const expanded = await expandChatResults(data.results, inputValue);
        setResults({ ...expanded, pagination: data?.pagination ?? null, sources: data?.sources ?? null });
        trackEvent("search_results_viewed", {
          result_count: expanded.rows.length,
          result_type: expanded.kind,
          database_results: Number(data?.sources?.database ?? 0),
          web_results: Number(data?.sources?.web ?? 0),
        });
        setSavingIdx({});
        upsertSearch.mutate({ tab: expanded.kind, query: { q: inputValue } });
      } else {
        setResults(null);
      }
    } catch (e) {
      setMessages((m) => [
        ...m,
        { role: "assistant", content: `Error: ${(e as Error).message}`, ts: new Date().toISOString() },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const loadMore = useCallback(async () => {
    if (!results || !results.pagination?.has_more || loadingMore || !lastQuery) return;
    setLoadingMore(true);
    try {
      const pag = results.pagination;
      const nextOffset = (pag.next_offset ?? (pag.offset + pag.limit)) as number;
      const { data, error } = await supabase.functions.invoke("chat", {
        body: { messages: [{ role: "user", content: lastQuery }], limit: SEARCH_PAGE_SIZE, offset: nextOffset },
      });
      if (error) throw error;
      if (data?.usage) applyServerUsage(data.usage);
      if (data?.results?.rows?.length) {
        setResults((prev) => prev ? {
          ...prev,
          rows: dedupeRows([...prev.rows, ...data.results.rows]),
          pagination: data.pagination ?? prev.pagination,
          sources: data.sources ?? prev.sources,
        } : prev);
      } else if (data?.pagination) {
        // No new rows but server still updates pagination (e.g. has_more flipped to false)
        setResults((prev) => prev ? { ...prev, pagination: data.pagination } : prev);
      }
    } catch (e) {
      toast.error((e as Error).message || "Could not load more");
    } finally {
      setLoadingMore(false);
    }
  }, [results, loadingMore, lastQuery, applyServerUsage]);

  useEffect(() => { loadMoreRef.current = loadMore; }, [loadMore]);

  const buyTokens = async (pack: TopupPack) => {
    if (!user) { navigate("/login"); return; }
    try {
      await startTopup(pack);
    } catch (e) {
      toast.error((e as Error).message || "Top-up failed");
    }
  };

  const handleSignOut = async () => { await signOut(); navigate("/"); };
  const newChat = () => {
    activeThreadIdRef.current = null;
    lastPersistedRef.current = "";
    setMessages([]); setResults(null); setSavingIdx({}); setEnrichingIdx({}); setInput("");
    if (threadId) navigate("/search");
  };

  const enrichEmail = async (idx: number) => {
    if (!results) return;
    const row = results.rows[idx];
    if (!row) return;
    setEnrichingIdx((s) => ({ ...s, [idx]: true }));
    try {
      const table: "journalist" | "creators" =
        row.source_table ?? (results.kind === "journalists" ? "journalist" : "creators");
      const payload = {
        name: row.name,
        outlet: row.outlet,
        title: row.title,
        source: row.source,
        source_id: row.source_id ?? null,
        source_table: row.source_table ?? table,
        url: row.source_url,
        country: row.country,
      };
      
      const { data, error } = await supabase.functions.invoke("enrich-contact", {
        body: payload,
      });
      if (error) {
        const context = (error as { context?: Response }).context;
        const errorPayload = context ? await context.clone().json().catch(() => null) as { error?: string } | null : null;
        throw new Error(errorPayload?.error || error.message);
      }
      if (data?.found && data?.email) {
        setResults((prev) => {
          if (!prev) return prev;
          const rows: Row[] = prev.rows.map((r, i) => i === idx ? { ...r, email: data.email } : r);
          return { ...prev, rows };
        });
        toast.success("Email found");
      } else {
        toast.message(data?.error ?? "No email found");
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Email lookup failed");
    } finally {
      setEnrichingIdx((s) => { const c = { ...s }; delete c[idx]; return c; });
    }
  };

  const enrichLinkedIn = async (idx: number) => {
    if (!results) return;
    const row = results.rows[idx];
    if (!row) return;
    setEnrichingIdx((s) => ({ ...s, [idx]: true }));
    try {
      const table: "journalist" | "creators" =
        row.source_table ?? (results.kind === "journalists" ? "journalist" : "creators");
      const payload = {
        name: row.name,
        outlet: row.outlet,
        title: row.title,
        source: row.source,
        source_id: row.source_id ?? null,
        source_table: row.source_table ?? table,
        url: row.source_url,
        country: row.country,
        fields: ["linkedin_url"],
      };
      const { data, error } = await supabase.functions.invoke("enrich-contact", { body: payload });
      if (error) throw error;
      if (data?.found && data?.linkedin_url) {
        setResults((prev) => {
          if (!prev) return prev;
          const rows: Row[] = prev.rows.map((r, i) => i === idx ? { ...r, linkedin_url: data.linkedin_url } : r);
          return { ...prev, rows };
        });
        toast.success("LinkedIn found");
      } else {
        toast.message(data?.error ?? "No LinkedIn found");
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "LinkedIn lookup failed");
    } finally {
      setEnrichingIdx((s) => { const c = { ...s }; delete c[idx]; return c; });
    }
  };

  const saveExaRow = async (idx: number) => {
    if (!results) return;
    const row = results.rows[idx];
    if (!row || row.source !== "exa") return;
    setSavingIdx((s) => ({ ...s, [idx]: "saving" }));
    try {
      const { data, error } = await supabase.functions.invoke("save-contact", {
        body: {
          kind: results.kind,
          row: {
            name: row.name,
            outlet: row.outlet,
            title: row.title,
            category: row.category,
            country: row.country,
            email: row.email,
            ig_handle: row.ig_handle,
            youtube_url: row.youtube_url,
            source_url: row.source_url,
          },
        },
      });
      if (error || !data?.ok) throw error || new Error(data?.error || "Save failed");
      setSavingIdx((s) => ({ ...s, [idx]: "saved" }));
      setResults((prev) => {
        if (!prev) return prev;
        const newSourceTable: "journalist" | "creators" = prev.kind === "journalists" ? "journalist" : "creators";
        const rows: Row[] = prev.rows.map((r, i) => i === idx
          ? { ...r, source: "database", source_id: data.id, source_table: newSourceTable }
          : r);
        return { ...prev, rows };
      });
    } catch (_) {
      toast.error("Could not save this web result");
      setSavingIdx((s) => { const c = { ...s }; delete c[idx]; return c; });
    }
  };

  const cols = results?.kind === "creators" ? CREATOR_COLS : JOURNALIST_COLS;
  const journalistOutlets = results?.kind === "journalists" ? results.rows.map((r) => r.outlet ?? null) : [];
  const authorities = useOutletAuthorities(journalistOutlets);

  return (
    <div className="h-screen bg-chat flex flex-col overflow-hidden">
      <Helmet><title>Search — Media AI</title></Helmet>

      <AppHeader active="search" hideNav />


      <div className="flex flex-1 min-h-0">
        {/* Primary rail */}
        <aside className={cn("hidden md:flex border-r border-border bg-white flex-col flex-shrink-0 transition-[width] duration-200", sidebarCollapsed ? "w-14" : "w-52")}>
          <div className={cn("py-3", sidebarCollapsed ? "px-2 flex justify-center" : "px-2")}>
            <button
              type="button"
              data-tour="sidebar-collapse"
              onClick={() => setSidebarCollapsed((v) => !v)}
              className={cn(
                "flex items-center gap-2 rounded-md text-sm text-gray-700 hover:bg-gray-100",
                sidebarCollapsed ? "justify-center p-2" : "w-full px-2 py-1.5",
              )}
              title={sidebarCollapsed ? "Expand" : "Collapse"}
              aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {sidebarCollapsed ? (
                <PanelLeftOpen className="h-[18px] w-[18px] text-gray-500" />
              ) : (
                <PanelLeftClose className="h-[18px] w-[18px] text-gray-500" />
              )}
              {!sidebarCollapsed && <span className="font-semibold text-xs">Collapse</span>}
            </button>

          </div>

          <nav className={cn("pb-2 space-y-0.5 flex-1", sidebarCollapsed ? "px-2" : "px-2")}>
            <div data-tour="nav-search"><SidebarNavItem icon={SearchIcon} label="Search" active collapsed={sidebarCollapsed} onClick={() => navigate("/search")} /></div>
            {hasGrowth && (
              <div data-tour="nav-database"><SidebarNavItem icon={Database} label="Database" collapsed={sidebarCollapsed} onClick={() => navigate("/database")} /></div>
            )}
            <div data-tour="nav-monitor"><SidebarNavItem icon={Radar} label="Monitor" collapsed={sidebarCollapsed} onClick={() => navigate("/monitor")} /></div>
            <div data-tour="nav-inbox"><InboxSheet triggerNode={<SidebarNavButton icon={InboxIcon} label="Inbox" collapsed={sidebarCollapsed} />} /></div>
            <div data-tour="nav-lists"><ListsSheet triggerNode={<SidebarNavButton icon={ListChecks} label="Lists" collapsed={sidebarCollapsed} />} /></div>
            <div data-tour="nav-export"><SidebarNavItem
              icon={Download}
              label="Export"
              collapsed={sidebarCollapsed}
              disabled={!results?.rows.length}
              onClick={() => {
                const rows = (results?.rows ?? []).map((row) => ({ ...row, category: topicValue(row, lastQuery) })) as Record<string, unknown>[];
                if (!rows.length) return;
                const headers = Object.keys(rows[0]);
                import("@/lib/audit").then(({ logWorkspaceEvent }) =>
                  logWorkspaceEvent("export_triggered", null, { source_page: "chat", row_count: rows.length, kind: results?.kind })
                );
                const filename = `${results!.kind}-${Date.now()}.csv`;
                downloadCsv(filename, toCsv(rows as never, headers));
                supabase.functions.invoke("send-export-notification", {
                  body: { filename, rowCount: rows.length, source: `chat (${results?.kind})` },
                }).catch(() => {});
                import("@/hooks/useNotifications").then(({ addNotification }) =>
                  addNotification({
                    title: "Export ready",
                    description: `${rows.length.toLocaleString()} ${results?.kind ?? "rows"} downloaded as CSV`,
                    kind: "export",
                  })
                );
              }}
            /></div>
          </nav>


          <div className={cn(sidebarCollapsed ? "p-2" : "p-3")}>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  type="button"
                  data-tour="buy-credits"
                  title={usage ? `${usage.used.toLocaleString()} / ${usage.allowance.toLocaleString()} monthly credits used${usage.credits > 0 ? ` · ${usage.credits.toLocaleString()} top-up credits` : ""}` : "Buy search credits"}
                  className={cn(
                    "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
                    sidebarCollapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
                    usage && usage.remaining <= 0 && "text-destructive",
                    usage && usage.remaining > 0 && usage.remaining < usage.allowance * 0.2 && "text-amber-600",
                  )}
                >
                  <Sparkles className="h-[18px] w-[18px] text-gray-500" />
                  {!sidebarCollapsed && <span className="font-semibold">Buy credits</span>}
                </button>

              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" side="top" className="w-64">
                <DropdownMenuLabel className="font-normal">
                  <div className="text-xs text-muted-foreground">{usage ? `${usage.remaining.toLocaleString()} credits left` : "Buy more search credits"}</div>
                  <div className="text-[11px] text-muted-foreground/70">One-time top-up, never expires</div>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={() => buyTokens("small")} className="flex items-center justify-between">
                  <span className="flex items-center gap-2"><Zap className="h-3.5 w-3.5" />100k credits</span>
                  <span className="text-xs text-muted-foreground">$10</span>
                </DropdownMenuItem>
                <DropdownMenuItem onSelect={() => buyTokens("medium")} className="flex items-center justify-between">
                  <span className="flex items-center gap-2"><Zap className="h-3.5 w-3.5" />500k credits</span>
                  <span className="text-xs text-muted-foreground">$40</span>
                </DropdownMenuItem>
                <DropdownMenuItem onSelect={() => buyTokens("large")} className="flex items-center justify-between">
                  <span className="flex items-center gap-2"><Zap className="h-3.5 w-3.5" />2M credits</span>
                  <span className="text-xs text-muted-foreground">$120</span>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={() => navigate("/pricing")} className="text-xs text-muted-foreground">
                  Or upgrade your plan →
                </DropdownMenuItem>
                <DropdownMenuItem asChild className="text-xs text-muted-foreground">
                  <a href="mailto:alex@trymedia.ai">Support</a>
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            <div className="mt-0.5 space-y-0.5">
              <button
                type="button"
                onClick={() => navigate("/account")}
                className={cn(
                  "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
                  sidebarCollapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
                )}
                title="Settings"
              >
                <Settings className="h-[18px] w-[18px] text-gray-500" />
                {!sidebarCollapsed && <span className="font-semibold">Settings</span>}
              </button>
              <a
                href="mailto:alex@trymedia.ai"
                className={cn(
                  "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
                  sidebarCollapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
                )}
                title="Help"
              >
                <HelpCircle className="h-[18px] w-[18px] text-gray-500" />
                {!sidebarCollapsed && <span className="font-semibold">Help</span>}
              </a>
            </div>
          </div>
        </aside>

        {/* Chat history panel — hidden the moment a search runs or when viewing results */}
        {!results && !loading && (
          <aside className="w-60 border-r border-border bg-white flex flex-col flex-shrink-0">
            <div className="px-3 pt-3 pb-2">
              <Button variant="outline" size="sm" className="w-full justify-center gap-1.5" onClick={newChat}>
                <Plus className="h-3.5 w-3.5" />New
              </Button>
            </div>

            <div className="flex-1 overflow-auto px-2 py-2">
              {(() => {
                const pinned = (savedSearches.data ?? []).filter((s) => s.pinned);
                return pinned.length > 0 ? (
                  <>
                    <div className="px-2 pt-1 pb-1 text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Pinned</div>
                    <ul className="space-y-0.5 mb-3">
                      {pinned.map((s) => (
                        <li key={s.id} className="group flex items-center gap-1 rounded-md hover:bg-secondary/60">
                          <button
                            type="button"
                            onClick={() => handleSend(s.query.q ?? s.name, true)}
                            className="flex-1 text-left px-2 py-1.5 text-sm truncate flex items-center gap-1.5"
                            title={s.query.q ?? s.name}
                          >
                            <Pin className="h-3 w-3 text-muted-foreground flex-shrink-0" />
                            <span className="truncate">{s.name}</span>
                          </button>
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              <button
                                type="button"
                                onClick={(e) => e.stopPropagation()}
                                className="opacity-0 group-hover:opacity-100 data-[state=open]:opacity-100 p-1 mr-1 rounded text-muted-foreground hover:text-foreground hover:bg-secondary"
                                aria-label="More options"
                              >
                                <MoreHorizontal className="h-4 w-4" />
                              </button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="start" className="w-40">
                              <DropdownMenuItem onSelect={() => togglePin.mutate({ id: s.id, pinned: false })}>
                                <PinOff className="h-4 w-4 mr-2" /> Unpin
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                onSelect={() => {
                                  const next = window.prompt("Rename search", s.name);
                                  if (next && next.trim() && next.trim() !== s.name) {
                                    renameSearch.mutate({ id: s.id, name: next.trim() });
                                  }
                                }}
                              >
                                <Pencil className="h-4 w-4 mr-2" /> Rename
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                className="text-destructive focus:text-destructive"
                                onSelect={() => deleteSearch.mutate(s.id)}
                              >
                                <Trash2 className="h-4 w-4 mr-2" /> Delete
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </li>
                      ))}
                    </ul>
                  </>
                ) : null;
              })()}

              <div className="px-2 pt-1 pb-1 text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Recent</div>
              {chatThreads.isLoading ? (
                <div className="px-2 py-2 text-xs text-muted-foreground">Loading…</div>
              ) : (chatThreads.data?.length ?? 0) === 0 ? (
                <div className="px-2 py-2 text-xs text-muted-foreground">Your searches will appear here.</div>
              ) : (
                <ul className="space-y-0.5 mb-3">
                  {chatThreads.data!.map((t) => {
                    const active = t.id === threadId;
                    return (
                      <li key={t.id} className={`group flex items-center gap-1 rounded-md ${active ? "bg-secondary" : "hover:bg-secondary/60"}`}>
                        <button
                          type="button"
                          onClick={() => navigate(`/search/${t.id}`)}
                          className="flex-1 text-left px-2 py-1.5 text-sm truncate"
                          title={t.title}
                        >
                          <span className="truncate">{t.title || "New search"}</span>
                        </button>
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <button
                              type="button"
                              onClick={(e) => e.stopPropagation()}
                              className="opacity-0 group-hover:opacity-100 data-[state=open]:opacity-100 p-1 mr-1 rounded text-muted-foreground hover:text-foreground hover:bg-secondary"
                              aria-label="More options"
                            >
                              <MoreHorizontal className="h-4 w-4" />
                            </button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="start" className="w-40">
                            <DropdownMenuItem
                              onSelect={() => {
                                const current = t.title || "";
                                const next = window.prompt("Rename search", current);
                                if (next && next.trim() && next.trim() !== current) {
                                  updateThread.mutate({ id: t.id, title: next.trim() });
                                }
                              }}
                            >
                              <Pencil className="h-4 w-4 mr-2" /> Rename
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              className="text-destructive focus:text-destructive"
                              onSelect={async () => {
                                try {
                                  await deleteThread.mutateAsync(t.id);
                                  if (active) { activeThreadIdRef.current = null; navigate("/search"); }
                                } catch (err) {
                                  toast.error((err as Error).message || "Could not delete");
                                }
                              }}
                            >
                              <Trash2 className="h-4 w-4 mr-2" /> Delete
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          </aside>
        )}

        <section className={`flex-col min-h-0 ${results ? "hidden md:flex md:w-[440px] border-r border-border" : "flex flex-1 items-center"}`}>
          <div ref={scrollRef} className={`flex-1 overflow-auto w-full ${results ? "px-4 py-6" : "max-w-2xl px-6 py-12"}`}>
            {messages.length === 0 ? (
              <div className="mt-12">
                <OnboardingChecklist />
                <div className="text-center mt-8">
                <div className="inline-flex items-center justify-center h-12 w-12 rounded-2xl bg-primary/10 text-primary mb-4">
                  <Sparkles className="h-6 w-6" />
                </div>
                <h1 className="text-2xl font-medium mb-2">What media are you looking for?</h1>
                <p className="text-sm text-muted-foreground max-w-md mx-auto">
                  Media AI finds journalists, creators, and podcasters who cover your topic — with verified contact details.
                </p>
                <div className="mt-8 flex flex-col items-center gap-2">
                  <p className="text-xs uppercase tracking-wide text-muted-foreground/70 mb-1">Try an example</p>
                  {EXAMPLE_SEARCHES.map((example) => (
                    <button
                      key={example}
                      type="button"
                      onClick={() => { setInput(example); void handleSend(example); }}
                      className="text-sm px-4 py-2 rounded-full border border-border bg-white hover:bg-secondary/60 hover:border-primary/40 transition-colors"
                    >
                      {example}
                    </button>
                  ))}
                </div>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                {messages.map((m, i) => (
                  <div key={i} className="flex flex-col items-start mr-12">
                    <div className={`text-sm rounded-xl px-4 py-3 ${m.role === "user" ? "bg-primary text-primary-foreground" : "bg-secondary"}`}>
                      {m.role === "assistant" ? (
                        <div className="prose prose-sm max-w-none prose-p:my-1 prose-headings:my-2">
                          <ReactMarkdown>{m.content}</ReactMarkdown>
                        </div>
                      ) : (
                        m.content
                      )}
                    </div>
                    <div className="mt-1 px-1 text-[11px] text-muted-foreground">
                      {formatMsgTime(m.ts)}
                    </div>
                  </div>
                ))}
                {loading && (
                  <div className="text-sm text-muted-foreground flex items-center gap-2">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />Searching journalists & creators…
                  </div>
                )}
              </div>
            )}
          </div>

          <div className={`w-full ${results ? "" : "max-w-2xl"} px-4 pb-6`}>
            <div
              data-tour="search-input"
              className="relative rounded-2xl border border-border bg-white shadow-sm focus-within:border-primary/60"
            >
              <Textarea
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    e.stopPropagation();
                    void handleSend();
                  }
                }}
                placeholder="Search journalists or creators in plain English…"
                disabled={loading}
                rows={2}
                className="resize-none border-0 focus-visible:ring-0 shadow-none px-4 py-3 pr-14 min-h-[64px]"
              />
              <Button
                type="button"
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  void handleSend();
                }}
                disabled={loading || !input.trim()}
                size="icon"
                className="absolute right-2 bottom-2 h-9 w-9 rounded-full"
              >
                <ArrowUp className="h-4 w-4" />
              </Button>
            </div>
            <p className="mt-2 text-xs text-muted-foreground text-center">Enter to send · Shift+Enter for newline</p>
          </div>
        </section>

        {results && (() => {
          const detectedCountry = detectCountryFromQuery(lastQuery);
          const detectedTopic = inferredTopicFromQuery(lastQuery);
          const total = results.pagination?.total_estimated ?? results.rows.length;
          const kindLabel = results.kind === "journalists" ? "journalists" : "creators";
          const topicLabel = detectedTopic ? `${detectedTopic} ` : "";
          const countryLabel = detectedCountry ? ` in ${detectedCountry}` : "";
          const summary = `Showing ${total.toLocaleString()} ${topicLabel}${kindLabel}${countryLabel}`;
          return (
          <section className="flex-1 min-w-0 overflow-auto bg-white">
            <div className="px-5 py-4 border-b border-border flex items-center justify-between sticky top-0 bg-white z-10">
              <div>
                <div className="text-sm font-medium">{summary}</div>
                <div className="text-xs text-muted-foreground mt-0.5">
                  {results.rows.length.toLocaleString()} loaded
                  {results.sources ? ` · ${results.sources.database} from database · ${results.sources.web} from web` : ""}
                </div>
              </div>
            </div>
            {results.rows.length === 0 ? (
              <div className="p-12 text-center max-w-md mx-auto">
                <div className="text-sm font-medium mb-2">No matches yet</div>
                <p className="text-sm text-muted-foreground mb-5">
                  We couldn't find {kindLabel} matching that query. Try broadening the topic or location, or one of these:
                </p>
                <div className="flex flex-col gap-2 items-center">
                  {EXAMPLE_SEARCHES.map((example) => (
                    <button
                      key={example}
                      type="button"
                      onClick={() => { setInput(example); void handleSend(example); }}
                      className="text-sm px-4 py-2 rounded-full border border-border bg-white hover:bg-secondary/60 hover:border-primary/40 transition-colors"
                    >
                      {example}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              <div className="overflow-x-auto">
              <table className="w-full min-w-[1400px] text-sm">
                <thead className="bg-white text-xs text-muted-foreground">
                  <tr>
                    <th className="w-10 px-2">
                      <Checkbox
                        checked={allRowsSelected ? true : someRowsSelected ? "indeterminate" : false}
                        onCheckedChange={toggleAllRows}
                        aria-label="Select all"
                        disabled={selectableKeys.length === 0}
                      />
                    </th>
                    <th className="w-8" />
                    <th className="w-[92px] text-left font-medium px-2 py-2.5">Fit</th>
                    {cols.map((c) => (
                      <th
                        key={String(c.key)}
                        className={`text-left font-medium px-4 py-2.5 ${c.key === "email" ? "w-[280px]" : ""}`}
                      >
                        {c.label}
                      </th>
                    ))}
                    
                  </tr>
                </thead>
                <tbody>
                  {results.rows.map((r, i) => {
                    const dbId = r.source === "database" && typeof r.source_id === "number" ? r.source_id : null;
                    const rowKey = computeRowKey(r);
                    const enriching = !!enrichingIdx[i];
                    const saving = savingIdx[i] === "saving";
                    const selected = rowKey ? selectedRows.has(rowKey) : false;
                    const feedbackKey = rowKey ? `${lastQuery}:${rowKey}` : null;
                    const rowFeedback = feedbackKey ? relevanceFeedback[feedbackKey] : null;
                    return (
                      <tr key={rowKey ?? `row-${i}`} className={`group border-b border-border hover:bg-muted/20 align-top ${selected ? "bg-primary/5" : ""}`}>
                        <td className="px-2 py-2.5">
                          {rowKey ? (
                            <Checkbox
                              checked={selected}
                              onCheckedChange={() => toggleRow(rowKey)}
                              aria-label="Select row"
                            />
                          ) : (
                            <span title="Missing contact identity" className="inline-block h-4 w-4 opacity-30 cursor-not-allowed" aria-label="Missing contact identity" />
                          )}
                        </td>
                        <td className="px-2 py-2.5">
                          {dbId !== null && (
                            <AddToListMenu
                              journalistId={results.kind === "journalists" ? dbId : undefined}
                              creatorId={results.kind === "creators" ? dbId : undefined}
                            />
                          )}
                        </td>
                        <td className="px-2 py-2.5">
                          {rowKey ? (
                            <div className="flex items-center gap-1">
                              <MatchExplanationPopover contact={r} query={lastQuery} score={r.match_score} />
                              <button
                                type="button"
                                onClick={() => submitRelevanceFeedback(r, rowKey, "relevant")}
                                aria-label="Mark result relevant"
                                title="Relevant"
                                className={`rounded p-1 transition-colors ${rowFeedback === "relevant" ? "bg-emerald-100 text-emerald-700" : "text-muted-foreground hover:bg-emerald-50 hover:text-emerald-700"}`}
                              >
                                <ThumbsUp className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                onClick={() => submitRelevanceFeedback(r, rowKey, "not_relevant")}
                                aria-label="Mark result not relevant"
                                title="Not relevant"
                                className={`rounded p-1 transition-colors ${rowFeedback === "not_relevant" ? "bg-rose-100 text-rose-700" : "text-muted-foreground hover:bg-rose-50 hover:text-rose-700"}`}
                              >
                                <ThumbsDown className="h-3.5 w-3.5" />
                              </button>
                            </div>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </td>
                        {cols.map((c) => {
                          const v = c.key === "authority"
                            ? null
                            : c.key === "category"
                              ? topicValue(r, lastQuery)
                              : r[c.key as keyof Row];
                          return (
                            <td
                              key={String(c.key)}
                              className={`px-4 py-2.5 ${c.key === "email" ? "w-[280px] whitespace-nowrap overflow-hidden text-ellipsis" : ""}`}
                            >
                              {c.key === "authority" ? (
                                <AuthorityBadge score={resolveAuthority(authorities.data, r.outlet)} />
                              ) : c.key === "email" ? (
                                (() => {
                                  const raw = typeof v === "string" ? v.trim().toLowerCase() : "";
                                  const valid = !!raw && !["null", "undefined", "-", "n/a"].includes(raw) && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(raw);
                                  if (valid) {
                                    return (
                                      <a href={`mailto:${raw}`} className="block truncate text-primary hover:underline" title={raw}>{raw}</a>
                                    );
                                  }
                                  return enriching ? (
                                    <span className="inline-flex whitespace-nowrap items-center gap-1 text-xs text-muted-foreground">
                                      <Loader2 className="h-3 w-3 animate-spin" />Finding…
                                    </span>
                                  ) : (() => {
                                    const nameLetters = ((r.name ?? "").match(/\p{L}/gu) ?? []).length;
                                    let host = "";
                                    try { host = r.source_url ? new URL(r.source_url).hostname : ""; } catch { host = ""; }
                                    const hasDomain = !!host || !!r.outlet;
                                    const canFind = nameLetters >= 2 && hasDomain;
                                    if (!canFind) {
                                      return (
                                        <span
                                          className="inline-flex whitespace-nowrap items-center gap-1 text-xs text-muted-foreground/60 cursor-not-allowed"
                                          title="Need a person name + outlet domain to enrich email."
                                        >
                                          <Sparkles className="h-3 w-3" />Find email
                                        </span>
                                      );
                                    }
                                    return (
                                      <button
                                        type="button"
                                        onClick={() => enrichEmail(i)}
                                        className="inline-flex whitespace-nowrap items-center gap-1 text-xs text-primary hover:underline"
                                        title="Find email"
                                      >
                                        <Sparkles className="h-3 w-3" />Find email
                                      </button>
                                    );
                                  })();
                                })()
                              ) : c.key === "linkedin_url" ? (
                                typeof v === "string" && /linkedin\.com\/in\//i.test(v) ? (
                                  <a href={v} target="_blank" rel="noreferrer" className="text-primary hover:underline text-xs">LinkedIn</a>
                                ) : (
                                  enriching ? (
                                    <span className="inline-flex whitespace-nowrap items-center gap-1 text-xs text-muted-foreground">
                                      <Loader2 className="h-3 w-3 animate-spin" />Finding…
                                    </span>
                                  ) : (
                                    <button
                                      type="button"
                                      onClick={() => enrichLinkedIn(i)}
                                      className="inline-flex whitespace-nowrap items-center gap-1 text-xs text-primary hover:underline"
                                      title="Find LinkedIn"
                                    >
                                      <Sparkles className="h-3 w-3" />Find LinkedIn
                                    </button>
                                  )
                                )
                              ) : c.key === "ig_handle" ? (
                                typeof v === "string" && v.trim() ? (() => {
                                  const handle = v.trim().replace(/^@/, "");
                                  return (
                                    <a href={`https://instagram.com/${handle}`} target="_blank" rel="noreferrer" className="text-primary hover:underline text-xs">@{handle}</a>
                                  );
                                })() : (
                                  <span className="text-muted-foreground">—</span>
                                )
                              ) : c.key === "youtube_url" ? (
                                typeof v === "string" && v.trim() ? (
                                  <a href={v.trim()} target="_blank" rel="noreferrer" className="text-primary hover:underline text-xs">YouTube</a>
                                ) : (
                                  <span className="text-muted-foreground">—</span>
                                )
                              ) : c.key === "xhandle" ? (
                                typeof v === "string" && v.trim() ? (() => {
                                  const handle = v.trim().replace(/^@/, "");
                                  return (
                                    <a href={`https://x.com/${handle}`} target="_blank" rel="noreferrer" className="text-primary hover:underline text-xs">@{handle}</a>
                                  );
                                })() : (
                                  <span className="text-muted-foreground">—</span>
                                )
                              ) : v == null || v === "" ? (
                                <span className="text-muted-foreground">—</span>
                              ) : typeof v === "number" ? (
                                v.toLocaleString()
                              ) : c.key === "name" && dbId !== null ? (
                                <button
                                  type="button"
                                  onClick={() => setOpenProfile({
                                    kind: results.kind === "journalists" ? "journalist" : "creator",
                                    id: dbId,
                                    query: lastQuery || undefined,
                                    matchScore: typeof r.match_score === "number" ? r.match_score : undefined,
                                  })}
                                  className="text-left font-medium hover:text-primary hover:underline"
                                >
                                  {String(v)}
                                </button>
                              ) : c.key === "name" && r.source === "exa" ? (
                                <span className="inline-flex items-center gap-2">
                                  {r.source_url ? (
                                    <a href={r.source_url} target="_blank" rel="noreferrer" className="hover:underline">{String(v)}</a>
                                  ) : (
                                    String(v)
                                  )}
                                  
                                </span>
                              ) : (
                                String(v)
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              </div>
            )}
            {results.pagination?.has_more && results.rows.length > 0 && (
              <div ref={loadMoreSentinelRef} className="p-4 flex justify-center">
                {loadingMore ? (
                  <div className="flex items-center text-xs text-muted-foreground">
                    <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />Loading more…
                  </div>
                ) : (
                  <Button variant="outline" size="sm" onClick={loadMore}>Load more</Button>
                )}
              </div>
            )}
          </section>
          );
        })()}
      </div>
      <BulkAddToListBar
        count={selectedCount}
        journalistIds={results?.kind === "journalists" ? selectedDbIds : undefined}
        creatorIds={results?.kind === "creators" ? selectedDbIds : undefined}
        resolveExtraIds={selectedWebRows.length === 0 ? undefined : async () => {
          const kind = results?.kind;
          if (!kind) return {};
          const ids: number[] = [];
          for (const row of selectedWebRows) {
            try {
              const { data, error } = await supabase.functions.invoke("save-contact", {
                body: {
                  kind,
                  row: {
                    name: row.name, outlet: row.outlet, title: row.title,
                    category: row.category, country: row.country, email: row.email,
                    ig_handle: row.ig_handle, youtube_url: row.youtube_url, source_url: row.source_url,
                  },
                },
              });
              if (error || !data?.ok) continue;
              if (typeof data.id === "number") ids.push(data.id);
            } catch { /* skip */ }
          }
          return kind === "journalists" ? { journalistIds: ids } : { creatorIds: ids };
        }}
        onClear={() => setSelectedRows(new Set())}
      />
      <ContactProfileSheet profile={openProfile} onOpenChange={(open) => { if (!open) setOpenProfile(null); }} />
    </div>
  );
};

export default Chat;
