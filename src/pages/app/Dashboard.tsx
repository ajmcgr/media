import { useEffect, useMemo, useRef, useState } from "react";
import { Helmet } from "react-helmet-async";
import { NavLink, useNavigate } from "react-router-dom";
import {
  Search, Users, Database, Download,
  User as UserIcon, Mail, Tag, Globe, AtSign, Building2, Briefcase, Hash,
  ChevronDown, ChevronRight, X, Bell, Instagram, Activity, Youtube,
  PanelLeftClose, PanelLeftOpen,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/contexts/AuthContext";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import logoMedia from "@/assets/brand/logo-media-blue.png";
import {
  useJournalistsInfinite, useCreatorsInfinite, type DirectoryFilters,
} from "@/hooks/useDirectory";
import { ListsSheet } from "@/components/dashboard/ListsSheet";
import { AuthorityBadge } from "@/components/dashboard/AuthorityBadge";
import { useOutletAuthorities, resolveAuthority } from "@/hooks/useOutletAuthority";
import { ArrowDown, ArrowUp, ArrowUpDown, BarChart3 } from "lucide-react";
import { AddToListMenu } from "@/components/dashboard/AddToListMenu";
import { BulkAddToListBar } from "@/components/dashboard/BulkAddToListBar";
import { Checkbox } from "@/components/ui/checkbox";
import { EnrichCell } from "@/components/dashboard/EnrichCell";
import { MessageSquare } from "lucide-react";
import { InboxSheet } from "@/components/dashboard/InboxSheet";
import { toCsv, downloadCsv } from "@/lib/csv";
import { supabase } from "@/integrations/supabase/client";
import { Spinner } from "@/components/ui/spinner";
import { useUpsertSavedSearch } from "@/hooks/useSavedSearches";
import AppHeader from "@/components/AppHeader";
import AppSidebar from "@/components/AppSidebar";

type Tab = "journalists" | "creators";

const JOURNALIST_COLS = ["Name", "Email", "LinkedIn", "Category", "Titles", "xHandle", "Outlet", "Authority", "Country"];
const CREATOR_COLS = ["Name", "Email", "LinkedIn", "IG Handle", "IG Followers", "Engagement", "Category", "YouTube", "YT Subs", "Country"];

const Cell = ({ children }: { children: React.ReactNode }) => (
  <div className="px-3 py-3 text-sm text-foreground truncate" title={typeof children === "string" ? children : undefined}>
    {children ?? <span className="text-muted-foreground">—</span>}
  </div>
);

type FilterKey =
  | "name" | "email" | "category" | "country" | "xhandle" | "outlet" | "title" | "topics"
  | "ig_followers_min" | "ig_engagement_min" | "youtube_subs_min"
  | "authority_min" | "authority_max";

const JOURNALIST_FILTERS: { key: FilterKey; label: string; icon: typeof UserIcon; placeholder?: string; inputType?: "text" | "number" }[] = [
  { key: "name", label: "Search by Names", icon: UserIcon },
  { key: "email", label: "Search by Emails", icon: Mail },
  { key: "category", label: "Filter by Category", icon: Tag },
  { key: "country", label: "Search by Country", icon: Globe },
  { key: "xhandle", label: "Search by xHandles", icon: AtSign },
  { key: "outlet", label: "Search by Outlet", icon: Building2 },
  { key: "title", label: "Search by Title", icon: Briefcase },
  { key: "topics", label: "Search by Topics", icon: Hash },
  { key: "authority_min", label: "Min Authority (DR)", icon: BarChart3, placeholder: "e.g. 70", inputType: "number" },
  { key: "authority_max", label: "Max Authority (DR)", icon: BarChart3, placeholder: "e.g. 100", inputType: "number" },
];

const CREATOR_FILTERS: { key: FilterKey; label: string; icon: typeof UserIcon; placeholder?: string; inputType?: "text" | "number" }[] = [
  { key: "name", label: "Search by Names", icon: UserIcon },
  { key: "email", label: "Search by Emails", icon: Mail },
  { key: "ig_followers_min", label: "Min IG Followers", icon: Instagram, placeholder: "e.g. 10000", inputType: "number" },
  { key: "ig_engagement_min", label: "Min Engagement %", icon: Activity, placeholder: "e.g. 2.5", inputType: "number" },
  { key: "category", label: "Filter by Category", icon: Tag },
  { key: "youtube_subs_min", label: "Min YT Subscribers", icon: Youtube, placeholder: "e.g. 50000", inputType: "number" },
  { key: "country", label: "Search by Country", icon: Globe },
];

const Dashboard = () => {
  const [tab, setTab] = useState<Tab>("journalists");
  const [search, setSearch] = useState("");
  const [filterValues, setFilterValues] = useState<Partial<Record<FilterKey, string>>>({});
  const [openFilter, setOpenFilter] = useState<FilterKey | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem("dashboard.sidebarCollapsed") === "1";
  });
  useEffect(() => {
    try { localStorage.setItem("dashboard.sidebarCollapsed", sidebarCollapsed ? "1" : "0"); } catch {}
  }, [sidebarCollapsed]);
  const { user, signOut } = useAuth();
  const navigate = useNavigate();
  const initials = (user?.email ?? "?").slice(0, 2).toUpperCase();

  const filters: DirectoryFilters = useMemo(() => {
    const f: DirectoryFilters = { q: search.trim() || undefined };
    for (const [k, v] of Object.entries(filterValues)) {
      if (k === "authority_min" || k === "authority_max") continue; // client-side
      const val = v?.trim();
      if (val) (f as Record<string, string>)[k] = val;
    }
    return f;
  }, [search, filterValues]);

  const hasActiveFilters = !!search.trim() || Object.values(filterValues).some(v => v?.trim());

  const journalists = useJournalistsInfinite(filters);
  const creators = useCreatorsInfinite(filters);
  const active = tab === "journalists" ? journalists : creators;

  const rawRows = useMemo(
    () => active.data?.pages.flatMap(p => p.rows) ?? [],
    [active.data]
  );
  const total = active.data?.pages[0]?.count ?? 0;

  // Authority sort + cache lookup (journalists only)
  const [authoritySort, setAuthoritySort] = useState<"none" | "desc" | "asc">("none");
  const journalistOutlets = useMemo(
    () => tab === "journalists" ? (rawRows as Array<{ outlet?: string | null }>).map(r => r.outlet ?? null) : [],
    [tab, rawRows]
  );
  const authorities = useOutletAuthorities(journalistOutlets);

  const allRows = useMemo(() => {
    if (tab !== "journalists") return rawRows;
    const min = Number(filterValues.authority_min);
    const max = Number(filterValues.authority_max);
    const hasMin = Number.isFinite(min) && min > 0;
    const hasMax = Number.isFinite(max) && max > 0;
    let rows = rawRows as Array<{ outlet?: string | null } & Record<string, unknown>>;
    if (hasMin || hasMax) {
      rows = rows.filter((r) => {
        const score = resolveAuthority(authorities.data, r.outlet);
        if (score == null) return false;
        if (hasMin && score < min) return false;
        if (hasMax && score > max) return false;
        return true;
      });
    }
    if (authoritySort !== "none") {
      rows = [...rows].sort((a, b) => {
        const sa = resolveAuthority(authorities.data, a.outlet) ?? -1;
        const sb = resolveAuthority(authorities.data, b.outlet) ?? -1;
        return authoritySort === "desc" ? sb - sa : sa - sb;
      });
    }
    return rows;
  }, [tab, rawRows, authorities.data, authoritySort, filterValues.authority_min, filterValues.authority_max]);

  // Bulk selection — reset on tab change
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  useEffect(() => { setSelectedIds(new Set()); }, [tab]);
  const toggleSelect = (id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };
  const allVisibleIds = useMemo(
    () => (allRows as Array<{ id: number }>).map((r) => r.id),
    [allRows]
  );
  const allSelected = allVisibleIds.length > 0 && allVisibleIds.every((id) => selectedIds.has(id));
  const someSelected = allVisibleIds.some((id) => selectedIds.has(id));
  const toggleSelectAll = () => {
    setSelectedIds((prev) => {
      if (allSelected) {
        const next = new Set(prev);
        for (const id of allVisibleIds) next.delete(id);
        return next;
      }
      const next = new Set(prev);
      for (const id of allVisibleIds) next.add(id);
      return next;
    });
  };

  // Auto-save searches with debounce
  const upsertSearch = useUpsertSavedSearch();
  const debounceRef = useRef<number | null>(null);
  useEffect(() => {
    const q = search.trim();
    if (!q || !user) return;
    if (debounceRef.current) window.clearTimeout(debounceRef.current);
    debounceRef.current = window.setTimeout(() => {
      upsertSearch.mutate({ tab, query: { q } });
    }, 800);
    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search, tab, user?.id]);

  // Infinite scroll sentinel
  const sentinelRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLElement>(null);
  useEffect(() => {
    const node = sentinelRef.current;
    if (!node) return;
    const obs = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && active.hasNextPage && !active.isFetchingNextPage) {
          active.fetchNextPage();
        }
      },
      { rootMargin: "400px" }
    );
    obs.observe(node);
    return () => obs.disconnect();
  }, [active]);

  const handleSignOut = async () => { await signOut(); navigate("/"); };
  const handleTab = (t: Tab) => { setTab(t); setFilterValues({}); setOpenFilter(null); };

  const handleExportView = () => {
    if (!allRows.length) return;
    const headers = Object.keys(allRows[0]);
    import("@/lib/audit").then(({ logWorkspaceEvent }) =>
      logWorkspaceEvent("export_triggered", null, { source_page: "dashboard", row_count: allRows.length, tab })
    );
    const filename = `${tab}-${Date.now()}.csv`;
    downloadCsv(filename, toCsv(allRows as Record<string, unknown>[] as never, headers));
    trackEvent("export_completed", { source_page: "dashboard", row_count: allRows.length, kind: tab });
    supabase.functions.invoke("send-export-notification", {
      body: { filename, rowCount: allRows.length, source: `dashboard (${tab})` },
    }).catch(() => {});
  };


  const filterDefs = tab === "journalists" ? JOURNALIST_FILTERS : CREATOR_FILTERS;

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Helmet><title>Dashboard — Media AI</title></Helmet>

      <AppHeader
        active="database"
        hideNav
        rightExtras={
          <Button
            variant="ghost"
            size="sm"
            className="font-medium text-sm px-3 py-2 h-auto rounded-full text-gray-700 hover:text-gray-900 hover:bg-gray-100"
            onClick={handleExportView}
            disabled={!allRows.length}
          >
            Export
          </Button>
        }
      />

      <div className="flex flex-1">
        <AppSidebar active="database" />

        <div className="flex flex-1 min-w-0">
        {sidebarCollapsed ? (
          <div className="border-r border-border bg-white flex-shrink-0">
            <button
              type="button"
              onClick={() => setSidebarCollapsed(false)}
              className="p-2 m-2 rounded-md hover:bg-secondary text-muted-foreground"
              title="Expand sidebar"
              aria-label="Expand sidebar"
            >
              <PanelLeftOpen className="h-4 w-4" />
            </button>
          </div>
        ) : (
        <aside className="w-64 border-r border-border bg-white flex flex-col flex-shrink-0 overflow-auto">
          <div className="p-3 space-y-1">
            <div className="flex justify-end -mt-1 -mr-1 mb-1">
              <button
                type="button"
                onClick={() => setSidebarCollapsed(true)}
                className="p-1 rounded-md hover:bg-secondary text-muted-foreground"
                title="Collapse sidebar"
                aria-label="Collapse sidebar"
              >
                <PanelLeftClose className="h-4 w-4" />
              </button>
            </div>
            <button type="button" onClick={() => handleTab("journalists")}
              className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${tab === "journalists" ? "bg-primary text-primary-foreground" : "text-foreground hover:bg-secondary"}`}>
              <Users className="h-4 w-4" />Journalists
            </button>
            <button type="button" onClick={() => handleTab("creators")}
              className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${tab === "creators" ? "bg-primary text-primary-foreground" : "text-foreground hover:bg-secondary"}`}>
              <Users className="h-4 w-4" />Creators
            </button>
          </div>


          <div className="px-3 pt-2 pb-3 border-t border-border">
            <div className="px-3 py-2 text-xs font-medium text-muted-foreground">Filters</div>
            <div className="space-y-0.5">
              {filterDefs.map(({ key, label, icon: Icon, placeholder, inputType }) => {
                const value = filterValues[key] ?? "";
                const isOpen = openFilter === key;
                const isActive = !!value.trim();
                return (
                  <div key={key}>
                    <button
                      type="button"
                      onClick={() => setOpenFilter(isOpen ? null : key)}
                      className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${isActive ? "text-primary font-medium" : "text-foreground"} hover:bg-secondary`}
                    >
                      <Icon className="h-4 w-4 flex-shrink-0" />
                      <span className="truncate text-left flex-1">{label}</span>
                      {isActive && (
                        <X
                          className="h-3.5 w-3.5 text-muted-foreground hover:text-destructive"
                          onClick={(e) => {
                            e.stopPropagation();
                            setFilterValues((f) => ({ ...f, [key]: "" }));
                          }}
                        />
                      )}
                      {isOpen ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
                    </button>
                    {isOpen && (
                      <div className="px-3 pb-2 pt-1 space-y-1.5">
                        {key === "authority_min" && (
                          <div className="flex flex-wrap gap-1">
                            {[90, 80, 70, 50, 30].map((n) => (
                              <button
                                key={n}
                                type="button"
                                onClick={() => setFilterValues((f) => ({ ...f, authority_min: String(n) }))}
                                className={`px-2 py-0.5 text-[11px] rounded border ${value === String(n) ? "bg-primary text-primary-foreground border-primary" : "bg-secondary border-border hover:bg-secondary/70"}`}
                              >
                                DR {n}+
                              </button>
                            ))}
                          </div>
                        )}
                        <Input
                          autoFocus
                          type={inputType ?? "text"}
                          inputMode={inputType === "number" ? "decimal" : undefined}
                          value={value}
                          onChange={(e) => setFilterValues((f) => ({ ...f, [key]: e.target.value }))}
                          placeholder={placeholder ?? label.replace(/^Search by |^Filter by |^Min /, "")}
                          className="h-8 text-sm"
                        />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </aside>
        )}

        <main ref={scrollContainerRef} className="flex-1 min-w-0 flex flex-col">
          <div className="p-3 border-b border-border bg-white flex items-center gap-3 sticky top-0 z-10">
            <div className="relative flex-1 max-w-md">
              <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder={tab === "journalists" ? "Search names, outlets, topics…" : "Search names, handles, bio…"}
                className="pl-9 h-9"
              />
            </div>
            {hasActiveFilters && (
              <span className="text-xs text-muted-foreground tabular-nums ml-auto">
                {allRows.length.toLocaleString()} of {total.toLocaleString()}
              </span>
            )}
          </div>

          {tab === "journalists" ? (
            <div className="min-w-[1250px]">
              <div className="border-b border-border bg-secondary/40 sticky top-[57px] z-10">
                <div className="grid grid-cols-[40px_minmax(180px,1.2fr)_minmax(240px,1.6fr)_150px_140px_160px_140px_160px_110px_120px] text-xs font-medium text-muted-foreground">
                  <div className="px-3 py-3 flex items-center">
                    <Checkbox
                      checked={allSelected ? true : someSelected ? "indeterminate" : false}
                      onCheckedChange={toggleSelectAll}
                      aria-label="Select all"
                    />
                  </div>
                  {JOURNALIST_COLS.map((c) => {
                    if (c === "Authority") {
                      const SortIcon = authoritySort === "desc" ? ArrowDown : authoritySort === "asc" ? ArrowUp : ArrowUpDown;
                      return (
                        <button
                          key={c}
                          type="button"
                          onClick={() => setAuthoritySort((s) => s === "none" ? "desc" : s === "desc" ? "asc" : "none")}
                          className="px-3 py-3 flex items-center gap-1 hover:text-foreground text-left"
                          title="Sort by Authority (Domain Rating)"
                        >
                          {c}
                          <SortIcon className="h-3 w-3" />
                        </button>
                      );
                    }
                    return <div key={c} className="px-3 py-3">{c}</div>;
                  })}
                </div>
              </div>
              {journalists.isLoading ? (
                <div className="p-12 flex justify-center"><Spinner size="lg" /></div>
              ) : journalists.error ? (
                <div className="p-8 text-center text-sm text-destructive">Failed to load journalists.</div>
              ) : allRows.length === 0 ? (
                <div className="p-12 text-center text-sm text-muted-foreground">No journalists match your search.</div>
              ) : (
                <>
                  {(allRows as any[]).map((r) => (
                    <div key={r.id} className={`group grid grid-cols-[40px_minmax(180px,1.2fr)_minmax(240px,1.6fr)_150px_140px_160px_140px_160px_110px_120px] border-b border-border hover:bg-secondary/30 ${selectedIds.has(r.id) ? "bg-primary/5" : ""}`}>
                      <div className="px-3 py-3 flex items-center">
                        <Checkbox
                          checked={selectedIds.has(r.id)}
                          onCheckedChange={() => toggleSelect(r.id)}
                          aria-label={`Select ${r.name ?? "row"}`}
                        />
                      </div>
                      <div className="px-3 py-3 text-sm flex items-center gap-2 min-w-0">
                        {r.name ? <button type="button" onClick={() => navigate(`/profiles/journalist/${r.id}`)} className="truncate text-left font-medium hover:text-primary hover:underline">{r.name}</button> : <span className="text-muted-foreground">—</span>}
                        <AddToListMenu journalistId={r.id} />
                      </div>
                      <EnrichCell value={r.email} kind="journalist" id={r.id} field="email" name={r.name} outletDomain={r.outlet} row={r} />
                      {r.linkedin_url ? (
                        <Cell><a href={r.linkedin_url} target="_blank" rel="noreferrer" className="text-primary hover:underline truncate block">LinkedIn</a></Cell>
                      ) : (
                        <EnrichCell value={null} kind="journalist" id={r.id} field="linkedin_url" name={r.name} outletDomain={r.outlet} row={r} />
                      )}
                      <EnrichCell value={r.category} kind="journalist" id={r.id} field="category" name={r.name} outletDomain={r.outlet} row={r} />
                      <EnrichCell value={r.titles} kind="journalist" id={r.id} field="titles" name={r.name} outletDomain={r.outlet} row={r} />
                      <EnrichCell value={r.xhandle} kind="journalist" id={r.id} field="xhandle" name={r.name} outletDomain={r.outlet} row={r} />
                      <EnrichCell value={r.outlet} kind="journalist" id={r.id} field="outlet" name={r.name} outletDomain={r.outlet} row={r} />
                      <div className="px-3 py-3 flex items-center">
                        <AuthorityBadge score={resolveAuthority(authorities.data, r.outlet)} />
                      </div>
                      <EnrichCell value={r.country} kind="journalist" id={r.id} field="country" name={r.name} outletDomain={r.outlet} row={r} />
                    </div>
                  ))}
                  <div ref={sentinelRef} className="h-12 flex items-center justify-center text-xs text-muted-foreground">
                    {active.isFetchingNextPage ? "Loading more…" : active.hasNextPage ? "Scroll for more" : "End of results"}
                  </div>
                </>
              )}
            </div>
          ) : (
            <div className="min-w-[1260px]">
              <div className="border-b border-border bg-secondary/40 sticky top-[57px] z-10">
                <div className="grid grid-cols-[40px_minmax(180px,1.2fr)_minmax(220px,1.3fr)_150px_160px_140px_140px_160px_minmax(160px,1fr)_120px_minmax(140px,1fr)] text-xs font-medium text-muted-foreground">
                  <div className="px-3 py-3 flex items-center">
                    <Checkbox
                      checked={allSelected ? true : someSelected ? "indeterminate" : false}
                      onCheckedChange={toggleSelectAll}
                      aria-label="Select all"
                    />
                  </div>
                  {CREATOR_COLS.map((c) => <div key={c} className="px-3 py-3">{c}</div>)}
                </div>
              </div>
              {creators.isLoading ? (
                <div className="p-12 flex justify-center"><Spinner size="lg" /></div>
              ) : creators.error ? (
                <div className="p-8 text-center text-sm text-destructive">Failed to load creators.</div>
              ) : allRows.length === 0 ? (
                <div className="p-12 text-center text-sm text-muted-foreground">No creators match your search.</div>
              ) : (
                <>
                  {(allRows as any[]).map((r) => (
                    <div key={r.id} className={`group grid grid-cols-[40px_minmax(180px,1.2fr)_minmax(220px,1.3fr)_150px_160px_140px_140px_160px_minmax(160px,1fr)_120px_minmax(140px,1fr)] border-b border-border hover:bg-secondary/30 ${selectedIds.has(r.id) ? "bg-primary/5" : ""}`}>
                      <div className="px-3 py-3 flex items-center">
                        <Checkbox
                          checked={selectedIds.has(r.id)}
                          onCheckedChange={() => toggleSelect(r.id)}
                          aria-label={`Select ${r.name ?? "row"}`}
                        />
                      </div>
                      <div className="px-3 py-3 text-sm flex items-center gap-2 min-w-0">
                        {r.name ? <button type="button" onClick={() => navigate(`/profiles/creator/${r.id}`)} className="truncate text-left font-medium hover:text-primary hover:underline">{r.name}</button> : <span className="text-muted-foreground">—</span>}
                        <AddToListMenu creatorId={r.id} />
                      </div>
                      <EnrichCell value={r.email} kind="creator" id={r.id} field="email" name={r.name} row={r} />
                      {r.linkedin_url ? (
                        <Cell><a href={r.linkedin_url} target="_blank" rel="noreferrer" className="text-primary hover:underline truncate block">LinkedIn</a></Cell>
                      ) : (
                        <EnrichCell value={null} kind="creator" id={r.id} field="linkedin_url" name={r.name} row={r} />
                      )}
                      <EnrichCell value={r.ig_handle} kind="creator" id={r.id} field="ig_handle" name={r.name} row={r} />
                      <EnrichCell value={r.ig_followers != null ? r.ig_followers.toLocaleString() : null} kind="creator" id={r.id} field="ig_followers" name={r.name} row={r} />
                      <EnrichCell value={r.ig_engagement_rate != null ? `${(r.ig_engagement_rate * 100).toFixed(2)}%` : null} kind="creator" id={r.id} field="ig_engagement_rate" name={r.name} row={r} />
                      <EnrichCell value={r.category} kind="creator" id={r.id} field="category" name={r.name} row={r} />
                      {r.youtube_url ? (
                        <Cell>
                          <a href={r.youtube_url} target="_blank" rel="noreferrer" className="text-primary hover:underline truncate block">
                            YouTube
                          </a>
                        </Cell>
                      ) : (
                        <EnrichCell value={null} kind="creator" id={r.id} field="youtube_url" name={r.name} row={r} />
                      )}
                      <EnrichCell value={r.youtube_subscribers != null ? r.youtube_subscribers.toLocaleString() : null} kind="creator" id={r.id} field="youtube_subscribers" name={r.name} row={r} />
                      <EnrichCell value={r.country} kind="creator" id={r.id} field="country" name={r.name} row={r} />
                    </div>
                  ))}
                  <div ref={sentinelRef} className="h-12 flex items-center justify-center text-xs text-muted-foreground">
                    {active.isFetchingNextPage ? "Loading more…" : active.hasNextPage ? "Scroll for more" : "End of results"}
                  </div>
                </>
              )}
            </div>
          )}
        </main>
        </div>
        <BulkAddToListBar
          count={selectedIds.size}
          journalistIds={tab === "journalists" ? Array.from(selectedIds) : undefined}
          creatorIds={tab === "creators" ? Array.from(selectedIds) : undefined}
          onClear={() => setSelectedIds(new Set())}
        />
      </div>
    </div>
  );
};

export default Dashboard;
