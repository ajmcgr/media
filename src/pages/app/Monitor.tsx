import { useMemo, useState } from "react";
import { Helmet } from "react-helmet-async";
import { NavLink, useNavigate } from "react-router-dom";
import {
  Plus, Pencil, Trash2, RefreshCw, Bell, BellOff, Database, MessageSquare, ExternalLink,
  Globe, CalendarClock, Mail, Users, ChevronDown,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Spinner } from "@/components/ui/spinner";
import { useAuth } from "@/contexts/AuthContext";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import logoMedia from "@/assets/brand/logo-media-blue.png";
import {
  useMonitors, useUpdates, useCreateMonitor, useDeleteMonitor,
  useUpdateMonitor, useRunMonitorCheck, type AlertFrequency, type BrandMonitor,
  type MonitorUpdate,
} from "@/hooks/useMonitor";
import { toast } from "sonner";
import { trackEvent } from "@/lib/analytics";
import { InboxSheet } from "@/components/dashboard/InboxSheet";
import { ListsSheet } from "@/components/dashboard/ListsSheet";
import AppHeader from "@/components/AppHeader";
import AppSidebar from "@/components/AppSidebar";
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip,
  PieChart, Pie, Cell, BarChart, Bar,
} from "recharts";

const GoogleBadge = ({ className = "" }: { className?: string }) => (
  <span className={`inline-flex items-center gap-1.5 text-[11px] text-muted-foreground ${className}`}>
    <img src="/google-news.svg" alt="" className="h-3.5 w-3.5" />
    Powered by Google News
  </span>
);

const TYPE_COLORS: Record<string, string> = {
  brand: "bg-blue-50 text-blue-700 border-blue-200",
  competitor: "bg-orange-50 text-orange-700 border-orange-200",
  founder: "bg-purple-50 text-purple-700 border-purple-200",
  keyword: "bg-emerald-50 text-emerald-700 border-emerald-200",
  product: "bg-pink-50 text-pink-700 border-pink-200",
};

const SENTIMENT_COLORS: Record<string, string> = {
  positive: "bg-emerald-50 text-emerald-700 border-emerald-200",
  neutral: "bg-secondary text-muted-foreground border-border",
  negative: "bg-red-50 text-red-700 border-red-200",
};

const Badge = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <span className={`text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded border ${className}`}>
    {children}
  </span>
);

const Monitor = () => {
  const { user, signOut } = useAuth();
  const navigate = useNavigate();
  const initials = (user?.email ?? "?").slice(0, 2).toUpperCase();

  const monitors = useMonitors();
  const updates = useUpdates();
  const createMon = useCreateMonitor();
  const deleteMon = useDeleteMonitor();
  const updateMon = useUpdateMonitor();
  const runCheck = useRunMonitorCheck();

  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [range, setRange] = useState<7 | 30>(7);
  const [debugFor, setDebugFor] = useState<string | null>(null);
  const emptyForm = {
    brand_name: "",
    website_url: "",
    competitor_urls: "",
    keywords: "",
    founder_names: "",
    product_names: "",
    email_alerts: true,
    alert_frequency: "daily" as AlertFrequency,
  };
  const [form, setForm] = useState(emptyForm);

  const openCreate = () => {
    setEditingId(null);
    setForm(emptyForm);
    setOpen(true);
  };

  const openEdit = (m: BrandMonitor) => {
    setEditingId(m.id);
    setForm({
      brand_name: m.brand_name ?? "",
      website_url: m.website_url ?? "",
      competitor_urls: (m.competitor_urls ?? []).join(", "),
      keywords: (m.keywords ?? []).join(", "),
      founder_names: (m.founder_names ?? []).join(", "),
      product_names: (m.product_names ?? []).join(", "),
      email_alerts: m.email_alerts,
      alert_frequency: m.alert_frequency,
    });
    setOpen(true);
  };

  const monitorById = useMemo(() => {
    const map = new Map<string, BrandMonitor>();
    (monitors.data ?? []).forEach((m) => map.set(m.id, m));
    return map;
  }, [monitors.data]);

  const allUpdates = useMemo(() => updates.data ?? [], [updates.data]);

  const cutoff = useMemo(() => Date.now() - range * 24 * 60 * 60 * 1000, [range]);
  const inRange = useMemo(
    () => allUpdates.filter((u) => new Date(u.published_at ?? u.detected_at).getTime() >= cutoff),
    [allUpdates, cutoff],
  );

  const stats = useMemo(() => {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const todayMs = today.getTime();
    const total = inRange.length;
    let mentionsToday = 0, competitor = 0, positive = 0, negative = 0;
    for (const u of inRange) {
      const ts = new Date(u.published_at ?? u.detected_at).getTime();
      if (ts >= todayMs) mentionsToday++;
      if (u.mention_type === "competitor") competitor++;
      if (u.sentiment === "positive") positive++;
      if (u.sentiment === "negative") negative++;
    }
    return { total, mentionsToday, competitor, positive, negative };
  }, [inRange]);

  const trendData = useMemo(() => {
    const buckets = new Map<string, number>();
    for (let i = range - 1; i >= 0; i--) {
      const d = new Date(); d.setHours(0, 0, 0, 0); d.setDate(d.getDate() - i);
      buckets.set(d.toISOString().slice(5, 10), 0);
    }
    for (const u of inRange) {
      const k = new Date(u.published_at ?? u.detected_at).toISOString().slice(5, 10);
      if (buckets.has(k)) buckets.set(k, (buckets.get(k) ?? 0) + 1);
    }
    return Array.from(buckets, ([day, count]) => ({ day, count }));
  }, [inRange, range]);

  const sourceData = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const u of inRange) {
      const k = u.source ?? "other";
      counts[k] = (counts[k] ?? 0) + 1;
    }
    return Object.entries(counts).map(([name, value]) => ({ name, value }));
  }, [inRange]);

  const sentimentData = useMemo(() => {
    const counts = { positive: 0, neutral: 0, negative: 0 };
    for (const u of inRange) {
      const k = (u.sentiment ?? "neutral") as keyof typeof counts;
      counts[k] = (counts[k] ?? 0) + 1;
    }
    return [
      { name: "Positive", value: counts.positive, fill: "#10b981" },
      { name: "Neutral", value: counts.neutral, fill: "#94a3b8" },
      { name: "Negative", value: counts.negative, fill: "#ef4444" },
    ];
  }, [inRange]);

  const PIE_COLORS = ["#1675e2", "#10b981", "#f59e0b", "#a855f7", "#94a3b8"];

  const splitList = (s: string) =>
    s.split(/[\n,]+/).map((x) => x.trim()).filter(Boolean);

  const handleSubmit = async () => {
    if (!form.brand_name.trim()) {
      toast.error("Brand or topic name is required");
      return;
    }
    const payload = {
      brand_name: form.brand_name.trim(),
      website_url: form.website_url.trim() || `https://${form.brand_name.trim().toLowerCase().replace(/\s+/g, "")}.com`,
      competitor_urls: splitList(form.competitor_urls),
      keywords: splitList(form.keywords),
      founder_names: splitList(form.founder_names),
      product_names: splitList(form.product_names),
      email_alerts: form.email_alerts,
      alert_frequency: form.alert_frequency,
    };
    try {
      if (editingId) {
        await updateMon.mutateAsync({ id: editingId, patch: payload as Partial<BrandMonitor> });
        toast.success("Monitor updated");
      } else {
        await createMon.mutateAsync(payload);
        trackEvent("monitor_created", { frequency: payload.alert_frequency });
        toast.success("Keyword monitor added");
      }
      setOpen(false);
      setEditingId(null);
      setForm(emptyForm);
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const handleRunCheck = async (id: string) => {
    console.log("MONITOR_RUN_CHECK_CLICKED", id);
    const payload = { monitor_id: id };
    console.log("MONITOR_RUN_CHECK_PAYLOAD", payload);
    try {
      const res = await runCheck.mutateAsync(id);
      console.log("MONITOR_RUN_CHECK_RESPONSE", res);
      const inserted = (res as { inserted?: number } | undefined)?.inserted ?? 0;
      if (inserted > 0) toast.success(`Found ${inserted} new mention${inserted === 1 ? "" : "s"}`);
      else toast("No new mentions found.");
    } catch (e) {
      console.error("MONITOR_RUN_CHECK_ERROR", e);
      toast.error((e as Error).message);
    }
  };

  const handleSignOut = async () => { await signOut(); navigate("/"); };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Helmet><title>Keyword Monitor — Media AI</title></Helmet>

      <AppHeader active="monitor" hideNav />

      <div className="flex flex-1">
        <AppSidebar active="monitor" />

        <main className="flex-1 min-w-0">
        <div className="max-w-6xl mx-auto p-6 space-y-8">
          {/* Title + trust row */}
          <section>
            <div className="flex items-start justify-between mb-2 gap-4">
              <div>
                <div className="flex items-center gap-2 flex-wrap">
                  <h1 className="text-2xl font-medium" style={{ fontFamily: "var(--font-heading)" }}>
                    Keyword Monitor
                  </h1>
                  <GoogleBadge />
                </div>
                <p className="text-sm text-muted-foreground mt-1">
                  Track brands, founders, competitors, products, and keywords across Google News.
                </p>
                <div className="mt-3 flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
                  <span className="inline-flex items-center gap-1.5"><Globe className="h-3 w-3" />Google News</span>
                  <span className="inline-flex items-center gap-1.5"><CalendarClock className="h-3 w-3" />Daily monitoring</span>
                  <span className="inline-flex items-center gap-1.5"><Mail className="h-3 w-3" />Email alerts</span>
                  <span className="inline-flex items-center gap-1.5"><Users className="h-3 w-3" />Competitor tracking</span>
                </div>
              </div>
              <Dialog open={open} onOpenChange={(v) => { setOpen(v); if (!v) { setEditingId(null); setForm(emptyForm); } }}>
                <Button size="sm" className="gap-1.5" onClick={openCreate}><Plus className="h-4 w-4" />Add monitor</Button>
                <DialogContent className="max-w-lg">
                  <DialogHeader><DialogTitle>{editingId ? "Edit keyword monitor" : "Add a keyword monitor"}</DialogTitle></DialogHeader>
                  <div className="space-y-3 max-h-[70vh] overflow-y-auto pr-1">
                    <div>
                      <Label>Brand or topic name</Label>
                      <Input value={form.brand_name} onChange={(e) => setForm({ ...form, brand_name: e.target.value })} placeholder="Acme Inc." />
                    </div>
                    <div>
                      <Label>Website URL (optional)</Label>
                      <Input value={form.website_url} onChange={(e) => setForm({ ...form, website_url: e.target.value })} placeholder="https://acme.com" />
                    </div>
                    <div>
                      <Label>Founder names (comma or newline)</Label>
                      <Textarea rows={2} value={form.founder_names} onChange={(e) => setForm({ ...form, founder_names: e.target.value })} placeholder="Jane Doe, John Smith" />
                    </div>
                    <div>
                      <Label>Competitors (URLs or names)</Label>
                      <Textarea rows={2} value={form.competitor_urls} onChange={(e) => setForm({ ...form, competitor_urls: e.target.value })} placeholder="competitor1.com, competitor2.com" />
                    </div>
                    <div>
                      <Label>Keywords</Label>
                      <Textarea rows={2} value={form.keywords} onChange={(e) => setForm({ ...form, keywords: e.target.value })} placeholder="funding, launch, partnership" />
                    </div>
                    <div>
                      <Label>Products</Label>
                      <Textarea rows={2} value={form.product_names} onChange={(e) => setForm({ ...form, product_names: e.target.value })} placeholder="Product A, Product B" />
                    </div>
                    <div className="flex items-center justify-between">
                      <div>
                        <Label>Email alerts</Label>
                        <p className="text-xs text-muted-foreground">Re-engagement emails when mentions are detected.</p>
                      </div>
                      <Switch checked={form.email_alerts} onCheckedChange={(v) => setForm({ ...form, email_alerts: v })} />
                    </div>
                    <div>
                      <Label>Frequency</Label>
                      <Select value={form.alert_frequency} onValueChange={(v) => setForm({ ...form, alert_frequency: v as AlertFrequency })}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="instant">Instant (priority alerts)</SelectItem>
                          <SelectItem value="daily">Daily digest</SelectItem>
                          <SelectItem value="weekly">Weekly digest</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
                    <Button onClick={handleSubmit} disabled={createMon.isPending || updateMon.isPending}>
                      {editingId
                        ? (updateMon.isPending ? "Saving…" : "Save changes")
                        : (createMon.isPending ? "Adding…" : "Add monitor")}
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>
          </section>

          {/* KPI cards */}
          <section className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {[
              { label: "Total mentions", value: stats.total },
              { label: "Mentions today", value: stats.mentionsToday },
              { label: "Competitor", value: stats.competitor },
              { label: "Positive", value: stats.positive },
              { label: "Negative", value: stats.negative },
            ].map((k) => (
              <div key={k.label} className="rounded-lg border border-border bg-white p-3">
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground">{k.label}</div>
                <div className="text-xl font-medium mt-1">{k.value}</div>
              </div>
            ))}
          </section>

          {/* Charts */}
          <section>
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-medium text-muted-foreground">Trends</h2>
              <div className="flex items-center gap-1 text-xs">
                <button
                  onClick={() => setRange(7)}
                  className={`px-2 py-1 rounded ${range === 7 ? "bg-secondary text-foreground" : "text-muted-foreground"}`}
                >7d</button>
                <button
                  onClick={() => setRange(30)}
                  className={`px-2 py-1 rounded ${range === 30 ? "bg-secondary text-foreground" : "text-muted-foreground"}`}
                >30d</button>
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div className="rounded-lg border border-border bg-white p-3 h-48">
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground mb-2">Mentions over time</div>
                <ResponsiveContainer width="100%" height="85%">
                  <LineChart data={trendData}>
                    <XAxis dataKey="day" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} allowDecimals={false} />
                    <Tooltip />
                    <Line type="monotone" dataKey="count" stroke="#1675e2" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
              <div className="rounded-lg border border-border bg-white p-3 h-48">
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground mb-2">Source breakdown</div>
                <ResponsiveContainer width="100%" height="85%">
                  <PieChart>
                    <Pie data={sourceData} dataKey="value" nameKey="name" outerRadius={55}>
                      {sourceData.map((_, i) => <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />)}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="rounded-lg border border-border bg-white p-3 h-48">
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground mb-2">Sentiment</div>
                <ResponsiveContainer width="100%" height="85%">
                  <BarChart data={sentimentData}>
                    <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="value" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </section>

          {/* Monitors list */}
          <section>
            <h2 className="text-sm font-medium text-muted-foreground mb-3">Your monitors</h2>
            <div className="rounded-lg border border-border bg-white overflow-hidden">
              {monitors.isLoading ? (
                <div className="p-8 flex justify-center"><Spinner /></div>
              ) : (monitors.data?.length ?? 0) === 0 ? (
                <div className="p-12 text-center text-sm text-muted-foreground">
                  <div className="mb-2 flex justify-center"><img src="/google-news.svg" alt="" className="h-6 w-6 opacity-60" /></div>
                  No monitors yet. Add a brand, competitor, or keyword to start tracking Google News.
                </div>
              ) : (
                <div className="divide-y divide-border">
                  {monitors.data!.map((m) => (
                    <div key={m.id} className="p-4">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="font-medium">{m.brand_name}</span>
                            {m.website_url && (
                              <a href={m.website_url} target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 truncate">
                                {m.website_url}<ExternalLink className="h-3 w-3" />
                              </a>
                            )}
                          </div>
                          <div className="text-xs text-muted-foreground mt-1">
                            {(m.founder_names?.length ?? 0)} founder{(m.founder_names?.length ?? 0) === 1 ? "" : "s"} • {m.competitor_urls.length} competitor{m.competitor_urls.length === 1 ? "" : "s"} • {m.keywords.length} keyword{m.keywords.length === 1 ? "" : "s"} • {(m.product_names?.length ?? 0)} product{(m.product_names?.length ?? 0) === 1 ? "" : "s"} • {m.alert_frequency}
                            {m.last_checked_at && <> • last checked {new Date(m.last_checked_at).toLocaleString()}</>}
                          </div>
                        </div>
                        <div className="flex items-center gap-2 flex-shrink-0">
                          <button
                            type="button"
                            onClick={() => updateMon.mutate({ id: m.id, patch: { email_alerts: !m.email_alerts } })}
                            className="text-muted-foreground hover:text-foreground"
                            title={m.email_alerts ? "Disable email alerts" : "Enable email alerts"}
                          >
                            {m.email_alerts ? <Bell className="h-4 w-4" /> : <BellOff className="h-4 w-4" />}
                          </button>
                          <Button
                            variant="outline" size="sm"
                            disabled={runCheck.isPending}
                            onClick={() => handleRunCheck(m.id)}
                          >
                            <RefreshCw className={`h-3.5 w-3.5 mr-1 ${runCheck.isPending ? "animate-spin" : ""}`} />Run check
                          </Button>
                          <button
                            type="button"
                            onClick={() => openEdit(m)}
                            className="text-muted-foreground hover:text-foreground"
                            title="Edit monitor"
                          >
                            <Pencil className="h-4 w-4" />
                          </button>
                          <button
                            type="button"
                            onClick={() => setDebugFor(debugFor === m.id ? null : m.id)}
                            className="text-muted-foreground hover:text-foreground"
                            title="Debug info"
                          >
                            <ChevronDown className={`h-4 w-4 transition-transform ${debugFor === m.id ? "rotate-180" : ""}`} />
                          </button>
                          <button
                            type="button"
                            onClick={() => {
                              if (confirm(`Delete monitor for ${m.brand_name}?`)) deleteMon.mutate(m.id);
                            }}
                            className="text-muted-foreground hover:text-destructive"
                            title="Delete"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      </div>
                      {debugFor === m.id && (
                        <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-2 text-[11px] bg-secondary/40 rounded p-2">
                          <div><span className="text-muted-foreground">Last check:</span> {m.last_checked_at ? new Date(m.last_checked_at).toLocaleString() : "—"}</div>
                          <div><span className="text-muted-foreground">Status:</span> {m.last_status ?? "—"}</div>
                          <div><span className="text-muted-foreground">Found:</span> {m.last_mentions_found ?? 0}</div>
                          <div className="truncate"><span className="text-muted-foreground">Error:</span> {m.last_error ?? "none"}</div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </section>

          {/* Updates */}
          <section>
            <h2 className="text-lg font-medium mb-3" style={{ fontFamily: "var(--font-heading)" }}>Updates</h2>
            <div className="rounded-lg border border-border bg-white overflow-hidden">
              {updates.isLoading ? (
                <div className="p-8 flex justify-center"><Spinner /></div>
              ) : allUpdates.length === 0 ? (
                <div className="p-12 text-center text-sm text-muted-foreground">
                  <div className="mb-2 flex justify-center"><img src="/google-news.svg" alt="" className="h-6 w-6 opacity-60" /></div>
                  No mentions yet. Click <strong>Run check</strong> on a monitor or wait for the daily scan.
                </div>
              ) : (
                <div className="divide-y divide-border">
                  {allUpdates.map((u: MonitorUpdate) => {
                    const m = monitorById.get(u.monitor_id);
                    const title = u.title ?? u.summary ?? "Mention";
                    const ts = u.published_at ?? u.detected_at;
                    return (
                      <div key={u.id} className="p-4 flex items-start gap-3">
                        {u.image_url && (
                          <img src={u.image_url} alt="" className="h-14 w-14 rounded object-cover flex-shrink-0 hidden sm:block" />
                        )}
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="font-medium text-sm">{m?.brand_name ?? "Mention"}</span>
                            {u.mention_type && (
                              <Badge className={TYPE_COLORS[u.mention_type] ?? "bg-secondary text-muted-foreground border-border"}>
                                {u.mention_type}
                              </Badge>
                            )}
                            {u.sentiment && (
                              <Badge className={SENTIMENT_COLORS[u.sentiment] ?? ""}>{u.sentiment}</Badge>
                            )}
                            <Badge className="bg-secondary text-muted-foreground border-border inline-flex items-center gap-1">
                              <img src="/google-news.svg" alt="" className="h-3 w-3" />
                              {u.source === "google_news" ? "Google News" : (u.source ?? "source")}
                            </Badge>
                          </div>
                          <a href={u.url} target="_blank" rel="noreferrer" className="block mt-1 text-sm text-foreground hover:text-primary">
                            {title}
                          </a>
                          <div className="text-xs text-muted-foreground mt-1 flex items-center gap-2 flex-wrap">
                            {u.publisher && <span>{u.publisher}</span>}
                            {u.matched_keyword && <span>· "{u.matched_keyword}"</span>}
                            <span>· {new Date(ts).toLocaleString()}</span>
                            <a href={u.url} target="_blank" rel="noreferrer" className="text-primary inline-flex items-center gap-0.5">
                              Open<ExternalLink className="h-3 w-3" />
                            </a>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </section>
        </div>
      </main>
      </div>
    </div>
  );
};

export default Monitor;
