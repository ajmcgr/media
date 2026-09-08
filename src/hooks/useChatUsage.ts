import { useCallback, useEffect, useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/contexts/AuthContext";

export interface ChatUsage {
  allowance: number;
  used: number;
  remaining: number;
  credits: number;
  period_ym: string;
}

const currentPeriod = () => new Date().toISOString().slice(0, 7);
const toNumber = (value: unknown) => Number(value ?? 0) || 0;

export const useChatUsage = () => {
  const { user } = useAuth();
  const [usage, setUsage] = useState<ChatUsage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async (forceDirect = false) => {
    if (!user) { setUsage(null); setError(null); setLoading(false); return; }
    setLoading(true);
    setError(null);
    if (forceDirect) {
      const fallback = await loadUsageFallback(user.id);
      setUsage(fallback);
      setError(fallback ? null : "Could not load search credits.");
      setLoading(false);
      return;
    }
    const { data, error } = await supabase.rpc("chat_usage_summary");
    if (!error && Array.isArray(data) && data[0]) {
      const row = data[0] as Record<string, unknown>;
      const rpcUsage = {
        allowance: toNumber(row.allowance),
        used: toNumber(row.used),
        remaining: toNumber(row.remaining),
        credits: toNumber(row.credits),
        period_ym: String(row.period_ym ?? ""),
      };
      setUsage(await loadUsageFallback(user.id, rpcUsage) ?? rpcUsage);
    } else if (error) {
      console.error("chat_usage_summary failed", error);
      const fallback = await loadUsageFallback(user.id);
      if (fallback) {
        setUsage(fallback);
        setError(null);
      } else {
        setUsage(null);
        setError(error.message || "Could not load search credits.");
      }
    }
    setLoading(false);
  }, [user]);

  useEffect(() => { refresh(); }, [refresh]);

  useEffect(() => {
    if (!user) return;

    const channel = supabase
      .channel(`chat-usage-${user.id}`)
      .on("postgres_changes", { event: "*", schema: "public", table: "profiles", filter: `id=eq.${user.id}` }, () => refresh(true))
      .on("postgres_changes", { event: "*", schema: "public", table: "chat_usage", filter: `user_id=eq.${user.id}` }, () => refresh(true))
      .on("postgres_changes", { event: "*", schema: "public", table: "topup_transactions", filter: `user_id=eq.${user.id}` }, () => refresh(true))
      .subscribe();

    return () => { supabase.removeChannel(channel); };
  }, [refresh, user]);

  const applyServerUsage = useCallback((u: Partial<ChatUsage> | null | undefined) => {
    if (!u) return;
    setUsage((prev) => ({
      allowance: Number(u.allowance ?? prev?.allowance ?? 0),
      used: Number(u.used ?? prev?.used ?? 0),
      remaining: Number(u.remaining ?? prev?.remaining ?? 0),
      credits: Number(u.credits ?? prev?.credits ?? 0),
      period_ym: String(u.period_ym ?? prev?.period_ym ?? ""),
    }));
  }, []);

  return { usage, loading, error, refresh, applyServerUsage };
};

async function loadUsageFallback(userId: string, base?: ChatUsage): Promise<ChatUsage | null> {
  const period = currentPeriod();
  const [profileResult, usageResult, topupResult] = await Promise.all([
    supabase
      .from("profiles")
      .select("chat_credits, sub_active, plan_identifier")
      .eq("id", userId)
      .maybeSingle(),
    supabase
      .from("chat_usage")
      .select("tokens_used")
      .eq("user_id", userId)
      .eq("period_ym", period)
      .maybeSingle(),
    supabase
      .from("topup_transactions")
      .select("tokens")
      .eq("user_id", userId),
  ]);

  if (profileResult.error && !base) return null;

  const profile = profileResult.data as {
    chat_credits?: number | string | null;
    sub_active?: boolean | null;
    plan_identifier?: string | null;
  } | null;
  const plan = String(profile?.plan_identifier ?? "").toLowerCase();
  const allowance = base?.allowance ?? (profile?.sub_active
    ? (["growth", "both", "media-pro", "pro", "enterprise"].includes(plan) ? 1_000_000 : 200_000)
    : 5_000);
  const used = base?.used ?? toNumber((usageResult.data as { tokens_used?: number | string } | null)?.tokens_used);
  const profileCredits = Math.max(
    toNumber(base?.credits),
    toNumber(profile?.chat_credits)
  );
  const ledgerPurchased = Array.isArray(topupResult.data)
    ? (topupResult.data as Array<{ tokens?: number | string }>).reduce((sum, row) => sum + toNumber(row.tokens), 0)
    : 0;
  const ledgerCredits = Math.max(ledgerPurchased - Math.max(used - allowance, 0), 0);
  const credits = Math.max(profileCredits, ledgerCredits);
  const monthlyRemaining = Math.max(allowance - used, 0);

  return {
    allowance,
    used,
    credits,
    remaining: Math.max(base?.remaining ?? 0, monthlyRemaining + credits),
    period_ym: base?.period_ym || period,
  };
}
