import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Check, ArrowRight, X, Sparkles } from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";
import { useLists } from "@/hooks/useLists";
import { useMonitors } from "@/hooks/useMonitor";
import { useTeamWorkspaces, useTeamMembers, useCurrentWorkspace } from "@/hooks/useTeams";
import { useSubscription } from "@/hooks/useSubscription";
import { isGrowthPlanIdentifier } from "@/lib/plans";

const DISMISS_KEY = "mediaai.onboarding.dismissed";
const FIRST_SEARCH_KEY = "mediaai.onboarding.firstSearch";

export function markFirstSearchComplete() {
  try { localStorage.setItem(FIRST_SEARCH_KEY, "1"); } catch { /* ignore */ }
  window.dispatchEvent(new CustomEvent("mediaai:onboarding-changed"));
}

type Step = {
  id: string;
  title: string;
  description: string;
  cta: string;
  href: string;
  done: boolean;
};

export default function OnboardingChecklist() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const { planIdentifier } = useSubscription();
  const hasGrowth = isGrowthPlanIdentifier(planIdentifier);
  const { data: lists } = useLists(user?.id);
  const { data: monitors } = useMonitors();
  const { data: workspaces } = useTeamWorkspaces(user?.id);
  const { workspace } = useCurrentWorkspace(workspaces);
  const { data: members } = useTeamMembers(workspace?.id);

  const [dismissed, setDismissed] = useState<boolean>(() => {
    try { return localStorage.getItem(DISMISS_KEY) === "1"; } catch { return false; }
  });
  const [firstSearch, setFirstSearch] = useState<boolean>(() => {
    try { return localStorage.getItem(FIRST_SEARCH_KEY) === "1"; } catch { return false; }
  });

  useEffect(() => {
    const handler = () => {
      try {
        setFirstSearch(localStorage.getItem(FIRST_SEARCH_KEY) === "1");
        setDismissed(localStorage.getItem(DISMISS_KEY) === "1");
      } catch { /* ignore */ }
    };
    window.addEventListener("mediaai:onboarding-changed", handler);
    window.addEventListener("storage", handler);
    return () => {
      window.removeEventListener("mediaai:onboarding-changed", handler);
      window.removeEventListener("storage", handler);
    };
  }, []);

  const steps: Step[] = useMemo(() => {
    const coreSteps: Step[] = [{
      id: "search",
      title: "Run your first AI search",
      description: "Ask Media AI to find journalists or creators in plain English.",
      cta: "Start Search",
      href: "/search",
      done: firstSearch,
    }, {
      id: "list",
      title: "Save contacts to a list",
      description: "Build a shortlist you can share or export anytime.",
      cta: hasGrowth ? "Browse database" : "Open Search",
      href: hasGrowth ? "/database" : "/search",
      done: (lists?.length ?? 0) > 0,
    }];
    if (!hasGrowth) return coreSteps;
    return [...coreSteps, {
      id: "monitor",
      title: "Set up a keyword monitor",
      description: "Track brands, founders, and competitors across Google News.",
      cta: "Open Monitor",
      href: "/monitor",
      done: (monitors?.length ?? 0) > 0,
    }, {
      id: "team",
      title: "Invite a teammate",
      description: "Collaborate on lists and outreach in a shared workspace.",
      cta: "Open Team",
      href: "/team",
      done: (members?.length ?? 0) > 1 || (workspaces?.length ?? 0) > 1,
    }];
  }, [firstSearch, hasGrowth, lists, monitors, members, workspaces]);

  const completed = steps.filter((s) => s.done).length;
  const total = steps.length;
  const allDone = completed === total;

  if (dismissed || allDone) return null;

  const dismiss = () => {
    try { localStorage.setItem(DISMISS_KEY, "1"); } catch { /* ignore */ }
    setDismissed(true);
  };

  return (
    <div className="rounded-2xl border border-border bg-white p-5 mb-6 text-left">
      <div className="flex items-start justify-between gap-3 mb-4">
        <div className="flex items-start gap-3">
          <div className="inline-flex items-center justify-center h-8 w-8 rounded-lg bg-primary/10 text-primary flex-shrink-0">
            <Sparkles className="h-4 w-4" />
          </div>
          <div>
            <h2 className="text-sm font-medium">Get started with Media AI</h2>
            <p className="text-xs text-muted-foreground mt-0.5">
              {completed} of {total} complete — finish setup to get the most out of Media AI.
            </p>
          </div>
        </div>
        <button
          type="button"
          onClick={dismiss}
          className="p-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-secondary"
          aria-label="Dismiss onboarding"
          title="Dismiss"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="h-1 w-full rounded-full bg-secondary overflow-hidden mb-4">
        <div
          className="h-full bg-primary transition-all"
          style={{ width: `${(completed / total) * 100}%` }}
        />
      </div>

      <ul className="space-y-2">
        {steps.map((s) => (
          <li
            key={s.id}
            className={`flex items-center gap-3 rounded-lg border border-border px-3 py-2.5 ${s.done ? "bg-secondary/40" : "bg-white"}`}
          >
            <span
              className={`inline-flex items-center justify-center h-5 w-5 rounded-full flex-shrink-0 ${
                s.done ? "bg-primary text-primary-foreground" : "border border-border bg-white"
              }`}
            >
              {s.done && <Check className="h-3 w-3" />}
            </span>
            <div className="flex-1 min-w-0">
              <div className={`text-sm ${s.done ? "text-muted-foreground line-through" : "font-medium"}`}>
                {s.title}
              </div>
              {!s.done && (
                <div className="text-xs text-muted-foreground mt-0.5">{s.description}</div>
              )}
            </div>
            {!s.done && (
              <button
                type="button"
                onClick={() => navigate(s.href)}
                className="text-xs font-medium text-primary hover:underline inline-flex items-center gap-1 flex-shrink-0"
              >
                {s.cta}<ArrowRight className="h-3 w-3" />
              </button>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
