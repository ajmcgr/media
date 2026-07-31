import type { Insight } from "@/lib/blog/insights";
import { BarChart3, Lightbulb, Newspaper, TrendingUp, Users } from "lucide-react";

const ICONS = {
  "Journalist Insight": Newspaper,
  Trend: TrendingUp,
  "Pitch Tip": Lightbulb,
  "Media Stat": BarChart3,
  "Creator Insight": Users,
} as const;

const InsightCard = ({ insight }: { insight: Insight }) => {
  const Icon = ICONS[insight.kind] ?? Lightbulb;
  return (
    <figure className="not-prose my-8 rounded-xl border border-border bg-muted/30 p-5 flex gap-4">
      <span className="mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-md bg-primary/10 text-primary" aria-hidden="true">
        <Icon className="h-4 w-4" />
      </span>
      <div>
        <figcaption className="text-xs uppercase tracking-[0.16em] text-muted-foreground mb-1.5">
          {insight.kind} · Media AI
        </figcaption>
        <p className="text-sm leading-relaxed text-foreground">{insight.body}</p>
      </div>
    </figure>
  );
};

export default InsightCard;
