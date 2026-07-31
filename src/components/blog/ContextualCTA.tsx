import { Link } from "react-router-dom";
import { ArrowRight } from "lucide-react";
import { CTAS, CtaKind } from "@/lib/blog/categories";

const ContextualCTA = ({ kind }: { kind: CtaKind }) => {
  const cta = CTAS[kind];
  return (
    <section className="rounded-2xl border border-border bg-muted/30 p-8">
      <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground mb-3">{cta.eyebrow}</p>
      <h2 className="text-2xl font-medium tracking-tight text-foreground mb-3">{cta.headline}</h2>
      <p className="text-muted-foreground mb-6 max-w-xl">{cta.body}</p>
      <div className="flex flex-wrap gap-3">
        <Link
          to={cta.href}
          className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2.5 text-sm font-medium text-primary-foreground hover:opacity-90 transition-opacity"
        >
          {cta.action}
          <ArrowRight className="h-4 w-4" />
        </Link>
        <Link
          to="/signup"
          className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-4 py-2.5 text-sm font-medium text-foreground hover:bg-muted transition-colors"
        >
          Start free
        </Link>
      </div>
    </section>
  );
};

export default ContextualCTA;
