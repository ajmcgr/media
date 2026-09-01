import { useCallback, useEffect, useLayoutEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useLocation, useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Sparkles, X, ArrowRight } from "lucide-react";

const STORAGE_KEY = "mediaai_product_tour_v2";
const START_EVENT = "mediaai:start-tour";

type Step = {
  id: string;
  title: string;
  body: string;
  route?: string;
  target?: string; // [data-tour="..."] selector
  requireGrowth?: boolean;
};

const BASE_STEPS: Step[] = [
  {
    id: "welcome",
    title: "Welcome to Media AI",
    body: "Run one guided search to see how Media AI finds relevant journalists and creators.",
    route: "/search",
  },
  {
    id: "search",
    title: "Ask in plain English",
    body: "Describe who you want to reach — e.g. \"tech journalists in the UK who cover AI startups\" — and we'll surface verified contacts.",
    route: "/search",
    target: "search-input",
  },
  {
    id: "done",
    title: "You're all set",
    body: "Describe who you need, review the ranked matches, then save the strongest contacts to a list.",
    route: "/search",
  },
];


function getTargetRect(selector?: string): DOMRect | null {
  if (!selector) return null;
  const el = document.querySelector(`[data-tour="${selector}"]`) as HTMLElement | null;
  if (!el) return null;
  return el.getBoundingClientRect();
}

export default function ProductTour() {
  const navigate = useNavigate();
  const location = useLocation();

  const steps = BASE_STEPS;

  const [open, setOpen] = useState(false);
  const [stepIdx, setStepIdx] = useState(0);
  const [rect, setRect] = useState<DOMRect | null>(null);
  const [tick, setTick] = useState(0);

  // Manual replay via event
  useEffect(() => {
    const handler = () => {
      setStepIdx(0);
      setOpen(true);
    };
    window.addEventListener(START_EVENT, handler);
    return () => window.removeEventListener(START_EVENT, handler);
  }, []);

  const step = steps[stepIdx];

  // Navigate if step has a different route
  useEffect(() => {
    if (!open || !step) return;
    if (step.route && location.pathname !== step.route) {
      navigate(step.route);
    }
  }, [open, step, location.pathname, navigate]);

  // Recompute target rect on step / route / resize / scroll
  useLayoutEffect(() => {
    if (!open || !step) return;
    let raf = 0;
    const update = () => {
      setRect(getTargetRect(step.target));
    };
    // Wait a bit for navigation/render
    const t = setTimeout(() => {
      update();
      raf = requestAnimationFrame(update);
    }, 120);
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    return () => {
      clearTimeout(t);
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [open, step, location.pathname, tick]);

  // Force-refresh rect once after small delay (catches late mounts)
  useEffect(() => {
    if (!open) return;
    const t = setTimeout(() => setTick((n) => n + 1), 400);
    return () => clearTimeout(t);
  }, [open, stepIdx]);

  const finish = useCallback(() => {
    try {
      localStorage.setItem(STORAGE_KEY, "1");
    } catch {
      // Closing the tour should still work when storage is unavailable.
    }
    setOpen(false);
  }, []);

  const next = useCallback(() => {
    if (stepIdx >= steps.length - 1) {
      finish();
      return;
    }
    setStepIdx((i) => i + 1);
  }, [stepIdx, steps.length, finish]);

  const back = useCallback(() => {
    setStepIdx((i) => Math.max(0, i - 1));
  }, []);

  if (!open || !step) return null;

  const PAD = 8;
  const hasTarget = !!rect;
  const spot = rect
    ? {
        top: Math.max(0, rect.top - PAD),
        left: Math.max(0, rect.left - PAD),
        width: rect.width + PAD * 2,
        height: rect.height + PAD * 2,
      }
    : null;

  // Position the card near the target, else center
  let cardStyle: React.CSSProperties = {
    position: "fixed",
    left: "50%",
    top: "50%",
    transform: "translate(-50%, -50%)",
    width: "min(420px, calc(100vw - 32px))",
  };
  if (spot) {
    const cardWidth = 380;
    const cardHeight = 200;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const below = spot.top + spot.height + 16;
    const above = spot.top - cardHeight - 16;
    const placeBelow = below + cardHeight < vh - 16;
    const top = placeBelow ? below : Math.max(16, above);
    let left = spot.left + spot.width / 2 - cardWidth / 2;
    left = Math.max(16, Math.min(left, vw - cardWidth - 16));
    cardStyle = {
      position: "fixed",
      top,
      left,
      width: cardWidth,
      transform: "none",
    };
  }

  const isFirst = stepIdx === 0;
  const isLast = stepIdx === steps.length - 1;

  return createPortal(
    <div className="fixed inset-0 z-[100]" aria-modal role="dialog">
      {/* Backdrop with spotlight cutout */}
      {spot ? (
        <>
          <div className="fixed bg-black/55" style={{ top: 0, left: 0, right: 0, height: spot.top }} />
          <div className="fixed bg-black/55" style={{ top: spot.top + spot.height, left: 0, right: 0, bottom: 0 }} />
          <div className="fixed bg-black/55" style={{ top: spot.top, left: 0, width: spot.left, height: spot.height }} />
          <div className="fixed bg-black/55" style={{ top: spot.top, left: spot.left + spot.width, right: 0, height: spot.height }} />
          <div
            className="fixed rounded-xl ring-2 ring-primary pointer-events-none transition-all duration-200"
            style={{
              top: spot.top,
              left: spot.left,
              width: spot.width,
              height: spot.height,
            }}
          />
        </>
      ) : (
        <div className="fixed inset-0 bg-black/55" />
      )}

      {/* Card */}
      <div
        style={cardStyle}
        className="rounded-2xl bg-white shadow-2xl border border-border p-5 animate-in fade-in zoom-in-95"
      >
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
              <Sparkles className="h-4 w-4 text-primary" />
            </div>
            <div className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">
              Step {stepIdx + 1} of {steps.length}
            </div>
          </div>
          <button
            type="button"
            onClick={finish}
            aria-label="Close tour"
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <h3 className="text-lg font-semibold text-foreground mb-1.5">{step.title}</h3>
        <p className="text-sm text-muted-foreground leading-relaxed mb-4">{step.body}</p>

        <div className="flex items-center gap-1.5 mb-4">
          {steps.map((_, i) => (
            <span
              key={i}
              className={`h-1.5 rounded-full transition-all ${
                i === stepIdx ? "w-6 bg-primary" : "w-1.5 bg-gray-200"
              }`}
            />
          ))}
        </div>

        <div className="flex items-center justify-between">
          <button
            type="button"
            onClick={finish}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            {isLast ? "Close" : "Skip"}
          </button>
          <div className="flex items-center gap-2">
            {!isFirst && (
              <Button variant="ghost" size="sm" onClick={back}>
                Back
              </Button>
            )}
            <Button size="sm" onClick={next} className="rounded-full px-4">
              {isFirst ? (
                <>Start tour <ArrowRight className="ml-1.5 h-3.5 w-3.5" /></>
              ) : isLast ? (
                "Finish"
              ) : (
                <>Next <ArrowRight className="ml-1.5 h-3.5 w-3.5" /></>
              )}
            </Button>
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}

export function startProductTour() {
  window.dispatchEvent(new CustomEvent(START_EVENT));
}
