import { useState, useEffect } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import Header from "@/components/Header";
import GoogleIcon from "@/components/GoogleIcon";
import { trackEvent } from "@/lib/analytics";

const APP_URL = "https://trymedia.ai";

const Signup = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [company, setCompany] = useState("");
  const [busy, setBusy] = useState(false);
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { user } = useAuth();
  const requestedNext = searchParams.get("next");
  const next = requestedNext?.startsWith("/") && !requestedNext.startsWith("//")
    ? requestedNext
    : "/search";

  useEffect(() => {
    if (user) navigate(next, { replace: true });
  }, [user, navigate, next]);

  const handleSignup = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    trackEvent("sign_up_started", { method: "email", next });

    const trimmedEmail = email.trim().toLowerCase();

    const { error } = await supabase.functions.invoke("send-signup", {
      body: {
        email: trimmedEmail,
        password,
        displayName,
        company,
        next,
      },
    });

    setBusy(false);

    if (error) {
      const ctx = (error as { context?: Response }).context;
      let detail = "";
      try {
        detail = ctx ? await ctx.clone().text() : "";
      } catch {
        // ignore
      }

      let parsed: { error?: string } | null = null;
      try {
        parsed = detail ? (JSON.parse(detail) as { error?: string }) : null;
      } catch {
        // ignore
      }

      return toast.error(parsed?.error || error.message || "Could not create account");
    }

    toast.success("Check your email to confirm your account");
    trackEvent("sign_up_submitted", { method: "email", next });
    navigate(`/login?next=${encodeURIComponent(next)}`);
  };

  const handleGoogle = async () => {
    trackEvent("sign_up_started", { method: "google", next });
    const { error } = await supabase.auth.signInWithOAuth({
      provider: "google",
      options: {
        redirectTo: `${APP_URL}${next}`,
      },
    });
    if (error) toast.error(error.message);
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="max-w-md mx-auto px-6 py-16">
        <h1 className="text-3xl font-medium mb-2">Create your account</h1>
        <p className="text-muted-foreground mb-8">Start finding journalists and creators in seconds.</p>

        <Button variant="outline" className="w-full gap-2" onClick={handleGoogle}>
          <GoogleIcon className="h-5 w-5" />
          Continue with Google
        </Button>

        <div className="my-6 flex items-center gap-3">
          <div className="h-px flex-1 bg-border" />
          <span className="text-xs text-muted-foreground">OR</span>
          <div className="h-px flex-1 bg-border" />
        </div>

        <form onSubmit={handleSignup} className="space-y-4">
          <div>
            <Label htmlFor="name">Full name</Label>
            <Input id="name" required value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="company">Company (optional)</Label>
            <Input id="company" value={company} onChange={(e) => setCompany(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="email">Work email</Label>
            <Input id="email" type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              required
              minLength={8}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? "Creating account…" : "Create account"}
          </Button>
        </form>

        <p className="mt-6 text-sm text-muted-foreground text-center">
          Already have an account?{" "}
          <Link to="/login" className="text-foreground underline">
            Sign in
          </Link>
        </p>
      </main>
    </div>
  );
};

export default Signup;
