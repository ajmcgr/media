import { useState, useEffect } from "react";
import { Link, useNavigate, useLocation, useSearchParams } from "react-router-dom";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import Header from "@/components/Header";
import GoogleIcon from "@/components/GoogleIcon";
import { trackEvent } from "@/lib/analytics";

const Login = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const [params] = useSearchParams();
  const { user } = useAuth();
  const from =
    params.get("next") ||
    (location.state as { from?: { pathname: string } })?.from?.pathname ||
    "/search";

  useEffect(() => {
    if (user) navigate(from, { replace: true });
  }, [user, from, navigate]);

  const handleEmailLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    const trimmedEmail = email.trim().toLowerCase();
    trackEvent("login_started", { method: "email", next: from });
    const { error } = await supabase.auth.signInWithPassword({
      email: trimmedEmail,
      password,
    });
    setBusy(false);
    if (error) {
      return toast.error(error.message || "Login failed");
    }
    trackEvent("login_completed", { method: "email", next: from });
    toast.success("Signed in");
    navigate(from, { replace: true });
  };

  const handleGoogle = async () => {
    trackEvent("login_started", { method: "google", next: from });
    const { error } = await supabase.auth.signInWithOAuth({
      provider: "google",
      options: { redirectTo: `${window.location.origin}${from}` },
    });
    if (error) toast.error(error.message);
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="max-w-md mx-auto px-6 py-16">
        <h1 className="text-3xl font-medium mb-2">Welcome back</h1>
        <p className="text-muted-foreground mb-8">Sign in to your Media AI account.</p>

        <Button variant="outline" className="w-full gap-2" onClick={handleGoogle}>
          <GoogleIcon className="h-5 w-5" />
          Continue with Google
        </Button>

        <div className="my-6 flex items-center gap-3">
          <div className="h-px flex-1 bg-border" />
          <span className="text-xs text-muted-foreground">OR</span>
          <div className="h-px flex-1 bg-border" />
        </div>

        <form onSubmit={handleEmailLogin} className="space-y-4">
          <div>
            <Label htmlFor="email">Email</Label>
            <Input id="email" type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="password">Password</Label>
            <Input id="password" type="password" required value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>
          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? "Signing in…" : "Sign in"}
          </Button>
        </form>

        <div className="mt-6 flex justify-between text-sm">
          <Link to="/forgot-password" className="text-muted-foreground hover:text-foreground">
            Forgot password?
          </Link>
          <Link to="/signup" className="text-muted-foreground hover:text-foreground">
            Create account
          </Link>
        </div>
      </main>
    </div>
  );
};

export default Login;
