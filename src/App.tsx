import { useEffect } from "react";
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate, useParams, useLocation, useNavigate } from "react-router-dom";
import { HelmetProvider } from "react-helmet-async";
import { AuthProvider } from "@/contexts/AuthContext";
import ProtectedRoute from "@/components/ProtectedRoute";
import PaidRoute from "@/components/PaidRoute";
import RecoveryRedirect from "@/components/RecoveryRedirect";
import { supabase } from "@/integrations/supabase/client";
import ProductTour from "@/components/ProductTour";
import Root from "./pages/Root";
import Index from "./pages/Index";
import ToolsHub from "./pages/ToolsHub";
import ToolTemplate from "./pages/ToolTemplate";
import Resources from "./pages/Resources";
import ResourceArticle from "./pages/ResourceArticle";
import About from "./pages/About";
import MediaKit from "./pages/MediaKit";
import Blog from "./pages/Blog";
import BlogPost from "./pages/BlogPost";
import BlogCategory from "./pages/blog/BlogCategory";
import BlogPillar from "./pages/blog/BlogPillar";
import Privacy from "./pages/Privacy";
import Terms from "./pages/Terms";
import NotFound from "./pages/NotFound";
import Login from "./pages/auth/Login";
import Signup from "./pages/auth/Signup";
import ForgotPassword from "./pages/auth/ForgotPassword";
import ResetPassword from "./pages/auth/ResetPassword";
import Dashboard from "./pages/app/Dashboard";
import Chat from "./pages/app/Chat";
import Monitor from "./pages/app/Monitor";
import Relevance from "./pages/app/Relevance";
import ContactProfile from "./pages/app/ContactProfile";
import Pricing from "./pages/Pricing";
import Account from "./pages/Account";
import Team from "./pages/Team";
import TeamInviteAccept from "./pages/TeamInviteAccept";
import BillingSuccess from "./pages/BillingSuccess";
import RequestDemo from "./pages/RequestDemo";
import SharedList from "./pages/SharedList";
import Discover from "./pages/Discover";
import DiscoverPage from "./pages/DiscoverPage";
import AdminSeoPages from "./pages/admin/SeoPages";
import CompareHub from "./pages/compare/CompareHub";
import ComparePage from "./pages/compare/ComparePage";
import GuidesHub from "./pages/guides/GuidesHub";
import GuidePage from "./pages/guides/GuidePage";

const queryClient = new QueryClient();

const RedirectWithSlug = ({ to }: { to: (slug: string) => string }) => {
  const { slug } = useParams();
  return <Navigate to={to(slug || "")} replace />;
};

const RESERVED_ROOT = new Set([
  "resources", "tools", "about", "blog", "privacy", "terms", "",
  "login", "signup", "forgot-password", "reset-password",
  "app", "dashboard", "database", "chat", "search", "monitor", "relevance", "profiles", "account", "team", "pricing", "billing", "request-demo", "shared",
  "discover", "admin", "compare", "guides",
]);

const LegacySlugRedirect = () => {
  const location = useLocation();
  const slug = location.pathname.replace(/^\/+/, "").split("/")[0];
  if (!slug || RESERVED_ROOT.has(slug)) return <NotFound />;
  return <Navigate to={`/resources/${slug}`} replace />;
};

const LegacyChatRedirect = () => {
  const { threadId } = useParams<{ threadId?: string }>();
  const location = useLocation();
  return <Navigate to={`/search${threadId ? `/${threadId}` : ""}${location.search}`} replace />;
};

const TopupSuccessRedirect = () => {
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  params.set("topup", "success");
  return <Navigate to={`/search?${params.toString()}`} replace />;
};

const AuthConfirm = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const params = new URLSearchParams(location.search);

  useEffect(() => {
    const tokenHash = params.get("token_hash");
    const requestedType = params.get("type");
    const type = requestedType === "signup" || requestedType === "recovery" ? requestedType : null;
    const next = params.get("next") || "/search";

    if (!tokenHash || !type) {
      navigate("/login", { replace: true });
      return;
    }

    if (type === "recovery") {
      navigate(`/reset-password?token_hash=${encodeURIComponent(tokenHash)}&type=recovery`, { replace: true });
      return;
    }

    supabase.auth.verifyOtp({ token_hash: tokenHash, type }).then(({ error }) => {
      if (error) {
        navigate("/login", { replace: true });
        return;
      }

      navigate(next, { replace: true });
    });
  }, [navigate, params]);

  return null;
};


const App = () => (
  <HelmetProvider>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <AuthProvider>
            <RecoveryRedirect />
            <ProductTour />
            <Routes>
              {/* Auth */}
              <Route path="/login" element={<Login />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/forgot-password" element={<ForgotPassword />} />
              <Route path="/reset-password" element={<ResetPassword />} />
              <Route path="/auth/confirm" element={<AuthConfirm />} />

              {/* Public marketing/billing */}
              <Route path="/pricing" element={<Pricing />} />
              <Route path="/request-demo" element={<RequestDemo />} />
              <Route path="/shared/:token" element={<SharedList />} />
              <Route path="/billing/success" element={<ProtectedRoute><BillingSuccess /></ProtectedRoute>} />

              {/* Authenticated (any plan) */}
              <Route path="/account" element={<ProtectedRoute><Account /></ProtectedRoute>} />
              <Route path="/team" element={<ProtectedRoute><Team /></ProtectedRoute>} />
              <Route path="/team/invite/:token" element={<TeamInviteAccept />} />

              {/* Paid-only app */}
              <Route path="/database" element={<PaidRoute requireGrowth><Dashboard /></PaidRoute>} />
              <Route path="/dashboard" element={<Navigate to="/database" replace />} />
              <Route path="/search" element={<ProtectedRoute><Chat /></ProtectedRoute>} />
              <Route path="/search/:threadId" element={<ProtectedRoute><Chat /></ProtectedRoute>} />
              <Route path="/chat" element={<LegacyChatRedirect />} />
              <Route path="/chat/:threadId" element={<LegacyChatRedirect />} />
              <Route path="/monitor" element={<PaidRoute requireGrowth><Monitor /></PaidRoute>} />
              <Route path="/relevance" element={<ProtectedRoute><Relevance /></ProtectedRoute>} />
              <Route path="/profiles/:kind/:id" element={<ProtectedRoute><ContactProfile /></ProtectedRoute>} />
              <Route path="/app" element={<Navigate to="/database" replace />} />

              {/* Marketing pages at root */}
              <Route path="/about" element={<About />} />
              <Route path="/media-kit" element={<MediaKit />} />
              <Route path="/blog" element={<Blog />} />
              <Route path="/blog/:slug" element={<BlogPost />} />
              <Route path="/privacy" element={<Privacy />} />
              <Route path="/terms" element={<Terms />} />

              {/* Resources content */}
              <Route path="/resources" element={<Resources />} />
              <Route path="/resources/home" element={<Index />} />
              <Route path="/resources/success" element={<ProtectedRoute><TopupSuccessRedirect /></ProtectedRoute>} />

              <Route path="/resources/:slug" element={<ResourceArticle />} />

              {/* Programmatic SEO discover pages */}
              <Route path="/discover" element={<Discover />} />
              <Route path="/discover/:slug" element={<DiscoverPage />} />
              <Route path="/admin/seo-pages" element={<AdminSeoPages />} />

              {/* Tools (canonical) */}
              <Route path="/tools" element={<ToolsHub />} />
              <Route path="/tools/:slug" element={<ToolTemplate />} />

              {/* Compare (vs competitors) */}
              <Route path="/compare" element={<CompareHub />} />
              <Route path="/compare/:slug" element={<ComparePage />} />

              {/* Guides (programmatic SEO: best / vs / alternatives / templates) */}
              <Route path="/guides" element={<GuidesHub />} />
              <Route path="/guides/:slug" element={<GuidePage />} />

              {/* Root: dashboard for signed-in, marketing for guests */}
              <Route path="/" element={<Root />} />

              {/* Legacy redirects */}
              <Route path="/resources/tools" element={<Navigate to="/tools" replace />} />
              <Route path="/resources/tools/:slug" element={<RedirectWithSlug to={(s) => `/tools/${s}`} />} />
              <Route path="/resources/about" element={<Navigate to="/about" replace />} />
              <Route path="/resources/blog" element={<Navigate to="/blog" replace />} />
              <Route path="/resources/privacy" element={<Navigate to="/privacy" replace />} />
              <Route path="/resources/terms" element={<Navigate to="/terms" replace />} />
              <Route path="/privacy-policy" element={<Navigate to="/privacy" replace />} />
              <Route path="/terms-of-service" element={<Navigate to="/terms" replace />} />

              <Route path="/:slug" element={<LegacySlugRedirect />} />
              <Route path="*" element={<NotFound />} />
            </Routes>
          </AuthProvider>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </HelmetProvider>
);

export default App;
