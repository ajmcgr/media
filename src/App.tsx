import { lazy, Suspense, useEffect } from "react";
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
import { trackEvent } from "@/lib/analytics";
import { FullscreenSpinner } from "@/components/ui/spinner";

const Root = lazy(() => import("./pages/Root"));
const Index = lazy(() => import("./pages/Index"));
const ToolsHub = lazy(() => import("./pages/ToolsHub"));
const ToolTemplate = lazy(() => import("./pages/ToolTemplate"));
const Resources = lazy(() => import("./pages/Resources"));
const ResourceArticle = lazy(() => import("./pages/ResourceArticle"));
const About = lazy(() => import("./pages/About"));
const MediaKit = lazy(() => import("./pages/MediaKit"));
const Blog = lazy(() => import("./pages/Blog"));
const BlogPost = lazy(() => import("./pages/BlogPost"));
const BlogCategory = lazy(() => import("./pages/blog/BlogCategory"));
const BlogPillar = lazy(() => import("./pages/blog/BlogPillar"));
const Privacy = lazy(() => import("./pages/Privacy"));
const Terms = lazy(() => import("./pages/Terms"));
const NotFound = lazy(() => import("./pages/NotFound"));
const Login = lazy(() => import("./pages/auth/Login"));
const Signup = lazy(() => import("./pages/auth/Signup"));
const ForgotPassword = lazy(() => import("./pages/auth/ForgotPassword"));
const ResetPassword = lazy(() => import("./pages/auth/ResetPassword"));
const Dashboard = lazy(() => import("./pages/app/Dashboard"));
const Chat = lazy(() => import("./pages/app/Chat"));
const Monitor = lazy(() => import("./pages/app/Monitor"));
const Relevance = lazy(() => import("./pages/app/Relevance"));
const ContactProfile = lazy(() => import("./pages/app/ContactProfile"));
const Pricing = lazy(() => import("./pages/Pricing"));
const Account = lazy(() => import("./pages/Account"));
const Team = lazy(() => import("./pages/Team"));
const TeamInviteAccept = lazy(() => import("./pages/TeamInviteAccept"));
const BillingSuccess = lazy(() => import("./pages/BillingSuccess"));
const RequestDemo = lazy(() => import("./pages/RequestDemo"));
const SharedList = lazy(() => import("./pages/SharedList"));
const Discover = lazy(() => import("./pages/Discover"));
const DiscoverPage = lazy(() => import("./pages/DiscoverPage"));
const AdminSeoPages = lazy(() => import("./pages/admin/SeoPages"));
const CompareHub = lazy(() => import("./pages/compare/CompareHub"));
const ComparePage = lazy(() => import("./pages/compare/ComparePage"));
const GuidesHub = lazy(() => import("./pages/guides/GuidesHub"));
const GuidePage = lazy(() => import("./pages/guides/GuidePage"));
const AIInfo = lazy(() => import("./pages/AIInfo"));

const queryClient = new QueryClient();

const RedirectWithSlug = ({ to }: { to: (slug: string) => string }) => {
  const { slug } = useParams();
  return <Navigate to={to(slug || "")} replace />;
};

const RESERVED_ROOT = new Set([
  "resources", "tools", "about", "blog", "privacy", "terms", "",
  "login", "signup", "forgot-password", "reset-password",
  "app", "dashboard", "database", "chat", "search", "monitor", "relevance", "profiles", "account", "team", "pricing", "billing", "request-demo", "shared",
  "discover", "admin", "compare", "guides", "ai-info",
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

  useEffect(() => {
    const params = new URLSearchParams(location.search);
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

      trackEvent("sign_up_completed", { method: "email", next });
      navigate(next, { replace: true });
    });
  }, [location.search, navigate]);

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
            <Suspense fallback={<FullscreenSpinner />}>
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
              <Route path="/blog/category/:slug" element={<BlogCategory />} />
              <Route path="/blog/guide/:slug" element={<BlogPillar />} />
              <Route path="/blog/:slug" element={<BlogPost />} />
              <Route path="/privacy" element={<Privacy />} />
              <Route path="/terms" element={<Terms />} />
              <Route path="/ai-info" element={<AIInfo />} />

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
            </Suspense>
          </AuthProvider>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </HelmetProvider>
);

export default App;
