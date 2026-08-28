import { NavLink, useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { InboxSheet } from "@/components/dashboard/InboxSheet";
import { ListsSheet } from "@/components/dashboard/ListsSheet";
import WorkspaceDropdown from "@/components/WorkspaceDropdown";
import NotificationsBell from "@/components/NotificationsBell";
import { useAuth } from "@/contexts/AuthContext";
import { useSubscription } from "@/hooks/useSubscription";
import { isGrowthPlanIdentifier } from "@/lib/plans";
import logoMedia from "@/assets/brand/logo-media-blue.png";
import { cn } from "@/lib/utils";
import type { ReactNode } from "react";
import { Menu } from "lucide-react";

type AppHeaderProps = {
  active?: "search" | "database" | "monitor";
  rightExtras?: ReactNode;
  hideNav?: boolean;
};

const pillBase = "font-medium text-sm px-3 py-2 h-auto";
const pillActive = "rounded-md bg-primary text-primary-foreground hover:bg-primary/90 hover:text-primary-foreground";
const pillPlain = "rounded-md text-gray-700 hover:text-gray-900 hover:bg-gray-100";

function PillNavButton({
  to,
  active,
  children,
  dataTour,
}: {
  to: string;
  active?: boolean;
  children: ReactNode;
  dataTour?: string;
}) {
  const navigate = useNavigate();
  return (
    <Button
      variant="ghost"
      size="sm"
      className={cn(pillBase, active ? pillActive : pillPlain)}
      onClick={() => navigate(to)}
      data-tour={dataTour}
    >
      {children}
    </Button>
  );
}


export default function AppHeader({ active, rightExtras, hideNav }: AppHeaderProps) {
  const navigate = useNavigate();
  const { user, signOut } = useAuth();
  const { planIdentifier } = useSubscription();
  const hasGrowth = isGrowthPlanIdentifier(planIdentifier);
  const initials = (user?.email ?? "?").slice(0, 2).toUpperCase();

  const handleSignOut = async () => {
    await signOut();
    navigate("/");
  };

  return (
    <header className="h-14 border-b border-border bg-white flex items-center justify-between px-4 flex-shrink-0 sticky top-0 z-30">
      <div className="flex items-center gap-10">
        <NavLink to="/search" className="flex items-center">
          <img src={logoMedia} alt="Media AI" className="h-5" />
        </NavLink>
        {hideNav && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button type="button" className="md:hidden rounded-md p-2 text-gray-700 hover:bg-gray-100" aria-label="Open navigation">
                <Menu className="h-5 w-5" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-52">
              <DropdownMenuItem onSelect={() => navigate("/search")}>Search</DropdownMenuItem>
              {hasGrowth && <DropdownMenuItem onSelect={() => navigate("/database")}>Database</DropdownMenuItem>}
              <DropdownMenuItem onSelect={() => navigate("/monitor")}>Monitor</DropdownMenuItem>
              <DropdownMenuItem onSelect={() => navigate("/team")}>Team</DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={() => navigate("/account")}>Settings</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )}
        {!hideNav && (
          <div className="flex items-center gap-1 sm:gap-2">
            <PillNavButton to="/search" dataTour="nav-search">Search</PillNavButton>
            {hasGrowth && (
              <PillNavButton to="/database" dataTour="nav-database">Database</PillNavButton>
            )}
            <PillNavButton to="/monitor" dataTour="nav-monitor">Monitor</PillNavButton>
            <span data-tour="nav-inbox" className="inline-flex"><InboxSheet /></span>
            <span data-tour="nav-lists" className="inline-flex"><ListsSheet /></span>
            {rightExtras}
          </div>
        )}
      </div>

      <div className="flex items-center gap-2">
        <span data-tour="workspace-dropdown" className="inline-flex"><WorkspaceDropdown /></span>
        <span data-tour="notifications-bell" className="inline-flex"><NotificationsBell /></span>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button type="button" data-tour="account-menu" className="ml-1 rounded-full focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1" aria-label="Account menu">
              <Avatar className="h-8 w-8 border border-border">
                <AvatarFallback className="bg-secondary text-foreground text-xs font-medium">{initials}</AvatarFallback>
              </Avatar>
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuLabel className="font-normal">
              <div className="text-xs text-muted-foreground">Signed in as</div>
              <div className="text-sm truncate">{user?.email}</div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem onSelect={() => navigate("/account")}>Settings</DropdownMenuItem>
            <DropdownMenuItem asChild>
              <a href="/pricing" target="_blank" rel="noopener noreferrer">Plans</a>
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={() => navigate("/team")}>Team</DropdownMenuItem>
            <DropdownMenuItem onSelect={() => navigate("/relevance")}>Search quality</DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onSelect={handleSignOut}>Sign out</DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}
