import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Database, Download, HelpCircle, Inbox as InboxIcon, ListChecks,
  PanelLeftClose, PanelLeftOpen, Radar, Search as SearchIcon, Settings, Sparkles,
} from "lucide-react";
import { InboxSheet } from "@/components/dashboard/InboxSheet";
import { ListsSheet } from "@/components/dashboard/ListsSheet";
import { isGrowthPlanIdentifier } from "@/lib/plans";
import { useSubscription } from "@/hooks/useSubscription";
import { cn } from "@/lib/utils";

type AppSidebarProps = {
  active?: "search" | "database" | "monitor" | "settings";
};

type IconType = React.ComponentType<{ className?: string }>;

const SidebarNavItem = ({
  icon: Icon, label, active, disabled, collapsed, onClick,
}: {
  icon: IconType; label: string; active?: boolean; disabled?: boolean;
  collapsed?: boolean; onClick?: () => void;
}) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    title={collapsed ? label : undefined}
    className={cn(
      "flex items-center rounded-lg text-sm transition-colors",
      collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
      active
        ? "bg-gray-900 text-white hover:bg-gray-900"
        : "text-gray-700 hover:bg-gray-100",
      disabled && "opacity-40 cursor-not-allowed hover:bg-transparent",
    )}
  >
    <Icon className={cn("h-[18px] w-[18px]", active ? "text-white" : "text-gray-500")} />
    {!collapsed && <span className="font-semibold">{label}</span>}
  </button>
);

const SidebarNavButton = React.forwardRef<
  HTMLButtonElement,
  { icon: IconType; label: string; collapsed?: boolean } & React.ButtonHTMLAttributes<HTMLButtonElement>
>(({ icon: Icon, label, collapsed, ...props }, ref) => (
  <button
    ref={ref}
    type="button"
    title={collapsed ? label : undefined}
    {...props}
    className={cn(
      "flex items-center rounded-lg text-sm text-gray-700 hover:bg-gray-100",
      collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
    )}
  >
    <Icon className="h-[18px] w-[18px] text-gray-500" />
    {!collapsed && <span className="font-semibold">{label}</span>}
  </button>
));
SidebarNavButton.displayName = "SidebarNavButton";

export default function AppSidebar({ active }: AppSidebarProps) {
  const navigate = useNavigate();
  const { planIdentifier } = useSubscription();
  const hasGrowth = isGrowthPlanIdentifier(planIdentifier);
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem("chat.sidebarCollapsed") === "1";
  });

  useEffect(() => {
    try { localStorage.setItem("chat.sidebarCollapsed", collapsed ? "1" : "0"); } catch { /* Storage can be unavailable in private browser contexts. */ }
  }, [collapsed]);

  return (
    <aside className={cn(
      "hidden md:flex h-screen sticky top-0 border-r border-border bg-white flex-col flex-shrink-0 transition-[width] duration-200",
      collapsed ? "w-14" : "w-52",
    )}>
      <div className={cn("py-3", collapsed ? "px-2 flex justify-center" : "px-2")}>
        <button
          type="button"
          onClick={() => setCollapsed((v) => !v)}
          className={cn(
            "flex items-center gap-2 rounded-md text-sm text-gray-700 hover:bg-gray-100",
            collapsed ? "justify-center p-2" : "w-full px-2 py-1.5",
          )}
          title={collapsed ? "Expand" : "Collapse"}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed
            ? <PanelLeftOpen className="h-[18px] w-[18px] text-gray-500" />
            : <PanelLeftClose className="h-[18px] w-[18px] text-gray-500" />}
          {!collapsed && <span className="font-semibold text-xs">Collapse</span>}
        </button>
      </div>

      <nav className="pb-2 space-y-0.5 flex-1 px-2">
        <SidebarNavItem icon={SearchIcon} label="Search" collapsed={collapsed} active={active === "search"} onClick={() => navigate("/search")} />
        <SidebarNavItem icon={Database} label={hasGrowth ? "Database" : "Database · Growth"} collapsed={collapsed} active={active === "database"} onClick={() => navigate("/database")} />
        <SidebarNavItem icon={Radar} label={hasGrowth ? "Monitor" : "Monitor · Growth"} collapsed={collapsed} active={active === "monitor"} onClick={() => navigate("/monitor")} />
        <InboxSheet triggerNode={<SidebarNavButton icon={InboxIcon} label="Inbox" collapsed={collapsed} />} />
        <ListsSheet triggerNode={<SidebarNavButton icon={ListChecks} label="Lists" collapsed={collapsed} />} />
        <SidebarNavItem icon={Download} label="Export" collapsed={collapsed} disabled />
      </nav>

      <div className={cn(collapsed ? "p-2" : "p-3")}>
        <button
          type="button"
          onClick={() => navigate("/account#credits")}
          title="Buy credits"
          className={cn(
            "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
            collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
          )}
        >
          <Sparkles className="h-[18px] w-[18px] text-gray-500" />
          {!collapsed && <span className="font-semibold">Buy credits</span>}
        </button>
        <div className="mt-0.5 space-y-0.5">
          <button
            type="button"
            onClick={() => navigate("/account")}
            className={cn(
              "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
              collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
              active === "settings" && "bg-gray-100",
            )}
            title="Settings"
          >
            <Settings className="h-[18px] w-[18px] text-gray-500" />
            {!collapsed && <span className="font-semibold">Settings</span>}
          </button>
          <a
            href="mailto:alex@trymedia.ai"
            className={cn(
              "flex items-center rounded-md text-sm text-gray-700 hover:bg-gray-100",
              collapsed ? "justify-center p-2 w-full" : "w-full gap-2.5 px-3 py-2",
            )}
            title="Help"
          >
            <HelpCircle className="h-[18px] w-[18px] text-gray-500" />
            {!collapsed && <span className="font-semibold">Help</span>}
          </a>
        </div>
      </div>
    </aside>
  );
}
