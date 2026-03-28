"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { api, type BillingInfo } from "@/lib/api";
import { ToastProvider } from "./toast";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "https://api.d3ftly.com";

interface User {
  github_login: string;
  email: string;
  avatar_url: string;
}

interface NavItem {
  href: string;
  label: string;
  icon: string;
}

interface NavGroup {
  label?: string;
  items: NavItem[];
}

const navGroups: NavGroup[] = [
  {
    items: [
      { href: "/", label: "Runs", icon: "▶" },
      { href: "/plans", label: "Plans", icon: "◉" },
    ],
  },
  {
    label: "Insights",
    items: [
      { href: "/analytics", label: "Analytics", icon: "◪" },
      { href: "/infrastructure", label: "Infrastructure", icon: "⬡" },
    ],
  },
  {
    label: "Configure",
    items: [
      { href: "/settings/repos", label: "Repos", icon: "◈" },
      { href: "/settings/guardrails", label: "Guardrails", icon: "⛨" },
      { href: "/settings/instructions", label: "Instructions", icon: "⚙" },
      { href: "/settings/voice", label: "Voice", icon: "✎" },
    ],
  },
  {
    label: "Account",
    items: [{ href: "/billing", label: "Billing", icon: "$" }],
  },
];

function canUsePlans(billing: BillingInfo | null): boolean {
  if (!billing) return false;
  return billing.subscription_status === "active";
}

export function ClientShell({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [billing, setBilling] = useState<BillingInfo | null>(null);
  const [authChecked, setAuthChecked] = useState(false);

  useEffect(() => {
    Promise.all([api.me(), api.getBilling()])
      .then(([u, b]) => {
        setUser(u);
        setBilling(b);
        setAuthChecked(true);
      })
      .catch(() => {
        window.location.href = `${API_BASE}/api/auth/github`;
      });
  }, []);

  if (!authChecked) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-zinc-950">
        <div className="flex flex-col items-center gap-3">
          <div className="w-5 h-5 border-2 border-zinc-700 border-t-zinc-300 rounded-full animate-spin" />
          <span className="text-zinc-500 text-sm">Loading...</span>
        </div>
      </div>
    );
  }

  return (
    <ToastProvider>
      <div className="flex min-h-screen bg-zinc-950">
        <Sidebar plansEnabled={canUsePlans(billing)} user={user} />
        <main className="flex-1 p-8">{children}</main>
      </div>
    </ToastProvider>
  );
}

function Sidebar({
  plansEnabled,
  user,
}: {
  plansEnabled: boolean;
  user: User | null;
}) {
  const pathname = usePathname();

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  return (
    <nav className="w-56 border-r border-zinc-800/60 bg-zinc-950 p-3 flex flex-col">
      <Link
        href="/"
        className="flex items-center gap-2.5 px-2 py-2.5 mb-2 rounded-md"
      >
        <span className="inline-flex w-6 h-6 items-center justify-center rounded bg-white text-black text-xs font-bold">
          d
        </span>
        <span className="text-sm font-semibold text-zinc-100">d3ftly</span>
      </Link>

      <div className="flex-1 space-y-4">
        {navGroups.map((group, index) => (
          <div key={index}>
            {group.label && (
              <p className="px-2 mb-1 text-[10px] font-semibold text-zinc-600 uppercase tracking-widest">
                {group.label}
              </p>
            )}

            <div className="space-y-1">
              {group.items.map((item) => {
                const locked = item.href === "/plans" && !plansEnabled;
                return (
                  <Link
                    key={item.href}
                    href={locked ? "/billing" : item.href}
                    className={`flex items-center justify-between px-2 py-1.5 rounded-md text-sm transition-colors ${
                      isActive(item.href)
                        ? "text-zinc-100 bg-zinc-800"
                        : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50"
                    }`}
                  >
                    <span className="flex items-center gap-2.5">
                      <span className="w-4 text-center text-xs">{item.icon}</span>
                      {item.label}
                    </span>
                    {locked && (
                      <span className="text-[9px] px-1.5 py-0.5 rounded-full border border-yellow-500/30 text-yellow-400">
                        Pro
                      </span>
                    )}
                  </Link>
                );
              })}
            </div>
          </div>
        ))}
      </div>

      {user && (
        <div className="mt-3 pt-3 border-t border-zinc-800/60 flex items-center gap-2">
          {user.avatar_url ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img
              src={user.avatar_url}
              alt={user.github_login}
              className="w-6 h-6 rounded-full"
            />
          ) : (
            <span className="w-6 h-6 rounded-full bg-zinc-800 text-zinc-400 text-xs flex items-center justify-center">
              {user.github_login.charAt(0).toUpperCase()}
            </span>
          )}
          <div className="min-w-0">
            <p className="text-xs text-zinc-300 truncate">{user.github_login}</p>
            <p className="text-[10px] text-zinc-500 truncate">{user.email}</p>
          </div>
        </div>
      )}
    </nav>
  );
}
