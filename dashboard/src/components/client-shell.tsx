"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ToastProvider } from "./toast";

const links = [
  { href: "/", label: "Runs", icon: "▶" },
  { href: "/plans", label: "Plans", icon: "◉" },
  { href: "/infrastructure", label: "Infrastructure", icon: "⬡" },
  { href: "/analytics", label: "Analytics", icon: "◪" },
  { href: "/settings/guardrails", label: "Guardrails", icon: "⛨" },
  { href: "/settings/voice", label: "Voice", icon: "✎" },
  { href: "/settings/instructions", label: "Instructions", icon: "⚙" },
  { href: "/settings/repos", label: "Repos", icon: "◈" },
  { href: "/billing", label: "Billing", icon: "$" },
];

export function ClientShell({ children }: { children: React.ReactNode }) {
  return (
    <ToastProvider>
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 p-8">{children}</main>
      </div>
    </ToastProvider>
  );
}

function Sidebar() {
  const pathname = usePathname();

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  return (
    <nav className="w-56 border-r border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-1">
      <Link href="/" className="text-lg font-bold tracking-tight mb-6 px-3">
        d3ftly
      </Link>
      {links.map((link) => (
        <Link
          key={link.href}
          href={link.href}
          className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
            isActive(link.href)
              ? "text-zinc-100 bg-zinc-800/70"
              : "text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800/50"
          }`}
        >
          <span className="w-4 text-center text-xs">{link.icon}</span>
          {link.label}
        </Link>
      ))}
    </nav>
  );
}
