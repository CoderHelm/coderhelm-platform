import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "d3ftly — Dashboard",
  description: "Manage your d3ftly settings and monitor runs",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-zinc-950 text-zinc-100 min-h-screen antialiased">
        <div className="flex min-h-screen">
          <Sidebar />
          <main className="flex-1 p-8">{children}</main>
        </div>
      </body>
    </html>
  );
}

function Sidebar() {
  const links = [
    { href: "/", label: "Runs", icon: "▶" },
    { href: "/analytics", label: "Analytics", icon: "◪" },
    { href: "/settings/guardrails", label: "Guardrails", icon: "⛨" },
    { href: "/settings/voice", label: "Voice", icon: "✎" },
    { href: "/settings/instructions", label: "Instructions", icon: "⚙" },
    { href: "/settings/repos", label: "Repos", icon: "◈" },
    { href: "/billing", label: "Billing", icon: "$" },
  ];

  return (
    <nav className="w-56 border-r border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-1">
      <a href="/" className="text-lg font-bold tracking-tight mb-6 px-3">
        d3ftly
      </a>
      {links.map((link) => (
        <a
          key={link.href}
          href={link.href}
          className="flex items-center gap-3 px-3 py-2 rounded-md text-sm text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800/50 transition-colors"
        >
          <span className="w-4 text-center text-xs">{link.icon}</span>
          {link.label}
        </a>
      ))}
      <div className="mt-auto pt-4 border-t border-zinc-800">
        <a
          href="/settings"
          className="flex items-center gap-3 px-3 py-2 rounded-md text-sm text-zinc-500 hover:text-zinc-300"
        >
          Settings
        </a>
      </div>
    </nav>
  );
}
