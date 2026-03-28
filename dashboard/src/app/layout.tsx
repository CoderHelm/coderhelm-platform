import type { Metadata } from "next";
import "./globals.css";
import { ClientShell } from "@/components/client-shell";

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
        <ClientShell>{children}</ClientShell>
      </body>
    </html>
  );
}
