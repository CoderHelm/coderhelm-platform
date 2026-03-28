import Link from "next/link";

export default function SettingsPage() {
  const sections = [
    { href: "/settings/guardrails", title: "Guardrails", description: "Must-rules d3ftly always follows. Never pushes to main." },
    { href: "/settings/voice", title: "Team Voice", description: "Control how d3ftly writes — tone, commit style, PR format." },
    { href: "/settings/instructions", title: "Custom Instructions", description: "Global conventions and preferences for all repos." },
    { href: "/settings/repos", title: "Repositories", description: "Connected repos and their status." },
  ];

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-6">Settings</h1>
      <div className="space-y-3">
        {sections.map((s) => (
          <Link
            key={s.href}
            href={s.href}
            className="block px-4 py-4 bg-zinc-900/50 border border-zinc-800 rounded-lg hover:border-zinc-700 transition-colors"
          >
            <h3 className="text-sm font-medium text-zinc-100">{s.title}</h3>
            <p className="text-xs text-zinc-500 mt-1">{s.description}</p>
          </Link>
        ))}
      </div>
    </div>
  );
}
