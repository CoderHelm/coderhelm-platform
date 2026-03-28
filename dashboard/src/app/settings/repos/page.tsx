"use client";

import { useEffect, useState } from "react";
import { api, type Repo } from "@/lib/api";
import { Skeleton } from "@/components/skeleton";

export default function ReposPage() {
  const [repos, setRepos] = useState<Repo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.listRepos().then((data) => {
      setRepos(data.repos);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-2">Repositories</h1>
      <p className="text-zinc-400 text-sm mb-6">
        Repos connected to d3ftly. d3ftly will never push directly to main — it always creates feature branches.
      </p>

      {loading ? (
        <div className="space-y-2">
          <Skeleton className="h-14 w-full" />
          <Skeleton className="h-14 w-full" />
          <Skeleton className="h-14 w-full" />
        </div>
      ) : repos.length === 0 ? (
        <div className="text-zinc-500 border border-zinc-800 rounded-lg p-8 text-center">
          <p>No repos connected yet.</p>
          <a
            href="https://github.com/apps/d3ftly-agent"
            className="text-zinc-300 underline mt-2 inline-block"
          >
            Install the GitHub App
          </a>
        </div>
      ) : (
        <div className="space-y-2">
          {repos.map((repo) => (
            <div
              key={repo.name}
              className="flex items-center justify-between px-4 py-3 bg-zinc-900/50 border border-zinc-800 rounded-lg"
            >
              <div>
                <span className="text-sm font-mono text-zinc-200">{repo.name}</span>
                <span className="ml-3 text-xs text-zinc-600">{repo.ticket_source}</span>
              </div>
              <span
                className={`px-2 py-0.5 rounded-full text-xs border ${
                  repo.enabled
                    ? "bg-green-500/10 text-green-400 border-green-500/20"
                    : "bg-zinc-800 text-zinc-500 border-zinc-700"
                }`}
              >
                {repo.enabled ? "Active" : "Disabled"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
