"use client";

import { useEffect, useState } from "react";
import { api, type Run } from "@/lib/api";

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.listRuns().then((data) => {
      setRuns(data.runs);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Runs</h1>
      {loading ? (
        <p className="text-zinc-500">Loading...</p>
      ) : runs.length === 0 ? (
        <div className="text-zinc-500 border border-zinc-800 rounded-lg p-8 text-center">
          <p className="text-lg mb-2">No runs yet</p>
          <p className="text-sm">Assign an issue to the d3ftly bot to start a run.</p>
        </div>
      ) : (
        <div className="border border-zinc-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-zinc-900 text-zinc-400 text-left">
              <tr>
                <th className="px-4 py-3 font-medium">Title</th>
                <th className="px-4 py-3 font-medium">Repo</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Cost</th>
                <th className="px-4 py-3 font-medium">Time</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {runs.map((run) => (
                <tr key={run.run_id} className="hover:bg-zinc-900/50">
                  <td className="px-4 py-3">
                    <a href={`/runs/${run.run_id}`} className="text-zinc-100 hover:underline">
                      {run.title}
                    </a>
                  </td>
                  <td className="px-4 py-3 text-zinc-400 font-mono text-xs">{run.repo}</td>
                  <td className="px-4 py-3">
                    <StatusBadge status={run.status} />
                  </td>
                  <td className="px-4 py-3 text-zinc-400">{run.cost_usd ? `$${run.cost_usd.toFixed(2)}` : "—"}</td>
                  <td className="px-4 py-3 text-zinc-400">{run.duration_s ? `${run.duration_s}s` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    running: "bg-blue-500/10 text-blue-400 border-blue-500/20",
    completed: "bg-green-500/10 text-green-400 border-green-500/20",
    failed: "bg-red-500/10 text-red-400 border-red-500/20",
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs border ${colors[status] || "bg-zinc-800 text-zinc-400 border-zinc-700"}`}>
      {status}
    </span>
  );
}
