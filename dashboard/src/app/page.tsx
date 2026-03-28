"use client";

import { useEffect, useState } from "react";
import { api, type Run } from "@/lib/api";
import { TableSkeleton } from "@/components/skeleton";

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    api.listRuns()
      .then((data) => setRuns(data.runs))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
         <div className="mb-8">
           <h1 className="text-xl font-semibold text-zinc-100">Runs</h1>
           <p className="text-sm text-zinc-500 mt-1">
             All agent runs across your repositories.
           </p>
         </div>
      {loading ? (
        <TableSkeleton rows={5} cols={5} />
      ) : error ? (
        <div className="text-red-400 border border-red-500/20 bg-red-500/5 rounded-lg p-8 text-center">
          <p className="text-sm">Failed to load runs. Please refresh.</p>
        </div>
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
                    <span className="text-zinc-100">{run.title}</span>
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
   const map: Record<string, { dot: string; text: string; bg: string }> = {
     running: { dot: "bg-blue-400", text: "text-blue-400", bg: "bg-blue-500/10 border-blue-500/20" },
     completed: { dot: "bg-emerald-400", text: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" },
     failed: { dot: "bg-red-400", text: "text-red-400", bg: "bg-red-500/10 border-red-500/20" },
     pending: { dot: "bg-yellow-400", text: "text-yellow-400", bg: "bg-yellow-500/10 border-yellow-500/20" },
   };
   const s = map[status] ?? { dot: "bg-zinc-500", text: "text-zinc-400", bg: "bg-zinc-800 border-zinc-700" };
   return (
     <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-medium border ${s.bg} ${s.text}`}>
       <span className={`w-1.5 h-1.5 rounded-full ${s.dot} ${status === "running" ? "animate-pulse" : ""}`} />
       {status}
     </span>
   );
}
