"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type BillingInfo, type Plan } from "@/lib/api";
import { TableSkeleton } from "@/components/skeleton";

const STATUS_STYLES: Record<string, string> = {
  draft: "bg-zinc-800 text-zinc-400 border-zinc-700",
  executing: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  done: "bg-green-500/10 text-green-400 border-green-500/20",
};

export default function PlansPage() {
  const [plans, setPlans] = useState<Plan[]>([]);
  const [billing, setBilling] = useState<BillingInfo | null>(null);
  const [loading, setLoading] = useState(true);

  const [error, setError] = useState(false);

  useEffect(() => {
    Promise.all([api.listPlans(), api.getBilling()])
      .then(([data, b]) => {
        setPlans(data.plans);
        setBilling(b);
      })
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  const plansEnabled = billing?.subscription_status === "active";

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Plans</h1>
          <p className="text-sm text-zinc-500 mt-1">
            Chat with d3ftly to break work into epics and ordered tasks, then approve and execute.
          </p>
        </div>
        {plansEnabled ? (
          <Link
            href="/plans/new"
            className="px-4 py-2 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors"
          >
            + New plan
          </Link>
        ) : (
          <Link
            href="/billing"
            className="px-4 py-2 border border-yellow-500/30 text-yellow-400 rounded-lg text-sm font-semibold hover:bg-yellow-500/10 transition-colors"
          >
            Upgrade to unlock Plans
          </Link>
        )}
      </div>

      {!loading && !error && !plansEnabled && (
        <div className="mb-6 rounded-lg border border-yellow-500/30 bg-yellow-500/10 p-4">
          <p className="text-sm text-yellow-300 font-medium">Plans is a paid feature</p>
          <p className="text-xs text-yellow-200/80 mt-1">
            Upgrade to Pro (or add the Plans add-on) to create, approve, and execute plans.
          </p>
        </div>
      )}

      {loading ? (
        <TableSkeleton rows={4} cols={4} />
      ) : error ? (
        <div className="text-red-400 border border-red-500/20 bg-red-500/5 rounded-lg p-8 text-center">
          <p className="text-sm">Failed to load plans. Please refresh.</p>
        </div>
      ) : !plansEnabled ? (
        <div className="border border-zinc-800 rounded-lg p-10 text-center text-zinc-500">
          <p className="text-lg mb-2 text-zinc-300">Plans is locked on Free</p>
          <p className="text-sm mb-4">Upgrade your subscription to unlock planning workflows.</p>
          <Link href="/billing" className="text-zinc-300 underline text-sm">
            Go to Billing
          </Link>
        </div>
      ) : plans.length === 0 ? (
        <div className="border border-zinc-800 rounded-lg p-12 text-center text-zinc-500">
          <p className="text-lg mb-2">No plans yet</p>
          <p className="text-sm mb-4">
            Create a plan to break a feature or epic into ordered GitHub issues that d3ftly will implement.
          </p>
          <Link href="/plans/new" className="text-zinc-300 underline text-sm">
            Create your first plan
          </Link>
        </div>
      ) : (
        <div className="border border-zinc-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-zinc-900 text-zinc-400 text-left">
              <tr>
                <th className="px-4 py-3 font-medium">Title</th>
                <th className="px-4 py-3 font-medium">Repo</th>
                <th className="px-4 py-3 font-medium">Tasks</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {plans.map((plan) => (
                <tr key={plan.plan_id} className="hover:bg-zinc-900/50">
                  <td className="px-4 py-3">
                    <Link href={`/plans/detail?id=${plan.plan_id}`} className="text-zinc-100 hover:underline font-medium">
                      {plan.title}
                    </Link>
                    {plan.description && (
                      <p className="text-zinc-500 text-xs mt-0.5 truncate max-w-xs">{plan.description}</p>
                    )}
                  </td>
                  <td className="px-4 py-3 text-zinc-400 font-mono text-xs">{plan.repo || "—"}</td>
                  <td className="px-4 py-3 text-zinc-400">{plan.task_count}</td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-0.5 rounded-full text-xs border ${STATUS_STYLES[plan.status] || STATUS_STYLES.draft}`}>
                      {plan.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-zinc-500 text-xs">
                    {new Date(plan.created_at).toLocaleDateString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
