"use client";

import { useEffect, useState } from "react";
import { api, type Billing } from "@/lib/api";

export default function BillingPage() {
  const [billing, setBilling] = useState<Billing | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getBilling().then((data) => {
      setBilling(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const openPortal = async () => {
    try {
      const { url } = await api.createPortal();
      window.location.href = url;
    } catch (e) {
      console.error("Failed to open billing portal:", e);
    }
  };

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-6">Billing</h1>

      {loading ? (
        <p className="text-zinc-500">Loading...</p>
      ) : billing ? (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
              <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">Plan</p>
              <p className="text-lg font-bold">{billing.plan}</p>
            </div>
            <div className="p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
              <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">Runs</p>
              <p className="text-lg font-bold">
                {billing.runs_used} <span className="text-zinc-500 text-sm">/ {billing.runs_limit}</span>
              </p>
            </div>
          </div>
          <button
            onClick={openPortal}
            className="px-4 py-2 bg-zinc-100 text-zinc-900 rounded-lg text-sm font-medium hover:bg-white transition-colors"
          >
            Manage subscription
          </button>
        </div>
      ) : (
        <p className="text-zinc-500">Unable to load billing info.</p>
      )}
    </div>
  );
}
