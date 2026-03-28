"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { useToast } from "@/components/toast";
import { Skeleton } from "@/components/skeleton";

export default function GuardrailsPage() {
  const [rules, setRules] = useState<string[]>([]);
  const [newRule, setNewRule] = useState("");
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  useEffect(() => {
    api.getGlobalRules().then((data) => {
      setRules(data.rules);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const addRule = () => {
    const trimmed = newRule.trim();
    if (!trimmed || rules.includes(trimmed)) return;
    const updated = [...rules, trimmed];
    setRules(updated);
    setNewRule("");
    save(updated);
  };

  const removeRule = (index: number) => {
    const updated = rules.filter((_, i) => i !== index);
    setRules(updated);
    save(updated);
  };

  const save = async (updated: string[]) => {
    setSaving(true);
    try {
      await api.updateGlobalRules(updated);
      toast("Rule saved");
    } catch {
      toast("Failed to save rules", "error");
    }
    setSaving(false);
  };

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-2">Guardrails</h1>
      <p className="text-zinc-400 text-sm mb-6">
        Must-rules that d3ftly will always follow. These are injected into every implement and review pass.
      </p>

      <div className="border border-zinc-800 rounded-lg p-4 mb-6 bg-zinc-900/30">
        <h3 className="text-xs font-medium text-zinc-500 uppercase tracking-wider mb-3">Built-in rules (always enforced)</h3>
        <div className="flex items-center gap-2 text-sm text-zinc-300">
          <span className="text-green-400">✓</span>
          Never push directly to the default/main branch
        </div>
      </div>

      {loading ? (
        <div className="space-y-2 mb-4">
          <Skeleton className="h-12 w-full" />
          <Skeleton className="h-12 w-full" />
          <Skeleton className="h-10 w-full" />
        </div>
      ) : (
        <>
          <div className="space-y-2 mb-4">
            {rules.map((rule, i) => (
              <div
                key={i}
                className="flex items-center justify-between gap-3 px-4 py-3 bg-zinc-900/50 border border-zinc-800 rounded-lg"
              >
                <span className="text-sm text-zinc-200">{rule}</span>
                <button
                  onClick={() => removeRule(i)}
                  className="text-zinc-600 hover:text-red-400 text-xs transition-colors"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>

          <div className="flex gap-2">
            <input
              type="text"
              value={newRule}
              onChange={(e) => setNewRule(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && addRule()}
              placeholder="e.g. Never delete database migrations"
              className="flex-1 px-4 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-zinc-500"
            />
            <button
              onClick={addRule}
              disabled={!newRule.trim() || saving}
              className="px-4 py-2 bg-zinc-100 text-zinc-900 rounded-lg text-sm font-medium hover:bg-white disabled:opacity-50 transition-colors"
            >
              Add rule
            </button>
          </div>

          {saving && <p className="text-xs text-zinc-500 mt-2">Saving...</p>}
        </>
      )}
    </div>
  );
}
