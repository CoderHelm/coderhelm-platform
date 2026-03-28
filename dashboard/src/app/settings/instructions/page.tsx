"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import { useToast } from "@/components/toast";
import { TextareaSkeleton } from "@/components/skeleton";

export default function InstructionsPage() {
  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  useEffect(() => {
    api.getGlobalInstructions().then((data) => {
      setContent(data.content);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const save = async () => {
    setSaving(true);
    try {
      await api.updateGlobalInstructions(content);
      toast("Instructions saved");
    } catch {
      toast("Failed to save instructions", "error");
    }
    setSaving(false);
  };

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-2">Custom Instructions</h1>
      <p className="text-zinc-400 text-sm mb-6">
        Global instructions that d3ftly follows for all repos. Use this for team-wide conventions, naming rules, or
        preferences that aren&apos;t covered by guardrails.
      </p>

      {loading ? (
        <TextareaSkeleton />
      ) : (
        <>
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={16}
            placeholder={`Example:\n- Always use TypeScript strict mode\n- Prefer named exports over default exports\n- Use kebab-case for file names\n- Write tests for all new functions`}
            className="w-full px-4 py-3 bg-zinc-900 border border-zinc-700 rounded-lg text-sm text-zinc-100 font-mono placeholder-zinc-700 focus:outline-none focus:border-zinc-500 resize-y"
          />
          <div className="flex items-center gap-3 mt-3">
            <button
              onClick={save}
              disabled={saving}
              className="px-4 py-2 bg-zinc-100 text-zinc-900 rounded-lg text-sm font-medium hover:bg-white disabled:opacity-50 transition-colors"
            >
              {saving ? "Saving..." : "Save"}
            </button>
            <span className="text-xs text-zinc-600">Plain text or markdown. Max 10KB.</span>
          </div>
        </>
      )}
    </div>
  );
}
