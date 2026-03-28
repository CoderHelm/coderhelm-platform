"use client";

import { useEffect, useState } from "react";
import { api, type Repo } from "@/lib/api";
import { useToast } from "@/components/toast";
import { TextareaSkeleton } from "@/components/skeleton";

export default function VoicePage() {
  const [repos, setRepos] = useState<Repo[]>([]);
  const [selectedRepo, setSelectedRepo] = useState("");
  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  useEffect(() => {
    api.listRepos().then((data) => {
      setRepos(data.repos);
      if (data.repos.length > 0) {
        setSelectedRepo(data.repos[0].name);
      }
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selectedRepo) return;
    setLoading(true);
    api.getRepoVoice(selectedRepo).then((data) => {
      setContent(data.content);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [selectedRepo]);

  const save = async () => {
    if (!selectedRepo) return;
    setSaving(true);
    try {
      await api.updateRepoVoice(selectedRepo, content);
      toast("Voice saved");
    } catch {
      toast("Failed to save voice", "error");
    }
    setSaving(false);
  };

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-2">Team Voice</h1>
      <p className="text-zinc-400 text-sm mb-6">
        Define how d3ftly writes PR descriptions, commit messages, and code comments.
        This is auto-generated during onboarding by analyzing your team&apos;s existing PRs and commits — but you can edit it anytime.
      </p>

      {repos.length > 1 && (
        <select
          value={selectedRepo}
          onChange={(e) => setSelectedRepo(e.target.value)}
          className="mb-4 px-3 py-2 bg-zinc-900 border border-zinc-700 rounded-lg text-sm text-zinc-100 focus:outline-none focus:border-zinc-500"
        >
          {repos.map((r) => (
            <option key={r.name} value={r.name}>{r.name}</option>
          ))}
        </select>
      )}

      {loading ? (
        <TextareaSkeleton />
      ) : (
        <>
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={20}
            placeholder={`# Team Voice\n\n## Tone\nCasual and direct. No emojis in code.\n\n## Commit Messages\nConventional commits (feat:, fix:, chore:). Imperative mood.\n\n## PR Descriptions\nStructured with Problem, Changes, Risk, Verification sections.\n\n## Language\nTechnical but concise. Avoid jargon.`}
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
            <span className="text-xs text-zinc-600">Markdown format. Applied to PR description pass.</span>
          </div>
        </>
      )}
    </div>
  );
}
