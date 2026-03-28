"use client";

import { Suspense, useEffect, useState, useCallback } from "react";
import { api, type InfraAnalysis, type InfraFinding } from "@/lib/api";
import { useToast } from "@/components/toast";
import { Skeleton } from "@/components/skeleton";
import { MermaidDiagram } from "@/components/mermaid-diagram";

const SEVERITY_STYLES = {
  error: "border-red-500/20 bg-red-500/5 text-red-400",
  warning: "border-yellow-500/20 bg-yellow-500/5 text-yellow-400",
  info: "border-zinc-700 bg-zinc-900/50 text-zinc-400",
};

const SEVERITY_ICONS = {
  error: "✕",
  warning: "⚠",
  info: "ℹ",
};

const CATEGORY_LABELS = {
  security: "Security",
  performance: "Performance",
  cost: "Cost",
  reliability: "Reliability",
};

function FindingCard({ finding }: { finding: InfraFinding }) {
  return (
    <div className={`p-3 rounded-lg border ${SEVERITY_STYLES[finding.severity] || SEVERITY_STYLES.info}`}>
      <div className="flex items-start gap-2">
        <span className="text-xs mt-0.5 flex-shrink-0">{SEVERITY_ICONS[finding.severity]}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-xs font-semibold">{finding.title}</span>
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-500 border border-zinc-700">{CATEGORY_LABELS[finding.category] ?? finding.category}</span>
          </div>
          <p className="text-xs text-zinc-500 mt-1 leading-relaxed">{finding.detail}</p>
          {finding.file && <p className="text-[10px] text-zinc-600 font-mono mt-1">{finding.file}</p>}
        </div>
      </div>
    </div>
  );
}

function InfrastructureContent() {
  const [analysis, setAnalysis] = useState<InfraAnalysis | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [activeTab, setActiveTab] = useState<"diagram" | "findings">("diagram");
  const [copied, setCopied] = useState(false);
  const { toast } = useToast();

  const load = useCallback(() => {
    api.getInfrastructure()
      .then(setAnalysis)
      .catch(() => toast("Failed to load infrastructure analysis", "error"))
      .finally(() => setLoading(false));
  }, [toast]);

  useEffect(() => {
    load();
  }, [load]);

  // Poll while pending
  useEffect(() => {
    if (analysis?.status !== "pending") return;
    const timer = setInterval(() => {
      api.getInfrastructure().then(setAnalysis).catch(() => {});
    }, 5000);
    return () => clearInterval(timer);
  }, [analysis?.status]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await api.refreshInfrastructure();
      setAnalysis((prev) => prev ? { ...prev, status: "pending" } : null);
      toast("Analysis started — this takes about 30 seconds");
    } catch {
      toast("Failed to start analysis", "error");
    } finally {
      setRefreshing(false);
    }
  };

  const handleCopyPrompt = () => {
    if (!analysis?.suggested_prompt) return;
    navigator.clipboard.writeText(analysis.suggested_prompt);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-4 w-96" />
        <Skeleton className="h-96 w-full rounded-xl" />
      </div>
    );
  }

  // No infrastructure code found
  if (!analysis || analysis.status === "no_infra") {
    return (
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold">Infrastructure</h1>
            <p className="text-zinc-500 text-sm mt-1">Architecture diagram and analysis of your connected repos</p>
          </div>
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="px-4 py-2 text-sm border border-zinc-700 rounded-lg text-zinc-400 hover:text-zinc-200 hover:border-zinc-500 transition-colors disabled:opacity-50"
          >
            {refreshing ? "Scanning..." : "Scan repos"}
          </button>
        </div>

        <div className="max-w-xl mx-auto mt-16 text-center">
          <div className="w-16 h-16 rounded-2xl bg-zinc-900 border border-zinc-800 flex items-center justify-center mx-auto mb-4 text-2xl">⬡</div>
          <h2 className="text-lg font-semibold mb-2">No infrastructure code found</h2>
          <p className="text-zinc-500 text-sm mb-6">
            d3ftly scanned your connected repos and couldn't find any CDK or Terraform files.
            Use the prompt below to create a Plan and generate your infrastructure from scratch.
          </p>

          {analysis?.suggested_prompt && (
            <div className="text-left">
              <div className="flex items-center justify-between mb-2">
                <p className="text-xs text-zinc-500 uppercase tracking-wider">Suggested plan prompt</p>
                <button onClick={handleCopyPrompt} className="text-xs text-zinc-500 hover:text-zinc-300 transition-colors">
                  {copied ? "Copied!" : "Copy"}
                </button>
              </div>
              <pre className="p-4 bg-zinc-900 border border-zinc-800 rounded-lg text-xs text-zinc-400 font-mono whitespace-pre-wrap leading-relaxed text-left overflow-auto max-h-64">
                {analysis.suggested_prompt}
              </pre>
              <a
                href="/plans/new"
                className="mt-4 inline-block px-5 py-2.5 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors"
              >
                Create a Plan →
              </a>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Pending analysis
  if (analysis.status === "pending") {
    return (
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold">Infrastructure</h1>
            <p className="text-zinc-500 text-sm mt-1">Architecture diagram and analysis of your connected repos</p>
          </div>
        </div>
        <div className="flex flex-col items-center justify-center py-32">
          <div className="w-8 h-8 border-2 border-zinc-600 border-t-zinc-300 rounded-full animate-spin mb-4" />
          <p className="text-zinc-400 text-sm">Analyzing your infrastructure...</p>
          <p className="text-zinc-600 text-xs mt-1">Scanning repos for CDK/Terraform code and generating diagram</p>
        </div>
      </div>
    );
  }

  // Has analysis
  const errorCount = analysis.findings?.filter((f) => f.severity === "error").length ?? 0;
  const warnCount = analysis.findings?.filter((f) => f.severity === "warning").length ?? 0;
  const infoCount = analysis.findings?.filter((f) => f.severity === "info").length ?? 0;

  return (
    <div>
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Infrastructure</h1>
          <div className="flex items-center gap-3 mt-1">
            <p className="text-zinc-500 text-sm">{analysis.diagram_title ?? "Architecture"}</p>
            {analysis.scanned_repos && analysis.scanned_repos.length > 0 && (
              <span className="text-xs text-zinc-600">
                {analysis.scanned_repos.length} repo{analysis.scanned_repos.length !== 1 ? "s" : ""} scanned
              </span>
            )}
            {analysis.cached_at && (
              <span className="text-xs text-zinc-700">
                cached {new Date(analysis.cached_at).toLocaleDateString()}
              </span>
            )}
          </div>
        </div>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="px-4 py-2 text-sm border border-zinc-700 rounded-lg text-zinc-400 hover:text-zinc-200 hover:border-zinc-500 transition-colors disabled:opacity-50"
        >
          {refreshing ? "Refreshing..." : "Refresh"}
        </button>
      </div>

      {/* Summary badges */}
      {(errorCount > 0 || warnCount > 0) && (
        <div className="flex gap-2 mb-4">
          {errorCount > 0 && (
            <span className="px-2.5 py-1 bg-red-500/10 border border-red-500/20 text-red-400 rounded-full text-xs">
              {errorCount} issue{errorCount !== 1 ? "s" : ""}
            </span>
          )}
          {warnCount > 0 && (
            <span className="px-2.5 py-1 bg-yellow-500/10 border border-yellow-500/20 text-yellow-400 rounded-full text-xs">
              {warnCount} warning{warnCount !== 1 ? "s" : ""}
            </span>
          )}
          {infoCount > 0 && (
            <span className="px-2.5 py-1 bg-zinc-800 border border-zinc-700 text-zinc-500 rounded-full text-xs">
              {infoCount} note{infoCount !== 1 ? "s" : ""}
            </span>
          )}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 mb-4 p-1 bg-zinc-900 border border-zinc-800 rounded-lg w-fit">
        {(["diagram", "findings"] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-1.5 rounded-md text-sm transition-colors capitalize ${
              activeTab === tab
                ? "bg-zinc-700 text-zinc-100"
                : "text-zinc-500 hover:text-zinc-300"
            }`}
          >
            {tab}
            {tab === "findings" && analysis.findings && analysis.findings.length > 0 && (
              <span className="ml-1.5 text-xs text-zinc-600">({analysis.findings.length})</span>
            )}
          </button>
        ))}
      </div>

      {activeTab === "diagram" && (
        <div className="p-6 bg-zinc-950 border border-zinc-800 rounded-xl min-h-64">
          {analysis.diagram ? (
            <MermaidDiagram chart={analysis.diagram} />
          ) : (
            <p className="text-zinc-600 text-sm text-center py-16">No diagram generated</p>
          )}
        </div>
      )}

      {activeTab === "findings" && (
        <div className="space-y-2">
          {!analysis.findings || analysis.findings.length === 0 ? (
            <div className="p-6 text-center text-zinc-600 text-sm border border-zinc-800 rounded-xl">
              No findings — looks great!
            </div>
          ) : (
            <>
              {/* Group by severity */}
              {(["error", "warning", "info"] as const).map((sev) => {
                const group = analysis.findings!.filter((f) => f.severity === sev);
                if (group.length === 0) return null;
                return (
                  <div key={sev}>
                    <p className="text-[10px] text-zinc-600 uppercase tracking-wider mb-1.5 mt-3">
                      {sev === "error" ? "Issues" : sev === "warning" ? "Warnings" : "Notes"}
                    </p>
                    <div className="space-y-1.5">
                      {group.map((finding, i) => (
                        <FindingCard key={i} finding={finding} />
                      ))}
                    </div>
                  </div>
                );
              })}
            </>
          )}
        </div>
      )}
    </div>
  );
}

export default function InfrastructurePage() {
  return (
    <Suspense fallback={<div className="space-y-4"><Skeleton className="h-8 w-64" /><Skeleton className="h-96 w-full" /></div>}>
      <InfrastructureContent />
    </Suspense>
  );
}
