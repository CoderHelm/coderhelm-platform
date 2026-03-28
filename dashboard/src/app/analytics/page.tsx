"use client";

import { useEffect, useState } from "react";
import { api, type Stats, type MonthStats } from "@/lib/api";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { CardSkeleton, ChartSkeleton } from "@/components/skeleton";

export default function AnalyticsPage() {
  const [stats, setStats] = useState<{ month: Stats; all_time: Stats } | null>(null);
  const [history, setHistory] = useState<MonthStats[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.getStats(), api.getStatsHistory()])
      .then(([s, h]) => {
        setStats(s);
        setHistory(h.months);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-6">Analytics</h1>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <CardSkeleton /><CardSkeleton /><CardSkeleton /><CardSkeleton />
          <CardSkeleton /><CardSkeleton /><CardSkeleton /><CardSkeleton />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ChartSkeleton /><ChartSkeleton /><ChartSkeleton /><ChartSkeleton />
        </div>
      </div>
    );
  }

  if (!stats) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-6">Analytics</h1>
        <p className="text-zinc-500">Unable to load analytics.</p>
      </div>
    );
  }

  const { month, all_time } = stats;

  const chartData = history.map((m) => ({
    ...m,
    // Display as "Mar" instead of "2026-03"
    label: new Date(m.period + "-01").toLocaleDateString("en", { month: "short" }),
    tokens: m.total_tokens_in + m.total_tokens_out,
  }));

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Analytics</h1>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KpiCard label="Runs this month" value={month.total_runs} />
        <KpiCard label="Merge rate" value={`${(month.merge_rate * 100).toFixed(0)}%`} />
        <KpiCard label="Cost this month" value={`$${month.total_cost_usd.toFixed(2)}`} />
        <KpiCard
          label="Tokens this month"
          value={formatNumber(month.total_tokens_in + month.total_tokens_out)}
        />
        <KpiCard label="All-time runs" value={all_time.total_runs} />
        <KpiCard label="All-time merge rate" value={`${(all_time.merge_rate * 100).toFixed(0)}%`} />
        <KpiCard label="All-time cost" value={`$${all_time.total_cost_usd.toFixed(2)}`} />
        <KpiCard
          label="All-time tokens"
          value={formatNumber(all_time.total_tokens_in + all_time.total_tokens_out)}
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard title="Runs per month">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="label" stroke="#71717a" fontSize={12} />
              <YAxis stroke="#71717a" fontSize={12} allowDecimals={false} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: 8 }}
                labelStyle={{ color: "#a1a1aa" }}
              />
              <Legend />
              <Bar dataKey="completed" name="Completed" fill="#22c55e" radius={[4, 4, 0, 0]} />
              <Bar dataKey="failed" name="Failed" fill="#ef4444" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Cost per month ($)">
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="label" stroke="#71717a" fontSize={12} />
              <YAxis stroke="#71717a" fontSize={12} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: 8 }}
                labelStyle={{ color: "#a1a1aa" }}
                formatter={(v) => [`$${Number(v).toFixed(2)}`, "Cost"]}
              />
              <Line
                type="monotone"
                dataKey="total_cost_usd"
                name="Cost"
                stroke="#8b5cf6"
                strokeWidth={2}
                dot={{ fill: "#8b5cf6", r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Tokens per month">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="label" stroke="#71717a" fontSize={12} />
              <YAxis stroke="#71717a" fontSize={12} tickFormatter={formatNumber} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: 8 }}
                labelStyle={{ color: "#a1a1aa" }}
                formatter={(v) => [formatNumber(Number(v)), ""]}
              />
              <Legend />
              <Bar dataKey="total_tokens_in" name="Input" fill="#3b82f6" radius={[4, 4, 0, 0]} />
              <Bar dataKey="total_tokens_out" name="Output" fill="#06b6d4" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Merge rate trend">
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={chartData.map((d) => ({
              ...d,
              merge_rate: d.total_runs > 0 ? (d.completed / d.total_runs) * 100 : 0,
            }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="label" stroke="#71717a" fontSize={12} />
              <YAxis stroke="#71717a" fontSize={12} domain={[0, 100]} tickFormatter={(v) => `${v}%`} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: 8 }}
                labelStyle={{ color: "#a1a1aa" }}
                formatter={(v) => [`${Number(v).toFixed(0)}%`, "Merge rate"]}
              />
              <Line
                type="monotone"
                dataKey="merge_rate"
                name="Merge rate"
                stroke="#22c55e"
                strokeWidth={2}
                dot={{ fill: "#22c55e", r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}

function KpiCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
      <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-xl font-bold">{value}</p>
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="p-5 bg-zinc-900/50 border border-zinc-800 rounded-lg">
      <h3 className="text-sm font-medium text-zinc-400 mb-4">{title}</h3>
      {children}
    </div>
  );
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}
