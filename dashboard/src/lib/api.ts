const API_BASE = process.env.NEXT_PUBLIC_API_URL || "https://app.d3ftly.com";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

export const api = {
  // Auth
  me: () => request<{ user_id: string; tenant_id: string; github_login: string; email: string; avatar_url: string }>("/api/me"),

  // Runs
  listRuns: () => request<{ runs: Run[] }>("/api/runs"),
  getRun: (id: string) => request<Run>(`/api/runs/${id}`),

  // Repos
  listRepos: () => request<{ repos: Repo[] }>("/api/repos"),

  // Stats
  getStats: () => request<{ month: Stats; all_time: Stats }>("/api/stats"),

  // Rules (guardrails)
  getGlobalRules: () => request<{ rules: string[] }>("/api/rules/global"),
  updateGlobalRules: (rules: string[]) => request<void>("/api/rules/global", { method: "PUT", body: JSON.stringify({ rules }) }),
  getRepoRules: (repo: string) => request<{ rules: string[] }>(`/api/rules/repo/${repo}`),
  updateRepoRules: (repo: string, rules: string[]) => request<void>(`/api/rules/repo/${repo}`, { method: "PUT", body: JSON.stringify({ rules }) }),

  // Voice
  getRepoVoice: (repo: string) => request<{ content: string }>(`/api/voice/repo/${repo}`),
  updateRepoVoice: (repo: string, content: string) => request<void>(`/api/voice/repo/${repo}`, { method: "PUT", body: JSON.stringify({ content }) }),

  // Agents
  getRepoAgents: (repo: string) => request<{ content: string }>(`/api/agents/repo/${repo}`),
  updateRepoAgents: (repo: string, content: string) => request<void>(`/api/agents/repo/${repo}`, { method: "PUT", body: JSON.stringify({ content }) }),

  // Instructions
  getGlobalInstructions: () => request<{ content: string }>("/api/instructions/global"),
  updateGlobalInstructions: (content: string) => request<void>("/api/instructions/global", { method: "PUT", body: JSON.stringify({ content }) }),

  // Notifications
  getNotifications: () => request<NotificationPrefs>("/api/notifications"),
  updateNotifications: (prefs: NotificationPrefs) => request<void>("/api/notifications", { method: "PUT", body: JSON.stringify(prefs) }),

  // Billing
  getBilling: () => request<Billing>("/api/billing"),
  createPortal: () => request<{ url: string }>("/api/billing/portal", { method: "POST" }),
};

export interface Run {
  run_id: string;
  status: string;
  ticket_id: string;
  title: string;
  repo: string;
  pr_url?: string;
  cost_usd?: number;
  duration_s?: number;
  created_at: string;
  current_pass?: string;
}

export interface Repo {
  name: string;
  enabled: boolean;
  ticket_source: string;
}

export interface Stats {
  total_runs: number;
  completed: number;
  failed: number;
  in_progress: number;
  total_cost_usd: number;
  merge_rate: number;
}

export interface NotificationPrefs {
  email_run_complete: boolean;
  email_run_failed: boolean;
  email_weekly_summary: boolean;
}

export interface Billing {
  plan: string;
  status: string;
  runs_used: number;
  runs_limit: number;
}
