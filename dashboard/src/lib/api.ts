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
  getStatsHistory: () => request<{ months: MonthStats[] }>("/api/stats/history"),

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
  getBilling: () => request<BillingInfo>("/api/billing"),
  createSubscription: (priceId: string) => request<{ subscription_id: string; client_secret: string }>("/api/billing/subscribe", { method: "POST", body: JSON.stringify({ price_id: priceId }) }),
  cancelSubscription: () => request<{ status: string }>("/api/billing/cancel", { method: "POST" }),
  reactivateSubscription: () => request<{ status: string }>("/api/billing/reactivate", { method: "POST" }),
  createSetupIntent: () => request<{ client_secret: string }>("/api/billing/payment-method", { method: "POST" }),
  listInvoices: () => request<{ invoices: Invoice[] }>("/api/billing/invoices"),
  getInvoicePdf: (id: string) => request<{ pdf_url: string }>(`/api/billing/invoices/${id}/pdf`),
};

export interface Run {
  run_id: string;
  status: string;
  ticket_id: string;
  title: string;
  repo: string;
  pr_url?: string;
  cost_usd?: number;
  tokens_in?: number;
  tokens_out?: number;
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
  total_tokens_in: number;
  total_tokens_out: number;
  merge_rate: number;
}

export interface MonthStats {
  period: string;
  total_runs: number;
  completed: number;
  failed: number;
  total_cost_usd: number;
  total_tokens_in: number;
  total_tokens_out: number;
}

export interface NotificationPrefs {
  email_run_complete: boolean;
  email_run_failed: boolean;
  email_weekly_summary: boolean;
}

export interface BillingInfo {
  subscription_status: string;
  plan_id: string | null;
  has_payment_method: boolean;
  last_payment_at: string | null;
  payment_retry_count: number;
  last_failure_reason: string | null;
  access_until: string | null;
  cancelled_at: string | null;
  current_period: {
    month: string;
    usage_cost: number;
    total_runs: number;
  };
  recent_payments: Payment[];
}

export interface Payment {
  invoice_number: string | null;
  amount_cents: number | null;
  status: string | null;
  created_at: string | null;
}

export interface Invoice {
  invoice_id: string | null;
  invoice_number: string | null;
  amount_cents: number | null;
  period: string | null;
  status: string | null;
  created_at: string | null;
}
