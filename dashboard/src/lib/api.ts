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

  // Plans
  listPlans: () => request<{ plans: Plan[] }>("/api/plans"),
  createPlan: (body: { title: string; description?: string; repo?: string; tasks?: Partial<Task>[] }) =>
    request<{ plan_id: string }>("/api/plans", { method: "POST", body: JSON.stringify(body) }),
  getPlan: (planId: string) => request<Plan & { tasks: Task[] }>(`/api/plans/${planId}`),
  updatePlan: (planId: string, body: Partial<{ title: string; description: string; status: string }>) =>
    request<void>(`/api/plans/${planId}`, { method: "PUT", body: JSON.stringify(body) }),
  deletePlan: (planId: string) => request<void>(`/api/plans/${planId}`, { method: "DELETE" }),
  addTask: (planId: string, body: Partial<Task>) =>
    request<{ task_id: string }>(`/api/plans/${planId}/tasks`, { method: "POST", body: JSON.stringify(body) }),
  updateTask: (planId: string, taskId: string, body: Partial<Task>) =>
    request<void>(`/api/plans/${planId}/tasks/${taskId}`, { method: "PUT", body: JSON.stringify(body) }),
  deleteTask: (planId: string, taskId: string) =>
    request<void>(`/api/plans/${planId}/tasks/${taskId}`, { method: "DELETE" }),
  approveTask: (planId: string, taskId: string) =>
    request<void>(`/api/plans/${planId}/tasks/${taskId}/approve`, { method: "POST" }),
  rejectTask: (planId: string, taskId: string) =>
    request<void>(`/api/plans/${planId}/tasks/${taskId}/reject`, { method: "POST" }),
  executePlan: (planId: string) =>
    request<{ status: string; tasks_queued: number }>(`/api/plans/${planId}/execute`, { method: "POST" }),

  // Infrastructure
  getInfrastructure: () => request<InfraAnalysis>("/api/infrastructure"),
  refreshInfrastructure: () =>
    request<{ status: string }>("/api/infrastructure/refresh", { method: "POST" }),
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

export interface Plan {
  plan_id: string;
  title: string;
  description: string;
  repo: string;
  status: "draft" | "executing" | "done" | string;
  task_count: number;
  created_at: string;
  updated_at: string;
  executed_at?: string;
  executed_by?: string;
  tasks?: Task[];
}

export interface Task {
  task_id: string;
  plan_id: string;
  title: string;
  description: string;
  acceptance_criteria: string;
  status: "draft" | "approved" | "rejected" | "queued" | "running" | "done" | string;
  order: number;
  issue_number?: number;
  issue_url?: string;
  run_id?: string;
  approved_at?: string;
  approved_by?: string;
  rejected_at?: string;
  rejected_by?: string;
  created_at: string;
}


export interface InfraFinding {
  severity: "error" | "warning" | "info";
  category: "security" | "performance" | "cost" | "reliability";
  title: string;
  detail: string;
  file?: string;
}

export interface InfraAnalysis {
  status: "pending" | "ready" | "no_infra";
  has_infra: boolean;
  diagram?: string;
  diagram_title?: string;
  findings?: InfraFinding[];
  suggested_prompt?: string;
  cached_at?: string;
  scanned_repos?: string[];
}
