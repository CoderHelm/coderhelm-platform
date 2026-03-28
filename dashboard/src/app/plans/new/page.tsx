"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { api, type BillingInfo } from "@/lib/api";
import { useToast } from "@/components/toast";

interface Message {
  role: "user" | "assistant";
  content: string;
}

interface DraftTask {
  title: string;
  description: string;
  acceptance_criteria: string;
  order: number;
}

interface DraftPlan {
  title: string;
  description: string;
  repo: string;
  tasks: DraftTask[];
}

// Simple client-side plan parser from AI-structured markdown
function parsePlan(text: string): DraftPlan | null {
  try {
    // Try to find JSON block first
    const jsonMatch = text.match(/```json\n([\s\S]*?)\n```/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[1]);
    }
    return null;
  } catch {
    return null;
  }
}

function generateSystemPrompt(): string {
  return `You are a planning assistant for d3ftly, an autonomous AI coding agent.
Your job is to help the user break down a feature, epic, or large piece of work into a structured plan.

When the user describes what they want to build, you should:
1. Ask clarifying questions to understand scope, tech stack, repo, constraints
2. Once you have enough context, generate a structured plan

When generating the final plan, output it in this EXACT format:

\`\`\`json
{
  "title": "Short epic title",
  "description": "1-2 sentence overview of the epic",
  "repo": "owner/repo",
  "tasks": [
    {
      "title": "Concise task title",
      "description": "What needs to be built and why. Be specific about files, APIs, UI components.",
      "acceptance_criteria": "- Bullet list\\n- Of verifiable criteria\\n- That define done",
      "order": 0
    }
  ]
}
\`\`\`

Rules:
- Tasks should be independently implementable (one PR each)
- Order matters — d3ftly works on them sequentially
- Each task title should be a GitHub issue title (imperative, max 60 chars)
- Acceptance criteria should be machine-verifiable where possible
- 3-10 tasks is ideal; break up anything larger
- Ask for repo name if not provided`;
}

const DEMO_RESPONSES: string[] = [
  "I can help you break that down into a structured plan. Could you tell me a bit more about:\n\n1. Which repository this will live in?\n2. What's the tech stack?\n3. Do you have any existing patterns I should follow?\n4. What's the rough scope — MVP or full feature?",
  "Great! Let me put together a plan based on what you've described. I'll generate the tasks now...",
];

export default function NewPlanPage() {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "assistant",
      content: "Hi! Tell me what you want to build and I'll help you break it into an ordered plan of GitHub issues that d3ftly can implement one by one.",
    },
  ]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [draft, setDraft] = useState<DraftPlan | null>(null);
  const [saving, setSaving] = useState(false);
  const [billing, setBilling] = useState<BillingInfo | null>(null);
  const [billingLoading, setBillingLoading] = useState(true);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const router = useRouter();
  const { toast } = useToast();
  const demoIdx = useRef(0);

  useEffect(() => {
    api
      .getBilling()
      .then((b) => setBilling(b))
      .catch(() => {})
      .finally(() => setBillingLoading(false));
  }, []);

  const plansEnabled = billing?.subscription_status === "active";

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = async () => {
    if (!input.trim() || sending) return;
    const userMsg = input.trim();
    setInput("");
    setSending(true);

    setMessages((prev) => [...prev, { role: "user", content: userMsg }]);

    // In production this calls an AI endpoint. For now, simulate with demo responses.
    await new Promise((r) => setTimeout(r, 800));

    const isLastDemo = demoIdx.current >= DEMO_RESPONSES.length - 1;
    let response: string;

    if (isLastDemo) {
      // Generate a plan demo based on user input
      const planTitle = userMsg.length > 50 ? userMsg.slice(0, 50) + "..." : userMsg;
      response = `Here's the structured plan I've generated:\n\n\`\`\`json\n${JSON.stringify({
        title: planTitle,
        description: `Implement: ${userMsg}`,
        repo: "",
        tasks: [
          {
            title: "Set up data model and migrations",
            description: "Define the database schema and any migrations needed for this feature. Include indexes for expected query patterns.",
            acceptance_criteria: "- Schema created and applied\n- Migration scripts committed\n- Tests pass",
            order: 0,
          },
          {
            title: "Build API endpoints",
            description: "Implement the backend API endpoints required. Follow existing REST patterns and add auth middleware.",
            acceptance_criteria: "- All endpoints return correct status codes\n- Auth enforced on protected routes\n- Input validation in place",
            order: 1,
          },
          {
            title: "Build UI components",
            description: "Create the frontend UI for this feature. Match existing design system patterns.",
            acceptance_criteria: "- Components render without errors\n- Loading and error states handled\n- Responsive layout",
            order: 2,
          },
          {
            title: "Write tests",
            description: "Add unit and integration tests for the new feature.",
            acceptance_criteria: "- Test coverage >80%\n- All tests passing in CI",
            order: 3,
          },
        ],
      }, null, 2)}\n\`\`\`\n\nFeel free to edit the tasks above before approving. Once you're happy with the plan, save it and approve the tasks you want d3ftly to work on!`;
    } else {
      response = DEMO_RESPONSES[demoIdx.current];
      demoIdx.current++;
    }

    const assistantMsg: Message = { role: "assistant", content: response };
    setMessages((prev) => [...prev, assistantMsg]);

    // Parse any plan from the response
    const parsed = parsePlan(response);
    if (parsed) {
      setDraft(parsed);
    }

    setSending(false);
  };

  const savePlan = async () => {
    if (!plansEnabled) {
      toast("Plans requires Pro or the Plans add-on", "error");
      return;
    }
    if (!draft) return;
    setSaving(true);
    try {
      const { plan_id } = await api.createPlan(draft);
      toast("Plan created!");
      router.push(`/plans/detail?id=${plan_id}`);
    } catch {
      toast("Failed to save plan", "error");
      setSaving(false);
    }
  };

  if (billingLoading) {
    return <div className="text-sm text-zinc-500">Loading...</div>;
  }

  if (!plansEnabled) {
    return (
      <div className="max-w-2xl">
        <a href="/plans" className="text-zinc-500 hover:text-zinc-300 text-sm">
          ← Plans
        </a>
        <div className="mt-4 rounded-lg border border-yellow-500/30 bg-yellow-500/10 p-6">
          <h1 className="text-lg font-semibold text-yellow-300">Plans is a paid feature</h1>
          <p className="text-sm text-yellow-200/80 mt-2">
            Upgrade to Pro (or add the Plans add-on) to use AI planning and create executable task lists.
          </p>
          <a
            href="/billing"
            className="inline-block mt-4 px-4 py-2 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200"
          >
            Go to Billing
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl flex flex-col h-[calc(100vh-4rem)]">
      <div className="flex items-center gap-3 mb-4">
        <a href="/plans" className="text-zinc-500 hover:text-zinc-300 text-sm">← Plans</a>
        <h1 className="text-xl font-bold">New plan</h1>
      </div>

      <div className="flex gap-6 flex-1 min-h-0">
        {/* Chat panel */}
        <div className="flex flex-col flex-1 min-h-0">
          <div className="flex-1 overflow-y-auto space-y-4 pb-4 pr-2">
            {messages.map((msg, i) => (
              <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                <div
                  className={`max-w-[85%] px-4 py-3 rounded-xl text-sm whitespace-pre-wrap leading-relaxed ${
                    msg.role === "user"
                      ? "bg-zinc-700 text-zinc-100"
                      : "bg-zinc-900 border border-zinc-800 text-zinc-200"
                  }`}
                >
                  {msg.content.replace(/```json\n[\s\S]*?\n```/g, "(structured plan generated →)")}
                </div>
              </div>
            ))}
            {sending && (
              <div className="flex justify-start">
                <div className="px-4 py-3 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-500 text-sm">
                  Thinking...
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>

          <div className="flex gap-2 pt-3 border-t border-zinc-800">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  sendMessage();
                }
              }}
              placeholder="Describe what you want to build... (Enter to send)"
              rows={2}
              className="flex-1 px-4 py-2.5 bg-zinc-900 border border-zinc-700 rounded-lg text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-zinc-500 resize-none"
            />
            <button
              onClick={sendMessage}
              disabled={!input.trim() || sending}
              className="px-4 py-2 bg-zinc-100 text-zinc-900 rounded-lg text-sm font-medium hover:bg-white disabled:opacity-50 transition-colors self-end"
            >
              Send
            </button>
          </div>
        </div>

        {/* Draft plan panel */}
        {draft && (
          <div className="w-80 flex-shrink-0">
            <div className="sticky top-0 bg-zinc-900 border border-zinc-800 rounded-lg p-4 space-y-4">
              <div>
                <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">Generated plan</p>
                <h3 className="text-sm font-bold text-zinc-100">{draft.title}</h3>
                {draft.description && <p className="text-xs text-zinc-500 mt-1">{draft.description}</p>}
                {draft.repo && <p className="text-xs text-zinc-600 mt-1 font-mono">{draft.repo}</p>}
              </div>

              <div className="space-y-2">
                {draft.tasks.map((task, i) => (
                  <div key={i} className="flex gap-2.5 text-xs">
                    <span className="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-800 text-zinc-500 flex items-center justify-center text-[10px] mt-0.5">
                      {i + 1}
                    </span>
                    <div>
                      <p className="text-zinc-300 font-medium">{task.title}</p>
                      {task.description && (
                        <p className="text-zinc-600 mt-0.5 leading-snug line-clamp-2">{task.description}</p>
                      )}
                    </div>
                  </div>
                ))}
              </div>

              <button
                onClick={savePlan}
                disabled={saving}
                className="w-full px-4 py-2.5 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors disabled:opacity-50"
              >
                {saving ? "Saving..." : "Save plan"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
