"use client";

import { useEffect, useState, useCallback } from "react";
import { api, type BillingInfo, type Invoice } from "@/lib/api";
import { loadStripe } from "@stripe/stripe-js";
import { Elements, PaymentElement, useStripe, useElements } from "@stripe/react-stripe-js";
import { useToast } from "@/components/toast";
import { CardSkeleton, TableSkeleton } from "@/components/skeleton";

const STRIPE_PK = process.env.NEXT_PUBLIC_STRIPE_PK || "";
const PRICE_ID = process.env.NEXT_PUBLIC_STRIPE_PRICE_ID || "";
const stripePromise = STRIPE_PK ? loadStripe(STRIPE_PK) : null;

export default function BillingPage() {
  const [billing, setBilling] = useState<BillingInfo | null>(null);
  const [invoices, setInvoices] = useState<Invoice[]>([]);
  const [loading, setLoading] = useState(true);
  const [showSubscribe, setShowSubscribe] = useState(false);
  const [showUpdateCard, setShowUpdateCard] = useState(false);
  const [clientSecret, setClientSecret] = useState<string | null>(null);
  const [setupSecret, setSetupSecret] = useState<string | null>(null);
  const [actionLoading, setActionLoading] = useState(false);
  const { toast } = useToast();

  const refresh = useCallback(() => {
    setLoading(true);
    Promise.all([api.getBilling(), api.listInvoices()])
      .then(([b, i]) => {
        setBilling(b);
        setInvoices(i.invoices);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const handleSubscribe = async () => {
    setActionLoading(true);
    try {
      const { client_secret } = await api.createSubscription(PRICE_ID);
      setClientSecret(client_secret);
      setShowSubscribe(true);
    } catch {
      toast("Failed to start subscription. Please try again.", "error");
    } finally {
      setActionLoading(false);
    }
  };

  const handleCancel = async () => {
    if (!confirm("Cancel your subscription? You'll retain access until the end of the billing period.")) return;
    setActionLoading(true);
    try {
      await api.cancelSubscription();
      toast("Subscription cancelled");
      refresh();
    } catch {
      toast("Failed to cancel. Please try again.", "error");
    } finally {
      setActionLoading(false);
    }
  };

  const handleReactivate = async () => {
    setActionLoading(true);
    try {
      await api.reactivateSubscription();
      toast("Subscription reactivated");
      refresh();
    } catch {
      toast("Failed to reactivate. Please try again.", "error");
    } finally {
      setActionLoading(false);
    }
  };

  const handleUpdateCard = async () => {
    setActionLoading(true);
    try {
      const { client_secret } = await api.createSetupIntent();
      setSetupSecret(client_secret);
      setShowUpdateCard(true);
    } catch {
      toast("Failed to start card update. Please try again.", "error");
    } finally {
      setActionLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="max-w-3xl">
        <h1 className="text-2xl font-bold mb-6">Billing</h1>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <CardSkeleton /><CardSkeleton /><CardSkeleton /><CardSkeleton />
        </div>
        <TableSkeleton rows={3} cols={4} />
      </div>
    );
  }

  if (!billing) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-6">Billing</h1>
        <p className="text-zinc-500">Unable to load billing info.</p>
      </div>
    );
  }

  const isActive = billing.subscription_status === "active";
  const isCancelling = billing.subscription_status === "active" && billing.access_until;
  const isPastDue = billing.subscription_status === "past_due";
  const isCancelled = billing.subscription_status === "cancelled";
  const isFree = billing.subscription_status === "none" || !billing.subscription_status;
  const canSubscribe = isFree || isCancelled;

  return (
    <div className="max-w-3xl">
      <h1 className="text-2xl font-bold mb-6">Billing</h1>

      {/* Stripe Elements modal for subscribing */}
      {showSubscribe && clientSecret && stripePromise && (
        <Modal onClose={() => { setShowSubscribe(false); setClientSecret(null); }}>
          <h2 className="text-lg font-bold mb-4">Subscribe to Pro</h2>
          <Elements stripe={stripePromise} options={{ clientSecret, appearance: stripeAppearance }}>
            <SubscribeForm onSuccess={() => { setShowSubscribe(false); setClientSecret(null); refresh(); }} />
          </Elements>
        </Modal>
      )}

      {/* Stripe Elements modal for updating card */}
      {showUpdateCard && setupSecret && stripePromise && (
        <Modal onClose={() => { setShowUpdateCard(false); setSetupSecret(null); }}>
          <h2 className="text-lg font-bold mb-4">Update payment method</h2>
          <Elements stripe={stripePromise} options={{ clientSecret: setupSecret, appearance: stripeAppearance }}>
            <UpdateCardForm onSuccess={() => { setShowUpdateCard(false); setSetupSecret(null); refresh(); }} />
          </Elements>
        </Modal>
      )}

      {/* Plan + Usage */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <StatCard label="Plan" value={canSubscribe ? "Free" : "Pro"} />
        <StatCard label="Status" value={isCancelling ? "Cancelling" : isCancelled ? "Cancelled" : billing.subscription_status || "free"} />
        <StatCard label="Runs this month" value={billing.current_period.total_runs} />
        <StatCard label="Cost this month" value={`$${billing.current_period.usage_cost.toFixed(2)}`} />
      </div>

      {/* Past due warning */}
      {isPastDue && (
        <div className="p-4 mb-6 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-sm">
          Payment failed{billing.last_failure_reason ? `: ${billing.last_failure_reason}` : ""}. Please update your payment method.
        </div>
      )}

      {/* Cancelling notice */}
      {isCancelling && (
        <div className="p-4 mb-6 bg-yellow-500/10 border border-yellow-500/30 rounded-lg text-yellow-400 text-sm">
          Your subscription will end on {new Date(billing.access_until!).toLocaleDateString()}.
          <button onClick={handleReactivate} disabled={actionLoading} className="ml-2 underline hover:no-underline">
            Undo cancellation
          </button>
        </div>
      )}

      {/* Cancelled notice */}
      {isCancelled && (
        <div className="p-4 mb-6 bg-zinc-500/10 border border-zinc-500/30 rounded-lg text-zinc-400 text-sm">
          Your subscription has ended. Subscribe again to continue using d3ftly Pro.
        </div>
      )}

      {/* Action buttons */}
      <div className="flex gap-3 mb-8">
        {canSubscribe && (
          <button
            onClick={handleSubscribe}
            disabled={actionLoading}
            className="px-5 py-2.5 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors disabled:opacity-50"
          >
            {actionLoading ? "..." : isCancelled ? "Re-subscribe to Pro — $199/mo" : "Subscribe to Pro — $199/mo"}
          </button>
        )}
        {(isActive || isPastDue) && !isCancelling && (
          <>
            <button
              onClick={handleUpdateCard}
              disabled={actionLoading}
              className="px-4 py-2 bg-zinc-800 border border-zinc-700 text-zinc-200 rounded-lg text-sm font-medium hover:bg-zinc-700 transition-colors disabled:opacity-50"
            >
              Update payment method
            </button>
            <button
              onClick={handleCancel}
              disabled={actionLoading}
              className="px-4 py-2 bg-zinc-900 border border-zinc-800 text-zinc-400 rounded-lg text-sm font-medium hover:text-red-400 hover:border-red-500/30 transition-colors disabled:opacity-50"
            >
              Cancel subscription
            </button>
          </>
        )}
      </div>

      {/* Add-ons */}
      <div className="mb-8 border border-zinc-800 rounded-lg p-4 bg-zinc-900/30">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-sm font-semibold text-zinc-100">Plans Add-on</p>
            <p className="text-xs text-zinc-500 mt-1">
              Unlock AI planning, task approval workflows, and plan execution.
            </p>
          </div>
          {isActive ? (
            <span className="px-2 py-0.5 rounded-full text-[11px] border border-emerald-500/30 text-emerald-400 bg-emerald-500/10">
              Included in Pro
            </span>
          ) : (
            <span className="px-2 py-0.5 rounded-full text-[11px] border border-yellow-500/30 text-yellow-400 bg-yellow-500/10">
              Paid feature
            </span>
          )}
        </div>
        <div className="mt-3 flex items-center justify-between text-xs text-zinc-500">
          <span>Price: $49/mo as standalone add-on (or included with Pro)</span>
          <button
            onClick={() => {
              if (isActive) {
                toast("Plans is included in your Pro subscription");
              } else {
                toast("Upgrade to Pro or contact support to enable standalone add-on");
              }
            }}
            className="px-3 py-1.5 rounded-md border border-zinc-700 text-zinc-300 hover:bg-zinc-800"
          >
            {isActive ? "Included" : "Enable add-on"}
          </button>
        </div>
      </div>

      {/* Recent Payments */}
      {billing.recent_payments.length > 0 && (
        <div className="mb-8">
          <h2 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">Recent payments</h2>
          <div className="border border-zinc-800 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-zinc-900 text-zinc-400 text-left">
                <tr>
                  <th className="px-4 py-2.5 font-medium">Invoice</th>
                  <th className="px-4 py-2.5 font-medium">Amount</th>
                  <th className="px-4 py-2.5 font-medium">Status</th>
                  <th className="px-4 py-2.5 font-medium">Date</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {billing.recent_payments.map((p, i) => (
                  <tr key={i} className="hover:bg-zinc-900/50">
                    <td className="px-4 py-2.5 text-zinc-300 font-mono text-xs">{p.invoice_number || "—"}</td>
                    <td className="px-4 py-2.5">{p.amount_cents ? `$${(p.amount_cents / 100).toFixed(2)}` : "—"}</td>
                    <td className="px-4 py-2.5">
                      <span className={`px-2 py-0.5 rounded-full text-xs border ${
                        p.status === "paid"
                          ? "bg-green-500/10 text-green-400 border-green-500/20"
                          : "bg-zinc-800 text-zinc-400 border-zinc-700"
                      }`}>{p.status || "—"}</span>
                    </td>
                    <td className="px-4 py-2.5 text-zinc-400">{p.created_at ? new Date(p.created_at).toLocaleDateString() : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Invoices */}
      {invoices.length > 0 && (
        <div>
          <h2 className="text-sm font-medium text-zinc-400 uppercase tracking-wider mb-3">Invoices</h2>
          <div className="border border-zinc-800 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-zinc-900 text-zinc-400 text-left">
                <tr>
                  <th className="px-4 py-2.5 font-medium">Invoice</th>
                  <th className="px-4 py-2.5 font-medium">Period</th>
                  <th className="px-4 py-2.5 font-medium">Amount</th>
                  <th className="px-4 py-2.5 font-medium">PDF</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {invoices.map((inv, i) => (
                  <tr key={i} className="hover:bg-zinc-900/50">
                    <td className="px-4 py-2.5 text-zinc-300 font-mono text-xs">{inv.invoice_number || "—"}</td>
                    <td className="px-4 py-2.5 text-zinc-400">{inv.period || "—"}</td>
                    <td className="px-4 py-2.5">{inv.amount_cents ? `$${(inv.amount_cents / 100).toFixed(2)}` : "—"}</td>
                    <td className="px-4 py-2.5">
                      {inv.invoice_id && (
                        <button
                          onClick={async () => {
                              try {
                                const { pdf_url } = await api.getInvoicePdf(inv.invoice_id!);
                                window.open(pdf_url, "_blank");
                              } catch {
                                toast("Failed to download invoice", "error");
                              }
                          }}
                          className="text-blue-400 hover:underline text-xs"
                        >
                          Download
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Sub-components ───────────────────────────────────────────────

function SubscribeForm({ onSuccess }: { onSuccess: () => void }) {
  const stripe = useStripe();
  const elements = useElements();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!stripe || !elements) return;
    setSubmitting(true);
    setError(null);
    const result = await stripe.confirmPayment({ elements, redirect: "if_required" });
    if (result.error) {
      setError(result.error.message || "Payment failed");
      setSubmitting(false);
    } else {
      onSuccess();
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <PaymentElement />
      {error && <p className="text-red-400 text-sm mt-3">{error}</p>}
      <button
        type="submit"
        disabled={!stripe || submitting}
        className="mt-4 w-full px-4 py-2.5 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors disabled:opacity-50"
      >
        {submitting ? "Processing..." : "Subscribe — $199/mo"}
      </button>
    </form>
  );
}

function UpdateCardForm({ onSuccess }: { onSuccess: () => void }) {
  const stripe = useStripe();
  const elements = useElements();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!stripe || !elements) return;
    setSubmitting(true);
    setError(null);
    const result = await stripe.confirmSetup({ elements, redirect: "if_required" });
    if (result.error) {
      setError(result.error.message || "Failed to update card");
      setSubmitting(false);
    } else {
      onSuccess();
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <PaymentElement />
      {error && <p className="text-red-400 text-sm mt-3">{error}</p>}
      <button
        type="submit"
        disabled={!stripe || submitting}
        className="mt-4 w-full px-4 py-2.5 bg-white text-zinc-900 rounded-lg text-sm font-semibold hover:bg-zinc-200 transition-colors disabled:opacity-50"
      >
        {submitting ? "Saving..." : "Update payment method"}
      </button>
    </form>
  );
}

function Modal({ onClose, children }: { onClose: () => void; children: React.ReactNode }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={onClose}>
      <div className="bg-zinc-900 border border-zinc-700 rounded-xl p-6 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
        {children}
        <button onClick={onClose} className="mt-4 w-full text-center text-sm text-zinc-500 hover:text-zinc-300">
          Cancel
        </button>
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
      <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-lg font-bold">{value}</p>
    </div>
  );
}

const stripeAppearance = {
  theme: "night" as const,
  variables: {
    colorPrimary: "#ffffff",
    colorBackground: "#18181b",
    colorText: "#e4e4e7",
    colorDanger: "#ef4444",
    borderRadius: "8px",
  },
};
