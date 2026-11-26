'use client';

export function CheckoutForm({ slug }: { slug: string }) {
  async function pay() {
    await fetch('/api/checkout', { method: 'POST', body: JSON.stringify({ slug }) });
  }
  return <button onClick={pay}>Pay for the season</button>;
}
