'use client';

export function TicketForm({ slug }: { slug: string }) {
  async function book() {
    await fetch('/api/tickets', { method: 'POST', body: JSON.stringify({ slug }) });
  }
  return <button onClick={book}>Book a seat</button>;
}
