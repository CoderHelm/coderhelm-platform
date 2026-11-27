export async function bookSlot(id: string) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/slots/${id}/bookings`, { method: 'POST' });
  return res.json();
}
