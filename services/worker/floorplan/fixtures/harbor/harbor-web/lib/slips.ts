export async function bookSlip(id: string) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/slips/${id}/bookings`, { method: 'POST' });
  return res.json();
}
