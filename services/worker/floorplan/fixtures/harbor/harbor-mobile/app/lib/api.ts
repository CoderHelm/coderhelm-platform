const BASE = process.env.EXPO_PUBLIC_HARBOR_API_URL;

export async function apiPost(path: string, body: unknown) {
  const res = await fetch(`${BASE}${path}`, { method: 'POST', body: JSON.stringify(body) });
  return res.json();
}
