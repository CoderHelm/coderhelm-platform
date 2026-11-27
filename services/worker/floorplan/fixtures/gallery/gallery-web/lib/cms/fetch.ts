import { cms } from './client';

/** Every content read goes through here. */
export async function cmsFetch(query: string, params: Record<string, string>) {
  const res = await cms.getEntries({ query, ...params });
  return res.items[0] ?? null;
}
