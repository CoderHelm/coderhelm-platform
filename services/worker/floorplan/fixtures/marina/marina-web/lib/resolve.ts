export type PageType = 'berth' | 'berth-index' | 'member-signup' | 'member-checkout' | 'notice';

export const PAGE_TYPES = {
  SIGNUP: 'member-signup',
  CHECKOUT: 'member-checkout',
} as const;

export type PageMatch = { type: PageType; slug: string };

/** What an address is: a few fixed paths, a berth by its number, and the rest from the content store. */
export async function resolvePage(parts: string[]): Promise<PageMatch | null> {
  const slug = '/' + parts.join('/');
  if (parts.length === 2 && parts[0] === 'members' && parts[1] === 'join') {
    return { type: PAGE_TYPES.SIGNUP, slug };
  }
  if (parts.length === 2 && parts[0] === 'members' && parts[1] === 'checkout') {
    return { type: PAGE_TYPES.CHECKOUT, slug };
  }
  if (parts.length === 2 && parts[0] === 'berths') {
    return { type: 'berth', slug };
  }
  const found = await fetch(`https://content.example.invalid/types?slug=${encodeURIComponent(slug)}`).then((r) => r.json());
  return found ? { type: found.type, slug } : null;
}
