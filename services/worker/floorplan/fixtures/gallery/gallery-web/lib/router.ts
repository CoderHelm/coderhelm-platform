import { cmsFetch } from './cms/fetch';
import { homeQuery, roomQuery, eventQuery, kindQuery } from './cms/queries';

export const KINDS = {
  HOME: 'home',
  ROOM: 'room',
  EVENT: 'event',
} as const;

export type Doc = { kind: string; slug: string; data: unknown };

const makeDoc = (kind: string, slug: string, data: unknown): Doc => ({ kind, slug, data });

/** Loads a document of the content store, or null. */
const loadFrom = async (query: string, slug: string) => {
  const data = await cmsFetch(query, { slug });
  return data ?? null;
};

const loadHome = async (slug: string) => makeDoc(KINDS.HOME, slug, await loadFrom(homeQuery, slug));

const loadRoom = async (slug: string) => {
  const data = await loadFrom(roomQuery, slug);
  return data ? makeDoc(KINDS.ROOM, slug, data) : null;
};

const loadEvent = async (slug: string) => {
  const data = await loadFrom(eventQuery, slug);
  return data ? makeDoc(KINDS.EVENT, slug, data) : null;
};

const LOADERS = {
  frontDoc: loadHome,
  room: loadRoom,
  event: loadEvent,
} as const;

/** The front page document lives at the site's root, which the front page route serves. */
export const isFront = (doc: { docType: string; path: string }) => doc.docType === 'frontDoc' && doc.path === '/';

/** What an address is, and its content. */
export async function resolveDoc(parts: string[]): Promise<Doc | null> {
  const slug = '/' + parts.join('/');
  const found = (await cmsFetch(kindQuery, { slug })) as { docType: string } | null;
  const load = found && LOADERS[found.docType as keyof typeof LOADERS];
  return load ? load(slug) : null;
}
