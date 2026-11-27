import { cmsFetch } from '../lib/cms/fetch';
import { homeQuery } from '../lib/cms/queries';
import { Blocks } from '../components/Blocks';

// The front page: its blocks come from the content store.
export default async function Home() {
  const doc = (await cmsFetch(homeQuery, { slug: '/' })) as { blocks: unknown[] };
  return (
    <main>
      <Blocks blocks={doc.blocks} />
      <a href="/rooms/north">The north room</a>
    </main>
  );
}
