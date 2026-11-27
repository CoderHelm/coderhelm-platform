import { notFound } from 'next/navigation';
import { resolveDoc } from '../../lib/router';
import { HomeView, RoomView, EventView } from '../../lib/views';

// One route for every page: the router says what the address is and loads it,
// and the table below says which view draws it.
const views = {
  home: HomeView,
  room: RoomView,
  event: EventView,
};

export default async function Page({ params }: { params: { slug: string[] } }) {
  const doc = await resolveDoc(params.slug);
  if (!doc) return notFound();
  const View = views[doc.kind as keyof typeof views];
  return <View doc={doc} />;
}
