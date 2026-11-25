import { notFound } from 'next/navigation';
import { resolvePage, type PageType } from '../../lib/resolve';
import { BerthRenderer, BerthListRenderer, MemberSignupRenderer, MemberCheckoutRenderer, NoticeRenderer } from '../../lib/renderers';

// One route for every page: the resolver says what the address is, and the
// table below says which renderer draws it.
const renderers = {
  berth: BerthRenderer,
  'berth-index': BerthListRenderer,
  'member-signup': MemberSignupRenderer,
  'member-checkout': MemberCheckoutRenderer,
  notice: NoticeRenderer,
} satisfies Record<PageType, unknown>;

export default async function Page({ params }: { params: { slug: string[] } }) {
  const match = await resolvePage(params.slug);
  if (!match) return notFound();
  const Renderer = renderers[match.type];
  return <Renderer match={match} />;
}
