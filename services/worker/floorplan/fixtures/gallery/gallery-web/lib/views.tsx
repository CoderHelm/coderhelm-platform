import type { Doc } from './router';
import { Blocks } from '../components/Blocks';
import { RoomLayout } from '../components/RoomLayout';
import { OpeningHours } from '../components/OpeningHours';
import { TicketForm } from '../components/TicketForm';

type Props = { doc: Doc };

/** The front page has a route of its own (app/page.tsx): this only answers if a link ever lands here. */
export function HomeView({ doc: _doc }: Props) {
  return <p>See the front page</p>;
}

/** One room of the gallery. */
export function RoomView({ doc }: Props) {
  return (
    <RoomLayout slug={doc.slug}>
      <RoomBody doc={doc} />
    </RoomLayout>
  );
}

function RoomBody({ doc }: Props) {
  return (
    <>
      <Blocks blocks={(doc.data as { blocks: unknown[] }).blocks} />
      <OpeningHours />
    </>
  );
}

/** An event, where visitors book a seat. */
export function EventView({ doc }: Props) {
  return <TicketForm slug={doc.slug} />;
}
