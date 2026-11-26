import { bookSlot } from '../../../lib/slots';

export default function SlotPage({ params }: { params: { id: string } }) {
  return <button onClick={() => bookSlot(params.id)}>Book this slot</button>;
}
