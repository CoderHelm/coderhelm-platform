import { bookSlip } from '../../../lib/slips';

export default function SlipPage({ params }: { params: { id: string } }) {
  return <button onClick={() => bookSlip(params.id)}>Book this slip</button>;
}
