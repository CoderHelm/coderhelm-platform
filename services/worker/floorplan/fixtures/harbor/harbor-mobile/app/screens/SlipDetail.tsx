import { Button } from 'react-native';
import { apiPost } from '../lib/api';

export default function SlipDetail({ route }: { route: { params: { id: string } } }) {
  const { id } = route.params;
  return <Button title="Book" onPress={() => apiPost(`/slips/${id}/bookings`, {})} />;
}
