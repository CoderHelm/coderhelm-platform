import { Button, View } from 'react-native';
import { useNavigation } from '@react-navigation/native';

export default function Slots() {
  const navigation = useNavigation();
  return (
    <View>
      <Button title="Slot 7" onPress={() => navigation.navigate('SlotDetail', { id: '7' })} />
    </View>
  );
}
