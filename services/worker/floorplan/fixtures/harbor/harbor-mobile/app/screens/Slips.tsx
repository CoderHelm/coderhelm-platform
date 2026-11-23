import { Button, View } from 'react-native';
import { useNavigation } from '@react-navigation/native';

export default function Slips() {
  const navigation = useNavigation();
  return (
    <View>
      <Button title="Slip 7" onPress={() => navigation.navigate('SlipDetail', { id: '7' })} />
    </View>
  );
}
