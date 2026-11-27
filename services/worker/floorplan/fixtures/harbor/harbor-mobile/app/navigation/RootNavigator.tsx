import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { useSession } from '../lib/session';
import SignIn from '../screens/SignIn';
import Slots from '../screens/Slots';
import SlotDetail from '../screens/SlotDetail';

const Stack = createNativeStackNavigator();

export default function RootNavigator() {
  const { user } = useSession();
  return (
    <Stack.Navigator>
      {!user ? (
        <Stack.Screen name="SignIn" component={SignIn} />
      ) : (
        <>
          <Stack.Screen name="Slots" component={Slots} />
          <Stack.Screen name="SlotDetail" component={SlotDetail} />
        </>
      )}
    </Stack.Navigator>
  );
}
