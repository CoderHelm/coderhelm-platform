import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { useSession } from '../lib/session';
import SignIn from '../screens/SignIn';
import Slips from '../screens/Slips';
import SlipDetail from '../screens/SlipDetail';

const Stack = createNativeStackNavigator();

export default function RootNavigator() {
  const { user } = useSession();
  return (
    <Stack.Navigator>
      {!user ? (
        <Stack.Screen name="SignIn" component={SignIn} />
      ) : (
        <>
          <Stack.Screen name="Slips" component={Slips} />
          <Stack.Screen name="SlipDetail" component={SlipDetail} />
        </>
      )}
    </Stack.Navigator>
  );
}
