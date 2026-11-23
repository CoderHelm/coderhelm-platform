import { Button } from 'react-native';

export default function SignIn({ navigation }: { navigation: { replace: (name: string) => void } }) {
  return <Button title="Sign in" onPress={() => navigation.replace('Slips')} />;
}
