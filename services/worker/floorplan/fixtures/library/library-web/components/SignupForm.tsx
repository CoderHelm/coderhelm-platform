'use client';
import { useRouter } from 'next/navigation';

export function SignupForm({ next, slug }: { next: string; slug: string }) {
  const router = useRouter();
  async function submit(form: FormData) {
    await fetch('/api/members', { method: 'POST', body: JSON.stringify({ email: form.get('email'), slug }) });
    router.push('/members/checkout');
  }
  return <form action={submit}><input name="email" /><button>Sign up</button></form>;
}
