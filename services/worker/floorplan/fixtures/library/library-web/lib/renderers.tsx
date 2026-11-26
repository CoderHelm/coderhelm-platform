import type { PageMatch } from './resolve';
import { SignupForm } from '../components/SignupForm';
import { CheckoutForm } from '../components/CheckoutForm';
import { BoothCard } from '../components/BoothCard';

type Props = { match: PageMatch };

/** One booth, by its number. */
export function BoothRenderer({ match }: Props) {
  return <BoothCard slug={match.slug} />;
}

/** Every booth. */
export function BoothListRenderer({ match }: Props) {
  return <ul data-slug={match.slug}><li>Booths</li></ul>;
}

/** The form a new member signs up with. */
export function MemberSignupRenderer({ match }: Props) {
  return <SignupForm next="/members/checkout" slug={match.slug} />;
}

/** Where a new member pays for the first season. */
export function MemberCheckoutRenderer({ match }: Props) {
  return <CheckoutForm slug={match.slug} />;
}

/** A notice to patrons. */
export function NoticeRenderer({ match }: Props) {
  return <article>{match.slug}</article>;
}
