import type { PageMatch } from './resolve';
import { SignupForm } from '../components/SignupForm';
import { CheckoutForm } from '../components/CheckoutForm';
import { BerthCard } from '../components/BerthCard';

type Props = { match: PageMatch };

/** One berth, by its number. */
export function BerthRenderer({ match }: Props) {
  return <BerthCard slug={match.slug} />;
}

/** Every berth. */
export function BerthListRenderer({ match }: Props) {
  return <ul data-slug={match.slug}><li>Berths</li></ul>;
}

/** The form a new member signs up with. */
export function MemberSignupRenderer({ match }: Props) {
  return <SignupForm next="/members/checkout" slug={match.slug} />;
}

/** Where a new member pays for the first season. */
export function MemberCheckoutRenderer({ match }: Props) {
  return <CheckoutForm slug={match.slug} />;
}

/** A notice to boaters. */
export function NoticeRenderer({ match }: Props) {
  return <article>{match.slug}</article>;
}
