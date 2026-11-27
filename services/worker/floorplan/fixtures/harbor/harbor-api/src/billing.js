const Stripe = require('stripe');

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

async function charge(id) {
  return stripe.paymentIntents.create({ amount: 4200, currency: 'usd', metadata: { slot: id } });
}

module.exports = { charge };
