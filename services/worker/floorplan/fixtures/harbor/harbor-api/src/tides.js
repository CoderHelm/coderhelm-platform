const TIDEWATCH_API_URL = process.env.TIDEWATCH_API_URL;

async function tideAt(slot) {
  const res = await fetch(`${TIDEWATCH_API_URL}/tides?slot=${slot}`);
  return res.json();
}

module.exports = { tideAt };
