const TIDEWATCH_API_URL = process.env.TIDEWATCH_API_URL;

async function tideAt(slip) {
  const res = await fetch(`${TIDEWATCH_API_URL}/tides?slip=${slip}`);
  return res.json();
}

module.exports = { tideAt };
