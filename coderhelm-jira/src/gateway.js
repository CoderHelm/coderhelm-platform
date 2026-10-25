// Where this copy of the app sends Jira events: the CoderHelm API.
//
// The hosted app talks to the hosted API. A self-hosted CoderHelm deploys its
// own copy of this app pointed at its own URL:
//
//   node scripts/set-gateway.js https://coderhelm.example.com
//
// which rewrites this line and the manifest's fetch permission (Forge only
// lets the app call hosts listed there), then `forge register && forge deploy`.

const GATEWAY_URL = "https://api.coderhelm.com";

module.exports = { GATEWAY_URL };
