import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { invoke } from "@forge/bridge";

function App() {
  const [installationId, setInstallationId] = useState("");
  const [tenantId, setTenantId] = useState("");
  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    invoke("getConfig").then((config) => {
      setInstallationId(config.installationId || "");
      setTenantId(config.tenantId || "");
      setLoading(false);
    });
  }, []);

  const save = async () => {
    setStatus("Saving…");
    const result = await invoke("saveConfig", { installationId, tenantId });
    if (!result.success) {
      setStatus(`Error: ${result.error}`);
    } else if (result.urlsRegistered) {
      setStatus("Saved — trigger URLs registered with Coderhelm");
    } else {
      setStatus(`Saved — URL registration failed: ${result.urlError || "unknown"}`);
    }
    setTimeout(() => setStatus(""), 5000);
  };

  if (loading) return <p style={styles.loading}>Loading…</p>;

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <svg viewBox="0 0 512 512" fill="none" style={styles.logo}>
          <polygon points="106,416 158,416 226,96 174,96" fill="#000" />
          <polygon points="196,416 248,416 316,96 264,96" fill="#3B82F6" />
          <polygon points="286,416 338,416 406,96 354,96" fill="#000" />
        </svg>
        <h2 style={styles.title}>coderhelm</h2>
      </div>
      <p style={styles.subtitle}>
        Connect your Jira site to coderhelm so assigned issues become pull requests automatically.
      </p>

      <div style={styles.card}>
        <label style={styles.label}>
          Installation ID
          <input
            style={styles.input}
            value={installationId}
            onChange={(e) => setInstallationId(e.target.value)}
            placeholder="e.g. 119699895"
          />
        </label>
        <p style={styles.hint}>
          Find this in your coderhelm dashboard under Settings → Jira.
        </p>

        <label style={styles.label}>
          Tenant ID <span style={styles.optional}>(optional)</span>
          <input
            style={styles.input}
            value={tenantId}
            onChange={(e) => setTenantId(e.target.value)}
            placeholder="e.g. TENANT#119699895"
          />
        </label>
        <p style={styles.hint}>
          Only needed if your tenant ID differs from your GitHub Installation ID.
        </p>

        <button style={styles.button} onClick={save}>
          Save
        </button>
        {status && <p style={styles.status}>{status}</p>}
      </div>

      <div style={styles.howTo}>
        <h3 style={styles.howToTitle}>How it works</h3>
        <ol style={styles.steps}>
          <li>Add a <code style={styles.code}>coderhelm</code> label to any Jira issue.</li>
          <li>Assign the issue — coderhelm determines the right repo from the ticket and starts working.</li>
          <li>coderhelm creates a branch, implements the change, and opens a draft PR.</li>
          <li>Track progress at <strong>app.coderhelm.com</strong>.</li>
        </ol>
      </div>
    </div>
  );
}

const styles = {
  container: { maxWidth: 520, margin: "0 auto", padding: 24, fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif" },
  header: { display: "flex", alignItems: "center", gap: 10, marginBottom: 4 },
  logo: { width: 28, height: 28 },
  title: { fontSize: 20, fontWeight: 700, margin: 0, color: "#172B4D" },
  subtitle: { fontSize: 14, color: "#626F86", marginBottom: 20, lineHeight: 1.5 },
  card: { background: "#F7F8F9", borderRadius: 8, padding: 20, marginBottom: 24 },
  label: { display: "block", fontSize: 13, fontWeight: 600, color: "#172B4D", marginBottom: 4 },
  input: { display: "block", width: "100%", padding: "8px 10px", fontSize: 14, border: "1px solid #DFE1E6", borderRadius: 4, marginTop: 4, marginBottom: 4, boxSizing: "border-box" },
  hint: { fontSize: 12, color: "#626F86", marginTop: 0, marginBottom: 16 },
  optional: { fontWeight: 400, color: "#626F86" },
  button: { background: "#0052CC", color: "#fff", border: "none", borderRadius: 4, padding: "8px 20px", fontSize: 14, fontWeight: 600, cursor: "pointer" },
  status: { fontSize: 13, color: "#00875A", marginTop: 8 },
  loading: { textAlign: "center", color: "#626F86", padding: 40 },
  howTo: { borderTop: "1px solid #DFE1E6", paddingTop: 20 },
  howToTitle: { fontSize: 15, fontWeight: 600, color: "#172B4D", marginBottom: 8 },
  steps: { fontSize: 13, color: "#44546F", lineHeight: 1.8, paddingLeft: 20 },
  code: { background: "#F1F2F4", padding: "2px 6px", borderRadius: 3, fontSize: 12, fontFamily: "monospace" },
};

const root = createRoot(document.getElementById("root"));
root.render(<App />);
