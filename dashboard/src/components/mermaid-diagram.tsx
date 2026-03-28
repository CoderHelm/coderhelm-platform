"use client";

import { useEffect, useRef, useState } from "react";

interface Props {
  chart: string;
  className?: string;
}

export function MermaidDiagram({ chart, className = "" }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [rendered, setRendered] = useState(false);

  useEffect(() => {
    if (!chart || !ref.current) return;

    let cancelled = false;

    (async () => {
      try {
        const mermaid = (await import("mermaid")).default;
        mermaid.initialize({
          startOnLoad: false,
          theme: "dark",
          themeVariables: {
            background: "#09090b",
            primaryColor: "#27272a",
            primaryTextColor: "#e4e4e7",
            lineColor: "#52525b",
            edgeLabelBackground: "#18181b",
            tertiaryColor: "#18181b",
          },
        });

        const id = `mermaid-${Math.random().toString(36).slice(2)}`;
        const { svg } = await mermaid.render(id, chart);

        if (!cancelled && ref.current) {
          ref.current.innerHTML = svg;
          // Make SVG responsive
          const svgEl = ref.current.querySelector("svg");
          if (svgEl) {
            svgEl.style.width = "100%";
            svgEl.style.height = "auto";
            svgEl.style.maxWidth = "100%";
          }
          setRendered(true);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Diagram render failed");
        }
      }
    })();

    return () => { cancelled = true; };
  }, [chart]);

  if (error) {
    return (
      <div className={`p-4 rounded-lg border border-red-500/20 bg-red-500/5 ${className}`}>
        <p className="text-xs text-red-400 font-mono">{error}</p>
        <pre className="text-[10px] text-zinc-600 mt-2 overflow-auto">{chart}</pre>
      </div>
    );
  }

  return (
    <div className={`relative ${className}`}>
      {!rendered && (
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="w-5 h-5 border-2 border-zinc-600 border-t-zinc-300 rounded-full animate-spin" />
        </div>
      )}
      <div ref={ref} className="w-full" />
    </div>
  );
}
