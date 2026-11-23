// One infra edge as the readers find it, before it becomes a map link. `how` is exact (one
// deterministic reference: a resource reference, a literal name equal to the evaluated IaC
// name) or inferred (a join on a glob name, a domain, an env key or a same-name output).
export function edge(a, b, k, via, how, conf, evidence, extra = {}) {
  return { a, b, k, via, how, conf: Math.round(conf * 100) / 100, evidence: evidence.map(([file, line]) => ({ file, line: line || 1 })), ...extra };
}
