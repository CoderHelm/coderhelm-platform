// A small IAM evaluator for the policies in infra/lib/access.ts: identity
// policy AND permissions boundary, explicit denies first, action wildcards
// (any case), resource globs and the condition operators these policies use.
// test/deploy-role-sim.test.ts checks it against AWS's IAM policy simulator.

import { PolicyDocument, Statement } from "../lib/access";

export interface Request {
  action: string;
  resource: string;
  context?: Record<string, string | string[] | undefined>;
}

export const list = (v: string | string[] | undefined): string[] => (v === undefined ? [] : Array.isArray(v) ? v : [v]);
export const glob = (pattern: string, value: string, ci = false) => {
  const re = new RegExp(`^${pattern.split("*").map((p) => p.split("?").map((q) => q.replace(/[.+^${}()|[\]\\]/g, "\\$&")).join(".")).join(".*")}$`, ci ? "i" : "");
  return re.test(value);
};

export function conditionHolds(cond: Statement["Condition"], ctx: Record<string, string | string[] | undefined>): boolean {
  for (const [op, kv] of Object.entries(cond ?? {})) {
    for (const [key, want] of Object.entries(kv)) {
      // Condition key names are not case-sensitive in IAM.
      const have = ctx[key] ?? Object.entries(ctx).find(([k]) => k.toLowerCase() === key.toLowerCase())?.[1];
      const wants = list(want as string | string[]);
      const haves = list(have);
      const any = (f: (h: string, w: string) => boolean) => haves.some((h) => wants.some((w) => f(h, w)));
      switch (op) {
        case "StringEquals": if (!any((h, w) => h === w)) return false; break;
        case "StringNotEquals": if (have !== undefined && any((h, w) => h === w)) return false; break;
        case "StringLike": if (!any((h, w) => glob(w, h))) return false; break;
        case "ArnLike": if (!any((h, w) => glob(w, h))) return false; break;
        // Negated: true when the key is missing or matches none of the values.
        case "ArnNotEquals": if (have !== undefined && any((h, w) => h === w)) return false; break;
        case "DateLessThan": if (!any((h, w) => Date.parse(h) < Date.parse(w))) return false; break;
        case "DateGreaterThan": if (!any((h, w) => Date.parse(h) > Date.parse(w))) return false; break;
        // "true": the key is absent from the request; "false": it is there.
        case "Null": if ((have === undefined) !== (wants[0] === "true")) return false; break;
        case "ForAnyValue:StringEqualsIgnoreCase": if (!any((h, w) => h.toLowerCase() === w.toLowerCase())) return false; break;
        case "ForAnyValue:StringEquals": if (!any((h, w) => h === w)) return false; break;
        case "ForAllValues:StringEquals": if (!haves.every((h) => wants.includes(h))) return false; break;
        case "ForAllValues:StringLike": if (!haves.every((h) => wants.some((w) => glob(w, h)))) return false; break;
        default: throw new Error(`the evaluator doesn't know ${op}`);
      }
    }
  }
  return true;
}

export function matches(s: Statement, r: Request): boolean {
  const actionHit = s.Action !== undefined ? list(s.Action).some((a) => glob(a, r.action, true)) : !list(s.NotAction).some((a) => glob(a, r.action, true));
  if (!actionHit) return false;
  const resHit = s.Resource !== undefined ? list(s.Resource).some((x) => glob(x, r.resource)) : !list(s.NotResource).some((x) => glob(x, r.resource));
  return resHit && conditionHolds(s.Condition, r.context ?? {});
}

/** Allowed when an identity statement allows, the boundary (if any) allows, and nothing denies. */
export function allowed(identity: PolicyDocument[], boundary: PolicyDocument | null, r: Request): boolean {
  const all = [...identity, ...(boundary ? [boundary] : [])].flatMap((d) => d.Statement);
  if (all.some((s) => s.Effect === "Deny" && matches(s, r))) return false;
  const byIdentity = identity.flatMap((d) => d.Statement).some((s) => s.Effect === "Allow" && matches(s, r));
  const byBoundary = !boundary || boundary.Statement.some((s) => s.Effect === "Allow" && matches(s, r));
  return byIdentity && byBoundary;
}

/** The simulator's three verdicts: an explicit deny wins, then an allow by both the identity policy and the boundary. */
export function decision(identity: PolicyDocument[], boundary: PolicyDocument | null, r: Request): "allowed" | "explicitDeny" | "implicitDeny" {
  const all = [...identity, ...(boundary ? [boundary] : [])].flatMap((d) => d.Statement);
  if (all.some((s) => s.Effect === "Deny" && matches(s, r))) return "explicitDeny";
  return allowed(identity, boundary, r) ? "allowed" : "implicitDeny";
}
