// A small HCL reader: blocks, attributes (kept as raw expression text), nested blocks and line
// numbers. Strings, ${} nesting, heredocs and comments are skipped correctly enough for block
// structure. Nothing is evaluated here; terraform.mjs evaluates string templates.

const IDENT = /[A-Za-z_][A-Za-z0-9_-]*/y;

export class Block {
  constructor(type, labels, line, file) {
    this.type = type; this.labels = labels; this.line = line; this.file = file;
    this.attrs = new Map(); // name -> [raw, line]
    this.blocks = [];
  }
  get(name) { return this.attrs.get(name)?.[0] ?? null; }
  sub(type) { return this.blocks.filter((b) => b.type === type); }
  *walk() { yield this; for (const b of this.blocks) yield* b.walk(); }
}

function skipString(s, i) {
  i++;
  let depth = 0;
  while (i < s.length) {
    const c = s[i];
    if (c === '\\') { i += 2; continue; }
    if (c === '$' && s[i + 1] === '{') { depth++; i += 2; continue; }
    if (c === '}' && depth) depth--;
    else if (c === '"' && !depth) return i + 1;
    else if (c === '"' && depth) { i = skipString(s, i); continue; }
    i++;
  }
  return s.length;
}

function skipHeredoc(s, i) {
  const m = /^<<-?\s*([A-Za-z_]+)[^\n]*\n/.exec(s.slice(i, i + 200));
  if (!m) return i + 2;
  const re = new RegExp(`^\\s*${m[1]}\\s*$`, 'gm');
  re.lastIndex = i + m[0].length;
  const e = re.exec(s);
  return e ? e.index + e[0].length : s.length;
}

function skipWs(s, i, newlines = true) {
  while (i < s.length) {
    const c = s[i];
    if (c === ' ' || c === '\t' || c === '\r' || (newlines && c === '\n')) i++;
    else if (c === '#' || (c === '/' && s[i + 1] === '/')) { const j = s.indexOf('\n', i); i = j < 0 ? s.length : j; }
    else if (c === '/' && s[i + 1] === '*') { const j = s.indexOf('*/', i); i = j < 0 ? s.length : j + 2; }
    else break;
  }
  return i;
}

function exprEnd(s, i) {
  let depth = 0;
  while (i < s.length) {
    const c = s[i];
    if (c === '"') { i = skipString(s, i); continue; }
    if (c === '<' && s[i + 1] === '<' && /^<<-?\s*[A-Za-z_]/.test(s.slice(i, i + 6))) { i = skipHeredoc(s, i); continue; }
    if (c === '#' || (c === '/' && s[i + 1] === '/')) { const j = s.indexOf('\n', i); if (depth === 0) return j < 0 ? i : j; i = j < 0 ? s.length : j; continue; }
    if (c === '/' && s[i + 1] === '*') { const j = s.indexOf('*/', i); i = j < 0 ? s.length : j + 2; continue; }
    if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') { if (depth === 0) return i; depth--; }
    else if (c === '\n' && depth === 0) return i;
    i++;
  }
  return s.length;
}

export function parse(text, file = '') {
  const starts = [0];
  for (let k = 0; k < text.length; k++) if (text.charCodeAt(k) === 10) starts.push(k + 1);
  const line = (i) => { let lo = 0, hi = starts.length; while (lo < hi) { const mid = (lo + hi) >> 1; if (starts[mid] <= i) lo = mid + 1; else hi = mid; } return lo; };
  const root = new Block('_root', [], 1, file);
  const body = (i, blk, guard = 0) => {
    if (guard > 64) return text.length;
    for (;;) {
      i = skipWs(text, i);
      if (i >= text.length) return text.length;
      if (text[i] === '}') return i + 1;
      IDENT.lastIndex = i;
      const m = IDENT.exec(text);
      if (!m) { const j = text.indexOf('\n', i); if (j < 0) return text.length; i = j + 1; continue; }
      const name = m[0], ln = line(i);
      let j = skipWs(text, i + name.length, false);
      if (text[j] === '=' && text[j + 1] !== '=') {
        const k = skipWs(text, j + 1, false);
        const e = exprEnd(text, k);
        blk.attrs.set(name, [text.slice(k, e).trim(), ln]);
        i = e;
        continue;
      }
      const labels = [];
      while (text[j] === '"') { const e = skipString(text, j); labels.push(text.slice(j + 1, e - 1)); j = skipWs(text, e, false); }
      for (;;) { IDENT.lastIndex = j; const mm = text[j] !== '{' && IDENT.exec(text); if (!mm) break; labels.push(mm[0]); j = skipWs(text, j + mm[0].length, false); }
      if (text[j] === '{') {
        const child = new Block(name, labels, ln, file);
        blk.blocks.push(child);
        i = body(j + 1, child, guard + 1);
        continue;
      }
      const e = text.indexOf('\n', i);
      i = e < 0 ? text.length : e + 1;
    }
  };
  body(0, root);
  return root;
}

const REF = /\b(module\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?|data\.[a-z0-9_]+\.[A-Za-z0-9_-]+|local\.[A-Za-z0-9_-]+|var\.[A-Za-z0-9_-]+|(?:aws|archive|null|local|google|azurerm)_[a-z0-9_]+\.[A-Za-z0-9_-]+)(?:\[[^\]]*\])?((?:\.[A-Za-z0-9_*-]+)*)/g;

/** [address, attribute path] pairs an expression references. */
export function refs(raw) {
  return [...String(raw || '').matchAll(REF)].map((m) => [m[1], m[2].replace(/^\./, '')]);
}

/** The value of a plain quoted string with no interpolation, else null. */
export function literal(raw) {
  if (raw && raw.length >= 2 && raw[0] === '"' && raw[raw.length - 1] === '"' && !raw.includes('${')) return raw.slice(1, -1);
  return null;
}
