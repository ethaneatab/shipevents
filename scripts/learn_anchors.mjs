#!/usr/bin/env node
// Scans source files for `// @learn:<id>` ... `// @learn:end <id>` marker pairs (the markers sit
// outside the range) and writes frontend/src/generated/anchors.json as {id: {path, start, end}}.
// It also rewrites markdown links written as [text](anything "anchor:<id>") in docs/ so their
// relative path and #Lstart-Lend always match. Line numbers are never typed by hand.
import { readdirSync, readFileSync, writeFileSync, mkdirSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const OUT = path.join(ROOT, "frontend/src/generated/anchors.json");
const SCAN = ["src", "frontend/src", "docs/problems/snippets", "scripts"];
const EXT = /\.(ts|tsx|sql|sh)$/;
const OPEN = /^\s*(?:\/\/|--|#)\s*@learn:([a-z0-9][a-z0-9-]*)\s*$/;
const CLOSE = /^\s*(?:\/\/|--|#)\s*@learn:end\s+([a-z0-9][a-z0-9-]*)\s*$/;

function walk(dir, out = []) {
  let entries;
  try { entries = readdirSync(dir); } catch { return out; }
  for (const name of entries) {
    const p = path.join(dir, name);
    if (name === "node_modules" || name === "generated") continue;
    if (statSync(p).isDirectory()) walk(p, out);
    else if (EXT.test(name)) out.push(p);
  }
  return out;
}

export function collectAnchors() {
  const anchors = {};
  const errors = [];
  for (const dir of SCAN) {
    for (const file of walk(path.join(ROOT, dir)).sort()) {
      const rel = path.relative(ROOT, file).split(path.sep).join("/");
      const lines = readFileSync(file, "utf8").split("\n");
      const open = new Map();
      lines.forEach((line, i) => {
        const c = line.match(CLOSE);
        const o = !c && line.match(OPEN);
        if (o) {
          if (anchors[o[1]] || open.has(o[1])) errors.push(`${rel}:${i + 1} duplicate anchor ${o[1]}`);
          open.set(o[1], i + 1);
        } else if (c) {
          const s = open.get(c[1]);
          if (s === undefined) errors.push(`${rel}:${i + 1} @learn:end ${c[1]} without a start`);
          else {
            anchors[c[1]] = { path: rel, start: s + 1, end: i };
            open.delete(c[1]);
          }
        }
      });
      for (const [id, line] of open) errors.push(`${rel}:${line} @learn:${id} never closed`);
    }
  }
  const sorted = Object.fromEntries(Object.keys(anchors).sort().map((k) => [k, anchors[k]]));
  return { anchors: sorted, errors };
}

export function serialize(anchors) {
  return JSON.stringify(anchors, null, 2) + "\n";
}

const LINK = /\]\(([^)\s]*) "anchor:([a-z0-9][a-z0-9-]*)"\)/g;

export function markdownFiles() {
  const out = [];
  const visit = (d) => {
    for (const name of readdirSync(d)) {
      const p = path.join(d, name);
      if (statSync(p).isDirectory()) visit(p);
      else if (name.endsWith(".md")) out.push(p);
    }
  };
  visit(path.join(ROOT, "docs"));
  out.push(path.join(ROOT, "README.md"));
  return out.sort();
}

/** Returns the rewritten text of one markdown file, plus unknown anchor ids it references. */
export function rewriteLinks(file, anchors) {
  const text = readFileSync(file, "utf8");
  const unknown = [];
  const next = text.replace(LINK, (whole, _target, id) => {
    const a = anchors[id];
    if (!a) {
      unknown.push(id);
      return whole;
    }
    const rel = path.relative(path.dirname(file), path.join(ROOT, a.path)).split(path.sep).join("/");
    return `](${rel}#L${a.start}-L${a.end} "anchor:${id}")`;
  });
  return { text, next, unknown };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { anchors, errors } = collectAnchors();
  if (errors.length) {
    console.error(errors.join("\n"));
    process.exit(1);
  }
  mkdirSync(path.dirname(OUT), { recursive: true });
  writeFileSync(OUT, serialize(anchors));
  let changed = 0;
  for (const f of markdownFiles()) {
    const { text, next } = rewriteLinks(f, anchors);
    if (next !== text) {
      writeFileSync(f, next);
      changed++;
    }
  }
  console.log(`${Object.keys(anchors).length} anchors -> ${path.relative(ROOT, OUT)}; ${changed} markdown file(s) updated`);
}
