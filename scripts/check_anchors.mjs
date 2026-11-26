#!/usr/bin/env node
// Fails (exit 1) if: an anchor id used by the frontend, docs or video scripts does not exist;
// an anchor's marker lines are not where anchors.json says; or anchors.json / doc links are stale.
import { readFileSync, existsSync, readdirSync, statSync } from "node:fs";
import path from "node:path";
import { collectAnchors, markdownFiles, OUT, ROOT, rewriteLinks, serialize } from "./learn_anchors.mjs";

const problems = [];
const { anchors, errors } = collectAnchors();
problems.push(...errors);

// 1. every referenced id exists
const refs = new Map(); // id -> where
const add = (id, where) => refs.set(id, [...(refs.get(id) ?? []), where]);
const scanTs = (d) => {
  for (const name of readdirSync(d)) {
    const p = path.join(d, name);
    if (name === "generated") continue;
    if (statSync(p).isDirectory()) scanTs(p);
    else if (/\.(ts|tsx)$/.test(name)) {
      const t = readFileSync(p, "utf8");
      for (const m of t.matchAll(/srcAnchor\("([a-z0-9][a-z0-9-]*)"\)|\banchor:\s*"([a-z0-9][a-z0-9-]*)"/g)) add(m[1] ?? m[2], path.relative(ROOT, p));
    }
  }
};
scanTs(path.join(ROOT, "frontend/src"));
for (const f of markdownFiles()) {
  const { unknown, next, text } = rewriteLinks(f, anchors);
  for (const m of text.matchAll(/"anchor:([a-z0-9-]+)"/g)) add(m[1], path.relative(ROOT, f));
  for (const id of unknown) problems.push(`${path.relative(ROOT, f)} links to unknown anchor ${id}`);
  if (next !== text) problems.push(`${path.relative(ROOT, f)} has stale anchor links (run node scripts/learn_anchors.mjs)`);
}
const probSrc = path.join(ROOT, "video/problems.src.json");
if (existsSync(probSrc)) for (const m of readFileSync(probSrc, "utf8").matchAll(/"anchor"\s*:\s*"([^"]+)"/g)) add(m[1], "video/problems.src.json");
for (const [id, where] of refs) if (!anchors[id]) problems.push(`unknown anchor "${id}" used in ${[...new Set(where)].join(", ")}`);

// 2. marker lines really are where the generated file says
const generated = existsSync(OUT) ? JSON.parse(readFileSync(OUT, "utf8")) : {};
for (const [id, a] of Object.entries(generated)) {
  const file = path.join(ROOT, a.path);
  if (!existsSync(file)) {
    problems.push(`${id}: ${a.path} does not exist`);
    continue;
  }
  const lines = readFileSync(file, "utf8").split("\n");
  if (!(a.start <= a.end + 1 && a.end <= lines.length)) problems.push(`${id}: bad range ${a.start}-${a.end}`);
  if (!lines[a.start - 2]?.includes(`@learn:${id}`)) problems.push(`${id}: line ${a.start - 1} of ${a.path} is not the @learn:${id} marker`);
  if (!lines[a.end]?.includes(`@learn:end ${id}`)) problems.push(`${id}: line ${a.end + 1} of ${a.path} is not the @learn:end ${id} marker`);
}

// 3. generated file is current
if (!existsSync(OUT) || readFileSync(OUT, "utf8") !== serialize(anchors)) problems.push(`${path.relative(ROOT, OUT)} is stale (run node scripts/learn_anchors.mjs)`);

if (problems.length) {
  console.error(`check_anchors: ${problems.length} problem(s)\n- ${problems.join("\n- ")}`);
  process.exit(1);
}
console.log(`check_anchors: ok (${Object.keys(anchors).length} anchors, ${refs.size} referenced ids)`);
