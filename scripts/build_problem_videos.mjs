#!/usr/bin/env node
// video/problems.src.json -> video/problems.walk.json for the walkthrough renderer.
// Code is never copied into the JSON by hand: it is cut from the source by @learn anchor.
// Checks: line numbers in range, narration has no code symbols or Arabic digits, and it prints
// every highlighted line next to its narration so a human can eyeball the pairing.
//   node scripts/build_problem_videos.mjs            build + verify (exit 1 on any problem)
//   node scripts/build_problem_videos.mjs --show ID  print an anchor's code with line numbers
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { collectAnchors, ROOT } from "./learn_anchors.mjs";

const { anchors } = collectAnchors();

function cut(ref, where) {
  const a = anchors[ref.anchor];
  if (!a) throw new Error(`${where}: unknown anchor ${ref.anchor}`);
  if (ref.path && ref.path !== a.path) throw new Error(`${where}: anchor ${ref.anchor} lives in ${a.path}, not ${ref.path}`);
  const lines = readFileSync(path.join(ROOT, a.path), "utf8").split("\n").slice(a.start - 1, a.end);
  const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^ */)[0].length));
  return lines.map((l) => l.slice(indent)).join("\n");
}

if (process.argv[2] === "--show") {
  const code = cut({ anchor: process.argv[3] }, "show");
  code.split("\n").forEach((l, i) => console.log(String(i + 1).padStart(3), l));
  process.exit(0);
}

const BAD = [/->/, /==/, /_/, /\//, /`/, /[{}()[\];=<>*#|\\]/, /[0-9]/];
const src = JSON.parse(readFileSync(path.join(ROOT, "video/problems.src.json"), "utf8"));
const problems = [];
const out = [];

function check(segs, code, where) {
  const n = code.split("\n").length;
  const lines = code.split("\n");
  for (const [i, s] of segs.entries()) {
    for (const re of BAD) if (re.test(s.text)) problems.push(`${where} segment ${i + 1}: narration contains ${re} -> "${s.text}"`);
    for (const l of s.lines) if (!(Number.isInteger(l) && l >= 1 && l <= n)) problems.push(`${where} segment ${i + 1}: line ${l} out of 1..${n}`);
    console.log(`  [${s.lines.join(",") || "-"}] ${s.text}`);
    for (const l of s.lines) console.log(`        ${String(l).padStart(3)} | ${lines[l - 1] ?? "<out of range>"}`);
  }
}

for (const p of src) {
  console.log(`\n== ${p.id}: ${p.title}`);
  try {
    const code = cut(p.code_from, p.id);
    const example = p.before_from ? cut(p.before_from, p.id) : "";
    if (p.segments.length + (p.before_segments?.length ?? 0) < 6 || p.segments.length + (p.before_segments?.length ?? 0) > 14) problems.push(`${p.id}: wants 6..14 segments in total`);
    for (const re of BAD.slice(0, 5)) if (re.test(p.title) || re.test(p.subtitle)) problems.push(`${p.id}: title/subtitle contains ${re}`);
    console.log(" before:");
    check(p.before_segments ?? [], example, `${p.id} before`);
    console.log(" after:");
    check(p.segments, code, `${p.id} after`);
    out.push({ id: p.id, title: p.title, subtitle: p.subtitle, lang: p.lang, code, segments: p.segments, example_code: example, example_segments: p.before_segments ?? [] });
  } catch (e) {
    problems.push(e.message);
  }
}
if (problems.length) {
  console.error(`\nbuild_problem_videos: ${problems.length} problem(s)\n- ${problems.join("\n- ")}`);
  process.exit(1);
}
writeFileSync(path.join(ROOT, "video/problems.walk.json"), JSON.stringify(out, null, 2) + "\n");
console.log(`\nwrote video/problems.walk.json (${out.length} problems)`);
