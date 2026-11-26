// Pulls one frame per chosen segment out of video/out/learn.webm (ffmpeg), at the middle of the segment,
// into video/frames/<id>.png, so the red boxes can be eyeballed. Usage: node video/check_frames.mjs s03 s10 s23 s30
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const { segments } = JSON.parse(readFileSync(path.join(DIR, "timeline.json"), "utf8"));
const ids = process.argv.slice(2);
mkdirSync(path.join(DIR, "frames"), { recursive: true });
for (const s of segments.filter((x) => ids.length === 0 || ids.includes(x.id))) {
  const at = (s.startMs + s.durMs * 0.75) / 1000;
  const out = path.join(DIR, "frames", `${s.id}.png`);
  execFileSync("ffmpeg", ["-v", "error", "-y", "-ss", at.toFixed(2), "-i", path.join(DIR, "out/learn.webm"), "-frames:v", "1", out]);
  console.log(`${s.id} @${at.toFixed(1)}s box=${JSON.stringify(s.box)} -> ${path.relative(process.cwd(), out)}`);
}
