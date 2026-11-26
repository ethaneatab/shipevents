// Records the /learn walkthrough as one webm (1600x900) driven by video/narration.json.
// Each segment: run its action -> draw a red box on the exact thing to look at -> hold for its duration.
// Durations come from video/durations.json ({id: seconds}); missing file = 4s per segment.
// Output: video/out/learn.webm and video/timeline.json {"segments":[{id, part, startMs, durMs, plannedMs, box}]}.
// Needs the stack from ./scripts/demo.sh and nothing else touching it (stop the test suite first).
import { chromium } from "@playwright/test";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const BASE = process.env.RECORD_BASE_URL ?? "http://127.0.0.1:5190";
const narration = JSON.parse(readFileSync(path.join(DIR, "narration.json"), "utf8"));
const durFile = path.join(DIR, "durations.json");
const durations = existsSync(durFile) ? JSON.parse(readFileSync(durFile, "utf8")) : {};
const W = 1600, H = 900;

// Narration rules: spoken Chinese, no code symbols, numbers in Chinese, parts contiguous.
const BAD = [/->/, /==/, /_/, /\//, /`/, /[0-9]/, /[{}<>[\]=*#]/];
const seenParts = [];
for (const s of narration) {
  for (const re of BAD) if (re.test(s.text)) throw new Error(`${s.id}: narration contains ${re}`);
  if (seenParts.at(-1) !== s.part) {
    if (seenParts.includes(s.part)) throw new Error(`${s.id}: part ${s.part} is not contiguous`);
    seenParts.push(s.part);
  }
}

const OUT = path.join(DIR, "out");
mkdirSync(OUT, { recursive: true });
const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: OUT, size: { width: W, height: H } } });
const page = await context.newPage();
const t0 = Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const STYLE = `
#rec-box{position:fixed;z-index:99999;pointer-events:none;border:3px solid #e5383b;border-radius:10px;
  box-shadow:0 0 0 6px rgba(229,56,59,.25);animation:recpop .45s ease-out}
#rec-box.circle{border-radius:50%}
@keyframes recpop{0%{transform:scale(1.25);opacity:0}60%{transform:scale(.97);opacity:1}100%{transform:scale(1)}}
#rec-cap{position:fixed;z-index:99999;pointer-events:none;background:#1e2a32;color:#fff;font:14px/1.4 ui-monospace,Menlo,monospace;
  padding:8px 12px;border-radius:6px;max-width:1100px;word-break:break-all;box-shadow:0 4px 18px rgba(0,0,0,.25)}`;

/** Draws the red box. "SEL|TEXT" boxes just the matching text inside SEL. Returns the rect, or null if nothing was found. */
async function box(highlight, shape = "box") {
  if (!highlight) return null;
  return page.evaluate(({ highlight, shape, STYLE }) => {
    if (!document.getElementById("rec-style")) {
      const st = document.createElement("style");
      st.id = "rec-style";
      st.textContent = STYLE;
      document.head.appendChild(st);
    }
    document.getElementById("rec-box")?.remove();
    const [sel, text] = highlight.split("|");
    const el = [...document.querySelectorAll(sel)].find((e) => e.getBoundingClientRect().width > 0);
    if (!el) return null;
    el.scrollIntoView({ block: "center", inline: "nearest" });
    let rect = el.getBoundingClientRect();
    if (text) {
      const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      let node;
      while ((node = walker.nextNode())) {
        const i = node.textContent.indexOf(text);
        if (i >= 0) {
          const line = node.textContent.slice(i).split("\n")[0];
          const r = document.createRange();
          r.setStart(node, i);
          r.setEnd(node, i + line.length);
          r.startContainer.parentElement.scrollIntoView({ block: "nearest" });
          // scroll the code block so the line is visible, then re-measure
          const rr = r.getBoundingClientRect();
          const scroller = el.closest("pre") ?? el;
          if (rr.top < scroller.getBoundingClientRect().top || rr.bottom > scroller.getBoundingClientRect().bottom) scroller.scrollTop += rr.top - scroller.getBoundingClientRect().top - 40;
          rect = r.getBoundingClientRect();
          break;
        }
      }
      if (!node) return null;
    }
    const pad = shape === "circle" ? 14 : 6;
    const b = document.createElement("div");
    b.id = "rec-box";
    if (shape === "circle") b.className = "circle";
    Object.assign(b.style, { left: `${rect.left - pad}px`, top: `${rect.top - pad}px`, width: `${rect.width + pad * 2}px`, height: `${rect.height + pad * 2}px` });
    document.body.appendChild(b);
    return { x: Math.round(rect.left), y: Math.round(rect.top), w: Math.round(rect.width), h: Math.round(rect.height) };
  }, { highlight, shape, STYLE });
}

async function caption(text, nearSelector) {
  await page.evaluate(({ text, nearSelector }) => {
    document.getElementById("rec-cap")?.remove();
    const el = document.querySelector(nearSelector);
    const r = el?.getBoundingClientRect();
    const c = document.createElement("div");
    c.id = "rec-cap";
    c.textContent = text;
    Object.assign(c.style, { left: `${Math.max(12, (r?.left ?? 40))}px`, top: `${(r?.bottom ?? 40) + 18}px` });
    document.body.appendChild(c);
  }, { text, nearSelector });
}
const clearCaption = () => page.evaluate(() => document.getElementById("rec-cap")?.remove());
const status = (stage, value, timeout = 40_000) => page.waitForSelector(`[data-testid=stage-${stage}][data-status=${value}]`, { timeout });

const BRANCH_DONE = { malformed: ["dlq", "info"], duplicate: ["dedupe", "skip"], stale: ["write", "info"], outage: ["dlq", "info"] };

async function clickBoxed(seg, ms = 1200) {
  const b = await box(seg.highlight, seg.shape);
  await sleep(ms);
  await page.click(seg.highlight.split("|")[0]);
  return b;
}

const actions = {
  async goto_learn(seg) { await page.goto(`${BASE}/learn`); await page.waitForSelector("[data-testid=submit-sample]"); return box(seg.highlight, seg.shape); },
  async none(seg) { return box(seg.highlight, seg.shape); },
  async click(seg) { const b = await clickBoxed(seg); await sleep(700); return (await box(seg.highlight, seg.shape)) ?? b; },
  async click_submit(seg) {
    const b = await clickBoxed(seg);
    await status("api", "done");
    return b;
  },
  async click_branch(seg) {
    const b = await clickBoxed(seg);
    const [stage, value] = BRANCH_DONE[seg.arg];
    await status(stage, value);
    return b;
  },
  async click_replay(seg) {
    const b = await clickBoxed(seg);
    await page.waitForSelector("[data-testid=replay-result]", { timeout: 20_000 });
    await status("write", "done");
    await page.locator("[data-testid=replay-result]").scrollIntoViewIfNeeded();
    return b;
  },
  async show_link(seg) {
    const b = await box(seg.highlight, seg.shape);
    const href = await page.getAttribute(seg.highlight, "href");
    await caption(`看源码 → ${href}`, seg.highlight);
    return b;
  },
  async goto_track(seg) { await page.goto(`${BASE}/track?id=SHP-DEMO0002`); await page.waitForSelector("[data-testid=track-status]"); return box(seg.highlight, seg.shape); },
  async goto_interview(seg) { const b = await clickBoxed(seg); await page.waitForSelector("[data-testid=interview]"); return b; },
  async goto_problems(seg) { const b = await clickBoxed(seg); await page.waitForSelector("[data-testid=problems]"); return b; },
  async open_question(seg) {
    const sum = page.locator("[data-testid=interview] details.q summary").first();
    await sum.scrollIntoViewIfNeeded();
    await box("[data-testid=interview] details.q summary");
    await sleep(1000);
    await sum.click();
    await sleep(400);
    return box(seg.highlight, seg.shape);
  },
  async open_problem(seg) {
    const sum = page.locator("[data-testid=problems] details.problem summary").first();
    await box("[data-testid=problems] details.problem summary");
    await sleep(1000);
    await sum.click();
    await sleep(400);
    return box(seg.highlight, seg.shape);
  },
};

const timeline = [];
for (const seg of narration) {
  const fn = actions[seg.action];
  if (!fn) throw new Error(`${seg.id}: unknown action ${seg.action}`);
  const plannedMs = Math.round((durations[seg.id] ?? 4) * 1000);
  const startMs = Date.now() - t0;
  await clearCaption();
  const b = await fn(seg);
  if (!b) console.warn(`WARN ${seg.id}: highlight not found: ${seg.highlight}`);
  const spent = Date.now() - t0 - startMs;
  if (spent < plannedMs) await sleep(plannedMs - spent);
  const durMs = Date.now() - t0 - startMs;
  timeline.push({ id: seg.id, part: seg.part, startMs, durMs, plannedMs, box: b });
  console.log(`${seg.id} ${seg.part} start ${startMs}ms dur ${durMs}ms box ${b ? `${b.x},${b.y} ${b.w}x${b.h}` : "MISSING"}`);
}
await page.evaluate(() => document.getElementById("rec-box")?.remove());
await sleep(800);
const video = page.video();
await context.close();
await browser.close();
const raw = await video.path();
renameSync(raw, path.join(OUT, "learn.webm"));
writeFileSync(path.join(DIR, "timeline.json"), JSON.stringify({ video: "video/out/learn.webm", size: [W, H], segments: timeline }, null, 2) + "\n");
const missing = timeline.filter((t) => !t.box || t.box.w === 0 || t.box.h === 0 || t.box.x < 0 || t.box.y < 0 || t.box.x + t.box.w > W || t.box.y + t.box.h > H);
console.log(`done: video/out/learn.webm, ${timeline.length} segments, ${((Date.now() - t0) / 1000).toFixed(1)}s; boxes missing/offscreen: ${missing.map((m) => m.id).join(",") || "none"}`);
