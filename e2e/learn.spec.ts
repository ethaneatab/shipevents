import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page } from "@playwright/test";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ORDER = ["produce", "schema", "dedupe", "state", "downstream", "write", "api"];
const LINK = /^https:\/\/github\.com\/[^/]+\/shipevents\/blob\/([0-9a-f]{40})\/([^#]+)#L(\d+)-L(\d+)$/;

/** Records every data-status change of every stage, with a timestamp, from inside the page. */
async function watchStages(page: Page) {
  await page.evaluate(() => {
    const w = window as unknown as { __log: { stage: string; status: string; t: number }[] };
    w.__log = [];
    const seen = new Map<string, string>();
    const scan = () => document.querySelectorAll<HTMLElement>("[data-stage]").forEach((el) => {
      const s = el.dataset.stage!, st = el.dataset.status!;
      if (seen.get(s) !== st) { seen.set(s, st); w.__log.push({ stage: s, status: st, t: performance.now() }); }
    });
    new MutationObserver(scan).observe(document.body, { subtree: true, attributes: true, childList: true, attributeFilter: ["data-status"] });
    scan();
  });
}
const stageLog = (page: Page) => page.evaluate(() => (window as unknown as { __log: { stage: string; status: string; t: number }[] }).__log);

function checkSourceLink(href: string) {
  const m = href.match(LINK);
  expect(m, `link format: ${href}`).not.toBeNull();
  const [, sha, file, start, end] = m!;
  expect(sha).not.toBe("main");
  const lines = execFileSync("git", ["show", `${sha}:${file}`], { cwd: ROOT }).toString().split("\n");
  expect(Number(end)).toBeGreaterThanOrEqual(Number(start));
  expect(lines.length).toBeGreaterThanOrEqual(Number(end));
  expect(lines[Number(start) - 2]).toContain("@learn:");
  expect(lines[Number(end)]).toContain("@learn:end");
}

test("sample event: stages light up in order with real data and real source links", async ({ page }) => {
  await page.goto("/learn");
  await watchStages(page);
  await page.getByTestId("submit-sample").click();
  await expect(page.getByTestId("stage-api")).toHaveAttribute("data-status", "done", { timeout: 30_000 });

  const log = await stageLog(page);
  const firstDone = ORDER.map((s) => log.find((e) => e.stage === s && e.status === "done")?.t ?? NaN);
  for (let i = 1; i < firstDone.length; i++) expect(firstDone[i], `${ORDER[i]} after ${ORDER[i - 1]}`).toBeGreaterThan(firstDone[i - 1]!);
  // the downstream stage was visibly retrying before it succeeded
  expect(log.some((e) => e.stage === "downstream" && e.status === "active")).toBe(true);

  await expect(page.getByTestId("summary-produce")).toHaveText(/分区 \d+ · offset \d+/);
  await expect(page.getByTestId("summary-state")).toHaveText("CREATED -> PICKED_UP");
  await expect(page.getByTestId("summary-downstream")).toContainText("attempt 1 failed: eta 503");
  await expect(page.getByTestId("summary-downstream")).toContainText("attempt 2 ok");
  await expect(page.getByTestId("summary-api")).toContainText("status PICKED_UP");

  await page.getByTestId("expand-write").click();
  await expect(page.getByTestId("data-write")).toContainText('"outcome": "applied"');
  await expect(page.getByTestId("data-write")).toContainText('"status_to": "PICKED_UP"');
  await page.getByTestId("expand-dedupe").click();
  await expect(page.getByTestId("data-dedupe")).toContainText('"processed_events_row"');

  for (const s of ORDER) checkSourceLink((await page.getByTestId(`src-${s}`).getAttribute("href"))!);

  await page.getByTestId("video-schema").click();
  await expect(page.getByTestId("videoslot-schema")).toHaveText("视频待生成");
});

test("branches: duplicate, stale, malformed, outage then dry-run and replay", async ({ page }) => {
  await page.goto("/learn");
  await page.getByTestId("submit-sample").click();
  await expect(page.getByTestId("stage-api")).toHaveAttribute("data-status", "done", { timeout: 30_000 });

  await page.getByTestId("branch-duplicate").click();
  await expect(page.getByTestId("stage-dedupe")).toHaveAttribute("data-status", "skip");
  await expect(page.getByTestId("summary-dedupe")).toContainText("already processed (applied)");

  await page.getByTestId("branch-stale").click();
  await expect(page.getByTestId("stage-write")).toHaveAttribute("data-status", "info", { timeout: 30_000 });
  await expect(page.getByTestId("summary-state")).toContainText("stale: seq 1 <= last applied seq 2");
  await expect(page.getByTestId("summary-api")).toContainText("status PICKED_UP");

  await page.getByTestId("branch-malformed").click();
  await expect(page.getByTestId("stage-schema")).toHaveAttribute("data-status", "fail");
  await expect(page.getByTestId("stage-dlq")).toHaveAttribute("data-status", "info");
  await expect(page.getByTestId("summary-schema")).toContainText("schema_invalid: seq");

  await page.getByTestId("branch-outage").click();
  await expect(page.getByTestId("stage-dlq")).toHaveAttribute("data-status", "info", { timeout: 30_000 });
  await expect(page.getByTestId("summary-dlq")).toContainText("downstream_exhausted after 4 attempts");
  await page.getByTestId("dry-run").click();
  await expect(page.getByTestId("plan-table").locator('tr[data-verdict="apply"]')).toHaveCount(1);
  await expect(page.getByTestId("plan-table").locator('tr[data-verdict="skip_invalid"]')).toHaveCount(1);
  await page.getByTestId("execute-replay").click();
  await expect(page.getByTestId("replay-result")).toContainText("发回 1 条，跳过 1 条");
  await expect(page.getByTestId("stage-write")).toHaveAttribute("data-status", "done", { timeout: 30_000 });
  await expect(page.getByTestId("summary-api")).toContainText("status IN_TRANSIT");
});

test("interview tab: open a question and see its answer", async ({ page }) => {
  await page.goto("/learn");
  await page.getByTestId("tab-interview").click();
  const q = page.locator("[data-testid=interview] details.q").first();
  await q.locator("summary").click();
  await expect(q.locator(".answer")).toBeVisible();
  await expect(q.locator(".answer")).toContainText("一句话答案");
});

test("problems tab: expand a problem", async ({ page }) => {
  await page.goto("/learn/problems");
  const p = page.locator("[data-testid=problems] details.problem").first();
  await p.locator("summary").click();
  await expect(p.locator(".answer")).toContainText("现象");
  await expect(p.locator(".video-missing")).toHaveText("视频待生成");
});
