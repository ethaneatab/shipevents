import { expect, test } from "@playwright/test";

test("tracking page: a late scan shows as stale and the status does not go back", async ({ page }) => {
  await page.goto("/track?id=SHP-DEMO0002");
  await expect(page.getByTestId("track-status")).toHaveAttribute("data-status", "OUT_FOR_DELIVERY");
  await expect(page.getByTestId("track-history").locator(".p-stale")).toHaveCount(2);
  await expect(page.getByTestId("latency-table")).toBeVisible();
});

test("ops page: DLQ list, entry detail and a dry-run plan", async ({ page }) => {
  await page.goto("/ops");
  const rows = page.getByTestId("ops-dlq").locator("tr[data-dlq-id]");
  await expect(rows.first()).toBeVisible();
  const id = await rows.first().getAttribute("data-dlq-id");
  await page.getByTestId(`dlq-expand-${id}`).click();
  await expect(page.getByTestId(`dlq-detail-${id}`)).toBeVisible();
  await page.getByTestId("replay-shipment").fill("SHP-DEMO0004");
  await page.getByTestId("ops-dry-run").click();
  await expect(page.getByTestId("ops-plan")).toContainText("skip_invalid");
});

test("ops page rejects a wrong token", async ({ page }) => {
  await page.goto("/ops");
  await page.getByTestId("ops-token").fill("wrong-token");
  await expect(page.getByTestId("ops-error")).toContainText("401");
});
