import { createPool } from "../../src/db/pool.ts";
import { migrate } from "../../src/db/migrate.ts";
import { TEST_DB_URL } from "./env.ts";

// Integration tests use their own database (shipevents_test) so they never mix with demo data.
export default async function setup(): Promise<void> {
  if (process.env.SKIP_DB_SETUP === "1") return;
  const pool = createPool(TEST_DB_URL, 2);
  try {
    await migrate(pool);
  } catch (e) {
    console.warn(`[global-setup] could not migrate ${TEST_DB_URL}: ${(e as Error).message} (unit tests still run)`);
  } finally {
    await pool.end();
  }
}
