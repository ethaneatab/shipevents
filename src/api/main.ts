import { config } from "../config.ts";
import { createPool } from "../db/pool.ts";
import { migrate } from "../db/migrate.ts";
import { log } from "../log.ts";
import { buildApi } from "./server.ts";

const pool = createPool();
await migrate(pool);
const app = buildApi(pool);
await app.listen({ port: config.apiPort, host: "127.0.0.1" });
log.info({ port: config.apiPort }, "api listening");
for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    await app.close();
    await pool.end();
    process.exit(0);
  });
}
