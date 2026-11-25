import { config } from "../config.ts";
import { log } from "../log.ts";
import { buildDownstream } from "./server.ts";

const { app } = buildDownstream();
await app.listen({ port: config.downstreamPort, host: "127.0.0.1" });
log.info({ port: config.downstreamPort }, "fake downstream listening");
for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, async () => (await app.close(), process.exit(0)));
