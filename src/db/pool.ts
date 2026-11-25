import pg from "pg";
import { config } from "../config.ts";

export type Pool = pg.Pool;
export type PoolClient = pg.PoolClient;

export function createPool(url = config.databaseUrl, max = 10): pg.Pool {
  return new pg.Pool({ connectionString: url, max });
}
