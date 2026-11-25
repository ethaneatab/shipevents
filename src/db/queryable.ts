import type pg from "pg";
/** Either a pool or a client inside a transaction. */
export type Queryable = Pick<pg.Pool | pg.PoolClient, "query">;
