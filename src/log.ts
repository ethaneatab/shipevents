import { pino } from "pino";
import { config } from "./config.ts";

// One JSON line per event. Every processing log carries shipmentId, eventId,
// partition and offset so a late customer update can be traced back to a partition.
export const log = pino({ level: config.logLevel, base: { svc: process.env.SVC_NAME ?? "shipevents" } });
export type Logger = typeof log;
