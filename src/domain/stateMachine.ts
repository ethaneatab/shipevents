import { TARGET_STATUS, type EventType, type Status } from "../events/types.ts";

/**
 * Legal moves. Forward skips along the main path are allowed (carriers drop scans),
 * IN_TRANSIT -> IN_TRANSIT is a hub scan, EXCEPTION can recover or end in RETURNED.
 * DELIVERED and RETURNED are terminal.
 */
export const ALLOWED: Record<Status, readonly Status[]> = {
  CREATED: ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  PICKED_UP: ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  IN_TRANSIT: ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  OUT_FOR_DELIVERY: ["DELIVERED", "EXCEPTION"],
  EXCEPTION: ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION", "RETURNED"],
  DELIVERED: [],
  RETURNED: [],
};

/** Statuses a shipment may start in when its earlier events are late or lost. */
export const ENTRY_STATUSES: readonly Status[] = ["CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"];

export interface Current {
  status: Status;
  lastSeq: number;
}

export type Decision =
  | { kind: "apply"; from: Status | null; to: Status }
  | { kind: "stale"; from: Status; to: Status; reason: string }
  | { kind: "reject"; from: Status | null; to: Status; reason: string };

// @learn:state-decide
/**
 * The only place that decides whether an event may change a shipment.
 * Order is by the carrier's per-shipment seq, not by arrival: an older event is
 * kept in history ("stale") but can never move the status backwards.
 */
export function decide(current: Current | null, event: { seq: number; type: EventType }): Decision {
  const to = TARGET_STATUS[event.type];
  if (current === null) {
    return ENTRY_STATUSES.includes(to)
      ? { kind: "apply", from: null, to }
      : { kind: "reject", from: null, to, reason: `cannot start a shipment in ${to}` };
  }
  if (event.seq <= current.lastSeq) {
    return { kind: "stale", from: current.status, to, reason: `seq ${event.seq} <= last applied seq ${current.lastSeq}` };
  }
  if (!ALLOWED[current.status].includes(to)) {
    return { kind: "reject", from: current.status, to, reason: `illegal transition ${current.status} -> ${to}` };
  }
  return { kind: "apply", from: current.status, to };
}
// @learn:end state-decide

// @learn:state-legacy
/** LAB_SABOTAGE=no_transition_guard: last writer wins, whatever arrives last becomes the status. */
export function decideLegacy(current: Current | null, event: { seq: number; type: EventType }): Decision {
  return { kind: "apply", from: current?.status ?? null, to: TARGET_STATUS[event.type] };
}
// @learn:end state-legacy
