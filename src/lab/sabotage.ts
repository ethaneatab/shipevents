// Failing controls. Each switch removes exactly one protection so the test that guards it must go red.
// Never set in production; scripts/failing-controls.sh flips them one at a time.
export const SABOTAGE_SWITCHES = ["no_dedupe", "no_transition_guard", "commit_before_write"] as const;
export type Sabotage = (typeof SABOTAGE_SWITCHES)[number];

export function sabotaged(name: Sabotage): boolean {
  return (process.env.LAB_SABOTAGE ?? "").split(",").map((s) => s.trim()).includes(name);
}

/**
 * Crash hook for restart tests: SIGKILL ourselves right after the first durable step for this event id,
 * i.e. in the gap between "write the database" and "commit the offset" (or the reverse under sabotage).
 */
export function crashPoint(eventId: string | undefined): void {
  const target = process.env.LAB_CRASH_ON_EVENT;
  if (target && eventId === target) {
    process.stderr.write(`LAB_CRASH_ON_EVENT hit for ${eventId}, killing pid ${process.pid}\n`);
    process.kill(process.pid, "SIGKILL");
  }
}
