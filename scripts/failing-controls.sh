#!/usr/bin/env bash
# Proves the tests are alive: for each protection, switch it off (LAB_SABOTAGE) and the test that guards it
# must FAIL; switch it back on and the same test must PASS. Exit 1 if any control does not behave.
# Needs Kafka + Postgres (docker-compose -p shipevents up -d kafka postgres).
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/logs
status=0
run() { # switch file
  local sw=$1 file=$2 log=".runtime/logs/control-${1:-none}-$(basename "$2" .test.ts).log"
  LAB_SABOTAGE=$sw npx vitest run "$file" >"$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E "^\s+Tests\s" "$log" | tail -1 | sed -E 's/^\s+//')
  printf '%s\t%s\t%s\n' "$rc" "$summary" "$log"
}
printf '%-22s %-42s %-8s %s\n' "LAB_SABOTAGE" "test" "result" "vitest summary"
for pair in "no_dedupe:test/integration/duplicates.test.ts" "no_transition_guard:test/integration/ordering.test.ts" "commit_before_write:test/integration/restart.test.ts"; do
  sw=${pair%%:*}; file=${pair#*:}
  IFS=$'\t' read -r rc summary log < <(run "$sw" "$file")
  if [ "$rc" -ne 0 ]; then verdict="RED ok"; else verdict="GREEN!!"; status=1; fi
  printf '%-22s %-42s %-8s %s\n' "$sw" "$file" "$verdict" "$summary"
  grep -E "^\s+→ " "$log" | head -2 | sed -E 's/^\s+/    /'
  IFS=$'\t' read -r rc summary log < <(run "" "$file")
  if [ "$rc" -eq 0 ]; then verdict="GREEN ok"; else verdict="RED!!"; status=1; fi
  printf '%-22s %-42s %-8s %s\n' "(off)" "$file" "$verdict" "$summary"
done
exit $status
