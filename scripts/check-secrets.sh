#!/usr/bin/env bash
# Run before every commit: lists anything that looks like a credential, and refuses build output / videos.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== possible credentials (must all be obvious dev placeholders) =="
git grep -nIiE "(secret|password|token|api[_-]?key)\s*[:=]" -- . ':!package-lock.json' ':!frontend/package-lock.json' || true
echo "== forbidden paths in index =="
bad=$(git ls-files --cached | grep -E '(^|/)(node_modules|dist|\.venv|target)/|\.env$|\.(mp4|webm|mov)$' || true)
if [ -n "$bad" ]; then echo "$bad"; echo "FORBIDDEN FILES STAGED"; exit 1; fi
echo "ok"
