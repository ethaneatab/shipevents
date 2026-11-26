# Before: demo.sh started the frontend like this.
# @learn:q04-frontend-start-before
(cd frontend && VITE_ADMIN_TOKEN=dev-admin-token-change-me node node_modules/vite/bin/vite.js --port 5190 --strictPort >"$ROOT/$RT/logs/frontend.log" 2>&1 & echo $! >"$ROOT/$RT/frontend.pid")
# 1) "cd frontend && node ..." is backgrounded as one subshell, so $! is that subshell, not vite
# 2) the subshell itself still holds demo.sh's stdout, so "demo.sh | tail" never finished
# @learn:end q04-frontend-start-before
