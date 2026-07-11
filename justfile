setup:
    npm install && cd frontend && npm install

dev:
    mprocs

gen-api:
    #!/usr/bin/env bash
    set -e
    if lsof -ti tcp:3000 > /dev/null 2>&1; then
        echo "Error: backend is already running on port 3000. Stop it before running gen-api." >&2
        exit 1
    fi
    ROOT="$(pwd)"
    cd "$ROOT/backend" && cargo run &
    BACKEND_PID=$!
    stop_backend() {
        kill $BACKEND_PID 2>/dev/null
        pkill -f "target/debug/lores-websites" 2>/dev/null || true
        wait $BACKEND_PID 2>/dev/null || true
    }
    trap stop_backend EXIT INT TERM
    echo "Waiting for backend to start..."
    until curl -sf http://localhost:3000/api-docs/openapi.json > /dev/null 2>&1; do
        sleep 1
    done
    cd "$ROOT/frontend" && npm run swagger
    stop_backend
    trap - EXIT

release:
    npm run release

docker:
    #!/usr/bin/env bash
    set -e
    docker build -t lores-chat-example .
    trap 'docker stop lores-chat-example' INT TERM EXIT
    docker run --rm --name lores-chat-example -p 3000:3000 lores-chat-example &
    echo "Press Control-C 3 times to exit"
    wait
