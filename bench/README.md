# Vussa benchmark

`vussa-bench` is a standalone release binary for measuring REST and WebSocket
capacity against a running Vussa environment. It does not embed or replace the
server process.

On Unix, the benchmark raises its own soft open-file limit to `256000` by
default, so it works even when launched from an older terminal that still has
a soft limit of `256`. Override this with `--nofile-limit` or
`VUSSA_NOFILE_LIMIT`. The host hard limit must be at least the requested value,
unless it is `unlimited`.

Build both the optimized server and benchmark:

```sh
RUSTFLAGS='-C target-cpu=native' cargo build -p vussa --release
RUSTFLAGS='-C target-cpu=native -C codegen-units=1 -C opt-level=3 -C panic=abort' \
  cargo build -p vussa-bench --release
```

Run the standalone binary with one exact concurrency value:

```sh
./target/release/vussa-bench \
  --base-url http://127.0.0.1:3000 \
  --ws-url ws://127.0.0.1:3000/api/v1/ws \
  --mode mixed \
  --concurrency 137 \
  --duration 60
```

Use `--capacity` to probe exact integer levels and report the highest passing
user count for the current environment. Use `--readonly` for an existing
environment. `--full-api` exercises mutation endpoints and requires
`--allow-mutations`; use it only with disposable benchmark data.

`mixed` mode authenticates each client, establishes every WebSocket, waits for
all clients to finish setup, and only then starts concurrent REST and realtime
traffic. `--message-rate` is messages per user per minute and `--api-rate` is
REST requests per user per minute. The REST workload is distributed across
public, account, channel, conversation, participant, and authorized admin read
endpoints. `--full-api --allow-mutations` additionally runs every state-changing
API against disposable fixtures before load starts. Set either rate to `0` to
disable that traffic class.

Authentication, WebSocket connection setup, and initial traffic are ramped
rather than started simultaneously; the default setup interval is 5 ms per
client. Reports separate authenticated clients, WebSocket connections, setup
failures, WebSocket failures, server-sent realtime errors, REST operations, and
message acknowledgements. Every operation prints p50/p95/p99/max latency,
including end-to-end `WS message delivery`. A partial setup, unacknowledged
message, or realtime server error can never be reported as passing.

The same binary is available through Cargo:

```sh
cargo run -p vussa-bench --release -- --mode readonly --concurrency 10
```

For a complete local run—including optimized release builds, PostgreSQL,
Valkey, the release backend, the validated connection ramp, and cleanup—use:

```sh
./scripts/bench.sh            # 3,000 users, whole API + traffic, 30 seconds
./scripts/bench.sh 5000 60    # 5,000 users for 60 seconds
```

Reports and backend/build logs are written under `.bench/`. The wrapper never
starts the frontend and does not stop PostgreSQL or Valkey if they were already
running before the benchmark.
