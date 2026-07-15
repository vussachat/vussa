# vussa Chat

Authenticated realtime chat with a Rust/Axum backend, SvelteKit frontend, PostgreSQL 18, and Valkey Pub/Sub.

The backend owns one shared Valkey Pub/Sub manager per process. It keeps one control subscription and subscribes to a room only while at least one local WebSocket user has joined it. Local users receive room events through Tokio broadcast channels; switching rooms updates the shared reference count instead of opening a new Valkey connection per user.

## Run locally

Use the short development wrapper:

```sh
./scripts/dev.sh start
./scripts/dev.sh stop
./scripts/dev.sh restart
./scripts/dev.sh clean restart
./scripts/dev.sh setup-limits
./scripts/dev.sh check-limits
```

It manages PostgreSQL, Valkey, the Rust backend, and the Svelte frontend. Logs are written to `.dev/`.
The wrapper owns all four processes: `start` builds/starts the backend and starts Vite, while `stop`, `restart`, and `clean` stop them as well as the Compose services.

The development launcher requires a high file-descriptor limit for realtime clients. On macOS, configure launchd once with `./scripts/dev.sh setup-limits`; this sets the default `VUSSA_NOFILE_LIMIT` of `256000` and requires `sudo`. Every `start` also applies and verifies that limit in its own shell, so it works from an already-open terminal. Override it for a smaller environment with `VUSSA_NOFILE_LIMIT=8192 ./scripts/dev.sh start`. Use `./scripts/dev.sh check-limits` to inspect the shell and launchd values.

To restore the macOS launchd default, run `sudo launchctl limit maxfiles 256 256`. A terminal that was already open may continue to show its old limit; restart it or let `dev.sh start` raise the limit for its child processes.

```sh
docker compose up -d --wait valkey postgres
DATABASE_URL=postgres://vussa_chat:vussa_chat@127.0.0.1:5432/vussa_chat \
VALKEY_URL=redis://127.0.0.1:6379 cargo run -p vussa
```

Run the frontend in a second terminal:

```sh
cd frontend
npm install       # only needed after dependency changes or a fresh checkout
npm run dev
```

Open <http://localhost:5173>. The backend must be running on port 3000; verify it with:

```sh
curl http://localhost:3000/api/v1/health
```

## Stop and clean up

Stop the frontend and backend with `Ctrl-C` in their terminals. Stop the database containers while keeping PostgreSQL data:

```sh
docker compose down --remove-orphans
```

To remove the containers and the PostgreSQL volume (this permanently deletes the local database and all persisted messages):

```sh
docker compose down --volumes --remove-orphans
```

For a clean development restart, remove Vussa’s containers, network, and PostgreSQL volume while keeping Docker images:

```sh
./scripts/dev.sh clean restart
# equivalent: ./scripts/dev.sh clean-restart
```

To also remove locally built Compose images:

```sh
docker compose down --volumes --remove-orphans --rmi local
```

The development Compose stack uses the standard host ports `5432` for PostgreSQL and `6379` for Valkey. If another deployment is using either port, stop that deployment before starting Vussa. For exceptional local setups, override them with `VUSSA_POSTGRES_PORT` and `VUSSA_VALKEY_PORT`. The backend defaults to `postgres://vussa_chat:vussa_chat@127.0.0.1:5432/vussa_chat` and `redis://127.0.0.1:6379`; `DATABASE_URL`, `VALKEY_URL`, and `PG_MAX_CONNECTIONS` are configurable.

The frontend requires Node.js and npm:

```sh
cd frontend
npm install
npm run dev
```

The protected `main` channel cannot be deleted. Authenticated users can create channels and send messages; moderation permissions control destructive operations. PostgreSQL 18 is the durable layer for channels and messages, retaining 90 days of cold history. Valkey keeps the latest 300 messages per active channel using bitcode payloads in a Hash and a sorted-set ordering index; the browser receives the newest 50 messages first and requests older 50-message keyset pages from the hot tier before falling through to PostgreSQL. PostgreSQL shared buffers provide the database-level page cache. Sessions are opaque and shared across replicas through Valkey, so WebSocket reconnects preserve account identity.

## Authentication and operations

The web application supports registration, login, logout, profile updates, cookie-backed Valkey sessions, fixed user/moderator/admin roles, and an admin user panel. Configure the optional first administrator with `ADMIN_EMAIL` and `ADMIN_PASSWORD`; bootstrap is idempotent. Set `COOKIE_SECURE=true` behind HTTPS.

The development wrapper enables deterministic test fixtures on every start: `test1` through `test6` use matching passwords (`test1:test1`, ..., `test6:test6`), and `test1` has the admin role. These predictable accounts are intentionally enabled only by the wrapper’s `SEED_TEST_ACCOUNTS=true` setting and must not be enabled in production.

Operational endpoints are `/api/v1/health/live`, `/api/v1/health/ready`, `/api/v1/health/startup`, and `/api/v1/metrics`. PostgreSQL and Valkey are required for readiness. The application uses PostgreSQL as the durable source of truth and a transactional outbox for authorization/session invalidation events.

The backend uses modern Rust file modules without `mod.rs`: top-level module
files such as `api.rs`, `auth.rs`, `repository.rs`, `websocket.rs`,
`notifications.rs`, and `storage.rs` own descriptive submodules under matching
directories. Process startup remains in `main.rs`.
Cookie-authenticated WebSocket upgrades enforce the configured `CORS_ORIGIN`,
or the request host when no explicit origin is configured; non-browser clients
without an `Origin` header remain supported.

Production Kubernetes resources are in `deploy/helm/vussa`. The chart expects externally managed PostgreSQL 18 and Valkey URLs by default:

```sh
helm install vussa ./deploy/helm/vussa \
  --set postgres.url='postgres://user:password@postgres.example/vussa' \
  --set valkey.url='redis://valkey.example:6379' \
  --set storage.backend=s3 \
  --set storage.endpoint='https://s3.example' \
  --set storage.bucket='vussa-files'
```

The chart defaults to shared S3-compatible object storage and disables the
local upload volume because multiple replicas must observe the same files.
Filesystem storage is available for single-replica development deployments;
the chart rejects it when more than one replica is configured. Keep storage
credentials in the referenced external Secret rather than in chart values.
Topology spreading defaults to `DoNotSchedule` so production replicas stay
separated by hostname and zone; single-node development clusters can explicitly
set `topologySpread.whenUnsatisfiable=ScheduleAnyway`.
The backend and frontend workloads do not require Kubernetes API access, so
their service-account token automounting is disabled by the chart.
Build and publish both images before installing the chart:

```sh
docker build -t registry.example/vussa:latest .
docker build -f frontend/Dockerfile -t registry.example/vussa-frontend:latest frontend
```

Set `image.repository` and `frontend.image.repository` to those image names.
Keep the chart’s `cookieSecure` default enabled behind HTTPS; disposable
HTTP-only port-forward smoke tests must explicitly set `--set cookieSecure=false`.
Set `notifications.vapidPublicKey` when enabling native browser push; the
private signing and delivery credentials belong to the configured browser
notification adapter and should remain in the external Secret.

PostgreSQL 18 asynchronous I/O is configured by the PostgreSQL operator. Use `io_method=io_uring` only when the node/container seccomp policy permits it; `worker` is the portable Kubernetes fallback.

The optional Helm restore Job is fail-closed: set
`restore.allowDestructive=true` only during an explicitly approved recovery,
because it runs `pg_restore --clean` against the configured database. The
disposable backup/restore smoke test restores into a separate database instead.

The operational smoke checks are `scripts/integration-smoke.sh` (auth, files,
WebSocket upgrade, recovery, drafts, and sessions),
`scripts/dependency-failure-smoke.sh` (cache outage and recovery), and
`scripts/ha-smoke.sh` (replica replacement and rolling restart). The disposable
Kubernetes checks are `scripts/kubernetes-dependency-smoke.sh` (Valkey outage),
`scripts/kubernetes-database-dependency-smoke.sh` (PostgreSQL outage), and
`scripts/kubernetes-backup-restore-smoke.sh` plus
`scripts/kubernetes-node-failure-smoke.sh` (single-node failure) and
`scripts/kubernetes-zone-failure-smoke.sh` (all workers in one labeled zone).
The node and zone checks control disposable kind node containers and must only
be run in an isolated test cluster. CI runs these against a kind
cluster with disposable dependencies; production deployments should use
operator-managed, multi-zone PostgreSQL and Valkey services and run the same
checks against those services before sign-off. The application uses a
transactional outbox with expiring claims so multiple replicas do not normally
claim the same event; delivery remains at-least-once for crash recovery.

## License
MIT or Apache 2.0
