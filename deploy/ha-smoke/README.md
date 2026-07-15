# Disposable HA smoke environment

`dependencies.yaml` supplies disposable PostgreSQL, Valkey, and S3-compatible
services for the CI kind-cluster job. It is intentionally not a production
database or object-storage topology; production deployments must use HA-capable
operators or managed services.
