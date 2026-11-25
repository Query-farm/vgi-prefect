# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3.
- `serve` extra is `vgi-python[http,oauth]`; the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The HEALTHCHECK uses `HEAD /health`, which stays
  anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.0 (unreleased)

Initial release: the Prefect 3 REST API (Prefect Cloud and self-hosted servers) as SQL.

- `prefect.main`: flows, deployments, deployment_schedules, flow_runs, task_runs, flow_run_logs,
  task_run_logs, work_pools, work_queues, workers, concurrency_limits, automations, events,
  artifacts, block_types, block_documents (names only), variables (masked);
  `variable_values(reveal := false)`.
- `prefect.unified`: runs, task_runs (shared orchestrator shape), `unified_status(state_type)`.
- `prefect` secret: api_url + api_key (Cloud bearer) or auth_string (server basic auth).
- SQL predicates are translated into Prefect's `POST /…/filter` bodies; offset / page-token
  paging with an externalized cursor; 429 / 5xx retried honouring `Retry-After`.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
