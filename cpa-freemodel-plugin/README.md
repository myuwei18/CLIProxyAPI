# CPA FreeModel Plugin PoC

This is a dynamic-library plugin PoC for CLIProxyAPI. It proves that the FreeModel direction can live outside CPA core and own its SQLite state.

## Capabilities

- Registers `management_api`.
- Adds a health endpoint: `GET /v0/management/freemodel-plugin/health`.
- Adds a resource/menu page: `GET /v0/resource/plugins/cpa-freemodel/status`.
- Owns a SQLite database compatible with the legacy FreeModel table names:
  - `fm_account`
  - `fm_quota_snapshot`

## Build

```bash
./scripts/build.sh
```

The script writes the platform plugin binary to `../plugins/cpa-freemodel.<ext>`.

## Local config

Use a local config like:

```yaml
plugins:
  enabled: true
  dir: plugins
  configs:
    cpa-freemodel:
      enabled: true
      priority: 10
      data_dir: plugins/cpa-freemodel-data
      # Optional explicit DB path. Defaults to data_dir/freemodel.db.
      # db_path: plugins/cpa-freemodel-data/freemodel.db
```

## Verify

Start CPA with a config that enables the plugin, then check:

```bash
curl -H 'Authorization: Bearer <management-key>' http://127.0.0.1:8317/v0/management/plugins
curl -H 'Authorization: Bearer <management-key>' http://127.0.0.1:8317/v0/management/freemodel-plugin/health
curl http://127.0.0.1:8317/v0/resource/plugins/cpa-freemodel/status
```

## PoC Management API

```bash
# Create or update an account. The cookie is stored but never returned by list APIs.
curl -H 'Authorization: Bearer <management-key>' \
  -H 'Content-Type: application/json' \
  -d '{"email":"poc@example.test","cookie":"bm_session=...","proxy":"direct"}' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/accounts

# List accounts.
curl -H 'Authorization: Bearer <management-key>' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/accounts

# List latest quota snapshots. Accounts without a snapshot are returned as pending.
# view=all returns all accounts.
# view=available returns only accounts with real available balance.
# view=other returns expired, pending, exhausted, or otherwise unavailable accounts.
curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/quota?view=all'

curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/quota?view=available'

curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/quota?view=other'

# Insert a test quota snapshot. This is a PoC/testing endpoint until the real sync worker is migrated.
curl -H 'Authorization: Bearer <management-key>' \
  -H 'Content-Type: application/json' \
  -d '{"email":"poc@example.test","plan_id":"poc-plan","plan_status":"active","credit_cents":1234,"window_5h":{"used_cents":100,"limit_cents":500},"window_week":{"used_cents":200,"limit_cents":1000}}' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/snapshots
```

## Quota classification

Quota responses include both raw fields and derived classification fields:

- `window_available_cents`: real window capacity, calculated as `min(window_5h_remaining, window_week_remaining)`.
- `extra_available_cents`: extra usable balance, currently `topup_cents + credit_cents + max(referral_credits - referral_used, 0) * 100`.
- `real_available_cents`: `window_available_cents + extra_available_cents`, unless the subscription is expired.
- `subscription_expired`: true when plan status is expired/canceled/inactive or `current_period_end` is in the past.
- `availability_status`: `available` or `other`.
- `availability_reason`: `real_balance_available`, `subscription_expired`, `pending_sync`, or `no_real_balance`.

The `available` view only returns accounts with a truly usable balance. The `other` view is for accounts that should not be treated as currently available, especially expired subscriptions.

## Next migration steps

- Migrate real FreeModel API sync and OTP login.
- Migrate proxy subscription parsing and proxy node persistence.
- Migrate gost lifecycle management behind plugin-owned APIs.
- Add host callbacks only if core integration becomes unavoidable.

## Operations read APIs for RELAYX

These APIs are Management API routes and require the CPA management key. They are designed for RELAYX and operator agents to consume JSON instead of reading the plugin database directly.

```bash
# Provider incidents. Current implementation returns stored incidents; real upstream error ingestion is a later step.
curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/incidents?limit=100'

# Supplier usage/quota snapshots. Email and proxy fields are masked.
curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/usage-snapshots?limit=100'

# Supplier reconciliation source. Empty until request-level supplier cost ingestion is implemented.
curl -H 'Authorization: Bearer <management-key>' \
  'http://127.0.0.1:8317/v0/management/freemodel-plugin/reconciliation-source?limit=100'
```

Sensitive fields policy:

- Account cookies are never returned.
- Emails are masked and accompanied by stable `email_hash` values.
- Proxy passwords are masked.
- Prompts, responses, raw authorization headers, and cookies must not be stored in these operational APIs.

## FreeModel auth and sync APIs

These endpoints are Management API routes and are intended for operator use. They do not expose stored cookies.

```bash
# Send a login OTP.
curl -H 'Authorization: Bearer <management-key>' \
  -H 'Content-Type: application/json' \
  -d '{"email":"operator@example.com","proxy":"direct"}' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/auth/send-otp

# Verify OTP, store bm_session, and create/update the account.
curl -H 'Authorization: Bearer <management-key>' \
  -H 'Content-Type: application/json' \
  -d '{"email":"operator@example.com","code":"123456","proxy":"direct"}' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/auth/verify-otp

# Sync all configured accounts by calling FreeModel usage/billing/referral/auth APIs.
curl -H 'Authorization: Bearer <management-key>' \
  -X POST http://127.0.0.1:8317/v0/management/freemodel-plugin/sync

# Read latest sync result.
curl -H 'Authorization: Bearer <management-key>' \
  http://127.0.0.1:8317/v0/management/freemodel-plugin/sync-status
```


## RELAYX Channel PoC

This plugin can declare `model_provider`, `model_router`, and `executor` capabilities in addition to the Management API when `model_executor_enabled: true` is set in the plugin config. This path is experimental and is kept behind a feature flag until CPA P0 is validated end to end. RELAYX should not block its first stage on this executor.

Static provider ID: `cpa-freemodel`

Initial static models:

- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`

Supported CPA inbound paths for RELAYX:

- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/responses`

RELAYX should use CPA as a normal OpenAI-compatible channel:

- `base_url`: `http://<cpa-host>:8317/v1`
- `api_key`: a CPA frontend API key, for example `poc-api-key` in the PoC config
- model names: use the FreeModel model IDs directly for the first stage

The plugin picks the first account whose latest quota snapshot is `availability_status=available` and `real_available_cents > 0`, and whose `api_key` is configured. It forwards model requests with `Authorization: Bearer <api_key>`. The stored `bm_session` cookie remains dashboard-only for quota sync. Neither cookie nor API key is returned in management or model responses. Streaming is bridged through the host HTTP stream callback and preserves upstream SSE chunks.

Limitations for this PoC:

- `model_executor_enabled` defaults to `false`; keep it disabled for RELAYX first-stage operations-only integration.
- FreeModel model API credential semantics still need to be verified with a real account; the executor now keeps the model API key separate from the dashboard `bm_session`.
- Account scheduling is intentionally minimal: first available account by local DB order.
- Upstream base URL is currently `https://api.freemodel.dev/v1`.

## Stable Management JSON Schemas

All routes below are under `/v0/management/freemodel-plugin` and currently use CPA Management API authentication. RELAYX should treat them as read-only supplier operations APIs and must not read the plugin SQLite database directly.

Common envelope:

- Success responses use `{"success": true, ...}` for plugin-owned JSON bodies, except the ABI wrapper that CPA strips before HTTP delivery.
- Error responses use `{"success": false, "message": string}`.
- Times are UTC RFC3339 strings, for example `2026-06-24T12:00:00Z`. Empty string means unknown or not yet synced.
- Money fields ending in `_cents` are integer cents. USD subtotal fields use decimal USD and are supplier-derived.
- Sensitive values are never returned: cookie, session, token, password, upstream API key, raw authorization header, prompt, response content, payment key, private key.
- Emails are masked and accompanied by `email_hash`, a stable SHA-256 hash of normalized email.
- Proxy URLs mask username/password.

### GET /health

Purpose: plugin and supplier-ops readiness check.

Fields:

- `success` boolean: true when the health handler itself succeeded.
- `status` string: `ok` for a loaded plugin.
- `plugin_id`, `name`, `version` strings: plugin identity.
- `schema_version` string: stable management schema version.
- `timestamp` string: health generation time.
- `store_status` string: `ok` or `unavailable`.
- `store_ok` boolean: SQLite store opened.
- `sync_worker_status` string: `disabled`, `idle`, or `last_sync_failed`.
- `last_sync_at` string: last completed sync time, empty when none.
- `last_error` string: last plugin configuration/runtime error, empty when none.
- `model_executor_enabled` boolean: experimental model executor flag.
- `executor_status` string: `disabled` or `experimental_enabled`.
- `db_path` string: local operator diagnostic; RELAYX should not depend on it.

### GET /accounts

Purpose: account pool inventory without secrets.

Response: `{"success": true, "data": account[]}`.

Account fields:

- `id` integer: stable local account id for cross-endpoint joins.
- `email` string: masked email.
- `email_hash` string: stable hash for operations joins.
- `user_id` integer: FreeModel dashboard user id when known, 0 when unknown.
- `model_api_configured` boolean: whether a model API key exists; the key is never returned.
- `proxy` string: masked proxy URL, empty/direct when not configured.
- `created_at`, `updated_at` strings: UTC timestamps.

### GET /quota?view=all|real|test|available|other

Purpose: latest per-account quota state and derived availability.

Views:

- `all`: every account with latest or pending quota state.
- `real`: non-test accounts. This is the UI default.
- `test`: demo/test/mock/sample/example accounts.
- `available`: only truly usable accounts: real account + dashboard CK configured + non-expired subscription + `real_available_cents > 0`.
- `other`: accounts that must not be treated as currently usable.

Response fields:

- `success` boolean.
- `view` string: normalized view.
- `summary` object: `all`, `available`, `other`, `subscription_expired`, `real_accounts`, `test_accounts`, `missing_cookie`, `real_available_cents`, `window_available_cents`, `extra_available_cents`.
- `data` array of quota records.

Quota record fields:

- `account_id` integer: stable local account id.
- `email`, `email_hash`: masked account identity.
- `plan_id`, `plan_status`: supplier plan fields, empty when unknown.
- `credit_cents`, `topup_cents`: supplier monetary balances in cents.
- `referral_credits`, `referral_used`: supplier referral credits in USD-equivalent units.
- `current_period_end` string: supplier subscription period end, empty when unknown.
- `cancel_at_period_end` boolean.
- `window_5h`, `window_week`: objects with `used_cents`, `limit_cents`, `resets_at` Unix seconds.
- `window_5h_remaining`, `window_week_remaining`, `window_available_cents`: derived cents.
- `extra_available_cents`: `topup_cents + credit_cents + max(referral_credits - referral_used, 0) * 100`, floored at zero.
- `real_available_cents`: real available balance; forced to 0 when the account is test/sample, has no dashboard CK, subscription expired, pending sync, or has no real balance.
- `subscription_expired` boolean.
- `availability_status` string: `available` or `other`.
- `availability_reason` string: `real_balance_available`, `subscription_expired`, `pending_sync`, `no_real_balance`, `test_account`, or `missing_cookie`.
- `sync_status` string: `ready` or `pending`.
- `dashboard_auth_configured` boolean: whether a dashboard CK is configured; the CK is never returned.
- `is_test_account` boolean: true for demo/test/mock/sample/example-style local accounts.
- `account_kind` string: `real` or `test`.
- `proxy` string: masked proxy URL.

Availability classification is intentionally strict. Test/sample accounts, missing CK, expired subscriptions, pending sync, and accounts with no real balance are excluded from `view=available` even when older raw window fields appear positive.

### GET /sync-status

Purpose: latest dashboard/quota sync result.

Response: `{"success": true, "data": sync_result|null}`. `data` is `null` when the sync worker has not run yet in the current process.

Sync result fields:

- `started_at`, `finished_at` strings: formatted timestamps, empty when unknown.
- `total`, `succeeded`, `failed` integers.
- `items` array: per-account masked sync result. Each item includes `email`, `email_hash`, `status`, and optional sanitized `message`.

### GET /incidents

Purpose: supplier risk and operational incident feed.

Response: `{"success": true, "data": incident[]}`.

Incident fields:

- `id` integer.
- `kind` string: planned values include `auth_expired`, `ip_account_conflict`, `insufficient_quota`, `rate_limited`, `payload_too_large`, `upstream_5xx`, `malformed_response`, `timeout`, `sync_failed`, `proxy_failed`.
- `severity` string: `info`, `warning`, or `critical`.
- `status` string: `open` or `resolved`.
- `status_code` integer: HTTP status when available, 0 otherwise.
- `message` string: sanitized provider/operator message; must not include secrets or prompt/response content.
- `reset_hint` string: optional operator hint.
- `account_email`, `email_hash`: masked account identity when known.
- `model`, `path`: request context when known.
- `request_id`: short hash, never raw upstream id.
- `detected_at`, `resolved_at`: UTC timestamps.
- `resolved` boolean.
- `source` string: currently `sync_worker`; future values include `model_executor` and `manual_probe`.

### GET /usage-snapshots

Purpose: supplier-side quota/usage snapshots for trend inspection.

Response: `{"success": true, "provider":"freemodel", "data": snapshot[]}`.

Snapshot fields include `account_id`, masked identity, `fetched_at`, plan fields, credit/topup/referral fields, 5h/week used and limit, total requests/tokens, derived availability fields, and subscription status. It does not include prompts, responses, cookies, or API keys.

### GET /reconciliation-source

Purpose: future supplier-side source records for RELAYX log reconciliation.

Response fields:

- `success` boolean.
- `provider`: `freemodel`.
- `currency`: `USD`.
- `rounding`: current rule label, `ceil_to_cent_per_request`.
- `data`: reconciliation records.

Record fields:

- `id` integer.
- `upstream_request_id` string: short hash, never raw id.
- `account_email`, `email_hash`: masked identity.
- `created_at` UTC time.
- `method`, `path`, `model` strings.
- `status` integer.
- `tokens_in`, `tokens_out`, `cache_read_tokens`, `cache_write_tokens` integers.
- `raw_subtotal_usd`, `charged_usd` decimals.
- `error_type` string: `auth_error`, `timeout`, `payload_too_large`, `rate_limited`, `upstream_5xx`, `upstream_error`, or empty.

## Import and Export APIs

- `GET /export/accounts`: redacted account export. Never includes cookie or API key.
- `GET /export/quota-snapshots?limit=100`: redacted quota snapshot export. Output includes `redacted:true`.
- `GET /export/incidents?limit=100&include_resolved=false`: redacted incident export. Output includes `redacted:true`.
- `POST /import/accounts`: accepts `{dry_run, validate_only, accounts:[{email,cookie,api_key,proxy}]}`. Output reports created/updated/skipped/errors. Use `dry_run:true` before writes.
- `POST /import/snapshots`: accepts `{dry_run, validate_only, snapshots:[...]}` for local testing only.

Sensitive export with cookies/tokens is intentionally not implemented for RELAYX. If ever added, it must be explicit, local/operator-only, audited, and disabled by default.

## RELAYX Consumption Boundary

RELAYX first stage may read `health`, `accounts`, and `quota`. External operations agents may additionally read `incidents`, `usage-snapshots`, and `reconciliation-source`. RELAYX should not hold FreeModel cookies, sessions, or model API keys, and should not read the plugin database.

`model_executor_enabled` remains experimental and defaults to false. With false, the plugin declares only `management_api`; with true, it may declare model execution capabilities for CPA-side validation only.

## RELAYX 对接边界

RELAYX 第一阶段可读接口：

- `GET /v0/management/freemodel-plugin/health`
- `GET /v0/management/freemodel-plugin/accounts`
- `GET /v0/management/freemodel-plugin/quota`

外部运营 agent 可读接口：

- `GET /v0/management/freemodel-plugin/incidents`
- `GET /v0/management/freemodel-plugin/usage-snapshots`
- `GET /v0/management/freemodel-plugin/reconciliation-source`
- `GET /v0/management/freemodel-plugin/export/accounts`
- `GET /v0/management/freemodel-plugin/export/quota-snapshots`
- `GET /v0/management/freemodel-plugin/export/incidents`

operator-only 能力：

- `POST /v0/management/freemodel-plugin/auth/send-otp`
- `POST /v0/management/freemodel-plugin/auth/verify-otp`
- `POST /v0/management/freemodel-plugin/sync`
- `POST /v0/management/freemodel-plugin/import/accounts`
- `POST /v0/management/freemodel-plugin/import/snapshots`
- `POST /v0/management/freemodel-plugin/snapshots`

实验能力：

- `model_executor_enabled` 默认必须保持 `false`。
- 当 `model_executor_enabled=false` 时，插件只声明 `management_api`。
- 当 `model_executor_enabled=true` 时，插件才声明 `model_provider`、`model_router`、`executor`，仅用于 CPA 侧验证，不阻塞 RELAYX MVP。

永不返回字段：

- FreeModel cookie/session/token/password。
- FreeModel upstream API key 或原始 Authorization header。
- prompt、response content、支付密钥、私钥。
- 含用户名/密码的原始 proxy URL。

RELAYX 不应直接读取 CPA 插件数据库，也不应持有 FreeModel cookie/session/API key。FreeModel 供应商状态由 CPA 插件通过脱敏 Management API 暴露。
