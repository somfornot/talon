# Worker Per-Tenant Origin Identity

## Status

Implementation proposal. Not implemented.

It specifies **L0** — the origin-credential half of the tenant isolation
series: how a worker turns a tenant's registry binding into usable,
tenant-scoped cloud credentials, and what happens when it cannot. The registry
itself — the `TenantOrigin` table, its coordinator distribution, and the
private-namespace addressing model it serves — is **L2**'s subject
([Per-Tenant Namespace Isolation](tenant-namespace-isolation.md)); this
document is what a binding's identity half *means*. **L1**
([Cache Data-Plane Capability](cache-data-plane-capability.md)) authenticates
the declared tenant and is **not a prerequisite** for anything here.

## Summary

A worker today holds exactly one origin identity, built once at startup from
environment variables, and uses it for every tenant's data:

```rust
// crates/talon-worker/src/main.rs:575
let backend: Arc<dyn BackendStore> = match backend_kind {
    "azure" => Arc::new(build_azure_backend(&cfg, http, credentials_observer).await?),
    "s3"    => Arc::new(build_s3_backend(&cfg, http, credentials_observer).await?),
    "gcs"   => Arc::new(build_gcs_backend(&cfg, http, credentials_observer).await?),
    ...
};
```

That single `Arc<dyn BackendStore>` lands in `WorkerRuntime.backend`
(`crates/talon-worker/src/runtime.rs:89`) and is the sole origin path for the
whole process. One credential reads every customer's bucket.

This proposal replaces it, for tenant-scoped requests, with **per-tenant
backend clients whose credentials are resolved from the tenant's registry
binding** — and adds the credential fetcher that resolution needs:
`sts:AssumeRole` with an `ExternalId`, which does not exist in the codebase
today.

## The principle: credentials follow addressing

One rule keeps L0 from becoming a privilege escalation: **credential selection
must be derived from the same registry binding that resolves the request's
physical location — never chosen independently of the data.**

The warning case, recorded because it is easy to reintroduce: in an
object-addressed model — where the client names the physical bucket and path
directly — keying credential selection on the client-declared
`TenantScopedRange.tenant` (`crates/talon-transport/src/data.rs:87`, which is
shape-validated only, `crates/talon-core/src/tenant.rs:37,84`) would let a
client pair *its own* choice of object with *another tenant's* fetching
identity. That is a capability that does not exist today, because today the
identity is not selectable at all.

The private-namespace model (L2) dissolves this by construction. The logical
address is `(tenant, path)`; the binding derived from the tenant supplies the
bucket, the prefix, **and** the credentials as one unit. There is no
combination in which one tenant's credentials fetch another tenant's objects,
because "which object" and "whose credentials" are the same lookup. What a
forged tenant declaration can still do — read that tenant's namespace — is the
trusted-partition risk L2 documents and L1 closes; it is not a credential
mis-pairing.

`TenantId` otherwise keeps its current job — a QoS label for the rate limiter
(`crates/talon-core/src/rate_limit.rs:159`) — and becomes a verified identity
under L1.

## What L0 does and does not buy

Being precise here matters, because L0 is easy to oversell.

**It does not improve client-facing isolation.** On the unauthenticated direct
plane, any client that declares a tenant is served that tenant's data. That
hole is L1's, and L1 is where it closes.

**It does buy four things that L1 cannot:**

1. **Blast radius.** No single credential in the fleet can read across
   customers. A misconfigured or leaked tenant role is bounded to that tenant.
2. **Customer-controlled revocation.** A customer revokes Talon's access to
   their own data by editing their own role's trust policy, with no coordination
   with Talon and no effect on any other customer. Today revocation means
   changing the one shared identity, which affects everyone.
3. **Attribution.** CloudTrail (and the equivalents) show which role read what,
   so origin-side audit is per tenant rather than per cluster.
4. **The compliance answer.** "The cache service does not hold a credential
   capable of reading across customers" becomes true, and it is the claim the
   whole programme started from.

## What exists today

**A credential abstraction that already fits.** `ProvideS3Credentials`
(`crates/talon-backend/src/credentials.rs:19`), `CredentialsFetch` (`:83`),
`RefreshingCredentials` with `bootstrap` (`:142,152`) and a `RefreshPolicy`
(`:99`), and a `CredentialsObserver` (`:65`) already wired to worker metrics via
`CredentialsMetricsObserver` (`main.rs:571`). Background refresh, expiry
tracking and observability are **already solved**; they are simply
instantiated once.

**Workload-identity fetchers for machine identity.**
`crates/talon-backend/src/workload_identity.rs` implements `AwsWebIdentity`
(`:165`, `sts:AssumeRoleWithWebIdentity`), `AliyunOidc` (`:250`,
`sts:AssumeRoleWithOIDC`), `TencentOidc` (`:339`), `HuaweiAgency` (`:445`),
`GcpMetadataToken` (`:502`), `AzureWorkloadIdentity` (`:558`), dispatched by
`resolve_s3_credentials_with_env` (`:666`).

**SigV4 with a parameterised service.** `sign_request(req, creds, region,
service, date, …)` (`crates/talon-backend/src/sigv4.rs:208`) takes the service
name as an argument, and its own tests already sign with `iam` (`:395`). Signing
an `sts` request needs no change to it.

**A backend that takes an injected provider.**
`S3Backend::with_credentials_provider(config, provider, http)`
(`main.rs:303`) — so a second, third, Nth backend instance costs nothing
structurally.

**Also present, but on no roadmap:** a request-scoped credential surface —
`S3PresignedQuery` / `AzureSas` (`crates/talon-backend/src/query_auth.rs`), the
`execute_presigned_*_raw` / `execute_sas_*_raw` backend executors, their
gateway `S3Origin`/`AzureOrigin` delegations, and the stubbed
`OriginAuthMode::TrustedPassthrough` config arm. It was built for a presigned
passthrough design that was rejected in favour of this layering. Nothing
constructs those credentials in production; a follow-up should either remove
the surface or explicitly reserve it for a future gateway trusted-passthrough
mode.

## What is missing

Four gaps, in descending order of size.

**1. There is no `sts:AssumeRole` fetcher, and no `ExternalId` anywhere.**
`grep -rn 'ExternalId\|external_id' crates/talon-backend/src/` returns nothing.
Every existing fetcher converts a *machine identity* into credentials in one
step. L0 needs the second link of a chain: base credentials →
`sts:AssumeRole(RoleArn, ExternalId, RoleSessionName)` → tenant credentials.
Unlike `AssumeRoleWithWebIdentity`, which is an anonymous POST, this call is
SigV4-signed with the base credentials.

`ExternalId` is not optional decoration. It is the documented defence against
the confused-deputy problem for exactly this topology — one provider principal
trusted by many customer roles (AWS Well-Architected SEC03-BP09). Without it,
any Talon operator who learns a customer's role ARN can assume it.

**2. One global backend, built from the environment.** `main.rs:566-569` states
the current model in its own comment: "Each backend reads its endpoint from
config and its credentials from the environment only". Environment variables
cannot express O(tenants) configuration — which is why the binding table is the
coordinator-distributed registry of L2, with `credential_ref` as the
indirection through which secrets still enter only from the environment or a
mounted secret.

**3. One backend *kind* per worker.** `ensure_configured_backend`
(`runtime.rs:239`) rejects a request whose backend differs from the process's
configured kind. Tenants spanning S3 and Azure on one worker are therefore out
of reach until that constraint is revisited.

**4. No fail-closed story for a revoked tenant.** There is nothing to fail
closed *from* today. Once there is, the behaviour on `AssumeRole` failure is the
single most consequential rule in this design — see below.

## Design

### The identity half of a binding

L2's `TenantOrigin` carries the addressing half (backend, bucket, prefix) and a
`credential_ref`. This document defines what the ref resolves to:

```rust
// crates/talon-backend/src/origin_identity.rs (new)

/// How a tenant binding's credentials are obtained.
pub enum OriginIdentity {
    /// The worker's own machine identity assumes a per-tenant role.
    AssumeRole {
        role_arn: String,
        /// Anti-confused-deputy shared secret, resolved via `credential_ref`.
        /// Required, not optional.
        external_id: SecretRef,
        session_name: String,
        duration_seconds: u32,
        region: String,
    },
    /// Static keys, resolved via `credential_ref`. Discouraged; supported.
    StaticKeys { access_key: SecretRef, secret_key: SecretRef },
}
```

Non-secret fields (`role_arn`, region, durations) travel in the registry
record; every secret is a `SecretRef` resolved by the worker's secret provider
at use time, so the registry carries no secret material (L2's rule).

Every type here needs a manual `Debug` that redacts — including on error
paths, where a "failed to parse binding: {entry:?}" is the usual way a secret
reaches a log aggregator. The precedent is
`crates/talon-backend/src/query_auth.rs`.

### `AssumeRoleFetch`

```rust
// crates/talon-backend/src/assume_role.rs (new)

/// Chains `sts:AssumeRole` onto a base credential provider, so a worker's own
/// workload identity can be exchanged for a per-tenant role.
pub struct AssumeRoleFetch {
    base: Arc<dyn ProvideS3Credentials>,
    role_arn: String,
    /// Anti-confused-deputy shared secret. Required, not optional.
    external_id: String,
    session_name: String,
    duration_seconds: u32,
    sts_endpoint: String,
    region: String,
    http: Arc<dyn HttpClient>,
}

impl CredentialsFetch for AssumeRoleFetch { /* … */ }
```

Implementation notes that are easy to get wrong:

- The POST is `Action=AssumeRole&Version=2011-06-15` with `RoleArn`,
  `RoleSessionName`, `ExternalId`, `DurationSeconds`, signed via
  `sigv4::sign_request(.., service = "sts", ..)`. The `AwsWebIdentity`
  implementation (`workload_identity.rs:200-230`) is the closest template for
  the request/response shape; the difference is that this one is signed.
- The **base** provider is whatever `resolve_s3_credentials` already returns for
  the worker's own machine identity. `AssumeRoleFetch` composes with it rather
  than replacing it, so IRSA / RRSA / TKE all keep working underneath.
- `RoleSessionName` must be stable and identifying — worker id plus tenant name
  — because it is what shows up in the customer's CloudTrail. A random session
  name destroys the attribution benefit that is one of L0's four reasons to
  exist.
- `DurationSeconds` is capped by the target role's `MaxSessionDuration`
  (default 3600s). Requesting more fails the call rather than being clamped, so
  the configured value must be validated against reality at bootstrap, not
  discovered at the first refresh.
- Wrapped in `RefreshingCredentials::bootstrap` (`credentials.rs:152`) with the
  existing `RefreshPolicy` and the existing metrics observer. One cell per
  binding.

### Wiring: per-tenant clients on the tenant-scoped paths

L2's stage 3 threads the tenant past admission into serving; this document's
wiring (L2's stage 4) makes the origin-facing call sites use the tenant's
client. The runtime's backend call sites, for reference — each takes the
binding's client instead of the process-wide `self.backend` when the request is
tenant-scoped:

| Line | Call | Enclosing function |
|---|---|---|
| `runtime.rs:632` | `list_objects(bucket, …)` | `list_objects` (`:606`) |
| `runtime.rs:681` | `head(object)` | `stat_object` (`:677`) |
| `runtime.rs:711` | `head(object)` | `resolve_version` (`:703`) |
| `runtime.rs:1043` | `fetch_range_if_match(&request.object, …)` | `fetch_and_commit_pages` |
| `runtime.rs:1164` | `head(object)` | `block_len` |
| `runtime.rs:1406` | `fetch_range_if_match(&request.object, …)` | `fetch_and_commit` |

Plus the write path: `put` (`:1545`), `put_file` (`:1610`), `delete` (`:1646`).

Per-tenant clients are cached and bounded in count (L2 mirrors the rate
limiter's `MAX_TENANT_CELLS` guard), and each holds its own
`RefreshingCredentials` cell.

### Registry updates without an STS stampede

On a registry update (coordinator push or poll), the worker builds the new
binding set side-by-side and swaps it under an `ArcSwap`. Bindings whose
identity configuration is unchanged **must keep their existing
`RefreshingCredentials` cell** rather than re-bootstrapping — otherwise every
unrelated registry edit causes an STS stampede proportional to tenant count. A
failed update keeps the previous set serving and raises a metric; it must not
take the worker down, and it must not partially apply. (These semantics are
mechanism-independent: they hold for coordinator push exactly as they would for
a mounted file.)

### Fail-closed

**A tenant whose credentials cannot be obtained must fail, never fall back.**

If acme's `AssumeRole` starts failing because acme revoked the trust policy,
fetches for acme's namespace must return an error. Falling back to the process
identity would silently undo the customer's revocation — the exact scenario
this whole programme was started to fix — and it would do so invisibly, since
the reads would keep succeeding. This is the credential half of L2's contract
("Talon never falls back to a default backend or a shared bucket").

This deserves its own error variant rather than a generic `Backend(String)`, so
it is greppable in logs and distinguishable in metrics from an origin 5xx.

**Legacy, non-tenant-scoped requests** (plain `GetRange` with no declared
tenant) are the separate question. Two modes:

- `legacy` (default, initially): serve them with the process-wide identity,
  i.e. today's behaviour, with a warning-rate metric so the remaining
  un-migrated traffic is visible.
- `strict`: reject. The end state.

The `legacy` mode must have a deletion plan for the same reason L1's `prefer`
does — a permanent fallback is a permanent hole. Note what is *not* configurable:
a **tenant-scoped** request never falls back, in either mode.

## Interaction with L1 and L2

**With L2:** the registry binding *is* the cache domain
([Per-Tenant Namespace Isolation](tenant-namespace-isolation.md), *The cache
domain*): one entry, one credential configuration, one partition, one
`domain_id`. Origin routing, cache partitioning, and quota accounting all read
the same lookup, so they cannot disagree. The two layers should land close
together — L0 without L2 means tenants share cache entries the origin no longer
shares credentials for; L2 without L0 means the cache is partitioned by a
domain that no origin identity corresponds to. Neither is wrong, both are half
a feature.

**With L1:** unchanged by this document. L1 is what makes the declared tenant
verified — until it lands, per-tenant credentials bound the *blast radius*, not
the *reader*. L1's per-domain signing keys are provisioned against the same
`domain_id` the binding carries, and `DropTenant` pairs the shard wipe with
`RevokeCapabilityKeys` once both exist.

## Implementation plan

| PR | Scope | Risk |
|---|---|---|
| **P1** | `AssumeRoleFetch` in `talon-backend`: signed STS call, `ExternalId`, response parsing, `CredentialsFetch` impl, unit tests against a mock `HttpClient` (the `workload_identity.rs` tests are the template). Pure library, no worker change. | Low |
| **P2** | `OriginIdentity` / `SecretRef` types + redacting `Debug` + secret-provider resolution. Pure library. Lands with L2's stage 1 (`TenantOrigin` types). | Low |
| **P3** | Wire it: per-tenant client cache keyed by binding, the call sites above take the binding's client on tenant-scoped requests, `legacy` mode default-on. This is L2's stage 4. | **Medium — touches the read path.** |
| **P4** | Update semantics: side-by-side rebuild, `ArcSwap`, cell reuse for unchanged bindings, update-failure metric. | Medium |
| **P5** | The distinct credential-failure error variant, per-binding metrics, `strict` mode, and the runbook. | Low |
| **P6** | Flip `strict`; delete `legacy`. | Low |

Follow-ups deliberately excluded: relaxing `ensure_configured_backend` so one
worker can serve several backend kinds; per-tenant Azure and GCS identities
(P1 is S3-only — the Azure and GCS equivalents are separate fetchers with the
same shape).

### Test obligations

- `AssumeRoleFetch` sends `ExternalId`, and a fetcher constructed without one
  fails to build rather than omitting it.
- The STS request is SigV4-signed with the base credentials and
  `service = "sts"`.
- A `DurationSeconds` exceeding the role's `MaxSessionDuration` fails at
  bootstrap, not at first refresh.
- A `Debug` of every identity/config type does not contain the `external_id`
  or any resolved secret. Assert on the rendered string; this is the test that
  actually prevents the leak.
- Credential failure for a bound tenant returns the distinct error and **does
  not** fall back, under both modes.
- `strict` rejects a legacy request; `legacy` serves it and increments the
  metric. A tenant-scoped request with a missing binding is rejected in both.
- A registry update with one binding changed re-bootstraps exactly one cell and
  leaves the others untouched — assert on the bootstrap count, since this is
  the STS stampede guard.
- A failed registry update leaves the previous bindings serving.

### Observability

Per binding: credential age, time-to-expiry, refresh success/failure counters,
`AssumeRole` latency. The existing `CredentialsObserver` (`credentials.rs:65`)
and `CredentialsMetricsObserver` (`main.rs:571`) already carry this shape; they
need a tenant dimension added rather than a new mechanism.

Worker-wide: binding count, legacy-request rate, update success/failure, and
last-successful-update timestamp. The legacy-request rate is the metric that
tells an operator whether `strict` can be turned on.

## Security boundary

**Provided:** no credential in the fleet reads across customers; a customer can
revoke Talon's access to their own data unilaterally and it takes effect at the
next refresh (bounded by `duration_seconds`), with in-flight credentials
remaining valid until then; origin-side audit attributes reads to a per-tenant
role; confused-deputy is closed by a mandatory `ExternalId`.

**Not provided:** anything about who may read the cache. On the unauthenticated
direct plane a client that declares a tenant is served that tenant's data.
**This is L1's boundary and it is not addressed here** — which is fine, and is
why the two are separate documents, but it must not be misread as "L0 makes the
cache multi-tenant-safe".

**Not provided:** protection of already-cached bytes. Revoking a role stops
future origin fetches; it does nothing to blocks already on disk. Deleting them
is L2's `DropTenant`; making them cryptographically unreadable would be
per-domain envelope encryption, deliberately deferred.

## Open questions

1. ~~**Tenant granularity of the binding.**~~ Resolved by L2's model: one
   binding per tenant; a tenant that later needs several bindings owns several
   domains.
2. **STS call rate at scale.** One `RefreshingCredentials` cell per binding,
   each refreshing on its own schedule. At N bindings and a 3600s duration this
   is N/3600 calls per second per worker, times the fleet — against the
   customer's STS quota, not Talon's. Needs a number before P4, and probably
   needs refresh jitter to avoid fleet-synchronised bursts after a rolling
   restart.
3. **Does a worker need every tenant's credentials, or only those it holds
   blocks for?** Placement means a worker only ever fetches objects it owns
   blocks for. Bootstrapping all N bindings eagerly at startup is simple but
   wasteful and slows startup; lazy bootstrap on first miss is cheaper but moves
   an STS call onto the read path's tail latency. Recommendation: lazy with an
   eager warm-up for tenants seen in the shard scan at startup — but this needs
   deciding in P3, not after.
4. **Azure and GCS.** P1 is S3-only. Azure's equivalent is a multi-tenant app
   with a service principal per customer tenant; GCS's is service-account
   impersonation. Both fit `OriginIdentity` as additional variants, but
   neither is specified.
5. **Whether `ensure_configured_backend` should be relaxed** (gap 3). Keeping it
   means a homogeneous worker fleet per cloud — a tenant's binding then also
   selects which fleet serves it. That may simply be the right deployment
   model; worth confirming rather than assuming.
