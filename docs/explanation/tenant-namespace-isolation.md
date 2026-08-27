# Per-Tenant Namespace Isolation and Fast Teardown

## Status

Design proposal. This is **L2 — the cache-partitioning layer** of the tenant
isolation series:

| Layer | Concern | Where |
|---|---|---|
| **L0** — origin identity | Per-tenant origin credentials: `sts:AssumeRole` with `ExternalId`, refreshed, held in worker memory only | [Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md) |
| **L1** — data-plane authentication | A signed, short-lived capability makes the declared tenant *verified* rather than *trusted* | [Cache Data-Plane Capability](cache-data-plane-capability.md) |
| **L2** — cache partitioning | Private per-tenant namespace, per-tenant storage shard, fast teardown. **This document.** | Here |

Companion to
[Real-Time Tenant Traffic Observability](tenant-traffic-observability.md),
which observes a tenant's aggregate traffic, and
[Distributed Tenant Cache Quotas](distributed-tenant-cache-quotas.md), which
bounds a tenant's resident *footprint* on the partition this document defines.
`TenantId`, the tenant-scoped wire frames (`GetRangeTenant` /
`GetCachedRangeTenant`), and the worker's per-tenant rate limiter this document
builds on are implemented, not proposed — see
`crates/talon-core/src/tenant.rs`, `crates/talon-transport/src/data.rs`, and
`crates/talon-core/src/rate_limit.rs`.

Scope is the **SDK-native direct data plane** — the native clients and FUSE that
connect straight to a worker. The object-store gateway is explicitly out of
scope; it has its own default-deny `principal → bucket/prefix` authorization and
is not the access path this design targets.

## Problem

A Talon cluster caches many tenants' objects on shared worker NVMe. Today the
cache is **object-addressed and shared across tenants**: the cache key
(`BlockId`) carries no tenant field, storage is one tree keyed by a hash of
that key, and `TenantId` is a quality-of-service attribution only — the tenant
module states outright that a declared tenant is "trusted for fairness, not
treated as a security boundary." Two requirements are unmet as a result:

- **Namespace isolation.** Different tenants must not commingle cached data. One
  tenant's blocks must be *physically* separate from another's, so that no bug
  can serve tenant A's bytes to tenant B, and so that two tenants which reuse the
  same logical path (e.g. both mount `/data/model.bin`) do not collide on a
  single shared cache entry.
- **Fast teardown.** When a tenant is decommissioned, the operator must be able
  to delete *all* of that tenant's cached data quickly — a bounded, near-constant
  operation, not a full scan of every cached block on every worker.

Neither is achievable while the tenant is absent from the cache identity, the
storage layout, and origin resolution.

## Model

Three choices fix the shape of the design. They are recorded here because each
one excludes a materially different implementation.

1. **Boundary = the SDK-declared TenantID, as a trusted partition.** The
   isolation boundary is the `TenantId` the native client declares on each
   request, *not* an object-store bucket/prefix and *not* a gateway principal.
   The direct plane is unauthenticated, so the declared tenant is **trusted**:
   it partitions data and drives lifecycle, but it is not proof of identity.
   Upgrading "trusted" to "verified" is L1's job, not this document's.

2. **Private per-tenant namespace.** The same logical path denotes *different
   data* for different tenants. `tenant=A, /data/model.bin` and
   `tenant=B, /data/model.bin` are two distinct objects. Tenant therefore
   participates in origin addressing, not only in the cache key.

3. **Per-tenant origin: each tenant has its own bucket and credentials.** A
   tenant's backing data lives in its own bucket (possibly a different backend or
   cloud account), reachable only with that tenant's credentials. Resolving a
   request to its origin requires a per-tenant binding, not a shared prefix rule.

One consequence of (2) and (3) worth naming: **credential selection is derived
from addressing, never claimed independently of it.** A client that declares
`tenant=B` is asking for B's namespace, and is served B's data under B's
credentials — data, partition, and identity all move together, so there is no
combination in which one tenant's credentials fetch another tenant's objects.
What a forged declaration *can* do (read B's namespace) is the trusted-partition
risk accepted in (1) and closed by L1.

## Contract

- **Isolation is partition isolation over a trusted tenant.** Tagging every
  cache entry, storage path, and origin fetch with the requesting tenant
  guarantees that a request attributed to tenant A can *structurally* only touch
  A's data — a different worker code path, a different directory subtree, a
  different origin bucket, and different credentials than B's. This prevents
  accidental commingling and same-path collisions, and it is what makes
  per-tenant teardown and per-tenant cache quotas possible. It is **not**
  a defense against a client that forges another tenant's id: on the
  unauthenticated direct plane a caller that declares `tenant=B` is served as B.
  Hardening that boundary is a separate, additive step, and it is specified:
  [Cache Data-Plane Capability](cache-data-plane-capability.md) authenticates
  the declared tenant on the native plane with a signed, short-lived capability.
  This document does not depend on it; landing it upgrades the partition into a
  boundary.

- **Teardown is cache teardown.** Dropping a tenant deletes Talon's cached copies
  of that tenant's blocks and forgets the tenant's origin binding. It does **not**
  delete the tenant's origin objects; the durable store's own lifecycle owns
  those. Talon is a cache, and it holds no authoritative per-tenant object
  inventory to delete against.

- **Fail closed.** A request whose tenant is unknown, disabled, or missing an
  origin binding is rejected. Talon never falls back to a default backend or a
  shared bucket for such a request, because that is exactly how one tenant's read
  would leak into another tenant's data.

## Tenant identity and the origin registry

The logical address a native client sends becomes `(TenantId, path)`. The
physical backend, bucket, prefix, and credentials come entirely from a
per-tenant binding held by the coordinator:

```text
TenantOrigin {
  tenant_id,
  backend,          // s3 | gcs | az
  bucket,           // the tenant's own bucket / container
  prefix,           // optional key prefix within that bucket
  credential_ref,   // indirection to a secret; never the secret itself
  domain_id,        // stable 32-byte identity of this binding; see below
  enabled,
  generation,       // bumped on every change; fences stale readers
}
```

The coordinator is the authoritative source for this registry. It distributes
bindings to workers by watch/push or short-TTL poll; each worker keeps a local, versioned
cache so the hot path never reads the coordinator. `credential_ref` is an
*indirection* — an environment variable name or a Kubernetes secret reference —
resolved by the worker's secret provider at use time; the registry record itself
carries no secret material, and secrets continue to enter the process only from
the environment.

*How* a `credential_ref` becomes a usable per-tenant origin credential — the
`sts:AssumeRole` chain with a mandatory `ExternalId`, refresh, memory-only
handling, and the fail-closed rules on failure — is L0's subject, specified in
[Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md).
This registry is the table; L0 is what a row's identity half means.

`generation` is the fence. A binding change (new bucket, rotated credential,
disable, delete) advances it; teardown and stale-read protection both key on it
(see *Tenant teardown*).

## The cache domain

The unit everything in this series partitions by is the **cache domain**:

> A **domain** is an equivalence class of origin credentials. Two requests
> belong to the same domain if and only if it is safe for bytes fetched under
> either request's authority to be read by the other.

Under this design's model — one bucket, one credential binding per tenant — **a
domain and a tenant coincide**: one `TenantOrigin` entry is one domain. The
finer definition is recorded because it is what survives the model growing: a
tenant that later holds several credential bindings (several buckets, or
prefixes under different roles) owns several domains, one per entry, and its
quota policy covers each of them. The registry entry is the domain's definition
on the worker side; there is exactly one derivation of "which domain is this
request in", shared by origin routing (L0), the cache partition (this
document), capacity accounting (quotas), and authorization (L1).

`domain_id` is the entry's stable identity for the layers that need a compact,
canonical name for it — most importantly L1, whose capability claims name a
domain, and whose per-domain signing keys are provisioned against it. Its
stability contract: **the id is fixed for the life of the binding**. Credential
rotation, a bucket migration, or any other mutation advances `generation` but
does not change `domain_id`; only `DropTenant` retires it. Deriving the id from
anything that rotates (an STS access-key id) or from a hash function that is
allowed to change would churn the id — and because the id qualifies the storage
path, churning it silently cold-starts the entire distributed cache. The
control plane provisions the same value to the capability issuer's
configuration and to this registry, so the two sides agree by provisioning
rather than by parallel hashing.

## Placement

Clients select a worker with the existing deterministic Maglev table, extended to
key on the tenant:

```text
placement_hash = H(tenant_id, path, offset, block_size)
```

A client needs no knowledge of the physical origin binding to place a request —
only the tenant it is acting as and the logical path. Two tenants that share a
logical path hash to independent keys and therefore spread across the fleet
instead of colliding on one worker. Because the placement input changes, the C,
Java, and Python clients must fold the tenant into the hash identically; Talon
already requires cross-language Maglev determinism, so this extends an existing
invariant rather than introducing a new one.

## Worker storage layout

Each worker shards its cache by tenant:

```text
cache_root/
  <tenant>/
    <shard>/<digest>.blk           # whole blocks
    <shard>/<digest>.pages/<n>.page # paged blocks
```

State is a `HashMap<TenantId, TenantShard>`, where a `TenantShard` owns that
tenant's block index and its `cache_root/<tenant>/` subtree. Lookup, insertion,
and page tracking all descend through the tenant shard first, so a request tagged
`A` cannot even name a block file under `B`'s subtree.

**Why the tenant wraps `BlockId` instead of living inside it.** `BlockId`
(`crates/talon-core/src/key.rs:171`) is a *cross-process* encoding — it crosses
the control plane as bincode in `ControlMessage::PlacementLookup`
(`crates/talon-transport/src/codec.rs:52`). Changing its layout would require a
`CONTROL_SCHEMA_VERSION` bump, and an old peer's `PlacementLookup` still
declares the minimum schema, so a new peer would accept and misinterpret it;
closing that forbids rolling upgrade. The shard map sidesteps all of it: the
tenant dimension lives in worker-local state — the map key, the directory, the
eviction tag — and `BlockId`, the placement messages, and the control schema are
untouched. `HashMap<TenantId, TenantShard{index}>` is the same structure as a
tenant-qualified cache key, held one level up.

**Adopting this layout cold-starts the cache once.** Existing blocks live at
untenanted paths the new scan does not index, so the upgrade is a silent cold
start rather than a crash — and because old `.blk` files are absent from the
rebuilt index, nothing ever evicts them. The upgrade procedure must explicitly
wipe the cache root.

**Disk amplification is accepted.** Under a private namespace two tenants'
identical logical paths are different objects, so there is nothing to
deduplicate; N tenants reading what happens to be byte-identical content store
it N times. Inherent to partition isolation, not an implementation artifact.

Eviction keeps a **single global byte budget** with the LRU *policy* aware of
tenants but the *budget* shared. This is deliberate and measured: sharding the
eviction policy is free, but giving each shard its own budget wastes capacity and
costs up to several points of hit rate when the working set is near capacity
(see the *Design* doc's worker-storage section and issue #273). Each cache unit
carries its tenant so an evicted unit is unlinked from the correct subtree; the
coldest-first victim selection still ranges over all tenants under one budget.
A per-tenant *bound* on top of this shared budget is the quota design's job,
opt-in per tenant, not a return to per-shard budgets.

## Read path

A read carries its tenant on the existing tenant-scoped frames
(`GetRangeTenant` / `GetCachedRangeTenant`, `crates/talon-transport/src/data.rs`).
The tenant, already validated and rate-limited at admission, is now also threaded past
admission into serving: the worker resolves the block within `tenants[tenant]`'s
shard and serves it with the same zero-copy `sendfile` path as today. Nothing
about the transfer changes; only the index and file it resolves against are
tenant-scoped.

## Miss path

A miss is where origin resolution and per-tenant credentials enter:

1. The worker looks up the tenant's `TenantOrigin` in its local registry cache.
   Unknown, disabled, or unbound ⇒ reject (fail closed); never fetch from a
   default or shared location.
2. It composes the physical object id from the binding:
   `backend/bucket/(prefix + path)`, and captures the binding `generation`.
3. It fetches the range with a **per-tenant backend client** whose credentials
   are resolved from `credential_ref` (per
   [Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md)).
   Backend clients are cached per tenant and bounded in count (mirroring the
   rate limiter's `MAX_TENANT_CELLS` guard), so a flood of distinct tenants
   cannot exhaust worker memory on the unauthenticated plane.
4. On completion it commits the block into `cache_root/<tenant>/…` and records
   the source version/etag guard so a later origin update invalidates the block,
   exactly as today.

Origin backend HTTP stays off the io_uring data plane, on the loader pool, as it
is now.

### Miss coalescing must be tenant-qualified

This is the one place the partition can silently leak if forgotten. The miss
path coalesces concurrent requests for the same block: `load_block_range`
(`crates/talon-worker/src/runtime.rs:1175`) admits on `LoadKey::Whole/Page`
(`crates/talon-worker/src/miss.rs:33`), elects one **leader** that performs the
single origin fetch under *its own* resolution, and parks followers on the
result. If `LoadKey` were still derived from the untenanted `BlockId`, two
tenants whose logical paths collide would coalesce into one cohort — the
follower would receive bytes fetched from the *leader's* bucket under the
*leader's* credentials, actively retrieved on its behalf.

`LoadKey` therefore includes the tenant, like every other cache identity in
this design. A cohort is single-tenant by construction, and the leader's
binding `generation` is the cohort's: a registry change mid-flight fails the
cohort rather than committing bytes under a retired binding.

## Write and delete path

`PUT` and `DELETE` gain tenant-scoped variants, following the same discipline
the tenant-scoped reads above used: a distinct opcode that old workers reject
(fail-closed) rather than silently mishandling. A tenant-scoped `PUT` stages and
commits into the tenant's subtree and, when write-through is enabled, writes to
the tenant's origin bucket with the tenant's credentials. A tenant-scoped
single-object `DELETE` removes that one object's cached block within the tenant's
shard and, for write-through, deletes it at the tenant's origin.

## Tenant teardown

`DropTenant(tenant)` is the fast decommission primitive:

1. **Coordinator** removes the tenant's `TenantOrigin` from the registry, so
   every subsequent miss for it fails closed, then fans the drop out to all
   workers with the retiring `generation`.
2. **Each worker** removes the `TenantShard` from its map (dropping the whole
   in-memory sub-index at once), subtracts the shard's resident bytes from the
   global eviction budget, clears its L1 pages, and reclaims disk by renaming
   `cache_root/<tenant>` into a trash directory and unlinking it in the
   background. It also drops the tenant's cached backend client and resolved
   credentials.

The worker returns as soon as the rename completes; the recursive unlink runs off
the hot path. Cost is proportional to the departing tenant's own footprint, not
to the whole cache — there is no scan of other tenants' blocks. The `generation`
fence makes the drop safe against races: a late in-flight miss from a lagging
worker cannot resurrect a dropped tenant's subtree, because its binding
generation is stale and rejected.

In-flight reads for the departing tenant are handled like eviction of a pinned
unit: the rename does not disturb already-open file descriptors, so active
transfers drain and new opens simply fail once the subtree is gone.

**With L1 deployed, teardown completes the story access-side too.** `DropTenant`
is paired with `RevokeCapabilityKeys` for the tenant's domain: deleting the
domain's signing key severs every outstanding capability sub-second, while the
shard drop removes the bytes. Revocation without deletion leaves plaintext on
disk; deletion without revocation leaves a window where a bound connection can
still read — issuing both from the same coordinator action closes both ends.

## Isolation guarantees and threat model

- **Guaranteed:** a request attributed to tenant A can only ever resolve against
  A's shard, A's subtree, A's origin bucket, and A's credentials. Accidental
  cross-tenant serving is structurally impossible — files, index, and credentials
  are three independent partitions, so no single bug bridges two tenants.
  Same-path collisions between tenants cannot occur, including under miss
  coalescing.
- **Not guaranteed here:** protection against a caller that deliberately
  declares another tenant's id. The direct plane is unauthenticated by decision;
  the declared tenant is trusted. A forged `tenant=B` is served B's data if the
  caller also knows B's paths. Closing this is
  [Cache Data-Plane Capability](cache-data-plane-capability.md)'s boundary: a
  capability binds the connection to a verified tenant and domain, and a
  declared tenant that differs from the bound one is rejected.

## Capacity and eviction

Per-tenant partitioning removes cross-tenant deduplication, but under a private
namespace there is nothing to deduplicate: two tenants' identical logical paths
are genuinely different objects. Capacity is still a single global budget shared
across tenants; a hot tenant and a cold tenant compete under one LRU as they do
today. Per-tenant cache *quotas* — bounding how much of that budget any one
tenant may hold — are a natural follow-on and are specified by
[Distributed Tenant Cache Quotas](distributed-tenant-cache-quotas.md), which
accounts by the same domain this document partitions by.

## Failure semantics

| Event | Behaviour |
|---|---|
| Request declares an unknown / disabled tenant | Rejected at admission; no origin fetch, no default fallback. |
| Tenant has no origin binding yet | Miss fails closed until the binding is distributed; cache hits (if any) still serve. |
| Credential resolution fails on a miss | The miss fails; no fallback credential or bucket is tried (see L0's fail-closed rule). |
| Registry update in flight (generation bump) | Workers admit under the last known binding until the new one arrives; a stale generation is fenced on teardown. |
| `DropTenant` races an in-flight miss | The miss's stale binding generation is rejected; it cannot recreate the dropped subtree. |
| Coordinator unreachable | Workers serve from their cached registry and last-known bindings; new tenants and drops wait rather than adopting a permissive default. |
| Worker restart | Per-tenant subtrees on disk are re-adopted by tenant on the startup scan; the registry cache is refetched from the coordinator. |

## Delivery plan

Staged as independent upstream PRs, each reviewable on its own, in dependency
order:

1. `talon-core`: `TenantOrigin` record, tenant-scoped identity types, and the
   registry trait.
2. Coordinator: registry store, distribution (watch/poll), and the
   `PutTenant` / `UpdateTenant` / `DropTenant` management API.
3. Worker: per-tenant cache shard (index + subtree) under the global eviction
   budget; thread the tenant through the read/serve path **and the coalescing
   `LoadKey`**; document the cache-root wipe in the upgrade note.
4. Worker miss path: registry resolution, per-tenant backend clients and
   credential resolution (the `AssumeRole` fetcher and fail-closed rules from
   [Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md)),
   bounded client count.
5. `DropTenant` fan-out and fast teardown (rename + background unlink +
   generation fence).
6. Tenant-scoped `PUT` / `DELETE` opcodes and the tenant-scoped FUSE mount /
   native-client tenant configuration.
7. Placement: fold the tenant into the Maglev hash across the C, Java, and Python
   clients.
8. Observability: per-tenant residency and hit/miss, reusing the tenant traffic
   monitoring surface.

L1 ([Cache Data-Plane Capability](cache-data-plane-capability.md)) is
independent of every stage here and can land before, between, or after them;
stage 5 gains the `RevokeCapabilityKeys` pairing once both exist.

## Non-goals

- Authenticating the tenant on the direct plane. The trusted-partition model is
  a decision; authentication is specified separately by
  [Cache Data-Plane Capability](cache-data-plane-capability.md) and is additive.
- Deleting a tenant's origin objects. Teardown is cache-scoped; the durable
  store owns object lifecycle.
- Cross-tenant deduplication. It is meaningless under a private namespace.
- Per-tenant cache quotas / capacity fairness. Specified by
  [Distributed Tenant Cache Quotas](distributed-tenant-cache-quotas.md); this
  document isolates and tears down, it does not bound footprint.
- Gateway-mediated access. The gateway keeps its own principal/bucket/prefix
  authorization; this design is the native direct plane.

## References

- [Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md) —
  L0: what a binding's credential half means (`AssumeRole` + `ExternalId`,
  refresh, fail-closed).
- [Cache Data-Plane Capability](cache-data-plane-capability.md) — L1: the
  native-plane tenant authentication this design's threat model defers to.
- [Distributed Tenant Cache Quotas](distributed-tenant-cache-quotas.md) — the
  capacity bound on the partition defined here.
- [Real-Time Tenant Traffic Observability](tenant-traffic-observability.md) —
  the per-tenant signal and management-API surface this design reuses for
  per-tenant residency visibility.
- The v1 *Design* document (`DESIGN.md`) — worker storage, eviction, and the
  measured decision to shard the eviction policy but not the budget.
