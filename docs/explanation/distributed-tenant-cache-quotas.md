# Distributed Tenant Cache Quotas

## Status

Draft design proposal.

This is the **capacity** facet of the tenant isolation series. It bounds how
much resident cache a tenant may hold on the partition defined by
[Per-Tenant Namespace Isolation](tenant-namespace-isolation.md) (L2), whose
baseline is a *single shared* eviction budget with tenant-tagged units — this
document is the opt-in bound on top of that shared budget, not a return to
per-shard budgets. The identity it accounts by — the cache domain — is defined
there; the authentication that makes any per-tenant guarantee enforceable
against a hostile client on the direct data plane is
[Cache Data-Plane Capability](cache-data-plane-capability.md) (L1) and is a
**dependency of this document's isolation guarantee, not an assumption it can
make today**: until L1 lands, the declared tenant is a trusted partition key,
and a client that forges another tenant's id consumes that tenant's quota.

## Summary

Talon needs cache-capacity isolation between tenants without placing a
coordinator, database, or quota service on the cache-fill hot path. This
document defines a distributed, safety-first quota model:

- a tenant's global cache maximum is represented by a fixed quantity of
  transferable **rights**;
- a worker can admit cache data only while its local resident and reserved
  bytes fit within the rights it currently holds;
- workers transfer unused rights directly to one another using a durable,
  idempotent peer protocol;
- a coordinator supplies policy, membership epochs, deterministic target
  distributions, and optional donor discovery, but is never the authority for
  individual cache fills or evictions.

The design deliberately chooses quota safety over availability during a worker
failure or network partition. Rights held by an unreachable worker are frozen
until they are safely returned or reclaimed through an explicit fencing
protocol.

## Goals

- Enforce a global, per-tenant hard maximum for L1 and L2 resident cache
  capacity across all active workers.
- Prevent one tenant from evicting another tenant's protected local cache
  working set.
- Keep cache-hit, cache-fill, eviction, and data transfer decisions local to a
  worker.
- Allow idle quota capacity to move to a hot worker without a central quota
  allocator.
- Preserve Talon's ordinary-object design: no durable per-object tenant
  metadata is stored in a coordinator or external metadata service.
- Reuse the existing coordinator membership/control-plane design only for
  bounded, low-frequency work.

## Non-goals

- Guarantee that a tenant always has useful data occupying its quota. A cache
  minimum is an eviction-protection policy, not an automatic prewarm promise.
- Immediately reclaim rights from an unreachable worker without a fencing
  guarantee.
- Account for physical NVMe IOPS per tenant. This proposal controls resident
  cache capacity; IO QoS is defined separately.
- Share private cached blocks between tenants in the initial implementation.

## Terminology

| Term | Meaning |
|---|---|
| Tenant | The resource owner named by `TenantContext`. On the direct data plane this identity is *trusted* (a partition key) until [Cache Data-Plane Capability](cache-data-plane-capability.md) authenticates it. |
| Cache domain | The cache-isolation and accounting identity, defined in [Per-Tenant Namespace Isolation](tenant-namespace-isolation.md): an equivalence class of origin credentials — one per tenant binding, so initially one domain equals one tenant. |
| Right | One byte of a domain's globally limited cache capacity, assigned to one worker. Rights are transferable but cannot be duplicated. |
| Resident bytes | Fully committed local cache bytes belonging to a domain. |
| Reserved bytes | Bytes approved for a cache fill but not yet committed. Reservations count against quota. |
| Target | A desired, elastic cache share. Capacity above it is preferentially reclaimed. |
| Minimum | An eviction protection floor. It is not a promise that the cache is populated. |
| Maximum | The hard capacity ceiling. The sum of all worker rights for the domain must not exceed it. |

## Invariants

For every `(cache_domain, tier)`:

```text
sum(worker.rights) <= policy.global_max

worker.resident + worker.reserved <= worker.rights

sum(worker.resident) <= policy.global_max
```

For every worker and cache tier:

```text
sum(domain.resident) + system_overhead <= worker.physical_capacity
```

An admission may fail because either invariant cannot be satisfied. Failure to
admit data to cache must not make a read unavailable: the worker or gateway
uses the configured origin-bypass path instead.

## Architecture

```text
                    Policy + membership epoch
               +--------------------------------+
               | Active-active Talon coordinator |
               | target plan and donor hints     |
               +--------------------------------+
                     ^                    ^
                     | low-frequency       | low-frequency
                     | summaries            | policy / membership
                     v                    v
     +-------------------+    direct   +-------------------+
     | Worker A          | <---------> | Worker B          |
     | TenantPool A/B... | rights      | TenantPool A/B... |
     | local LRU         | transfers   | local LRU         |
     +-------------------+             +-------------------+
```

The coordinator is not a quota leader. It must not receive a request for every
cache fill, hold a global resident-byte counter, select every eviction victim,
or proxy cache data.

## Policy

The policy is versioned and distributed as bounded control-plane state:

```toml
[cache_domains.tenant-a.l2]
global_max_bytes = 1099511627776 # 1 TiB
global_target_bytes = 805306368000
global_min_bytes = 429496729600
rebalance_threshold_bytes = 8589934592

[cache_domains.tenant-a.l1]
global_max_bytes = 34359738368
global_target_bytes = 17179869184
global_min_bytes = 8589934592
```

Policy changes create a `QuotaEpoch` derived from the policy version and the
canonical healthy-worker membership version. A policy increase can mint new
rights under the new epoch. A decrease enters a draining state: workers stop
admitting above the reduced target, evict or transfer excess rights, and
acknowledge completion before the lower hard maximum is considered active.

## Local tenant pools

Each worker implements a CacheLib-style pool for every active cache domain and
tier. CacheLib's `MemoryPool` provides the relevant local model: allocations
are made from an explicitly selected bounded pool; pools may be resized; and
background work physically releases capacity after a resize. Talon applies the
same model to L1 pages and L2 cache units rather than CacheLib slabs.

```rust
struct TenantPool {
    domain: CacheDomainId,
    tier: CacheTier,
    rights_bytes: u64,
    resident_bytes: u64,
    reserved_bytes: u64,
    min_bytes: u64,
    target_bytes: u64,
    lru: Lru<CacheUnit>,
}
```

Every cache key includes the domain in the initial strict-isolation mode:

```text
CacheKey = (cache_domain, object_identity, object_version, block_or_page)
```

This is the partition
[Per-Tenant Namespace Isolation](tenant-namespace-isolation.md) specifies and
delivers — its per-tenant shard map holds the domain one level above `BlockId`,
which already encodes the object, version, and block, so the tuple above and
the shard map name one structure. This proposal consumes that partition for
accounting and eviction ownership rather than shipping its own key change.
Authorization of the *reader* before a cache lookup is supplied by
[Cache Data-Plane Capability](cache-data-plane-capability.md); until it lands,
domain-qualified keys make accounting and eviction ownership unambiguous, but
they do not stop a client that declares another tenant's id.

### Admission

Before beginning a backend download, the worker reserves the proposed size:

```text
if resident + reserved + bytes > rights:
    evict eligible local entries from this domain

if resident + reserved + bytes > rights:
    return CACHE_BYPASS

reserved += bytes
```

The reservation is committed only after the staged cache data is durable and
the cache index entry is visible:

```text
reserved -= actual_bytes
resident += actual_bytes
insert into the domain LRU
```

A failed, cancelled, or stale fill releases its reservation. Reserving before
download prevents concurrent cache misses from oversubscribing a pool.

### Eviction

A normal pool eviction chooses the coldest unpinned entry in the same domain.
It does not use another domain's cache simply because the requesting domain is
full. This is the strict-isolation baseline.

When the worker's total physical capacity is under pressure, an elastic policy
may choose a victim from a domain above its local target. Domains at or below
their local minimum are protected while another eligible victim exists.

An entry being read is pinned and cannot be removed until the read releases its
pin. A failed eviction is not an excuse to exceed a domain's rights; the new
fill bypasses cache instead.

### Resizing

Changing `rights_bytes` downward does not synchronously delete large blocks.
The pool first becomes over-target, rejects new fills, and a background evictor
drains cold unpinned entries in bounded batches. This mirrors CacheLib's
`PoolResizer`: changing the configured size and physically releasing capacity
are separate operations.

## Initial rights distribution

At cluster bootstrap, every worker derives the same initial allocation from a
shared policy and canonical membership snapshot:

```text
worker_target = global_max * worker_effective_capacity
                              / sum(effective_capacity of healthy workers)
```

The integer remainder is assigned deterministically by stable worker ID. Thus
the computed rights sum exactly to `global_max`, without a quota-master write.

This allocation is a bootstrap and target distribution, not permission to
instantaneously rewrite live rights after membership changes. On a worker join,
the new worker starts with zero transferable rights; existing workers transfer
their unused rights toward the new target. On a graceful leave, the departing
worker transfers or returns rights before it exits.

## Peer-to-peer rights transfer

When a worker has local cache demand but insufficient rights, it may request a
transfer from another worker. The coordinator can return donors based on
low-frequency summaries, but this is an advisory lookup. The donor is the only
party that can decide whether it has transferable capacity.

The donor may transfer only:

```text
transferable = rights - resident - reserved - locked_outgoing
```

Transfers use a durable, idempotent two-phase protocol. Each record includes:

```text
transfer_id, cache_domain, tier, bytes,
from_worker, from_incarnation,
to_worker, to_incarnation,
quota_epoch, sequence
```

### Protocol

1. The recipient sends `TransferRequest(bytes)` to a candidate donor.
2. The donor verifies its local transferable balance, appends `PREPARED_OUT`
   to its local quota WAL, and adds the bytes to `locked_outgoing`.
3. The donor sends `Prepare(transfer_id, bytes)` to the recipient.
4. The recipient durably records `PREPARED_IN` but does not make the rights
   usable, then acknowledges.
5. The donor durably records `COMMITTED_OUT`, subtracts the rights locally,
   and sends `Commit(transfer_id)`.
6. The recipient durably records `COMMITTED_IN`, adds the rights locally, and
   makes them available for cache admission.

Every message and terminal record is idempotent. A retry queries or replays
the existing `transfer_id`; it never creates a second allocation. If either
peer fails during transfer, the rights are locked or stranded rather than
being usable by both peers. Recovery reconciles the two WALs before either
side may reuse the transfer amount.

The transfer protocol is inspired by escrow/bounded-counter designs such as
AntidoteDB's bounded counter: a replica may consume or transfer only
permissions it locally owns. Talon uses that invariant for transferable
capacity rights, while local resident-byte accounting remains separate.

## Coordinator responsibilities

The existing coordinator membership mechanism is useful but must remain
lightweight:

| Coordinator responsibility | Frequency / boundedness |
|---|---|
| Distribute versioned quota policies | Only on configuration changes |
| Publish canonical membership and quota epoch | On membership changes or normal membership refresh |
| Compute deterministic bootstrap/target distributions | On policy or membership changes |
| Aggregate bounded worker capacity and optional top-demand summaries | Existing heartbeat cadence; never per object |
| Return candidate donors for a right-transfer request | Slow path only; advisory only |
| Surface aggregate quota health and rebalance lag | Management API / metrics |

The coordinator is explicitly not the authority for a right transfer. A stale
donor hint only causes a peer request to be rejected; it cannot produce an
over-allocation.

## Membership changes and fencing

Membership changes cannot safely cause every worker to recompute and begin
using a new rights map immediately. Existing resident data and in-flight
transfers belong to the previous allocation.

The coordinator publishes a new target epoch. Workers retain their current
rights and converge to the target through direct transfers and local eviction.
No new rights are invented solely because a target changed.

### Unreachable workers

The safe default is conservative:

```text
worker unreachable -> its rights are frozen
```

They are not reassigned merely because a heartbeat is late. This avoids double
spending during a network partition.

An optional later fenced-reclaim mode may use the existing membership lease:

1. A worker that cannot renew its membership lease enters `QuotaFenced`.
2. It stops new cache admission and outgoing rights transfer.
3. It drains active transfers and stops advertising its tenant cache before a
   fixed fence deadline.
4. After the lease and fence grace period, the coordinator may authorize
   recovery of its rights.

This requires a hard implementation guarantee that a fenced worker cannot
resume serving its old quota state without rejoining and reconciling. Until
that guarantee is implemented and tested, automatic reclaim is out of scope.

## Restart and recovery

The cache contents themselves remain disposable. The quota WAL is compact,
bounded control state rather than per-object metadata. On restart a worker:

1. replays its quota WAL;
2. scans or reloads the local cache index to rebuild resident bytes by domain;
3. verifies `resident + reserved <= rights` for every pool;
4. evicts or disables any domain that cannot be proven safe;
5. reconciles incomplete transfers with peers before making their rights
   available.

If the worker cannot reconstruct a safe quota state, it must discard the
affected cache-domain entries and rejoin with no usable rights for that
domain. Safety takes priority over preserving warm cache data.

## Observability

Worker-level metrics should expose, per tier and in a cardinality-controlled
form:

- resident, reserved, rights, target, and minimum bytes;
- cache admission success, bypass due to rights, and bypass due to local
  physical pressure;
- eviction bytes and eviction reason;
- pinned bytes and failed-eviction count;
- transfer request, prepared, committed, aborted, and recovery counts;
- rights frozen because of a peer or membership failure;
- rebalance distance from the coordinator's target plan.

Prometheus labels must not expose unbounded raw tenant IDs. Use aggregate
cluster metrics, allow-listed tenant labels, top-N reporting, and a management
API for per-tenant drill-down.

## Validation

The implementation requires more than unit tests:

- property tests that generate concurrent fills, evictions, and transfers and
  assert the rights invariants after every operation;
- fault-injection tests at every transfer protocol step, including duplicate,
  delayed, reordered, and lost messages;
- process-termination recovery tests with persistent cache directories;
- membership join, graceful leave, and partition tests;
- tests proving that an unpinned cold entry is selected before a protected or
  pinned entry;
- multi-worker integration tests that verify a tenant cannot exceed its global
  maximum while another tenant retains its protected local floor.

## Delivery plan

1. On the per-tenant partition delivered by
   [Per-Tenant Namespace Isolation](tenant-namespace-isolation.md), add strict
   per-domain local pools, reservations, per-domain accounting, and local
   `min/target/max` on one worker.
2. Add deterministic bootstrap rights allocation and expose worker summaries.
3. Enforce global maximum through static rights only; no dynamic lending.
4. Add coordinator donor discovery and the P2P transfer WAL/protocol.
5. Add target-plan convergence and operator visibility.
6. Consider fenced reclaim only after explicit partition and restart testing.

## Alternatives considered

### Coordinator-owned global counter

Provides a simple hard maximum but adds a centralized authority and can become
a fill-path bottleneck. It is rejected for the initial design.

### Gubernator-style eventual accounting

Works well for rates because short-lived excess naturally drains over time.
Resident cache bytes are stock, not flow: an asynchronous over-admission
persists until eviction. It cannot provide strict cache quota isolation.

### Static per-worker quotas only

Simple and strict, but leaves capacity stranded when a tenant's workload is
skewed to a small subset of workers. It is retained as the first delivery
stage, then extended with P2P rights transfer.

### Per-object metadata in a central store

Would make quota accounting straightforward but would introduce object-scale
metadata and contradict Talon's cache/object-store design.
