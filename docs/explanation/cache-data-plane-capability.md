# Cache Data-Plane Capability

## Status

Implementation proposal. Not implemented.

It owns **L1** of the tenant isolation series, and implements the
native-plane tenant-authentication hook that the namespace design deliberately
leaves open ("the declared tenant is trusted … hardening that boundary is a
separate, additive step"):

| Layer | Concern | Owner |
|---|---|---|
| **L0** — origin identity | Talon holds its own service account and assumes a per-tenant role to reach customer storage. STS credentials live in worker memory only. | [Worker Per-Tenant Origin Identity](worker-per-tenant-origin-identity.md). **Independent of this document.** |
| **L1** — data-plane authentication | A signed, short-lived capability authorizes a client to read and fill one cache domain. **This document.** | Here |
| **L2** — cache partitioning | Private per-tenant namespace, per-tenant storage shard, tenant-qualified `LoadKey`, fast teardown. | [Per-Tenant Namespace Isolation](tenant-namespace-isolation.md). **Independent of this document.** |

An **L3** — per-domain envelope encryption, which would make already-cached
bytes cryptographically unreadable after revocation — was considered and
**deliberately deferred**. See *Security boundary*.

## Summary

The direct client↔worker data plane has no authentication of any kind. The
worker's data listener is plain TCP (`crates/talon-worker/src/main.rs:704`);
mTLS exists only on the privileged coordinator↔worker control channel
(`main.rs:616-617`, `crates/talon-transport/src/control_tls.rs`). The one piece
of caller-supplied identity on the wire, `TenantId`, is self-declared and
shape-validated only (`crates/talon-core/src/tenant.rs:37,84`), and is consumed
solely for QoS (`crates/talon-core/src/rate_limit.rs:159`). Any client can
declare any tenant.

This proposal introduces a **capability**: a short-lived claim set — domain,
tenant, scope, permissions, validity window — carried with an HMAC-SHA256 tag
under a per-domain signing key. A client presents it **once per connection** via
a new `BindCapability` frame; the worker verifies the tag, pins the connection
to the verified claims, and answers subsequent range requests against the pinned
domain rather than against anything the client says per request.

Because the signing key is per-domain and Talon controls both the issuer and the
verifier, **revocation is a key deletion, not a token expiry**: dropping domain
D's key from the workers invalidates every outstanding capability for D
immediately. That is a materially stronger revocation story than the cloud
providers offer for their own vended credentials (see *Revocation*).

## Goals

- Make a cache hit an authorized operation, not merely an addressed one.
- Authenticate the `TenantId` that already drives per-tenant QoS, so a client
  cannot consume another tenant's budget by relabelling its traffic.
- Keep the hot path free: verification cost belongs at connection setup, not per
  range request.
- Introduce no new cryptographic dependency and no new online service on the
  read path.
- Give operators a revocation lever whose effect is immediate and observable.

## Non-goals

- **Defend against a compromised worker.** Under L0 a worker holds every
  domain's STS credentials, and blocks are stored as plaintext. A worker that is
  compromised has already lost everything a capability could protect. Closing
  this is L3's job and is out of scope by decision.
- **Audit to an end-user identity.** A capability's subject is a domain, not a
  natural person. AWS Trusted Identity Propagation-style attribution is a
  different and much larger project.
- **Encrypt the data plane.** A capability authenticates the *request*; it does
  not make the connection confidential. Deployments needing that should
  terminate TLS in front of the worker or run on a private network.
- **Replace L0.** A capability says a client may read a domain. It says nothing
  about how the worker reaches the origin on a miss.

## Terminology

- **Capability** — a claim set plus a MAC, authorizing a bearer to operate on
  one domain and scope for a bounded window.
- **Domain** — an equivalence class of origin credentials, defined in
  [Per-Tenant Namespace Isolation](tenant-namespace-isolation.md) (*The cache
  domain*). Under the private-namespace model, one domain is one tenant
  binding.
- **Bind** — the handshake in which a client presents a capability and the
  worker pins a connection to its verified claims.
- **Scope** — the bucket and prefix a capability covers.

## Why the issuer is milvus-storage, not the coordinator

Three candidates were considered.

| | Issuer | New machinery required | Trust surface |
|---|---|---|---|
| **A** | The client (milvus-storage), using a per-domain key provisioned by the control plane | None | **Unchanged** |
| B | The Talon coordinator; clients exchange for a capability | A whole client→coordinator authentication system | Enlarged: the coordinator becomes an online read-path dependency |
| C | An external control plane, signing centrally | A new signing service plus a distribution channel | Unchanged, but the largest build |

**A is selected**, and the argument is that it does not enlarge the trust
surface in either direction:

- **On the client side**, a milvus-storage process already holds domain D's
  origin credential configuration — `role_arn`, external id, or static AK/SK
  (see `cpp/src/filesystem/s3/s3_filesystem_producer.cpp:180-260` in
  milvus-storage). A process that is compromised can read D's objects straight
  from the origin. Handing it an additional key that mints capabilities for D
  and only D confers no new capability on an attacker.
- **On the worker side**, HMAC is symmetric, so a worker holds every domain's
  verification key — which looks like amplification until you observe that under
  L0 the worker already holds every domain's STS credentials. Again unchanged.

What a capability *does* buy is precisely the property neither of those already
has: **lateral isolation between clients**. The milvus process for tenant a
cannot read domain b's cached blocks, and cannot fill into domain b, because it
cannot mint a capability naming b.

B's cost is concrete rather than theoretical. The coordinator today has only a
shared bearer token on its management surface
(`crates/talon-coordinator/src/security.rs`, `AuthMode::{Disabled, BearerToken}`).
Making it a capability issuer means first answering "who is this client",
i.e. building a client identity system, and then accepting the coordinator into
the read path as a hard online dependency.

**When B or C becomes worth it**: when clients are no longer operated by the
same party as the cache — a user-run Milvus dialling a shared Talon — or when
compliance demands attribution to an end user. The claim set below is unchanged
under either; only the issuer moves. That upgrade path is deliberate.

## The claim set

New file `crates/talon-core/src/capability.rs`.

```rust
/// Domain separation for the capability MAC. Same idiom as the placement
/// hash's `BLOCK_DOMAIN` (`crates/talon-core/src/placement.rs:11`).
const CAPABILITY_DOMAIN: &[u8] = b"talon-capability-v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CapabilityClaims {
    /// Claim-set schema version, bumped independently of
    /// `CONTROL_SCHEMA_VERSION`.
    pub version: u8,
    /// Which signing key produced the MAC. Enables two-live-key rotation
    /// without a flag day.
    pub key_id: u32,
    /// The cache partition this capability authorizes. A 32-byte identity
    /// provisioned by the control plane to both the issuer and the workers'
    /// tenant bindings; see tenant-namespace-isolation.md for its stability
    /// contract.
    pub domain: DomainId,
    /// Authenticated, no longer client-declared. Drives per-tenant QoS.
    pub tenant: TenantId,
    /// What the capability covers.
    pub scope: Scope,
    /// `READ | ADMIT`. `WRITE`/`DELETE` reserved, rejected if set.
    pub perms: Perms,
    /// Validity window, unix seconds.
    pub not_before: u64,
    pub not_after: u64,
    /// Uniqueness. Not checked today; present so replay bookkeeping can be
    /// added later without a format change.
    pub nonce: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub bucket: String,
    /// Empty means the whole bucket.
    pub prefix: String,
}

/// Wire form: `canonical(claims) ‖ HMAC-SHA256(key, CAPABILITY_DOMAIN ‖ canonical(claims))`
pub struct SignedCapability {
    pub claims: CapabilityClaims,
    pub mac: [u8; 32],
}
```

Roughly 300 bytes encoded — far below `MAX_CONTROL_PAYLOAD_LEN`
(`crates/talon-transport/src/limits.rs:31`, 1 MiB).

`hmac = "0.12"` and `sha2 = "0.10"` are already workspace dependencies
(`Cargo.toml:45-46`), so this adds no crate. MAC comparison must go through
`hmac`'s `verify_slice`, which is constant-time; a hand-rolled `==` on the tag
is a timing oracle and must not appear.

### The signed bytes are a specified layout, not `bincode`

**`canonical(claims)` must be an explicitly specified byte encoding, and the
`Serialize`/`Deserialize` derives must never be the definition of what gets
signed.** The issuer is a C++ process (milvus-storage). Asking it to reproduce
`bincode` byte-for-byte, and to keep reproducing it across `bincode` and `serde`
upgrades, is a silent-breakage generator: a layout drift does not fail to
compile, it fails to verify at runtime, in production, on the read path.

The codebase already has the right precedent — `cache_block_hash`
(`crates/talon-core/src/placement.rs:18`) is an explicitly specified byte
encoding under a versioned domain constant, for the same reason.

Specification: all integers big-endian; every variable-length field preceded by
a `u32` big-endian byte length; fields emitted in declaration order; `TenantId`
encoded as a `u8` tag (`0` = `Unattributed`, `1` = `Named`) followed by the
length-prefixed UTF-8 name; `Perms` as a `u32` bitset. No padding, no optional
fields, no reordering. A conformance vector — a fixed claim set and its expected
canonical bytes and MAC, as a hex constant — must live in both repositories and
be asserted by a test in each, so a drift on either side fails CI rather than
production.

The `bincode` derives stay, but only for the *frame envelope* that wraps the
already-signed bytes, which is produced and consumed exclusively by Rust.

### Other deliberate choices

Four further choices in the claim set:

**`domain` is in the claims, not in `BlockId`.** The cache partition itself is
worker-local state — the tenant shard map of
[Per-Tenant Namespace Isolation](tenant-namespace-isolation.md) — and `BlockId`
and the placement path are untouched. In the claims, the domain is the
*authorization target* the worker checks a request's binding-derived domain
against; see *Domain* below.

**`tenant` is in the claims.** This fixes a live defect independent of anything
else here: today `TenantScopedRange.tenant`
(`crates/talon-transport/src/data.rs:87`) is whatever the client writes, so a
client can spend another tenant's rate-limit budget simply by relabelling. Once
bound, the worker must **reject a request whose declared tenant differs from the
bound one** rather than silently substituting the bound value — silent
substitution hides client bugs that are worth surfacing.

**`perms` carries `ADMIT` separately from `READ`.** `admit_block`
(`crates/talon-cache-client/src/block_reader.rs:236`) is a client-driven fill
path. Domain binding already contains the blast radius — a client can only fill
its own domain — but a separate bit allows issuing read-only capabilities to
consumers that must never populate the cache.

**`version` is separate from `CONTROL_SCHEMA_VERSION`.** A capability is minted
by a different process than the one that speaks the control protocol, on its own
release cadence. Coupling them would force a Talon control-schema bump every
time a claim is added.

## Binding is per connection, not per request

This is the load-bearing performance decision.

A new `MsgType::BindCapability = 10` (control-plane framed, bincode payload) is
sent as the first frame on a data connection. The worker verifies the MAC once,
then stores `(domain, tenant, scope, perms, not_after)` in the per-connection
state of `handle_conn` (`crates/talon-worker/src/tokio_conn.rs:74`, whose `loop`
at `:80` already provides exactly the right scope) and its `uring_conn`
counterpart. Every subsequent `GetRange` / `GetRangeTenant` /
`GetCachedRange` / `GetCachedRangeTenant` is checked against the pinned claims
with integer and prefix comparisons only.

**Hot-path cost is therefore approximately zero.** One HMAC over ~300 bytes at
connect time; a few nanoseconds of comparison per request. Per-request
capabilities would instead add ~300 bytes and one HMAC to every range request,
which is measurable in the small-IO high-IOPS regime this cache exists to serve.

Two obligations follow.

**The connection pool must be re-keyed.** `ConnectionPool` keys idle sockets by
peer address alone (`crates/talon-cache-client/src/pool.rs:94,173,211`). A
connection bound to domain A handed out for a domain-B request would either fail
closed or, worse, succeed against the wrong domain. The key becomes
`(addr, capability_fingerprint)`. This is a change worth making regardless:
per-domain connection isolation is also cleaner for QoS accounting.

**Expiry must be enforced mid-connection.** The worker records `not_after` and,
once passed, fails requests on that connection with a new
`DataErrorCode::CapabilityExpired`; the client re-binds on the same socket
without a TCP reconnect. Checking only at bind time would make a long-lived
connection an unbounded grant — a common misconfiguration of Alluxio's
equivalent mechanism, whose
`alluxio.security.authorization.capability.lifetime.ms` defaults to one hour.

## Wire format

```rust
// crates/talon-transport/src/frame.rs
BindCapability = 10,   // client → worker, bincode payload
BindAck        = 11,   // worker → client, bincode payload

// crates/talon-transport/src/limits.rs — `max_payload_for` is an exhaustive
// match, so adding these variants fails the build until they are handled.
MsgType::BindCapability => MAX_CONTROL_PAYLOAD_LEN,   // 1 MiB
MsgType::BindAck        => MAX_CONTROL_PAYLOAD_LEN,
```

`MsgType::from_u8` rejects unknown discriminants (`frame.rs:85`), so an
un-upgraded worker refuses a `BindCapability` frame rather than misreading it.
That is the safe direction; *Rolling upgrade* covers how to roll out anyway.

`BindAck` returns the effective `not_after` the worker computed, so the client
schedules its refresh against the worker's clock rather than its own. Clock skew
between issuer and verifier is otherwise an operational trap: the worker must
apply a small tolerance (recommended 30 s) on `not_before` and **none** on
`not_after`.

A new data-plane error code is required:

```rust
// crates/talon-transport/src/data.rs — appended last, so existing
// discriminants are unchanged (same convention as `RateLimited`).
Unauthorized,       // no capability bound, or claims do not cover the request
CapabilityExpired,  // bound capability's not_after has passed; re-bind
```

These are distinct on purpose. `Unauthorized` means stop; `CapabilityExpired`
means re-bind and retry. Collapsing them would turn a routine refresh into an
error the client cannot distinguish from a policy denial.

## Key management

**Provisioning.** The control plane issues one HMAC key per domain and delivers
it to two places: the milvus-storage configuration for that tenant, and the
Talon coordinator.

**Distribution to workers.** The coordinator pushes keys to workers over the
existing mTLS control channel, following the established coordinator→worker
push pattern (`ControlMessage::Load`, `ControlMessage::EpochBump`). Workers hold
keys **in memory only, never persisted** — the same handling L0 gives STS
credentials, and the same handling Alluxio documents for the AssumeRole tokens
its masters distribute to workers.

```rust
// crates/talon-transport/src/codec.rs
/// Coordinator → worker: the current set of capability verification keys.
CapabilityKeys { keys: Vec<CapabilityKey> },   // { key_id, domain, secret }
/// Coordinator → worker: drop these domains' keys immediately.
RevokeCapabilityKeys { domains: Vec<DomainId> },
```

Both are schema 6: `CONTROL_SCHEMA_VERSION` goes 5 → 6 and `minimum_schema()`
gains a `=> 6` arm for them, matching how zone-aware membership was fenced at 5
(`codec.rs:224-246`). Adding variants without touching existing layouts means
the fence works as designed — an older peer rejects them at the envelope rather
than misinterpreting them.

**Rotation.** `key_id` supports two live keys. The control plane publishes a new
`key_id`; workers accept both; clients begin minting under the new one at their
next refresh; the old key is withdrawn one capability TTL later. No flag day.

## Revocation

This is where the design earns its keep.

Because keys are per-domain and Talon owns both the issuer and the verifier,
**revoking domain D is deleting D's key from the workers**. Every outstanding
capability for D fails verification on its next use — including capabilities
already bound to open connections, since the bound state holds the claims, and
the check on withdrawal is to drop the pinned state for any connection whose
`key_id`/domain was revoked. Effect is sub-second, not TTL-bounded.

Compare against what the cloud providers offer for their own vended
credentials:

| Mechanism | Revocation delay |
|---|---|
| AWS S3 Access Grants | Deleting a grant does not invalidate issued credentials; 15 min – 12 h |
| Azure user delegation SAS | Unassign the RBAC role; subject to a caching delay |
| GCP downscoped tokens | Bearer tokens, not revocable before expiry |
| Alluxio capability | ≤ `capability.lifetime.ms`, default 1 h |
| Alluxio Ranger plugin | ≤ `ranger.plugin.hdfs.policy.pollIntervalMs`, default 30 s |
| **This proposal (drop the domain key)** | **Sub-second** |

TTL therefore demotes to a backstop rather than the primary control. Recommended
default 300 s, hard maximum 900 s to align with the `load_frequency=900` STS
refresh cadence, with clients refreshing at 50 % of the window. milvus-storage
already has this exact idiom implemented for Azure SAS —
`AzureSasTokenPolicy::IsFresh` / `FetchSasToken`
(`cpp/include/milvus-storage/filesystem/azure/azure_sas_token_policy.h:64-66`) —
and the client-side capability store should mirror it.

**What revocation does not do.** The blocks are still on disk in plaintext. The
access path is severed immediately; the residue is not. For tenant
decommission the answer is structural: `DropTenant`
([Per-Tenant Namespace Isolation](tenant-namespace-isolation.md)) pairs this
key deletion with the shard wipe, so the bytes go with the access. For a
revocation that is *not* a decommission, a best-effort coordinator broadcast
should purge the revoked domain's blocks, but that is advisory — a partitioned
or restarting worker will miss it. Eliminating that residue requires L3
envelope encryption, which is deferred. **This is a known accepted risk and
must be stated to anyone evaluating the security posture.**

## Domain

What a domain is, how it partitions the worker's cache and storage, and the
fast-teardown machinery around it are all specified in
[Per-Tenant Namespace Isolation](tenant-namespace-isolation.md). What this
document needs from it:

- **The request's domain is derived on the worker**, from the tenant's registry
  binding — the same lookup that selects the origin credential (L0) and the
  storage shard (L2). A client influences it only by declaring a tenant, and a
  declared tenant that differs from the bound one is rejected.
- **The `domain` claim is an authorization target, not a request parameter.**
  On every bound request the worker requires the binding-derived domain to
  equal the bound capability's domain. A capability for domain A is useless
  against domain B's data — cached or not. This is what closes the
  "unauthorized hit" gap the namespace design's trusted-partition model
  accepts.
- **`DomainId` is provisioned, not computed twice.** The control plane assigns
  the id to the tenant binding and to the issuer's configuration, so issuer and
  verifier agree by provisioning rather than by parallel hashing; the id's
  stability contract (fixed for the life of the binding) is defined there.

The partition mechanics themselves — the tenant shard map, the
tenant-qualified `LoadKey` that keeps a coalescing cohort single-domain, the
`BlockId`-stays-untouched argument, and the cold-start/wipe costs of adopting
the layout — live in the namespace document and are delivered by its plan, not
this one.

## Rolling upgrade

Two switches, and a deletion plan for one of them.

- Worker: `require_capability: bool`, default `false`. When `true`, **any**
  data frame received before a successful bind is refused with
  `DataErrorCode::Unauthorized` — including the plain `MsgType::GetRange` path.
  The type check at `tokio_conn.rs:174` is the natural place for this gate.
- Client: `capability_mode: { off, prefer, require }`. `prefer` attempts a bind
  and falls back to the unbound path on `InvalidRequest`, which is what an
  un-upgraded worker returns for an unknown `MsgType`.

Sequence: upgrade all workers with `require_capability=false` → upgrade all
clients to `prefer` → watch the bind-success-rate metric until it is 1.0 →
flip workers to `require_capability=true` → move clients to `require` → **delete
the `prefer` branch in the following release**.

That last step is not optional bookkeeping. A permanent `prefer` mode is a
permanent downgrade channel, and an attacker who can cause a bind to fail gets
the unauthenticated path for free.

## Implementation plan

| PR | Scope | Wire change |
|---|---|---|
| **P1** | `crates/talon-core/src/capability.rs`: claim set, `Scope`, `Perms`, `DomainId`, sign/verify, `CAPABILITY_DOMAIN`, tests. Pure library. | None |
| **P2** | `MsgType::{BindCapability, BindAck}`, `limits.rs` arms, `DataErrorCode::{Unauthorized, CapabilityExpired}`, per-connection bound state in `tokio_conn` and `uring_conn`, `require_capability` (default off). | Data plane, fail-closed |
| **P3** | Key distribution: coordinator tenant-key config, `ControlMessage::{CapabilityKeys, RevokeCapabilityKeys}`, `CONTROL_SCHEMA_VERSION` 5 → 6, worker in-memory keyring, two-live-key rotation. | Control plane, fenced at 6 |
| **P4** | Client: `CapabilityStore` (cache + refresh at 50 % of the window), `ConnectionPool` re-keyed to `(addr, fingerprint)`, `capability_mode`. | None |
| **P5** | Flip `require_capability=true`; delete the `prefer` branch. | None |

P1–P4 are independent of the milvus-storage side, which mints capabilities
under the provisioned per-domain keys; that work is specified in
`docs/talon-integration-design.md` in the milvus-storage repository. The cache
partition itself (the tenant shard and tenant-qualified `LoadKey`) is delivered
by [Per-Tenant Namespace Isolation](tenant-namespace-isolation.md)'s plan, not
here.

**L0 and L2 are the higher priority and should land first.** Both are
independent of this document. Without L0 the workers still fetch under a single
process-wide service identity, so L1 alone delivers lateral isolation between
clients but leaves one credential able to read every customer's bucket — which
is the posture question the programme started from. Without L2 there is no
partition for a capability's domain to authorize against.

### Test obligations

- A capability signed under key A is rejected when only key B is loaded.
- A tampered claim set (any single bit) fails `verify_slice`.
- MAC comparison is constant-time — assert the code path uses `verify_slice`,
  since a review cannot catch a regression to `==` by reading a test.
- A request whose declared tenant differs from the bound tenant is rejected, not
  silently corrected.
- A request outside the bound scope is rejected.
- A request arriving after `not_after` yields `CapabilityExpired`, and a re-bind
  on the same socket succeeds.
- With `require_capability=true`, an unbound `GetRange` yields `Unauthorized`.
- A bound request for an object outside the bound domain's namespace is
  rejected. (The partition tests themselves — two tenants, two entries, cohort
  never spans domains — belong to the namespace design's stages.)
- After `RevokeCapabilityKeys`, an already-bound connection's next request
  fails.
- A schema-5 peer rejects `CapabilityKeys` at the envelope.
- Round-trip and size-bound tests for the new frames, matching the existing
  `data.rs` round-trip test conventions.
- The canonical-encoding conformance vector verifies, and a deliberate
  reordering of two fields makes it fail. The same vector must be asserted in
  milvus-storage; a change to either encoder that is not mirrored must break CI
  on at least one side.

## Security boundary

Stated plainly, because the value of this proposal is entirely in what it does
and does not promise:

**A capability authorizes the hit as well as the fill — against a client.**
This is precisely the upgrade from the namespace design's trusted partition to
a verified one: a forged tenant declaration stops working, because the declared
tenant must match a bound, MAC-verified claim.

**A capability does not protect against a compromised worker.** The worker holds
every domain's verification key and every domain's plaintext blocks. Revoking a
domain severs access immediately but leaves plaintext on disk until purge
succeeds.

**Closing that residue is L3** — per-domain data keys wrapped by a customer KMS
key, blocks stored as ciphertext, with a TTL on the in-memory unwrapped-key
cache. The TTL matters: a wrapped-key scheme without one leaves the plaintext
key resident in worker memory after revocation, which is why comparable designs
pair key revocation with terminating the compute that held it. **L3 is
deliberately not being built**; this paragraph exists so that decision is a
recorded choice rather than an oversight.

## Open questions

1. **Where does the signing key come from in a self-hosted deployment?** Zilliz
   Cloud has an existing per-cluster secret distribution path — it is how
   `role_arn` arrives. An open-source Milvus against a self-hosted Talon does
   not. Either the key degrades to a static config-file entry, or
   `require_capability` stays `false` in that topology. This needs an explicit
   position rather than a default.
2. **Bucket-level or prefix-level `scope`?** Prefix level is tighter but makes
   the client's capability count grow with collections, and with per-domain
   connection pooling that inflates socket count too. Recommendation: **start at
   bucket level**; the `Scope` field is present so narrowing later needs no
   format change.
3. **FUSE and gateway clients.** Both reach the worker data plane and would need
   keys under `require_capability=true`. The gateway's origin authentication is
   still a `"not available in this build"` stub
   (`crates/talon-gateway/src/main.rs:441,516,954`), so that path may need
   separate treatment regardless.
4. **Is `TenantId::Unattributed` permitted under `require_capability=true`?**
   Recommendation: no. But that affects every internal flow that does not
   currently attribute its traffic, which needs an inventory first.
5. **Replay.** `nonce` is carried but unchecked. A capability leaked within its
   window is replayable by anyone who can reach a worker. Options are a
   worker-side seen-nonce window, or binding the capability to a client
   certificate — the latter requires data-plane mTLS, which is out of scope
   here. Recording that the field exists so the decision is not foreclosed.
