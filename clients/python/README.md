# talon-client

Python client for [Talon](https://github.com/milvus-io/talon), a distributed
object-store cache.

Reads objects through a Talon cache cluster instead of from the origin, so
repeated reads across a fleet are served from local NVMe.

Wheels are `abi3`, so one artifact per platform covers CPython 3.8 and newer:
Linux x86_64 and aarch64 (manylinux 2.28), and macOS x86_64 and arm64. Windows
is not built — the worker is Linux-only, so a Windows client would be talking to
a cluster it cannot host.

```python
import talon

with talon.Client("coordinator-host:7000") as client:
    info = client.stat("az://container/datasets/train.parquet")
    print(info.size, info.version)

    # A ranged read; spans block boundaries transparently.
    chunk = client.read("az://container/datasets/train.parquet",
                        offset=0, length=1 << 20)

    for entry in client.list("az/container/datasets"):
        print(entry.path, entry.size)
```

URIs use the same namespaces as the FUSE mount — `s3://`, `gcs://`, `az://` —
so a path addresses the same object through either client.

Blocking calls release the GIL, so threaded loaders are limited by the network
rather than serialised on the interpreter.

Passing both `version` and `size` to `read` skips the metadata lookup and pins
the read to that exact source generation; Talon never substitutes newer bytes.
Multi-block reads keep at most 64 worker requests active at once. Independent
reads on one `Client` share an aggregate budget of 1024 active worker requests,
so a single huge read cannot monopolize the client and many small reads remain
bounded without being serialized behind a per-read window.

**Read-only in this release.** Writes go through the FUSE mount or the Rust
client; `put`/`delete` are tracked separately.

See the [documentation](https://milvus-io.github.io/talon/) for cluster setup
and the [use cases](https://milvus-io.github.io/talon/use-cases/overview.html)
this is built for.
