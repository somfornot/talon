# Python client

A binding over Talon's Rust client core. Reads objects through the cache
instead of from the origin, so repeated reads across a fleet are served from
local NVMe.

## Install

```sh
pip install talon-client
```

Wheels are `abi3`, so one artifact per platform covers CPython 3.8 and newer:

| platform | wheel |
|---|---|
| Linux x86_64 | manylinux 2.28 |
| Linux aarch64 | manylinux 2.28 |
| macOS x86_64 | 10.12+ |
| macOS arm64 | 11.0+ |

Windows is not built, since the worker is Linux-only.

Wheels are built on release tags by the `release wheels` workflow; the per-PR
job builds and tests a Linux x86_64 wheel as a regression check.

To build from a checkout:

```sh
pip install maturin
maturin build --release --manifest-path clients/python/Cargo.toml --out dist
pip install dist/*.whl
```

## Reading

```python
import talon

with talon.Client("coordinator-host:7000", block_size=8 << 20) as client:
    info = client.stat("az://container/datasets/train.parquet")
    print(info.size, info.version)

    chunk = client.read("az://container/datasets/train.parquet",
                        offset=0, length=1 << 20)
```

`block_size` **must match the workers' configured block size**. Placement is
computed per block, so a mismatch addresses blocks that do not exist.

URIs use the same namespaces as the FUSE mount — `s3://`, `gcs://`, `az://` —
so a path addresses the same object through either.

## Reading many ranges of one object

`read` resolves the object's version with a `stat` when one is not supplied.
That is one extra round trip per call, which is wasted work when reading many
ranges of the same object: a version is stable for an object generation.

```python
info = client.stat(uri)
for offset in range(0, info.size, chunk_size):
    part = client.read(uri, version=info.version, size=info.size,
                       offset=offset, length=chunk_size)
```

Passing `size` as well lets the client clamp at end-of-file without another
lookup. The supplied version identifies one exact source generation. A worker
may serve that generation from cache or conditionally fetch it from the origin;
if the object was replaced and that generation is unavailable, the read fails
with a version mismatch instead of returning bytes from the replacement.

A production Talon block is 256 MiB, so ordinary KiB/MiB ranges issue one
worker request and application throughput comes from independent calls. All
reads issued through the same `Client` share a 1024-request aggregate budget;
rare cross-block reads use an internal fairness window.

## Threads

Blocking calls release the GIL, so a threaded loader is limited by the network
rather than serialised on the interpreter:

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=8) as pool:
    parts = list(pool.map(
        lambda off: client.read(uri, version=info.version, size=info.size,
                                offset=off, length=chunk_size),
        offsets))
```

## Short reads

A read at or past end-of-file returns an empty buffer, and a read overlapping
the end is truncated to what exists — POSIX short-read semantics. **Check the
returned length** rather than assuming the requested count:

```python
data = client.read(uri, offset=offset, length=length)
if len(data) < length:
    ...  # reached the end of the object
```

## Errors

Failures raise `OSError` with the worker's own message preserved, so "object
does not exist" stays distinguishable from "every replica is down". Malformed
URIs raise `ValueError` before any I/O happens.

## Not yet available

`list` is implemented but returns an error until the backends gain a listing
capability ([#332](https://github.com/milvus-io/talon/issues/332)). Writes go
through the FUSE mount or the Rust client.
