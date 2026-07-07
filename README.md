# @quicore/tracelet

A lightweight, dependency-free execution context for Node.js that bundles
**lifecycle tracking**, **distributed-trace propagation**, **business metrics**,
and **I/O performance stats** into a single self-instrumenting object — then
serializes the whole thing to a structured snapshot or a flat log line.

## Features

- **Lifecycle state machine** — `PENDING → RUNNING → COMPLETED | FAILED | SKIPPED`, with terminal-state guards so a context can't be re-completed.
- **Trace propagation** — emit/parse `x-trace-id` / `x-span-id` / `x-tenant-id` / `x-integration-id` headers and spawn in-process child spans.
- **First-class tenant & integration identity** — `tenantId` and `integrationId` flow automatically into child spans and across service boundaries.
- **Business metrics** — separate counter and gauge namespaces.
- **I/O telemetry** — call counts, success/error rates, latency percentiles (p50/p95/p99), and throughput.
- **Bounded memory** — a fixed-size circular latency buffer, allocated lazily so idle contexts cost nothing.
- **Hardened serialization** — null-prototype status maps; safe against `__proto__`/`constructor` injection.
- **Zero dependencies** — uses only `node:crypto` and `node:perf_hooks`.

## Install

```bash
npm install @quicore/tracelet
```

## Quick start

```js
import { Tracelet } from '@quicore/tracelet';

const ctx = new Tracelet({
  operationName: 'checkout.process',
  resourceType: 'http',
  tenantId: 'acme-co',
  integrationId: 'stripe-conn-42',
});

ctx.start();

try {
  const t0 = performance.now();
  const res = await fetch('https://api.example.com/charge');

  ctx.recordIO({
    status: res.status,
    isSuccess: res.ok,
    durationMs: performance.now() - t0,
    bytesIn: Number(res.headers.get('content-length')) || 0,
  });

  ctx.increment('items.processed', 3);
  ctx.gauge('cart.total_usd', 129.99);
  ctx.tag('region', 'us-east-1');

  ctx.complete();
} catch (err) {
  ctx.fail({ message: err.message, code: 'CHARGE_FAILED' });
}

console.log(JSON.stringify(ctx.toSnapshot(), null, 2));
```

Both `tenantId` and `integrationId` are nullable — omit them for system-level or
internal operations — but when set, they propagate automatically (see below).

## Core concepts

### Lifecycle

A context starts in `PENDING`. Call `start()` to move it to `RUNNING`, then
exactly one of `complete()`, `fail()`, or `skip()` to reach a terminal state.
Once terminal, further transitions are ignored — the methods are idempotent and
safe to call defensively.

```js
ctx.start();
ctx.complete();      // -> COMPLETED
ctx.fail();          // no-op, already terminal

ctx.isComplete;      // true
ctx.totalDurationMs; // wall-clock duration (frozen once terminal)
```

`fail()` accepts an optional metadata object that is stored verbatim and surfaced
under `lifecycle.error` in the snapshot:

```js
ctx.fail({ message: 'upstream timeout', code: 'ETIMEDOUT', retryable: true });
```

### Identity

`tenantId` and `integrationId` are first-class identity fields. Both are optional
(`null` by default), both are carried into child spans via `createChildSpan`, both
are emitted as propagation headers, and both appear in `toSnapshot()` and
`toLogContext()`.

```js
const ctx = new Tracelet({
  operationName: 'sync.run',
  tenantId: 'acme-co',
  integrationId: 'salesforce-conn-7',
});
```

### Tags

Free-form labels for everything that isn't first-class identity. `tagAll` ignores
non-object input.

```js
ctx.tag('region', 'us-east-1');
ctx.tagAll({ env: 'prod', version: '4.2.0' });
```

### Business metrics: counters vs gauges

Counters accumulate; gauges record a latest value. They live in **separate
namespaces**, so a counter and a gauge can share a key without clobbering each
other. Non-finite values (`NaN`, `Infinity`) are rejected.

```js
ctx.increment('rows.written');        // +1
ctx.increment('rows.written', 250);   // +250  -> 251
ctx.gauge('queue.depth', 17);         // latest value wins
ctx.gauge('queue.depth', 4);          // -> 4
```

### I/O telemetry

Call `recordIO` once per outbound call. Negative and non-finite inputs are
clamped to `0`. Latency feeds a bounded circular buffer (default 1000 samples)
used to compute percentiles.

```js
ctx.recordIO({
  status: 200,       // any value; coerced to a string key
  isSuccess: true,
  durationMs: 42.7,
  bytesIn: 1024,
  bytesOut: 256,
});

ctx.resetIO(); // clear all I/O stats and release the latency buffer
```

The latency buffer is allocated **lazily** on the first `recordIO`, so contexts
that never perform I/O carry no per-sample memory. Buffer size is configurable
per context via `historySize` (clamped to `[1, 100000]`).

## Distributed tracing

### In-process child spans

`tenantId` and `integrationId` are inherited automatically.

```js
const parent = new Tracelet({
  operationName: 'api.request',
  tenantId: 'acme-co',
  integrationId: 'stripe-conn-42',
}).start();

const dbSpan = parent.createChildSpan('db.query', 'postgres');
// dbSpan.traceId       === parent.traceId
// dbSpan.parentSpanId  === parent.spanId
// dbSpan.tenantId      === 'acme-co'
// dbSpan.integrationId === 'stripe-conn-42'
```

### Across service boundaries

Emit headers on the way out, reconstruct on the way in. Header parsing is
case-insensitive and tolerates `null`/array values. Tenant and integration IDs
travel as `x-tenant-id` and `x-integration-id`, emitted only when set.

```js
// Caller
const parent = new Tracelet({
  operationName: 'api.request',
  tenantId: 'acme-co',
  integrationId: 'stripe-conn-42',
}).start();

await fetch('https://worker.internal/run', {
  headers: parent.toTraceHeaders(),
  // {
  //   'x-trace-id': '...',
  //   'x-span-id': '...',
  //   'x-tenant-id': 'acme-co',
  //   'x-integration-id': 'stripe-conn-42'
  // }
});

// Callee
const child = Tracelet.fromTraceHeaders(
  req.headers,
  'worker.handle',
);
// child.traceId === parent.traceId
// child.parentSpanId === parent.spanId
// child.tenantId === 'acme-co'
// child.integrationId === 'stripe-conn-42'
```

## Output

### `toSnapshot()`

A structured, JSON-serializable view of the entire context. The `meta.schemaVersion`
field lets downstream consumers branch on the format.

```js
{
  meta: {
    schemaVersion: 4,
    traceId: '…',
    spanId: '…',
    parentSpanId: null,
    operationName: 'checkout.process',
    resourceType: 'http',
    tenantId: 'acme-co',
    integrationId: 'stripe-conn-42',
    startTime: '2026-06-29T12:00:00.000Z',
    totalDurationMs: 87.421,
    tags: { region: 'us-east-1' }
  },
  lifecycle: {
    currentState: 'COMPLETED',
    isFinal: true,
    phases: [ { state: 'PENDING', timestamp: 1750000000000 }, /* … */ ],
    error: null
  },
  businessMetrics: {
    counters: { 'items.processed': 3 },
    gauges:   { 'cart.total_usd': 129.99 }
  },
  ioMetrics: {
    calls:      { total: 1, success: 1, failed: 0, successRatePct: 100, errorRatePct: 0 },
    latency:    { avgMs: 42.7, minMs: 42.7, maxMs: 42.7, p50Ms: 42.7, p95Ms: 42.7, p99Ms: 42.7 },
    throughput: { rps: 11.44, activeRps: 0, mibPerSec: 0.0114 },
    network:    { bytesIn: 1024, bytesOut: 256, totalBytes: 1280 },
    statusCodes:{ '200': 1 }
  }
}
```

A few semantics worth knowing:

- **`rps`** is calls per second over the full context lifetime (includes idle time).
- **`activeRps`** is calls per second over the wall-clock window between the first and last `recordIO` — it excludes leading/trailing idle time. It reports `0` until at least two I/O events span measurable time, so prefer `rps` for single-call contexts.
- **`mibPerSec`** is mebibytes per second (1024²) over the lifetime. Multiply by 8 if you need megabits.
- **Percentiles** are nearest-rank over the most recent `historySize` samples — a windowed view, not a lifetime distribution.

### `toLogContext()`

A flat, minimal object for per-line structured logging.

```js
logger.info('request finished', ctx.toLogContext());
// { traceId, spanId, parentSpanId, operationName, tenantId, integrationId, state, durationMs }
```

## Constructor options

| Option          | Type     | Default            | Description                                            |
| --------------- | -------- | ------------------ | ------------------------------------------------------ |
| `operationName` | string   | — (required)       | Name of the operation; throws if missing.              |
| `resourceType`  | string   | `'system'`         | Free-form resource category (e.g. `http`, `db`).       |
| `tenantId`      | string   | `null`             | Optional tenant identifier; propagated to children.    |
| `integrationId` | string   | `null`             | Optional integration identifier; propagated to children. |
| `traceId`       | string   | random UUID        | Trace identifier; shared across a trace.               |
| `parentSpanId`  | string   | `null`             | Parent span identifier, if any.                        |
| `historySize`   | number   | `1000`             | Latency buffer capacity, clamped to `[1, 100000]`.     |

## API summary

| Member                                          | Description                                          |
| ----------------------------------------------- | ---------------------------------------------------- |
| `start()` / `complete()` / `fail(meta?)` / `skip()` | Lifecycle transitions (chainable, idempotent).   |
| `get isComplete` / `get totalDurationMs`        | Lifecycle accessors.                                 |
| `tag(key, value)` / `tagAll(obj)`               | Attach labels.                                       |
| `increment(name, value=1)` / `gauge(name, value)` | Business counters and gauges.                      |
| `recordIO(opts)` / `resetIO()`                  | Record or reset I/O telemetry.                       |
| `createChildSpan(name, resourceType?)`          | Spawn an in-process child span.                      |
| `toTraceHeaders()`                              | Emit propagation headers.                            |
| `Tracelet.fromTraceHeaders(headers, name, opts?)` | Rebuild a context from inbound headers. |
| `toSnapshot()` / `toLogContext()`               | Serialize.                                           |

**Exports:** `Tracelet` (named), `LIFECYCLE_STATES`, `SNAPSHOT_SCHEMA_VERSION`.

## Schema versioning

`SNAPSHOT_SCHEMA_VERSION` is exported and embedded in every snapshot at
`meta.schemaVersion`. Bump it whenever the snapshot shape changes so consumers
can branch safely.

## License
MIT