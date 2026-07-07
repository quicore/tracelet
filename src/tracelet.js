import crypto from 'node:crypto';
import { performance } from 'node:perf_hooks';

const LIFECYCLE_STATES = Object.freeze({
    PENDING: 'PENDING',
    RUNNING: 'RUNNING',
    COMPLETED: 'COMPLETED',
    FAILED: 'FAILED',
    SKIPPED: 'SKIPPED'
});

const TERMINAL_STATES = new Set([
    LIFECYCLE_STATES.COMPLETED,
    LIFECYCLE_STATES.FAILED,
    LIFECYCLE_STATES.SKIPPED
]);

const MAX_PHASES = 50;
const MIN_HISTORY_SIZE = 1;
const MAX_HISTORY_SIZE = 100_000;

const SNAPSHOT_SCHEMA_VERSION = 1;

// Coerce to a non-negative, finite number. Rejects Infinity/-Infinity/NaN and
// negatives (all → 0), unlike `Math.max(0, Number(x) || 0)`, which lets +Infinity through.
function nonNegativeFinite(value) {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : 0;
}

export class Tracelet {
    constructor({
        operationName,
        resourceType = 'system',
        tenantId = null,
        integrationId = null,
        traceId = crypto.randomUUID(),
        parentSpanId = null,
        historySize = 1000
    } = {}) {
        if (!operationName) {
            throw new Error('Tracelet requires operationName');
        }

        // Validate and clamp historySize
        const parsed = Math.trunc(Number(historySize));
        if (!Number.isFinite(parsed) || parsed < MIN_HISTORY_SIZE) {
            this.historySize = MIN_HISTORY_SIZE;
        } else if (parsed > MAX_HISTORY_SIZE) {
            this.historySize = MAX_HISTORY_SIZE;
        } else {
            this.historySize = parsed;
        }

        // Identity & Tracing
        this.traceId = traceId;
        this.spanId = crypto.randomUUID();
        this.parentSpanId = parentSpanId;
        this.operationName = operationName;
        this.resourceType = resourceType;
        this.tenantId = tenantId;
        this.integrationId = integrationId;

        // Lifecycle & Timing
        this.state = LIFECYCLE_STATES.PENDING;
        this.phases = [{ state: this.state, timestamp: Date.now() }];
        this.startTimeEpoch = Date.now();
        this.startTimePerf = performance.now();
        this.endTimePerf = null;
        this.errorMetadata = null;

        // Business Counters & Gauges — separate namespaces so a counter and a
        // gauge sharing a key can no longer clobber each other.
        this.counters = new Map();
        this.gauges = new Map();

        // Tags / Labels
        this.tags = new Map();

        // I/O & Network Performance
        this.io = this._createIOBucket();

        // Bounded circular buffer — allocated lazily on first recordIO so that
        // spans which never record I/O carry no histogram weight.
        this.latencyHistory = null;
        this.historyIndex = 0;
        this.historyCount = 0;

        // Active I/O window (perf timestamps of first/last recordIO) — used to
        // compute throughput over the time I/O actually occurred, excluding
        // leading/trailing idle time.
        this.firstIOPerf = null;
        this.lastIOPerf = null;

        // Cache
        this._latencyCacheDirty = true;
        this._sortedLatencyCache = null;
    }

    // ─── LIFECYCLE ───────────────────────────────────────────────────────

    start() {
        if (this.state !== LIFECYCLE_STATES.PENDING) return this;
        this._transition(LIFECYCLE_STATES.RUNNING);
        return this;
    }

    complete() {
        if (TERMINAL_STATES.has(this.state)) return this;
        this.endTimePerf = performance.now();
        this._transition(LIFECYCLE_STATES.COMPLETED);
        return this;
    }

    fail(errorMetadata = {}) {
        if (TERMINAL_STATES.has(this.state)) return this;
        this.endTimePerf = performance.now();
        this.errorMetadata = errorMetadata;
        this._transition(LIFECYCLE_STATES.FAILED);
        return this;
    }

    skip() {
        if (TERMINAL_STATES.has(this.state)) return this;
        this.endTimePerf = performance.now();
        this._transition(LIFECYCLE_STATES.SKIPPED);
        return this;
    }

    get isComplete() {
        return TERMINAL_STATES.has(this.state);
    }

    get totalDurationMs() {
        const end = this.endTimePerf ?? performance.now();
        return end - this.startTimePerf;
    }

    _transition(newState) {
        if (TERMINAL_STATES.has(this.state)) return;
        this.state = newState;
        if (this.phases.length >= MAX_PHASES) this.phases.shift();
        this.phases.push({ state: newState, timestamp: Date.now() });
    }

    // ─── TAGS ────────────────────────────────────────────────────────────

    tag(key, value) {
        this.tags.set(key, value);
        return this;
    }

    // #5 Fix: Guard against null/undefined/non-object inputs
    tagAll(tagsObj) {
        if (!tagsObj || typeof tagsObj !== 'object') return this;
        for (const [k, v] of Object.entries(tagsObj)) {
            this.tags.set(k, v);
        }
        return this;
    }

    // ─── BUSINESS METRICS ────────────────────────────────────────────────

    increment(metricName, value = 1) {
        const delta = Number(value);
        if (!Number.isFinite(delta)) return this;
        const current = this.counters.get(metricName) || 0;
        this.counters.set(metricName, current + delta);
        return this;
    }

    gauge(metricName, value) {
        const v = Number(value);
        if (!Number.isFinite(v)) return this;
        this.gauges.set(metricName, v);
        return this;
    }

    // ─── I/O TRACKING ────────────────────────────────────────────────────

    recordIO({ status = 'unknown', isSuccess = true, durationMs = 0, bytesIn = 0, bytesOut = 0 } = {}) {
        durationMs = nonNegativeFinite(durationMs);
        bytesIn = nonNegativeFinite(bytesIn);
        bytesOut = nonNegativeFinite(bytesOut);

        // Mark the active I/O window.
        const now = performance.now();
        if (this.firstIOPerf === null) this.firstIOPerf = now;
        this.lastIOPerf = now;

        this.io.totalCalls++;
        this.io.bytesIn += bytesIn;
        this.io.bytesOut += bytesOut;
        this.io.totalLatency += durationMs;

        if (isSuccess) this.io.successes++;
        else this.io.failures++;

        // #1 Fix: Safe because statusCodes has null prototype — no pollution possible
        const code = String(status);
        this.io.statusCodes[code] = (this.io.statusCodes[code] || 0) + 1;

        if (durationMs < this.io.minLatency) this.io.minLatency = durationMs;
        if (durationMs > this.io.maxLatency) this.io.maxLatency = durationMs;

        // Lazy allocation: first I/O event materializes the histogram buffer.
        if (this.latencyHistory === null) {
            this.latencyHistory = new Float64Array(this.historySize);
        }
        this.latencyHistory[this.historyIndex] = durationMs;
        this.historyIndex = (this.historyIndex + 1) % this.historySize;
        if (this.historyCount < this.historySize) this.historyCount++;

        this._latencyCacheDirty = true;
        return this;
    }

    resetIO() {
        this.io = this._createIOBucket();
        // Drop the buffer rather than zero-fill, so a reset context goes back
        // to carrying nothing until I/O is recorded again.
        this.latencyHistory = null;
        this.historyIndex = 0;
        this.historyCount = 0;
        this.firstIOPerf = null;
        this.lastIOPerf = null;
        this._latencyCacheDirty = true;
        this._sortedLatencyCache = null;
    }

    // #1 Fix: Object.create(null) — no prototype chain, immune to __proto__ / constructor injection
    _createIOBucket() {
        return {
            totalCalls: 0,
            successes: 0,
            failures: 0,
            bytesIn: 0,
            bytesOut: 0,
            minLatency: Infinity,
            maxLatency: 0,
            totalLatency: 0,
            statusCodes: Object.create(null)
        };
    }

    // ─── CHILD SPANS / PROPAGATION ──────────────────────────────────────

    createChildSpan(operationName, resourceType) {
        return new Tracelet({
            operationName,
            resourceType: resourceType || this.resourceType,
            tenantId: this.tenantId,
            integrationId: this.integrationId,
            traceId: this.traceId,
            parentSpanId: this.spanId,
            historySize: this.historySize
        });
    }

    toTraceHeaders() {
        const headers = {
            'x-trace-id': this.traceId,
            'x-span-id': this.spanId
        };
        if (this.tenantId) headers['x-tenant-id'] = this.tenantId;
        if (this.integrationId) headers['x-integration-id'] = this.integrationId;
        return headers;
    }

    /**
     * Reconstruct context from incoming trace headers.
     * #2 Fix: Default headers to {} to prevent throw on null/undefined.
     * #3 Fix: Normalize all keys to lowercase once, then read from normalized map.
     */
    static fromTraceHeaders(headers, operationName, options = {}) {
        const normalized = Tracelet._normalizeHeaders(headers);

        return new Tracelet({
            operationName,
            traceId: normalized['x-trace-id'] || crypto.randomUUID(),
            parentSpanId: normalized['x-span-id'] || null,
            tenantId: normalized['x-tenant-id'] || null,
            integrationId: normalized['x-integration-id'] || null,
            ...options
        });
    }

    // #2 + #3 Fix: Single-pass normalization to lowercase keys, handles null/arrays
    static _normalizeHeaders(headers) {
        if (!headers || typeof headers !== 'object') return Object.create(null);

        const out = Object.create(null);
        for (const [key, val] of Object.entries(headers)) {
            const k = key.toLowerCase();
            if (Array.isArray(val)) {
                out[k] = val[0] != null ? String(val[0]) : null;
            } else if (val != null) {
                out[k] = String(val);
            }
        }
        return out;
    }

    // ─── DATA EXPORT ─────────────────────────────────────────────────────

    toSnapshot() {
        const duration = this.totalDurationMs;
        const durationSec = duration / 1000;
        const sortedLatencies = this._getSortedLatencies();

        return {
            meta: {
                schemaVersion: SNAPSHOT_SCHEMA_VERSION,
                traceId: this.traceId,
                spanId: this.spanId,
                parentSpanId: this.parentSpanId,
                operationName: this.operationName,
                resourceType: this.resourceType,
                tenantId: this.tenantId,
                integrationId: this.integrationId,
                startTime: new Date(this.startTimeEpoch).toISOString(),
                totalDurationMs: Number(duration.toFixed(3)),
                tags: Object.fromEntries(this.tags)
            },
            lifecycle: {
                currentState: this.state,
                isFinal: TERMINAL_STATES.has(this.state),
                phases: this.phases.map(p => ({ ...p })),
                error: this.errorMetadata ? { ...this.errorMetadata } : null
            },
            businessMetrics: {
                counters: Object.fromEntries(this.counters),
                gauges: Object.fromEntries(this.gauges)
            },
            ioMetrics: this._buildIOMetrics(durationSec, sortedLatencies)
        };
    }

    toLogContext() {
        return {
            traceId: this.traceId,
            spanId: this.spanId,
            parentSpanId: this.parentSpanId,
            operationName: this.operationName,
            tenantId: this.tenantId,
            integrationId: this.integrationId,
            state: this.state,
            durationMs: Number(this.totalDurationMs.toFixed(3))
        };
    }

    // ─── INTERNALS ───────────────────────────────────────────────────────

    _getSortedLatencies() {
        if (!this._latencyCacheDirty && this._sortedLatencyCache) {
            return this._sortedLatencyCache;
        }
        if (this.historyCount === 0) {
            // Also the state when latencyHistory is still null — guarded here,
            // so the null branch below is never reached.
            this._sortedLatencyCache = new Float64Array(0);
        } else {
            this._sortedLatencyCache = this.latencyHistory.slice(0, this.historyCount).sort();
        }
        this._latencyCacheDirty = false;
        return this._sortedLatencyCache;
    }

    _getPercentile(sortedArray, percentile) {
        if (sortedArray.length === 0) return 0;
        const index = Math.floor((percentile / 100) * (sortedArray.length - 1));
        return sortedArray[index];
    }

    _buildIOMetrics(durationSec, sortedLatencies) {
        const io = this.io;
        const totalBytes = io.bytesIn + io.bytesOut;

        // Wall-clock span between first and last recorded I/O. Zero when fewer
        // than two events exist (or all share a timestamp), in which case no
        // meaningful active rate exists and activeRps falls back to 0.
        const activeWindowSec = (this.firstIOPerf !== null && this.lastIOPerf !== null)
            ? (this.lastIOPerf - this.firstIOPerf) / 1000
            : 0;

        return {
            calls: {
                total: io.totalCalls,
                success: io.successes,
                failed: io.failures,
                successRatePct: io.totalCalls
                    ? Number(((io.successes / io.totalCalls) * 100).toFixed(2))
                    : 0,
                errorRatePct: io.totalCalls
                    ? Number(((io.failures / io.totalCalls) * 100).toFixed(2))
                    : 0
            },
            latency: {
                avgMs: io.totalCalls ? Number((io.totalLatency / io.totalCalls).toFixed(2)) : 0,
                minMs: io.minLatency === Infinity ? 0 : Number(io.minLatency.toFixed(2)),
                maxMs: Number(io.maxLatency.toFixed(2)),
                p50Ms: Number(this._getPercentile(sortedLatencies, 50).toFixed(2)),
                p95Ms: Number(this._getPercentile(sortedLatencies, 95).toFixed(2)),
                p99Ms: Number(this._getPercentile(sortedLatencies, 99).toFixed(2))
            },
            throughput: {
                // Calls/sec over the full context lifetime (includes idle time).
                rps: durationSec > 0 ? Number((io.totalCalls / durationSec).toFixed(2)) : 0,
                // Calls/sec over the active I/O window (excludes idle time).
                activeRps: activeWindowSec > 0
                    ? Number((io.totalCalls / activeWindowSec).toFixed(2))
                    : 0,
                // Mebibytes/sec (1024^2) over the full context lifetime.
                mibPerSec: durationSec > 0
                    ? Number((totalBytes / durationSec / (1024 * 1024)).toFixed(4))
                    : 0
            },
            network: {
                bytesIn: io.bytesIn,
                bytesOut: io.bytesOut,
                totalBytes
            },
            statusCodes: { ...io.statusCodes }
        };
    }
}

export { LIFECYCLE_STATES, SNAPSHOT_SCHEMA_VERSION };