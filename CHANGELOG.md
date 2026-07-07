# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-XX

### Added
- Initial release of @quicore/tracelet
- Lifecycle state machine (PENDING → RUNNING → COMPLETED | FAILED | SKIPPED)
- Distributed trace propagation with x-trace-id, x-span-id, x-tenant-id, x-integration-id headers
- First-class tenant and integration identity
- Business metrics (counters and gauges)
- I/O telemetry with latency percentiles (p50/p95/p99)
- Bounded memory circular buffer for latency history
- Hardened serialization against prototype pollution
- Zero external dependencies (uses only node:crypto and node:perf_hooks)
