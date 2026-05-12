# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-11

### Added
- Core dispatcher with builder pattern API
- Worker pool with least-loaded selection algorithm
- Task schema with priority, timeout, retry, and trace ID support
- WebSocket transport for networked workers
- Unix stdio transport for CLI tool integration
- Docker transport for containerized workers
- SSH transport for remote workers
- WASM transport for sandboxed execution
- Input validation to prevent command injection
- Dead worker detection via heartbeat timeout
- Backpressure signaling from workers
- camelCase JSON wire protocol for cross-language compatibility
- SDKs: TypeScript, Python, Go, Java, C#, Ruby, Elixir, Swift, PHP
- 185 tests with 95.58% code coverage
