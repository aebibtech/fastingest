# Changelog

All notable changes to FastIngest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.5.0] - 2026-10-01

### Added
- **ASP.NET Core & SignalR Integration** (`FastIngest.AspNetCore`): Native drop-in integration for minimal APIs and background ingestion workers.
- `IServiceCollection.AddFastIngestAspNetCore()` to register `IngestJobQueue` and `IngestBackgroundService`.
- `IEndpointRouteBuilder.MapFastIngestHub()` to map a SignalR hub for real-time `ReceiveProgress` and `ReceiveCompletion` telemetry.
- `IEndpointRouteBuilder.MapFastIngestUpload<TRecord>()` for a `202 Accepted` non-blocking streaming upload endpoint with background file staging.
- Documentation: `docs/guide/aspnetcore.md` — ASP.NET Core & SignalR guide page.

---

## [0.4.0] - 2026-10-01

### Added
- **Resilience & Retry Policies** (`FastIngest.Core`): Opt-in transient fault handling for `WriteBatchAsync` via `Microsoft.Extensions.Resilience` (Polly v8). Configure via `pipeline.WithResilience(o => { o.Enabled = true; o.MaxRetryAttempts = 3; })`.
- `ResilienceOptions` class with `Enabled`, `MaxRetryAttempts`, `BaseDelay`, `MaxDelay`, `UseJitter`, and `ShouldRetry` delegate properties.
- Built-in `TransientFaultPredicates.IsTransient` covering Npgsql, SQL Server / Azure SQL, and MySqlConnector transient exception patterns.
- `IngestProgress.Message` property for retry telemetry and stage updates via the `OnProgress` callback.
- `WithResilience(Action<ResilienceOptions>)` fluent method on `IFastIngestPipeline<TRecord>`.
- Documentation: `docs/guide/resilience.md` — Resilience & Retry Policies guide page.

### Changed
- `Directory.Build.props`: Version bumped from `0.3.2` to `0.4.0`.
- `PipelineOptions`: Added `Resilience` property.

---

## [0.3.2] - 2026-09-27

### Added
- Constant-memory streaming pipeline with producer-consumer `System.Threading.Channels` pipelining.
- Support for JSON Lines (`FileType.JsonLines` / NDJSON) streaming reader via zero-intermediate allocation `Utf8JsonReader`.
- Database sinks for PostgreSQL (Binary COPY), SQL Server (SqlBulkCopy), MySQL/MariaDB (MySqlBulkCopy), SQLite (WAL Batch), MongoDB (BulkWrite), Azure Cosmos DB, and Elasticsearch.
- FluentValidation integration with `FailFast` and `CollectAndContinue` error strategies.
- Dependency injection extensions via `FastIngest.Extensions.DependencyInjection`.
