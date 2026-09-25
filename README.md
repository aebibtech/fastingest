# FastIngest

[![CI/CD](https://github.com/aebibtech/fastingest/actions/workflows/ci.yml/badge.svg)](https://github.com/aebibtech/fastingest/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![NuGet](https://img.shields.io/nuget/v/FastIngest.Core.svg)](https://www.nuget.org/packages/FastIngest.Core)

**FastIngest** is a high-throughput, constant-memory bulk ingestion pipeline for .NET (CSV, XLSX, and NDJSON/JSONL to PostgreSQL, SQL Server, MySQL, SQLite, MongoDB, Cosmos DB, and Elasticsearch). Designed for enterprise workloads processing millions of rows without memory spikes, FastIngest leverages zero-allocation streaming readers, concurrent producer-consumer bounded channels, fluent validation, and native database bulk protocols (such as PostgreSQL binary `COPY`, SQL Server `SqlBulkCopy`, and MongoDB unordered `BulkWriteAsync`).

---

## Architecture Overview

FastIngest processes incoming tabular and line-delimited data using a decoupled producer-consumer pipeline that keeps memory usage constant ($O(1)$) regardless of file size:

```
Stream (CSV / XLSX / JSONL / NDJSON)
        │
        ▼
┌─────────────────────────────────┐
│ Producer: Streaming Reader      │  <-- Sylvan CSV or PipeReader JSON Lines (CPU)
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: Expression / Utf8 JSON│  <-- Zero-reflection binders or Utf8JsonReader
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: FluentValidation      │  <-- FailFast or CollectAndContinue
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ System.Threading.Channels       │  <-- Bounded channel (Capacity: 2 batches)
│ (BoundedChannelFullMode.Wait)   │  <-- Backpressure: strict O(1) memory
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Consumer: Native Database Sink  │  <-- High-throughput batch streaming (I/O)
└─────────────────────────────────┘
```

---

## Key Features

- **Concurrent Producer-Consumer Pipelining**: Decouples CPU parsing/validation from database I/O socket operations using bounded `System.Threading.Channels` with backpressure.
- **Constant-Memory Streaming**: Stream arbitrarily large files (gigabytes to tens of gigabytes) with strict $O(1)$ memory guarantees.
- **Line-Delimited JSON (NDJSON / JSONL)**: High-speed streaming parser over `System.IO.Pipelines.PipeReader` and `System.Text.Json.Utf8JsonReader` with automatic format heuristics (`.jsonl`, `.ndjson`, or `{` content peeking).
- **7 Native Database Sinks**: Direct bulk protocol integrations for PostgreSQL (`COPY`), SQL Server (`SqlBulkCopy`), MySQL (`MySqlBulkCopy`), SQLite (`WAL`), MongoDB (`BulkWriteAsync`), Azure Cosmos DB, and Elasticsearch.
- **Validation Strategies**:
  - `FailFast`: Immediately halts ingestion on the first invalid record.
  - `CollectAndContinue`: Collects invalid row details and exports an error report CSV while allowing valid records to proceed.
- **Fluent Pipeline API**: Composable, chainable pipeline configuration with channel capacity tuning (`WithChannelCapacity`), custom JSON serializer options (`WithJsonOptions`), and progress tracking.
- **SemVer 2.0 Driven by Git Tags**: Automated versioning via MinVer and seamless CI/CD publishing.

---

## Quickstart

### 1. Installation

```bash
dotnet add package FastIngest.Core
dotnet add package FastIngest.PostgreSql
```

### 2. Define Record & Validator

```csharp
using FluentValidation;

public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

public class CustomerValidator : AbstractValidator<CustomerRecord>
{
    public CustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty().MaximumLength(100);
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}
```

### 3. Run Pipeline with PostgreSQL COPY

```csharp
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

await using var stream = File.OpenRead("large_customers.csv");
await using var connection = new NpgsqlConnection("Host=localhost;Database=mydb;Username=postgres;Password=secret");
await connection.OpenAsync();

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(mapping =>
    {
        mapping.Map(x => x.Id, "customer_id");
        mapping.Map(x => x.Email, "email");
        mapping.Map(x => x.FullName, "full_name");
        mapping.Map(x => x.Balance, "balance");
    })
    .ValidateWith<CustomerValidator>(options =>
    {
        options.ErrorStrategy = ErrorStrategy.CollectAndContinue;
    })
    .WithBatchSize(5000)
    .WithChannelCapacity(2)
    .OnProgress(progress =>
    {
        Console.WriteLine($"Processed {progress.RowsProcessed} rows ({progress.PercentComplete:F1}%)...");
    })
    .WriteToPostgresAsync(connection, "customers", CancellationToken.None);

Console.WriteLine($"Ingestion Complete! Succeeded: {result.TotalSucceeded}, Failed: {result.TotalFailed}");

if (!result.IsSuccess)
{
    var errorCsv = result.ExportErrorsToCsv();
    await File.WriteAllBytesAsync("ingest_errors.csv", errorCsv);
}
```

### 4. Stream Line-Delimited JSON (NDJSON / JSONL)

FastIngest natively streams `.jsonl` / `.ndjson` files without loading the entire document into RAM:

```csharp
await using var jsonlStream = File.OpenRead("customers.jsonl");

var jsonlResult = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(jsonlStream, FileType.JsonLines) // Or FileType.Ndjson
    .WithJsonOptions(opt => opt.PropertyNameCaseInsensitive = true)
    .ValidateWith<CustomerValidator>(opt => opt.ErrorStrategy = ErrorStrategy.CollectAndContinue)
    .WithBatchSize(5000)
    .WriteToPostgresAsync(connection, "customers");
```

---

## Performance Benchmarks

Official **BenchmarkDotNet** suite results comparing FastIngest streaming binary COPY against Entity Framework Core 9 (`net9.0, Apple M4, PostgreSQL 16 Alpine via Testcontainers`):

| Method | RowCount | Mean | Ratio | Gen 0 | Gen 1 | Gen 2 | Allocated | Alloc Ratio |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **FastIngest_Pipeline** | **25,000** | **143.2 ms** | **0.14 (7.2x faster)** | **2,000** | **1,000** | **-** | **18.64 MB** | **0.08 (-92%)** |
| EfCore_Naive (Baseline) | 25,000 | 1,029.0 ms | 1.00 | 25,000 | 9,000 | 2,000 | 222.15 MB | 1.00 |
| EfCore_Batched (1k) | 25,000 | 1,190.8 ms | 1.16 | 26,000 | 12,000 | 3,000 | 210.10 MB | 0.95 |
| | | | | | | | | |
| **FastIngest_Pipeline** | **100,000** | **439.6 ms** | **0.16 (6.1x faster)** | **10,000** | **4,000** | **1,000** | **73.59 MB** | **0.08 (-92%)** |
| EfCore_Batched (1k) | 100,000 | 2,442.1 ms | 0.91 | 107,000 | 53,000 | 17,000 | 825.17 MB | 0.94 |
| EfCore_Naive (Baseline) | 100,000 | 2,691.6 ms | 1.00 | 95,000 | 32,000 | 3,000 | 876.09 MB | 1.00 |

*Run the benchmarks yourself with `./benchmarks/run-benchmarks.sh`. See full analysis in [`docs/benchmarks/performance.md`](docs/benchmarks/performance.md).*

---

## Repository Structure

```
├── .github/workflows/ci.yml       # GitHub Actions CI/CD Pipeline
├── Directory.Build.props          # Centralized MSBuild & MinVer configuration
├── FastIngest.sln
├── benchmarks/
│   └── FastIngest.Benchmarks/     # BenchmarkDotNet performance suite
├── docs/                          # VitePress documentation website
├── src/
│   ├── FastIngest.Core/           # Core pipeline, channels, binders, CSV & NDJSON/JSONL streaming readers
│   ├── FastIngest.PostgreSql/     # PostgreSQL native binary COPY sink
│   ├── FastIngest.SqlServer/      # Microsoft SQL Server SqlBulkCopy sink
│   ├── FastIngest.MySql/          # MySQL MySqlBulkCopy sink
│   ├── FastIngest.Sqlite/         # SQLite WAL batch sink
│   ├── FastIngest.MongoDb/        # MongoDB unordered BulkWrite sink
│   ├── FastIngest.CosmosDb/       # Azure Cosmos DB bulk executor sink
│   ├── FastIngest.Elasticsearch/  # Elasticsearch BulkAsync sink
│   └── FastIngest.Extensions.DependencyInjection/ # Engine, DI, and profile registry
├── samples/
│   └── FastIngest.SampleApi/      # Minimal Web API demonstrating ingestion
└── tests/
    └── FastIngest.Tests/          # Unit and integration test suites
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Developed & Maintained by **Paul Abib Camano** ([Aebibtech](https://github.com/aebibtech)).
