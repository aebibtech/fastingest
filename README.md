# FastIngest

[![CI/CD](https://github.com/aebibtech/fastingest/actions/workflows/ci.yml/badge.svg)](https://github.com/aebibtech/fastingest/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![NuGet](https://img.shields.io/nuget/v/FastIngest.Core.svg)](https://www.nuget.org/packages/FastIngest.Core)

**FastIngest** is a high-throughput, constant-memory bulk ingestion pipeline for .NET (CSV/XLSX to PostgreSQL, SQL Server, and MongoDB). Designed for enterprise workloads processing millions of rows without memory spikes, FastIngest leverages zero-allocation streaming readers, fluent validation, and native database bulk protocols (such as PostgreSQL binary `COPY`, SQL Server `SqlBulkCopy`, and MongoDB unordered `BulkWriteAsync`).

---

## Architecture Overview

FastIngest processes incoming tabular data using a pipeline pattern that keeps memory usage constant regardless of file size:

```
Stream (CSV / XLSX)
        │
        ▼
┌───────────────────────────────┐
│ Sylvan Zero-Allocation Reader │  <-- Constant-memory row streaming
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│ Expression-based Mapper       │  <-- Strongly-typed record mapping
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│ FluentValidation Engine       │  <-- FailFast or CollectAndContinue
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│ Batch Buffering & Partitioning│  <-- Configurable batch chunks (e.g. 5,000)
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│ Native Database COPY Sink     │  <-- PostgreSQL Binary COPY FROM STDIN
└───────────────────────────────┘
```

---

## Key Features

- **Constant-Memory Streaming**: Stream arbitrarily large files (gigabytes to tens of gigabytes) with fixed memory footprint.
- **Native Database COPY**: High-speed binary ingestion utilizing PostgreSQL `COPY ... FROM STDIN (FORMAT BINARY)`.
- **Validation Strategies**:
  - `FailFast`: Immediately halts ingestion on the first invalid record.
  - `CollectAndContinue`: Collects invalid row details and exports an error report CSV while allowing valid records to proceed.
- **Fluent Pipeline API**: Composable, chainable pipeline configuration with progress tracking.
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

---

## Performance Benchmarks

Official **BenchmarkDotNet** suite results comparing FastIngest streaming binary COPY against Entity Framework Core 9 (`net9.0, Apple M4, PostgreSQL 16 Alpine via Testcontainers`):

| Method | RowCount | Mean | Ratio | Gen 0 | Gen 1 | Gen 2 | Allocated | Alloc Ratio |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **FastIngest_Pipeline** | **25,000** | **178.7 ms** | **0.17 (5.9x faster)** | **2,000** | **1,000** | **-** | **18.44 MB** | **0.08 (-92%)** |
| EfCore_Batched (1k) | 25,000 | 1,033.0 ms | 0.97 | 25,000 | 12,000 | 3,000 | 207.63 MB | 0.94 |
| EfCore_Naive (Baseline) | 25,000 | 1,067.0 ms | 1.00 | 25,000 | 9,000 | 2,000 | 219.95 MB | 1.00 |
| | | | | | | | | |
| **FastIngest_Pipeline** | **100,000** | **496.6 ms** | **0.17 (5.7x faster)** | **9,000** | **3,000** | **-** | **72.81 MB** | **0.08 (-92%)** |
| EfCore_Batched (1k) | 100,000 | 2,592.3 ms | 0.91 | 107,000 | 53,000 | 17,000 | 825.18 MB | 0.94 |
| EfCore_Naive (Baseline) | 100,000 | 2,843.0 ms | 1.00 | 95,000 | 32,000 | 3,000 | 876.09 MB | 1.00 |

*Run the benchmarks yourself with `./benchmarks/run-benchmarks.sh`. See full analysis in [`docs/benchmarks/performance.md`](docs/benchmarks/performance.md).*

---

## Repository Structure

```
├── .github/workflows/ci.yml       # GitHub Actions CI/CD Pipeline
├── Directory.Build.props          # Centralized MSBuild & MinVer configuration
├── FastIngest.sln
├── benchmarks/
│   └── FastIngest.Benchmarks/     # BenchmarkDotNet performance suite
├── src/
│   ├── FastIngest.Core/           # Core interfaces, pipeline, and CSV parsers
│   ├── FastIngest.PostgreSql/     # PostgreSQL native binary COPY sink
│   ├── FastIngest.SqlServer/      # Microsoft SQL Server SqlBulkCopy sink
│   ├── FastIngest.MongoDb/        # MongoDB unordered BulkWrite sink
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
