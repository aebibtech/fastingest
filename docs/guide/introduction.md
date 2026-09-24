# Introduction to FastIngest

**FastIngest** is a high-throughput, constant-memory bulk ingestion pipeline designed specifically for .NET 9+ workloads. It enables you to stream massive CSV and Excel files (ranging from hundreds of megabytes to tens of gigabytes) directly into your database or search engine without exhausting RAM or suffering Garbage Collection freezes.

---

## The Problem with Traditional Ingestion

Processing large data imports in .NET frequently runs into three major bottlenecks:

1. **Memory Bloat ($O(N)$ Growth)**: Traditional libraries (like CsvHelper or standard deserializers) often materialize row collections into memory before saving. For a 5GB file containing 15 million rows, materializing domain models or DataTables can consume 12–20 GB of RAM, causing `OutOfMemoryException` or crippling GC pauses (Gen 2 collection freezes).
2. **Slow Row-by-Row Database Inserts**: Using Entity Framework Core or standard ADO.NET `INSERT INTO` statements generates individual round-trips over the network. Even with basic transaction batching, throughput is usually capped at 2,000–5,000 rows/second.
3. **Reflection Overhead**: Dynamically mapping string columns to record properties at runtime using standard reflection consumes CPU cycles and generates temporary object allocations on every single cell.

---

## How FastIngest Solves It

FastIngest combines four core pillars to achieve maximum throughput with fixed memory:

```
Stream (CSV / XLSX)
        │
        ▼
┌─────────────────────────────────┐
│ Producer: Sylvan Stream Reader  │  <-- Zero-allocation row streaming (CPU)
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: Pre-Compiled Mapper   │  <-- Zero-reflection property binding
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: FluentValidation      │  <-- FailFast or CollectAndContinue
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ System.Threading.Channels       │  <-- Bounded channel backpressure (O(1) Memory)
│ (BoundedChannelFullMode.Wait)   │  <-- Keeps 2-3 batches in flight concurrently
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Consumer: Native Database Sink  │  <-- Binary COPY, SqlBulkCopy, etc. (I/O)
└─────────────────────────────────┘
```

1. **Zero-Allocation Streaming**: Built on [Sylvan.Data.Csv](https://github.com/MarkPflug/Sylvan), the fastest CSV reader in the .NET ecosystem, reading records as raw spans and UTF-8 bytes with minimal heap allocation.
2. **Pre-Compiled Expression Trees**: Column mappings are compiled into high-performance delegate expressions once and cached for the lifetime of your application. Property extraction behaves at compiled code speed with zero reflection overhead.
3. **Concurrent Producer-Consumer Pipelining**: Uses bounded `System.Threading.Channels` to decouple CPU stream parsing and validation from database socket operations. Both tasks execute in parallel without lockstep waits, while backpressure ensures that in-flight batches never exceed the configured channel capacity ($O(1)$ memory).
4. **Native Bulk Transport Protocols**: FastIngest avoids generic SQL queries and binds directly to native database streaming interfaces:
   - PostgreSQL: Native binary `COPY ... FROM STDIN (FORMAT BINARY)`
   - SQL Server: `SqlBulkCopy` backed by a custom `BatchDataReader<TRecord>`
   - MySQL / MariaDB: `MySqlBulkCopy` and batched multi-row transactions
   - SQLite: Parameterized command loops optimized with Write-Ahead Logging (`WAL` mode)
   - MongoDB: Unordered `BulkWriteAsync` with `InsertOneModel<TRecord>`
   - Azure Cosmos DB: Concurrent asynchronous item creation with `AllowBulkExecution`
   - Elasticsearch: High-speed bulk indexing via `BulkAsync`

---

## Key Benefits

- **Flat Memory Footprint**: Ingest 100 rows or 50,000,000 rows with the same ~25 MB working set.
- **CPU & I/O Concurrency**: Channel pipelining ensures database sockets write batches while subsequent rows are simultaneously parsed and validated.
- **Fail-Fast or Error Quarantine**: Stop immediately on the first bad record or collect errors into an exportable CSV manifest while saving valid records.
- **First-Class Dependency Injection**: Configure destination profiles in your startup pipeline and inject `IFastIngestEngine` into ASP.NET Core Minimal APIs or background workers.
- **Clean Developer Experience**: Fluent, chainable builder API with built-in progress events and cancellation token support.
