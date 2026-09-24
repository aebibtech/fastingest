# SQLite Sink (WAL Batching)

The SQLite sink provides high-speed bulk ingestion into embedded SQLite databases for local caching, desktop applications, edge computing, and offline sync engines.

---

## Technical Overview

SQLite's default transactional behavior commits every statement to disk with full file synchronization (`PRAGMA synchronous = FULL`), reducing bulk insert performance to a few hundred rows per second.

FastIngest's `SqliteBulkSink<TRecord>` solves this with three optimizations:

1. **Write-Ahead Logging (WAL Mode)**: Automatically executes `PRAGMA journal_mode = WAL;` and `PRAGMA synchronous = NORMAL;`, dramatically reducing fsync operations while maintaining ACID safety.
2. **Batch Transactions**: Every batch chunk (e.g. 5,000–10,000 rows) is enclosed within a dedicated `SqliteTransaction`.
3. **Reused Parameterized Commands**: A single `SqliteCommand` with pre-allocated parameters is reused across the entire batch, eliminating query parsing and bytecode generation overhead.

---

## Installation

```bash
dotnet add package FastIngest.Sqlite
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.Sqlite.Extensions;

await using var stream = File.OpenRead("sensors.csv");
var connStr = "Data Source=telemetry.db;Cache=Shared;";

var result = await FastIngestPipeline<SensorReading>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.DeviceId, "device_id");
        m.Map(x => x.Temperature, "temperature");
        m.Map(x => x.Humidity, "humidity");
        m.Map(x => x.Timestamp, "timestamp");
    })
    .WithBatchSize(10_000)
    .WriteToSqliteAsync(connStr, "readings");

Console.WriteLine($"Ingested {result.TotalSucceeded:N0} sensor records into SQLite.");
```

### 2. Using an Existing `SqliteConnection`

```csharp
using FastIngest.Sqlite.Extensions;
using Microsoft.Data.Sqlite;

await using var connection = new SqliteConnection("Data Source=telemetry.db;");
await connection.OpenAsync();

var result = await FastIngestPipeline<SensorReading>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* mappings */ })
    .WriteToSqliteAsync(connection, "readings");
```

### 3. Dependency Injection Setup

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddSqliteSink("Data Source=cache.db;");
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Performance Tips

- **Batch Size**: For SQLite, a batch size of **5,000** to **10,000** rows offers the optimal balance between transaction memory overhead and commit frequency.
- **In-Memory Databases**: You can ingest directly into SQLite in-memory databases (`Data Source=:memory:;Mode=Memory;Cache=Shared`) for ultra-fast unit testing or ephemeral ETL pipelines at 100,000+ rows/sec.
