# PostgreSQL Sink (Native Binary COPY)

The PostgreSQL sink is the highest-throughput relational sink in FastIngest. It bypasses standard SQL parsing and query planning by streaming pre-compiled binary tuples directly into PostgreSQL via `Npgsql`'s `BeginBinaryImportAsync` interface.

---

## Technical Overview

Traditional database drivers insert rows using `INSERT INTO ... VALUES (...)` statements or multi-row insert batches. Even with unnesting or arrays, each statement incurs SQL parsing, query planning, WAL logging, and row serialization overhead.

FastIngest's `PostgreSqlSink<TRecord>` executes the native PostgreSQL binary streaming protocol:

```sql
COPY "public"."customers" ("id", "email", "full_name", "balance") FROM STDIN (FORMAT BINARY)
```

```
┌─────────────────────────────────┐
│ FastIngest Row Pipeline         │
└──────────────┬──────────────────┘
               │ IReadOnlyList<TRecord>
               ▼
┌─────────────────────────────────┐
│ PostgreSqlSink<TRecord>         │
│   - BeginBinaryImportAsync(...) │
│   - StartRowAsync(...)          │
│   - WriteAsync(colVal)          │
│   - WriteNullAsync()            │
└──────────────┬──────────────────┘
               │ Raw Binary Stream (TCP)
               ▼
┌─────────────────────────────────┐
│ PostgreSQL Engine               │
│   - Direct Heap Insertion       │
│   - Zero Query Parser Overhead  │
└─────────────────────────────────┘
```

### Key Advantages

- **Zero SQL Parsing**: Data is sent as raw binary values in PostgreSQL internal representations (e.g. 4-byte integers, 8-byte doubles, UTF-8 byte buffers).
- **Sub-Millisecond Batch Flush**: Capable of writing 180,000+ rows/second over a local or low-latency gigabit network.
- **Null Safety**: Transparently dispatches `WriteNullAsync` when pre-compiled getter expressions return `null`.

---

## Installation

```bash
dotnet add package FastIngest.PostgreSql
```

---

## Usage Examples

### 1. Fluent Pipeline Ingestion

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

await using var stream = File.OpenRead("large_customers.csv");
await using var connection = new NpgsqlConnection("Host=localhost;Database=mydb;Username=postgres;Password=secret");
await connection.OpenAsync();

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.Id, "customer_id");
        m.Map(x => x.Email, "email");
        m.Map(x => x.FullName, "full_name");
        m.Map(x => x.Balance, "balance");
    })
    .WithBatchSize(10_000)
    .WriteToPostgresAsync(connection, "public.customers");

Console.WriteLine($"Ingested {result.TotalSucceeded} rows into PostgreSQL.");
```

### 2. Passing Connection String Directly

You can also pass a connection string directly; FastIngest will open, execute, and dispose the connection automatically:

```csharp
var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* mappings */ })
    .WriteToPostgresAsync(
        connectionString: "Host=localhost;Database=mydb;Username=postgres;Password=secret",
        tableName: "customers");
```

### 3. Dependency Injection Configuration

Register PostgreSQL defaults in your ASP.NET Core service setup:

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddPostgreSqlSink(builder.Configuration.GetConnectionString("Postgres"));
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Schema & Performance Considerations

1. **Table Matching**: Target table column names in `m.Map(x => x.Property, "column_name")` must match the PostgreSQL column names exactly. Identifier quotes (`"column_name"`) are automatically managed.
2. **Indexes & Foreign Keys**: For maximum throughput on multi-million row loads, consider dropping secondary indexes or foreign keys before import and rebuilding them concurrently afterward (`CREATE INDEX CONCURRENTLY`).
3. **Batch Sizing**: Optimal batch size for PostgreSQL binary `COPY` is typically between **5,000** and **25,000** rows per chunk depending on record width.
