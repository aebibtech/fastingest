# SQL Server Sink (SqlBulkCopy)

The SQL Server sink provides high-throughput ingestion into Microsoft SQL Server and Azure SQL Database using native `Microsoft.Data.SqlClient.SqlBulkCopy` backed by FastIngest's zero-allocation `BatchDataReader<TRecord>`.

---

## Technical Overview

`SqlBulkCopy` is Microsoft's fastest data-loading API for SQL Server. However, standard .NET implementations suffer from two common flaws:
1. Converting data into an intermediary `DataTable` (which allocates huge managed heap objects and can easily trigger `OutOfMemoryException`).
2. Reflection-based object readers that allocate boxed values for every field.

FastIngest solves this with **`BatchDataReader<TRecord>`**, a custom, lightweight implementation of `IDataReader`:

```
┌────────────────────────────────────────┐
│ FastIngest Batch (IReadOnlyList<T>)    │
└──────────────────┬─────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────┐
│ BatchDataReader<TRecord> (IDataReader) │
│  - Zero intermediate DataTable         │
│  - Pre-compiled getter delegates       │
│  - Strongly typed column metadata      │
└──────────────────┬─────────────────────┘
                   │ Direct TDS Protocol Stream
                   ▼
┌────────────────────────────────────────┐
│ Microsoft.Data.SqlClient.SqlBulkCopy   │
│  - DestinationTableName                │
│  - ColumnMappings                      │
│  - CheckConstraints / TableLock        │
└────────────────────────────────────────┘
```

---

## Installation

```bash
dotnet add package FastIngest.SqlServer
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.SqlServer.Extensions;
using Microsoft.Data.SqlClient;

await using var stream = File.OpenRead("transactions.csv");
var connStr = "Server=localhost;Database=SalesDb;User Id=sa;Password=your_password;TrustServerCertificate=True;";

var result = await FastIngestPipeline<TransactionRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.TransactionId, "TransactionId");
        m.Map(x => x.AccountId, "AccountId");
        m.Map(x => x.Amount, "Amount");
        m.Map(x => x.CreatedAt, "CreatedAt");
    })
    .WithBatchSize(10_000)
    .WriteToSqlServerAsync(connStr, "dbo.Transactions");

Console.WriteLine($"Ingested {result.TotalSucceeded} rows into SQL Server.");
```

### 2. Custom `SqlBulkCopyOptions` & Transactions

You can customize `SqlBulkCopyOptions` (e.g. enabling `TableLock`, `KeepIdentity`, or `FireTriggers`) and run the operation under an existing `SqlTransaction`:

```csharp
using FastIngest.SqlServer.Extensions;
using Microsoft.Data.SqlClient;

await using var connection = new SqlConnection(connStr);
await connection.OpenAsync();

await using var transaction = connection.BeginTransaction();

try
{
    var result = await FastIngestPipeline<TransactionRecord>.Create()
        .FromStream(stream, FileType.Csv)
        .WithMapping(m => { /* mappings */ })
        .WriteToSqlServerAsync(
            connection,
            "dbo.Transactions",
            options: SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints,
            ct: CancellationToken.None);

    await transaction.CommitAsync();
}
catch (Exception)
{
    await transaction.RollbackAsync();
    throw;
}
```

### 3. Dependency Injection Setup

Register SQL Server bulk copy sink in your ASP.NET Core application:

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddSqlServerSink(builder.Configuration.GetConnectionString("SqlServer"));
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Performance Best Practices

1. **`SqlBulkCopyOptions.TableLock`**: If your ingestion runs exclusively or in an ETL staging window, enabling `TableLock` significantly reduces locking overhead in SQL Server and switches logging to bulk-logged or minimally logged mode (depending on the database recovery model).
2. **Column Mappings**: FastIngest automatically configures explicit `ColumnMappings.Add(colName, colName)` to avoid column ordinal mismatch issues in schema migrations.
3. **Recovery Model**: For massive one-time backfills, switching the database recovery model to `BULK_LOGGED` or `SIMPLE` drastically reduces transaction log (`LDF`) growth.
