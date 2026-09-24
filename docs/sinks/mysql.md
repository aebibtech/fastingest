# MySQL & MariaDB Sink

The MySQL sink persists streaming batches into MySQL and MariaDB databases using `MySqlConnector.MySqlBulkCopy` backed by FastIngest's zero-allocation `BatchDataReader<TRecord>`.

---

## Technical Overview

MySQL's native bulk loading relies on the `LOAD DATA LOCAL INFILE` protocol under the hood. FastIngest binds directly to this interface using `MySqlBulkCopy`, bypassing individual row-level `INSERT` statements and network roundtrips.

```
┌────────────────────────────────────────┐
│ FastIngest Ingestion Batch             │
└──────────────────┬─────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────┐
│ BatchDataReader<TRecord> (IDataReader) │
│  - Pre-compiled getter expressions     │
│  - Type-safe column conversion         │
└──────────────────┬─────────────────────┘
                   │ High-speed local stream
                   ▼
┌────────────────────────────────────────┐
│ MySqlConnector.MySqlBulkCopy           │
│  - DestinationTableName                │
│  - Automatic Column Mapping            │
│  - External Transaction Support        │
└────────────────────────────────────────┘
```

---

## Installation

```bash
dotnet add package FastIngest.MySql
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.MySql.Extensions;

await using var stream = File.OpenRead("inventory.csv");
var connStr = "Server=localhost;Database=warehouse;Uid=root;Pwd=secret;AllowLoadLocalInfile=True;";

var result = await FastIngestPipeline<InventoryRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.Sku, "sku");
        m.Map(x => x.Quantity, "quantity");
        m.Map(x => x.WarehouseId, "warehouse_id");
        m.Map(x => x.LastUpdated, "last_updated");
    })
    .WithBatchSize(10_000)
    .WriteToMySqlAsync(connStr, "inventory");

Console.WriteLine($"Ingested {result.TotalSucceeded} items into MySQL.");
```

### 2. Using an Existing Connection & Transaction

```csharp
using FastIngest.MySql.Extensions;
using MySqlConnector;

await using var connection = new MySqlConnection(connStr);
await connection.OpenAsync();

await using var transaction = await connection.BeginTransactionAsync();

try
{
    var result = await FastIngestPipeline<InventoryRecord>.Create()
        .FromStream(stream, FileType.Csv)
        .WithMapping(m => { /* mappings */ })
        .WriteToMySqlAsync(connection, "inventory", transaction);

    await transaction.CommitAsync();
}
catch (Exception)
{
    await transaction.RollbackAsync();
    throw;
}
```

### 3. Dependency Injection Setup

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddMySqlSink(builder.Configuration.GetConnectionString("MySql"));
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Important Configuration: `AllowLoadLocalInfile`

Because `MySqlBulkCopy` leverages MySQL's local file transfer capability, your connection string and MySQL server instance must allow local infile operations:

1. **Connection String**: Append `AllowLoadLocalInfile=True;` to your connection string.
2. **MySQL Server Configuration**: Ensure the MySQL server has `local_infile=ON` set in `my.cnf` or via SQL:
   ```sql
   SET GLOBAL local_infile = 1;
   ```
