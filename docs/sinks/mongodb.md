# MongoDB Sink (Unordered BulkWrite)

The MongoDB sink provides high-speed document ingestion into MongoDB replica sets, sharded clusters, and MongoDB Atlas using unordered `BulkWriteAsync` batches.

---

## Technical Overview

Inserting documents one-by-one via `InsertOneAsync` imposes substantial roundtrip latency over the network and serializes write operations on the primary replica.

FastIngest's `MongoDbBulkSink<TRecord>` chunks incoming streams into batches of `InsertOneModel<TRecord>` and sends them via `BulkWriteAsync`:

```
┌─────────────────────────────────┐
│ FastIngest Row Stream           │
└──────────────┬──────────────────┘
               │ IReadOnlyList<TRecord>
               ▼
┌─────────────────────────────────┐
│ MongoDbBulkSink<TRecord>        │
│   - Creates InsertOneModel<T>   │
│   - Sets IsOrdered = false      │
│   - Dispatches BulkWriteAsync   │
└──────────────┬──────────────────┘
               │ Wire Protocol Batch Chunks
               ▼
┌─────────────────────────────────┐
│ MongoDB Replica / Shard Cluster │
│   - Parallel Document Inserts   │
│   - Maximized IOPS Utilization  │
└─────────────────────────────────┘
```

### Unordered Bulk Writes (`IsOrdered = false`)

By default, FastIngest configures `BulkWriteOptions { IsOrdered = false }`. In unordered mode, MongoDB executes inserts concurrently across shards and data nodes, rather than waiting for preceding documents in the batch to commit. If an individual document fails a schema validation or unique index constraint, the remaining documents in the batch continue inserting.

---

## Installation

```bash
dotnet add package FastIngest.MongoDb
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.MongoDb.Extensions;
using MongoDB.Driver;

await using var stream = File.OpenRead("catalog_products.csv");
var connStr = "mongodb://localhost:27017";

var result = await FastIngestPipeline<ProductRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.Sku, "sku");
        m.Map(x => x.Title, "title");
        m.Map(x => x.Price, "price");
        m.Map(x => x.Category, "category");
    })
    .WithBatchSize(5000)
    .WriteToMongoDbAsync(connStr, "ecommerce", "products");

Console.WriteLine($"Ingested {result.TotalSucceeded} products into MongoDB.");
```

### 2. Using an Existing `IMongoCollection<TRecord>`

```csharp
using FastIngest.MongoDb.Extensions;
using MongoDB.Driver;

var client = new MongoClient("mongodb://localhost:27017");
var collection = client.GetDatabase("ecommerce").GetCollection<ProductRecord>("products");

var result = await FastIngestPipeline<ProductRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* mappings */ })
    .WriteToMongoDbAsync(collection, options =>
    {
        // Custom bulk write configuration
        options.IsOrdered = false;
        options.BypassDocumentValidation = false;
    });
```

### 3. Dependency Injection Setup

Register MongoDB sink in your ASP.NET Core service setup:

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddMongoDbSink(
        connectionString: builder.Configuration.GetConnectionString("MongoDb")!,
        databaseName: "ecommerce");

    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Performance Best Practices

1. **Document `_id` Generation**: If your model defines an `[BsonId]` property, ensure it is populated or use MongoDB's default `ObjectId` generation to avoid server-side ID collisions.
2. **Chunk Sizing**: A batch size of **2,500** to **5,000** records is optimal to remain well within MongoDB's 16MB BSON wire protocol message limit.
