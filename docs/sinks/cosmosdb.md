# Azure Cosmos DB Sink

The Azure Cosmos DB sink enables high-throughput document ingestion into Azure Cosmos DB for NoSQL containers using concurrent task dispatch with SDK bulk execution enabled.

---

## Technical Overview

Azure Cosmos DB's .NET SDK features internal bulk optimization. When `CosmosClientOptions.AllowBulkExecution` is set to `true`, the SDK groups independent point operations destined for the same physical partition into micro-batches, drastically reducing HTTP/TCP overhead and maximizing Request Unit (RU/s) efficiency.

FastIngest's `CosmosDbBulkSink<TRecord>` harnesses this capability by dispatching batches of asynchronous item creations concurrently:

```
┌─────────────────────────────────────────┐
│ FastIngest Row Stream                   │
└────────────────────┬────────────────────┘
                     │ IReadOnlyList<TRecord>
                     ▼
┌─────────────────────────────────────────┐
│ CosmosDbBulkSink<TRecord>               │
│  - PartitionKey Selector Extraction     │
│  - Concurrent CreateItemAsync Tasks     │
│  - Task.WhenAll Concurrent Await        │
└────────────────────┬────────────────────┘
                     │ High-Throughput Bulk Dispatch
                     ▼
┌─────────────────────────────────────────┐
│ Azure Cosmos DB SDK Engine              │
│  - AllowBulkExecution = true            │
│  - Micro-batching per Partition         │
└─────────────────────────────────────────┘
```

---

## Installation

```bash
dotnet add package FastIngest.CosmosDb
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.CosmosDb.Extensions;
using Microsoft.Azure.Cosmos;

await using var stream = File.OpenRead("events.csv");
var connStr = "AccountEndpoint=https://my-account.documents.azure.com:443/;AccountKey=secret;";

var result = await FastIngestPipeline<EventRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.Id, "id");
        m.Map(x => x.TenantId, "tenant_id");
        m.Map(x => x.EventType, "event_type");
        m.Map(x => x.Payload, "payload");
    })
    .WithBatchSize(2500)
    .WriteToCosmosDbAsync(
        connectionString: connStr,
        databaseName: "TelemetryDb",
        containerName: "Events",
        partitionKeySelector: x => x.TenantId);

Console.WriteLine($"Ingested {result.TotalSucceeded} events into Cosmos DB.");
```

### 2. Using an Existing `Container` Instance

```csharp
using FastIngest.CosmosDb.Extensions;
using Microsoft.Azure.Cosmos;

var client = new CosmosClient(connStr, new CosmosClientOptions
{
    AllowBulkExecution = true
});

var container = client.GetContainer("TelemetryDb", "Events");

var result = await FastIngestPipeline<EventRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* mappings */ })
    .WriteToCosmosDbAsync(container, record => new PartitionKey(record.TenantId));
```

### 3. Dependency Injection Setup

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddCosmosDbSink(
        connectionString: builder.Configuration.GetConnectionString("CosmosDb")!,
        databaseName: "TelemetryDb",
        containerName: "Events");

    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Performance Tips

1. **Partition Key Distribution**: Choose a high-cardinality partition key (e.g. `TenantId`, `CustomerId`, or `DeviceId`) to spread write volume evenly across all physical partitions and prevent "hot partition" throttling (HTTP 429).
2. **Provisioned Throughput**: During massive migration windows, temporarily increase container RU/s or enable autoscale to accommodate the incoming stream.
