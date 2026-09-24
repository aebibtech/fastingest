# Elasticsearch Sink

The Elasticsearch sink provides high-speed document indexing into Elasticsearch indices using the official `Elastic.Clients.Elasticsearch` client and NDJSON `BulkAsync` operations.

---

## Technical Overview

Indexing individual documents via `client.IndexAsync(document)` incurs massive HTTP roundtrip overhead. Elasticsearch provides the `_bulk` API endpoint designed to accept NDJSON (Newline Delimited JSON) streams containing multiple action/document pairs in a single HTTP request.

FastIngest's `ElasticsearchBulkSink<TRecord>` translates incoming pipeline chunks into `BulkRequest` operations containing `BulkIndexOperation<TRecord>` entries:

```
┌─────────────────────────────────────────┐
│ FastIngest Row Stream                   │
└────────────────────┬────────────────────┘
                     │ IReadOnlyList<TRecord>
                     ▼
┌─────────────────────────────────────────┐
│ ElasticsearchBulkSink<TRecord>          │
│  - Optional IdSelector Extraction       │
│  - Creates BulkIndexOperation<T>        │
│  - Dispatches BulkAsync(...)            │
└────────────────────┬────────────────────┘
                     │ NDJSON HTTP Payload (Gzip supported)
                     ▼
┌─────────────────────────────────────────┐
│ Elasticsearch Cluster                   │
│  - Primary Shard Ingestion Routing      │
│  - Lucene Inverted Index Flush          │
└─────────────────────────────────────────┘
```

---

## Installation

```bash
dotnet add package FastIngest.Elasticsearch
```

---

## Usage Examples

### 1. Fluent Ingestion Pipeline

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.Elasticsearch.Extensions;

await using var stream = File.OpenRead("log_events.csv");
var endpoint = new Uri("https://es-cluster.internal:9200");
var apiKey = "my_api_key_secret";

var result = await FastIngestPipeline<LogEventRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.EventId, "event_id");
        m.Map(x => x.Message, "message");
        m.Map(x => x.LogLevel, "log_level");
        m.Map(x => x.Timestamp, "timestamp");
    })
    .WithBatchSize(5000)
    .WriteToElasticsearchAsync(
        endpoint: endpoint,
        indexName: "application-logs-2026",
        apiKey: apiKey,
        idSelector: x => x.EventId);

Console.WriteLine($"Indexed {result.TotalSucceeded} logs in Elasticsearch.");
```

### 2. Using an Existing `ElasticsearchClient`

```csharp
using Elastic.Clients.Elasticsearch;
using FastIngest.Elasticsearch.Extensions;

var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
    .CertificateFingerprint("xx:xx:xx...")
    .Authentication(new BasicAuthentication("elastic", "changeme"));

var client = new ElasticsearchClient(settings);

var result = await FastIngestPipeline<LogEventRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* mappings */ })
    .WriteToElasticsearchAsync(
        client: client,
        indexName: "application-logs",
        idSelector: x => x.EventId);
```

### 3. Dependency Injection Setup

```csharp
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddElasticsearchSink(
        endpoint: new Uri(builder.Configuration["Elasticsearch:Uri"]!),
        apiKey: builder.Configuration["Elasticsearch:ApiKey"],
        defaultIndex: "application-logs");

    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});
```

---

## Performance Best Practices

1. **Refresh Interval**: When performing massive historical data loads, temporarily disable index refresh (`"refresh_interval": "-1"`) and reset it to `"1s"` once ingestion finishes.
2. **Replica Count**: Setting `"number_of_replicas": 0` during the bulk load avoids redundant replica synchronization over the network.
3. **Chunk Sizing**: A batch size of **2,500** to **5,000** records is optimal to balance HTTP payload size and Elasticsearch node queue capacity.
