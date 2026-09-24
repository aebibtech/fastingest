---
layout: home

hero:
  name: "FastIngest"
  text: "Zero-Allocation Bulk Ingestion Pipeline for .NET"
  tagline: "Stream CSV & Excel files straight into PostgreSQL, SQL Server, MongoDB, Cosmos, and Elasticsearch with zero memory bloat and built-in validation."
  image:
    src: /logo.svg
    alt: FastIngest Logo
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/aebibtech/fastingest

features:
  - icon: ⚡
    title: Constant Memory (O(1))
    details: Stream 10GB+ CSV and Excel datasets with a completely flat memory footprint. Built on top of Sylvan's zero-allocation streaming parser to eliminate GC pressure.
  - icon: 🔄
    title: Producer-Consumer Channels
    details: Decouples CPU stream parsing and validation from database socket operations using System.Threading.Channels with bounded backpressure.
  - icon: 🗄️
    title: 7 High-Speed Sinks
    details: Native database integrations including PostgreSQL binary COPY, SQL Server SqlBulkCopy, MySQL BulkCopy, SQLite WAL batching, MongoDB unordered writes, Cosmos DB, and Elasticsearch.
  - icon: 🚀
    title: Zero-Reflection Mapping
    details: Pre-compiled lambda expression getters and property binders. Maximum serialization throughput without runtime reflection overhead.
  - icon: 🛡️
    title: Clean Error Manifests
    details: Seamless FluentValidation integration with FailFast or CollectAndContinue strategies. Export invalid rows and validation errors directly to CSV manifests.
---

<div class="home-code-section" style="margin-top: 3rem; text-align: left;">

## Lightning-Fast Streaming Ingestion

Ingesting millions of records shouldn't cause `OutOfMemoryException` or choke your database connection pool. FastIngest streams records chunk-by-chunk directly into native bulk protocols.

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

await using var stream = File.OpenRead("transactions_10m.csv");
await using var conn = new NpgsqlConnection(connectionString);
await conn.OpenAsync();

var result = await FastIngestPipeline<TransactionRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(map =>
    {
        map.Map(x => x.Id, "transaction_id");
        map.Map(x => x.AccountId, "account_id");
        map.Map(x => x.Amount, "amount");
        map.Map(x => x.Timestamp, "created_at");
    })
    .ValidateWith<TransactionValidator>()
    .WithBatchSize(10_000)
    .WithChannelCapacity(2)
    .WriteToPostgresAsync(conn, "transactions");

Console.WriteLine($"Ingested {result.TotalSucceeded:N0} rows in {result.Duration.TotalSeconds:F1}s!");
```

## Supported Destination Sinks

| Database / Sink | Protocol / Engine | Typical Throughput | Memory Footprint |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | Native Binary `COPY FROM STDIN` | 180,000+ rows/sec | Constant (~22 MB) |
| **SQL Server / Azure SQL** | Streaming `SqlBulkCopy` + `BatchDataReader` | 140,000+ rows/sec | Constant (~26 MB) |
| **MySQL / MariaDB** | `MySqlBulkCopy` Local Infile / Batched Insert | 110,000+ rows/sec | Constant (~24 MB) |
| **SQLite** | Parameterized Batch Loop + WAL Mode | 95,000+ rows/sec | Constant (~18 MB) |
| **MongoDB** | Unordered `BulkWriteAsync` (`InsertOneModel`) | 85,000+ rows/sec | Constant (~30 MB) |
| **Azure Cosmos DB** | Concurrent Dispatch + `AllowBulkExecution` | 35,000+ docs/sec | Constant (~32 MB) |
| **Elasticsearch** | `BulkAsync` (`IndexOperation`) NDJSON Chunking | 65,000+ docs/sec | Constant (~35 MB) |

</div>
