# ASP.NET Core & SignalR Integration

FastIngest provides native, drop-in integration for ASP.NET Core minimal APIs and SignalR. The `FastIngest.AspNetCore` package allows you to mount bulk ingestion endpoints that accept file uploads (`multipart/form-data`) and offload the actual streaming and database insertion to a background worker. It also wires up a SignalR Hub so connecting clients can monitor the exact progress and completion of their upload in real-time.

## Installation

```bash
dotnet add package FastIngest.AspNetCore
```

## Basic Setup

In your `Program.cs`, register the ASP.NET Core integration along with your chosen database sink:

```csharp
using FastIngest.AspNetCore.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Register ASP.NET Core services (adds SignalR, background queues, and engines)
builder.Services.AddFastIngestAspNetCore(ingest =>
{
    // Register your preferred sink
    ingest.AddPostgreSqlSink("Host=localhost;Database=mydb;Username=postgres;Password=secret");
    
    // Scan assembly for FastIngestProfile configurations
    ingest.RegisterProfilesFromAssemblyContaining<CustomerImportProfile>();
});

var app = builder.Build();

// 1. Map the Real-Time SignalR Hub
app.MapFastIngestHub("/hubs/fastingest");

// 2. Map the minimal API bulk upload endpoint
app.MapFastIngestUpload<CustomerRecord>("/api/customers/bulk-upload");

app.Run();
```

## How It Works

When a client POSTs a file to `/api/customers/bulk-upload`:

1. **Staging:** The endpoint safely copies the `IFormFile` stream to a temporary location on disk.
2. **Background Queue:** An `IngestJob` is enqueued into a high-performance in-memory `System.Threading.Channels` queue.
3. **Immediate Response:** The endpoint immediately returns a `202 Accepted` JSON response containing a unique `JobId`:
   ```json
   {
     "jobId": "4a7b5d92f1...",
     "message": "File accepted and ingestion queued for background processing.",
     "fileName": "large_customers.csv",
     "byteCount": 10485760
   }
   ```
4. **Processing & Real-time Telemetry:** A `BackgroundService` picks up the job, opens the temporary file, and streams it using the native database sink. It continuously broadcasts progress (`RowsProcessed`, `RowsSucceeded`, `RowsFailed`) to any SignalR clients subscribed to the `JobId` group.
5. **Cleanup:** Once complete, the background service cleans up the temporary file automatically.

## Listening for Progress via SignalR

Frontend clients (e.g. React, Angular, Vue, or Vanilla JS) can use the `@microsoft/signalr` package to subscribe to updates.

By generating a `JobId` on the client side and passing it in the query string (`?jobId=123`) or headers (`X-Job-Id`), you can ensure the client subscribes to the SignalR hub *before* starting the upload, preventing race conditions on small files.

### Example: JavaScript Client

```javascript
import * as signalR from "@microsoft/signalr";

const jobId = crypto.randomUUID().replace(/-/g, "");

// 1. Connect to the Hub
const connection = new signalR.HubConnectionBuilder()
    .withUrl("/hubs/fastingest")
    .build();

// Listen for progress events
connection.on("ReceiveProgress", (id, progress) => {
    console.log(`Job ${id} Progress: ${progress.percentComplete}%`);
    console.log(`Processed: ${progress.rowsProcessed}`);
});

// Listen for completion
connection.on("ReceiveCompletion", (id, processed, succeeded, failed) => {
    console.log(`Job ${id} completed! Succeeded: ${succeeded}`);
    connection.stop(); // Stop connection when done
});

await connection.start();

// 2. Join the job group using the ID we generated
await connection.invoke("JoinJob", jobId);

// 3. Start the upload via HTTP POST, passing the JobId
const formData = new FormData();
formData.append("file", fileInputElement.files[0]);

const response = await fetch(`/api/customers/bulk-upload?jobId=${jobId}`, {
    method: "POST",
    body: formData
});

if (response.status === 202) {
    console.log("Upload accepted. Waiting for SignalR telemetry...");
}
```

## Advanced Scenarios

### Table Name Overrides

If you want to route uploads to different tables dynamically without creating multiple profiles, `MapFastIngestUpload` accepts a `tableName` override parameter:

```csharp
app.MapFastIngestUpload<CustomerRecord>("/api/customers/import-staging", tableName: "customers_staging");
```

### Custom Sinks

If you wrote a custom sink implementing `IIngestionSink<TRecord>`, you can explicitly map the endpoint to resolve and write to that specific sink using the generic overload:

```csharp
app.MapFastIngestUpload<CustomerRecord, MyCustomAnalyticsSink>("/api/customers/analytics");
```
