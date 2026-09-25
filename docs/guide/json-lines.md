# Line-Delimited JSON (NDJSON / JSONL) Streaming

FastIngest provides native, high-performance streaming ingestion for line-delimited JSON (**JSON Lines**, also known as **NDJSON** or `.jsonl` / `.ndjson`).

Unlike traditional JSON deserializers that require loading an entire JSON array into memory ($O(N)$ RAM usage), FastIngest uses a constant-memory ($O(1)$) streaming reader based on `System.IO.Pipelines.PipeReader` and `System.Text.Json.Utf8JsonReader`. Each line is sliced and deserialized directly from the underlying stream as raw byte spans, validated, and pushed into the concurrent bounded channel pipeline.

---

## What is JSON Lines / NDJSON?

JSON Lines is a plain-text format where each line represents a valid, independent JSON object separated by a newline character (`\n` or `\r\n`):

```json
{"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
{"id": 2, "email": "bob@example.com", "fullName": "Bob Jones", "balance": 200.00}
{"id": 3, "email": "carol@example.com", "fullName": "Carol White", "balance": 99.99}
```

Key advantages of JSON Lines for bulk data pipelines:
- **Streamable**: Records can be parsed incrementally without waiting for an end-of-array token (`]`).
- **Resilient**: A malformed record on line $N$ can be isolated and quarantined without discarding the rest of the file.
- **Append-Friendly**: Loggers, message queues, and export jobs can continuously append records to a file without re-serializing.

---

## Streaming Architecture

```
Stream (.jsonl / .ndjson)
        │
        ▼
┌─────────────────────────────────┐
│ Producer: PipeReader Slicer     │  <-- Zero-allocation \n buffer slicing
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: Utf8JsonReader        │  <-- Memory<byte> span deserialization (CPU)
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Producer: FluentValidation      │  <-- FailFast or CollectAndContinue
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ System.Threading.Channels       │  <-- Bounded channel (Capacity: 2 batches)
│ (BoundedChannelFullMode.Wait)   │  <-- Backpressure: strict O(1) memory
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Consumer: Native Database Sink  │  <-- High-speed batch streaming (I/O)
└─────────────────────────────────┘
```

1. **`PipeReader` Line Slicing**: Slices line sequences asynchronously across buffer segments without allocating intermediate `string` objects.
2. **`Utf8JsonReader`**: Reads `ReadOnlySequence<byte>` buffers directly and deserializes into `TRecord` using `JsonSerializer.Deserialize<TRecord>(ref utf8JsonReader, options)`.
3. **CRLF & Whitespace Handling**: Automatically normalizes Windows CRLF (`\r\n`), ignores empty or whitespace-only lines, and verifies single-record integrity per line.
4. **Bounded Channel Backpressure**: Batches valid records into `System.Threading.Channels` while keeping memory consumption bounded.

---

## Quickstart: Streaming JSON Lines Pipeline

```csharp
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

await using var stream = File.OpenRead("customers.jsonl");
await using var connection = new NpgsqlConnection("Host=localhost;Database=mydb;Username=postgres;Password=secret");
await connection.OpenAsync();

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.JsonLines) // Or FileType.Ndjson
    .ValidateWith<CustomerValidator>(options =>
    {
        options.ErrorStrategy = ErrorStrategy.CollectAndContinue;
    })
    .WithBatchSize(5000)
    .WithChannelCapacity(2)
    .OnProgress(progress =>
    {
        Console.WriteLine($"Processed {progress.RowsProcessed:N0} rows...");
    })
    .WriteToPostgresAsync(connection, "customers");

Console.WriteLine($"Ingested: {result.TotalSucceeded:N0} | Failed: {result.TotalFailed:N0}");
```

---

## Automatic File Format Detection

FastIngest can automatically infer whether an incoming stream is CSV or JSON Lines:

```csharp
// 1. Explicit parameter
pipeline.FromStream(stream, FileType.JsonLines);

// 2. Extension heuristic via FromStream overload
pipeline.FromStream(stream, "data.jsonl", FileType.AutoDetect);

// 3. Direct file path heuristic
pipeline.FromFile("data.ndjson", FileType.AutoDetect);

// 4. Content inspection heuristic (seekable streams)
// If the first non-whitespace character is '{', FastIngest automatically selects FileType.JsonLines.
pipeline.FromStream(stream, FileType.AutoDetect);
```

### Detection Heuristic Priority
1. **Explicit Setting**: If `FileType.JsonLines`, `FileType.Ndjson`, or `FileType.Csv` is explicitly configured, it is used immediately.
2. **File Extension**: Evaluates `.jsonl` or `.ndjson` from file paths, upload file names, or `FileStream.Name`.
3. **Content Inspection**: For seekable streams (`stream.CanSeek == true`), inspects the first non-whitespace byte (skipping UTF-8 BOM if present). If `{` is detected, `FileType.JsonLines` is selected.
4. **Default Fallback**: Reverts to `FileType.Csv`.

---

## Customizing JSON Serializer Options

By default, FastIngest enables case-insensitive property matching (`PropertyNameCaseInsensitive = true`) so JSON properties matching `camelCase`, `snake_case`, or `PascalCase` deserialize smoothly.

You can customize `JsonSerializerOptions` using `.WithJsonOptions(...)`:

```csharp
using System.Text.Json;

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.JsonLines)
    .WithJsonOptions(options =>
    {
        options.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
        options.AllowTrailingCommas = true;
        options.ReadCommentHandling = JsonCommentHandling.Skip;
    })
    .WriteToPostgresAsync(connection, "customers");
```

Alternatively, configure options on `PipelineOptions`:

```csharp
pipeline.WithOptions(opt =>
{
    opt.JsonSerializerOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
});
```

---

## Error Handling & Quarantine

When parsing JSON Lines, malformed JSON syntax or schema mismatches (e.g. string supplied for an integer property) are captured with exact line numbers and the offending JSON snippet.

### 1. `ErrorStrategy.FailFast`
Immediately halts the pipeline, cancels in-flight batches, and throws `FastIngestValidationException`:

```csharp
try
{
    await pipeline
        .ValidateWith<CustomerValidator>(opt => opt.ErrorStrategy = ErrorStrategy.FailFast)
        .WriteToPostgresAsync(connection, "customers");
}
catch (FastIngestValidationException ex)
{
    var err = ex.Errors[0];
    Console.WriteLine($"Syntax/validation error on line {err.RowIndex}!");
    Console.WriteLine($"Offending snippet: {err.AttemptedValue}");
    Console.WriteLine($"Reason: {err.ErrorMessage}");
}
```

### 2. `ErrorStrategy.CollectAndContinue`
Quarantines the invalid line, logs an `IngestRowError`, and continues processing remaining rows:

```csharp
var result = await pipeline
    .ValidateWith<CustomerValidator>(opt => opt.ErrorStrategy = ErrorStrategy.CollectAndContinue)
    .WriteToPostgresAsync(connection, "customers");

if (!result.IsSuccess)
{
    Console.WriteLine($"Failed rows: {result.TotalFailed}");

    // Export error manifest with line numbers and snippets to CSV
    byte[] errorReport = result.ExportErrorsToCsv();
    await File.WriteAllBytesAsync("errors.csv", errorReport);
}
```

---

## Dependency Injection & Ingestion Profiles

In ASP.NET Core applications using `FastIngest.Extensions.DependencyInjection`, define a profile with `WithFileType(FileType.JsonLines)` and custom options:

```csharp
using FastIngest.Core.Common;
using FastIngest.Extensions.DependencyInjection.Profiles;

public class CustomerJsonLinesProfile : FastIngestProfile<CustomerRecord>
{
    public CustomerJsonLinesProfile()
    {
        ToTable("customers");
        WithBatchSize(5000);
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.JsonLines);
        WithJsonOptions(new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });
    }
}
```

Inject `IFastIngestEngine` into your Minimal API:

```csharp
app.MapPost("/api/customers/import-jsonl", async (
    IFormFile file,
    IFastIngestEngine engine,
    CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "Empty file." });
    }

    await using var stream = file.OpenReadStream();
    var result = await engine.IngestAsync<CustomerRecord>(stream, cancellationToken: ct);

    return Results.Ok(new
    {
        processed = result.TotalProcessed,
        succeeded = result.TotalSucceeded,
        failed = result.TotalFailed
    });
});
```
