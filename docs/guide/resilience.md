# Resilience & Retry Policies

In production systems, bulk data ingestion pipelines often encounter transient infrastructure hiccups: brief network timeouts, socket disconnects, deadlocks during peak database load, or connection resets in cloud environments like AWS RDS or Azure SQL.

FastIngest provides built-in, opt-in resilience powered by **`Microsoft.Extensions.Resilience`** and **Polly v8**. When configured, transient errors occurring during destination sink batch writes (`WriteBatchAsync`) are automatically retried with exponential backoff and decorrelated jitter before failing.

---

## Quickstart: Enabling Resilience

Enable transient fault retries directly on your ingestion pipeline using `.WithResilience()`:

```csharp
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql;

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithBatchSize(5000)
    .WithResilience(options =>
    {
        options.MaxRetryAttempts = 3;
        options.BaseDelay = TimeSpan.FromMilliseconds(500);
        options.UseJitter = true;
    })
    .WriteToPostgresAsync(connection, "customers");
```

Calling `WithResilience(...)` automatically activates retry handling (`Enabled = true`).

---

## Configuration Options

The `ResilienceOptions` class exposes parameters for tuning backoff timing and retry behavior:

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `Enabled` | `bool` | `false` | Enables or disables the retry policy for the pipeline. |
| `MaxRetryAttempts` | `int` | `3` | Maximum number of retry attempts per batch write before giving up. |
| `BaseDelay` | `TimeSpan` | `500ms` | Initial delay applied on the first retry. Doubles per attempt with exponential backoff. |
| `MaxDelay` | `TimeSpan` | `30s` | Maximum upper limit cap for retry backoff delays. |
| `UseJitter` | `bool` | `true` | Adds random jitter to delay intervals to prevent thundering herd problems on destination clusters. |
| `ShouldRetry` | `Func<Exception, bool>?` | `null` | Custom predicate for classifying exceptions as transient. When `null`, uses built-in heuristics. |

---

## Built-in Transient Fault Detection

When `ShouldRetry` is not specified, FastIngest utilizes `TransientFaultPredicates.IsTransient` to automatically identify transient errors across major database drivers:

- **General I/O**: `TimeoutException`
- **PostgreSQL (`Npgsql`)**: Exceptions flagged with `IsTransient = true`
- **SQL Server / Azure SQL (`Microsoft.Data.SqlClient`)**:
  - `1205`: Deadlock victim
  - `-2`: Query/execution timeout
  - `233`, `10054`, `10060`: Broken connections or socket resets
  - `4060`, `40197`, `40501`, `40613`, `49918`, `49919`, `49920`: Azure SQL transient throttling and failovers
- **MySQL / MariaDB (`MySqlConnector`)**:
  - Connection faults with SQLState class `08xxx`
  - Exceptions flagged with `IsTransient = true`

---

## Custom Retry Predicates

If you need application-specific fault classification, supply a custom delegate to `ShouldRetry`:

```csharp
.WithResilience(options =>
{
    options.MaxRetryAttempts = 5;
    options.BaseDelay = TimeSpan.FromSeconds(1);
    options.ShouldRetry = ex => ex switch
    {
        TimeoutException => true,
        Npgsql.NpgsqlException npgEx => npgEx.IsTransient,
        Microsoft.Data.SqlClient.SqlException sqlEx => sqlEx.Number == 1205, // Only retry deadlocks
        _ => false
    };
})
```

---

## Monitoring Retries via Progress Telemetry

When a transient error occurs and a retry is scheduled, FastIngest emits an informational message through the `OnProgress` callback. You can log or inspect retry notifications in real time:

```csharp
var pipeline = FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithResilience(options =>
    {
        options.MaxRetryAttempts = 3;
    })
    .OnProgress(progress =>
    {
        if (!string.IsNullOrEmpty(progress.Message))
        {
            logger.LogWarning("Pipeline retry notification: {Message}", progress.Message);
        }
    });
```

---

## What Is (and Isn't) Retried

| Fault Category | Retried? | Rationale |
| :--- | :--- | :--- |
| **Transient Database Sockets / Deadlocks** |  **Yes** | Network/concurrency errors are temporary and typically succeed upon backoff. |
| **Parsing / CSV Format Errors** | ❌ **No** | Malformed rows are deterministic; retrying will never resolve syntax issues. |
| **FluentValidation Violations** | ❌ **No** | Handled deterministically via `ValidationOptions.ErrorStrategy`. |
| **Permanent Database Schema Errors** | ❌ **No** | Missing columns, syntax errors, or primary key duplicates are not transient. |
