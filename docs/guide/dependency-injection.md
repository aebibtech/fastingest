# Dependency Injection & ASP.NET Core

FastIngest provides enterprise-grade Dependency Injection via the `FastIngest.Extensions.DependencyInjection` package. This enables you to pre-compile mapping expressions, register ingestion profiles, and inject `IFastIngestEngine` directly into your Minimal API endpoints or background workers.

---

## 1. Installation

Install the Dependency Injection package:

```bash
dotnet add package FastIngest.Extensions.DependencyInjection
```

---

## 2. Defining an Ingestion Profile

Ingestion profiles encapsulate destination tables, column mappings, batch sizes, and error strategies for a specific record type. Pre-compiled mapping expressions are registered as singletons in memory, avoiding redundant compilation per request.

```csharp
using FastIngest.Core.Common;
using FastIngest.Extensions.DependencyInjection.Profiles;

public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

public class CustomerImportProfile : FastIngestProfile<CustomerRecord>
{
    public CustomerImportProfile()
    {
        ToTable("customers");
        WithBatchSize(5000);
        WithChannelCapacity(3); // Keep up to 3 batches in flight concurrently
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.Csv);

        // Map model properties to tabular column headers
        Map(x => x.Id, "customer_id");
        Map(x => x.Email, "email");
        Map(x => x.FullName, "full_name");
        Map(x => x.Balance, "balance");
    }
}
```

---

## 3. Registering Services in `Program.cs`

In your ASP.NET Core application, register FastIngest using `builder.Services.AddFastIngest`:

```csharp
using FastIngest.Extensions.DependencyInjection;
using FluentValidation;

var builder = WebApplication.CreateBuilder(args);

// Register FastIngest with default database sink and profile discovery
builder.Services.AddFastIngest(ingest =>
{
    // Tune default channel capacity and batch sizes
    ingest.ChannelCapacity = 3;
    ingest.DefaultBatchSize = 5000;

    // Configure default PostgreSQL connection string
    ingest.AddPostgreSqlSink(builder.Configuration.GetConnectionString("DefaultConnection")
        ?? "Host=localhost;Database=mydb;Username=postgres;Password=secret");

    // Automatically scan and register all FastIngestProfile classes in the assembly
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});

// Register FluentValidation validator (IFastIngestEngine automatically resolves it)
builder.Services.AddScoped<IValidator<CustomerRecord>, CustomerValidator>();

var app = builder.Build();
```

### Profile Registration Options

You can register profiles individually or discover them automatically:

```csharp
builder.Services.AddFastIngest(ingest =>
{
    // Register individual profile explicitly
    ingest.RegisterProfile<CustomerRecord, CustomerImportProfile>();

    // Or register all profiles found in target assemblies
    ingest.RegisterProfilesFromAssembly(typeof(CustomerImportProfile).Assembly);
    ingest.RegisterProfilesFromAssemblies(typeof(Program).Assembly, typeof(OtherProfile).Assembly);
});
```

---

## 4. Injecting `IFastIngestEngine` into Minimal APIs

Stream uploaded files directly from client HTTP requests into your database without buffering the whole file in RAM or saving temporary files to disk:

```csharp
using FastIngest.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Mvc;

app.MapPost("/api/customers/import", async (
    IFormFile file,
    IFastIngestEngine engine,
    CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "No file uploaded or file is empty." });
    }

    // Stream directly from HTTP request stream with constant O(1) memory
    await using var stream = file.OpenReadStream();

    var result = await engine.IngestAsync<CustomerRecord>(
        stream,
        onProgress: progress =>
        {
            app.Logger.LogInformation("Processed {Count} rows ({Percent:F1}%)",
                progress.RowsProcessed, progress.PercentComplete);
        },
        cancellationToken: ct);

    if (!result.IsSuccess)
    {
        // Return summary and top errors
        return Results.UnprocessableEntity(new
        {
            success = false,
            totalProcessed = result.TotalProcessed,
            totalSucceeded = result.TotalSucceeded,
            totalFailed = result.TotalFailed,
            durationMs = result.Duration.TotalMilliseconds,
            errors = result.Errors.Take(50)
        });
    }

    return Results.Ok(new
    {
        success = true,
        totalProcessed = result.TotalProcessed,
        totalSucceeded = result.TotalSucceeded,
        durationMs = result.Duration.TotalMilliseconds
    });
})
.WithName("ImportCustomers")
.DisableAntiforgery();
```

---

## 5. Downloading Error Manifest CSV from API

If you configure `ErrorStrategy.CollectAndContinue`, users can download the rejected rows with exact cell error reasons:

```csharp
app.MapPost("/api/customers/import-with-report", async (
    IFormFile file,
    IFastIngestEngine engine,
    CancellationToken ct) =>
{
    await using var stream = file.OpenReadStream();
    var result = await engine.IngestAsync<CustomerRecord>(stream, cancellationToken: ct);

    if (result.TotalFailed > 0)
    {
        byte[] csvReport = result.ExportErrorsToCsv();
        return Results.File(csvReport, "text/csv", "invalid_records_report.csv");
    }

    return Results.Ok(new { message = $"All {result.TotalSucceeded} rows imported successfully!" });
});
```
