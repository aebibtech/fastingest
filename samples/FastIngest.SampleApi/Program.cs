using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Configure OpenAPI specification support for .NET 9
builder.Services.AddOpenApi();

// Register FastIngest dependency injection layer, PostgreSQL sink defaults, and profiles from assembly
builder.Services.AddFastIngest(ingest =>
{
    ingest.AddPostgreSqlSink(builder.Configuration.GetConnectionString("DefaultConnection")
        ?? "Host=localhost;Database=fastingest;Username=postgres;Password=postgres");
    ingest.RegisterProfilesFromAssembly(typeof(Program).Assembly);
});

// Register FluentValidation validator in DI so IFastIngestEngine automatically resolves and applies it
builder.Services.AddScoped<IValidator<CustomerRecord>, CustomerValidator>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Idiomatic Dependency Injection endpoint demonstrating IFastIngestEngine and CustomerImportProfile
app.MapPost("/api/customers/import", async (IFormFile file, IFastIngestEngine engine, CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "No file uploaded or file is empty." });
    }

    // Stream directly from upload without saving file to disk or buffering whole content in memory
    await using var stream = file.OpenReadStream();

    // Use in-memory sink for local demonstration when running without a PostgreSQL instance.
    // In production with PostgreSQL, simply call: await engine.IngestAsync<CustomerRecord>(stream, ct);
    var sink = new InMemorySink<CustomerRecord>();
    var result = await engine.IngestAsync(stream, sink, cancellationToken: ct);

    return Results.Ok(new
    {
        totalProcessed = result.TotalProcessed,
        totalSucceeded = result.TotalSucceeded,
        totalFailed = result.TotalFailed,
        isSuccess = result.IsSuccess,
        errors = result.Errors.Take(100),
        inMemoryCount = sink.Records.Count
    });
})
.WithName("ImportCustomers")
.DisableAntiforgery();

// Sample ingestion endpoint accepting CSV file uploads and streaming records through FastIngest manual pipeline.
app.MapPost("/api/ingest/customers", async (IFormFile file, CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "No file uploaded or file is empty." });
    }

    // Stream directly from upload without saving file to disk or buffering whole content in memory
    await using var stream = file.OpenReadStream();

    // Use in-memory sink for local demonstration (in production, use WriteToPostgresAsync)
    var sink = new InMemorySink<CustomerRecord>();

    // Configure and execute the streaming ingestion pipeline
    var result = await FastIngestPipeline<CustomerRecord>.Create()
        .FromStream(stream, FileType.Csv)
        .WithMapping(mapping =>
        {
            mapping.Map(x => x.Id, "customer_id");
            mapping.Map(x => x.Email, "email");
            mapping.Map(x => x.FullName, "full_name");
            mapping.Map(x => x.Balance, "balance");
        })
        .ValidateWith<CustomerValidator>(options =>
        {
            // Collect invalid records and continue processing valid rows
            options.ErrorStrategy = ErrorStrategy.CollectAndContinue;
        })
        .WithBatchSize(1000)
        .OnProgress(progress =>
        {
            app.Logger.LogInformation("Ingested {Processed} rows (Succeeded: {Succeeded}, Failed: {Failed})",
                progress.RowsProcessed, progress.RowsSucceeded, progress.RowsFailed);
        })
        .WriteToSinkAsync(sink, ct);

    return Results.Ok(new
    {
        totalProcessed = result.TotalProcessed,
        totalSucceeded = result.TotalSucceeded,
        totalFailed = result.TotalFailed,
        isSuccess = result.IsSuccess,
        errors = result.Errors.Take(100),
        inMemoryCount = sink.Records.Count
    });
})
.WithName("IngestCustomers")
.DisableAntiforgery();

// Sample ingestion endpoint accepting JSON Lines (.jsonl / .ndjson) file uploads and streaming records through FastIngest pipeline.
app.MapPost("/api/ingest/customers/jsonl", async (IFormFile file, CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "No file uploaded or file is empty." });
    }

    // Stream directly from upload without saving file to disk or buffering whole content in memory
    await using var stream = file.OpenReadStream();

    // Use in-memory sink for local demonstration
    var sink = new InMemorySink<CustomerRecord>();

    // Configure and execute the streaming ingestion pipeline for JSON Lines
    var result = await FastIngestPipeline<CustomerRecord>.Create()
        .FromStream(stream, file.FileName, FileType.JsonLines)
        .ValidateWith<CustomerValidator>(options =>
        {
            // Collect invalid records and continue processing valid rows
            options.ErrorStrategy = ErrorStrategy.CollectAndContinue;
        })
        .WithBatchSize(1000)
        .OnProgress(progress =>
        {
            app.Logger.LogInformation("Ingested {Processed} JSONL rows (Succeeded: {Succeeded}, Failed: {Failed})",
                progress.RowsProcessed, progress.RowsSucceeded, progress.RowsFailed);
        })
        .WriteToSinkAsync(sink, ct);

    return Results.Ok(new
    {
        totalProcessed = result.TotalProcessed,
        totalSucceeded = result.TotalSucceeded,
        totalFailed = result.TotalFailed,
        isSuccess = result.IsSuccess,
        errors = result.Errors.Take(100),
        inMemoryCount = sink.Records.Count
    });
})
.WithName("IngestCustomersJsonLines")
.DisableAntiforgery();

app.Run();

/// <summary>
/// Domain record representing an imported customer row.
/// </summary>
public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

/// <summary>
/// FastIngest profile configuring column mappings, table destination, and error strategy for customer records.
/// Pre-compiled mapping expressions are cached as singletons. Supports both CSV and JSON Lines automatically.
/// </summary>
public class CustomerImportProfile : FastIngestProfile<CustomerRecord>
{
    public CustomerImportProfile()
    {
        ToTable("customers");
        WithBatchSize(1000);
        WithErrorStrategy(ErrorStrategy.CollectAndContinue);
        WithFileType(FileType.AutoDetect);

        Map(x => x.Id, "customer_id");
        Map(x => x.Email, "email");
        Map(x => x.FullName, "full_name");
        Map(x => x.Balance, "balance");
    }
}

/// <summary>
/// FluentValidation rules applied to each parsed customer record.
/// </summary>
public class CustomerValidator : AbstractValidator<CustomerRecord>
{
    public CustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty().MaximumLength(100);
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}

/// <summary>
/// In-memory sink implementation for demo and testing purposes.
/// </summary>
public class InMemorySink<T> : IIngestionSink<T>
{
    public List<T> Records { get; } = new();

    public Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        Records.AddRange(batch);
        return Task.FromResult((long)batch.Count);
    }
}
