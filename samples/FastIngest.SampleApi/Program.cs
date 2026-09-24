using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Ingest CSV endpoint demonstration
app.MapPost("/api/ingest/customers", async (IFormFile file, CancellationToken ct) =>
{
    if (file == null || file.Length == 0)
    {
        return Results.BadRequest(new { message = "No file uploaded." });
    }

    await using var stream = file.OpenReadStream();
    var sink = new InMemorySink<CustomerRecord>();

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

app.Run();

public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

public class CustomerValidator : AbstractValidator<CustomerRecord>
{
    public CustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty().MaximumLength(100);
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}

public class InMemorySink<T> : IIngestionSink<T>
{
    public List<T> Records { get; } = new();

    public Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        Records.AddRange(batch);
        return Task.FromResult((long)batch.Count);
    }
}
