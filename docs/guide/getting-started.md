# Quickstart Guide

This guide walks you through installing FastIngest and executing your first bulk data import pipeline in under 5 minutes.

---

## 1. Installation

Install the core FastIngest package alongside the database sink of your choice via the .NET CLI or Package Manager Console:

::: code-group

```bash [PostgreSQL]
dotnet add package FastIngest.Core
dotnet add package FastIngest.PostgreSql
```

```bash [SQL Server]
dotnet add package FastIngest.Core
dotnet add package FastIngest.SqlServer
```

```bash [MySQL]
dotnet add package FastIngest.Core
dotnet add package FastIngest.MySql
```

```bash [SQLite]
dotnet add package FastIngest.Core
dotnet add package FastIngest.Sqlite
```

```bash [MongoDB]
dotnet add package FastIngest.Core
dotnet add package FastIngest.MongoDb
```

```bash [Cosmos DB]
dotnet add package FastIngest.Core
dotnet add package FastIngest.CosmosDb
```

```bash [Elasticsearch]
dotnet add package FastIngest.Core
dotnet add package FastIngest.Elasticsearch
```

:::

---

## 2. Define Your Record & Validator

Create a strongly typed record representing each row in your input data, and optionally define validation rules using FluentValidation:

```csharp
using FluentValidation;

// Define your target model
public record CustomerRecord(int Id, string Email, string FullName, decimal Balance);

// Define validation rules
public class CustomerValidator : AbstractValidator<CustomerRecord>
{
    public CustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty().MaximumLength(100);
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}
```

---

## 3. Run Ingestion Pipeline (10-Line Example)

Use `FastIngestPipeline<TRecord>.Create()` to stream the file straight from disk or network into your database:

```csharp
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

await using var stream = File.OpenRead("customers.csv");
await using var connection = new NpgsqlConnection("Host=localhost;Database=mydb;Username=postgres;Password=secret");
await connection.OpenAsync();

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => {
        m.Map(x => x.Id, "customer_id");
        m.Map(x => x.Email, "email");
        m.Map(x => x.FullName, "full_name");
        m.Map(x => x.Balance, "balance");
    })
    .ValidateWith<CustomerValidator>(opt => opt.ErrorStrategy = ErrorStrategy.CollectAndContinue)
    .WithBatchSize(5000)
    .OnProgress(p => Console.WriteLine($"Processed {p.RowsProcessed} rows ({p.PercentComplete:F1}%)..."))
    .WriteToPostgresAsync(connection, "customers");

Console.WriteLine($"Done! Succeeded: {result.TotalSucceeded:N0}, Failed: {result.TotalFailed:N0}");
```

---

## 4. Inspecting Ingestion Results

The pipeline returns an `IngestResult` object containing comprehensive execution metrics:

```csharp
if (!result.IsSuccess)
{
    Console.WriteLine($"Encountered {result.TotalFailed} invalid records!");

    // Iterate through validation errors
    foreach (var error in result.Errors.Take(10))
    {
        Console.WriteLine($"Row #{error.RowNumber}: Field '{error.PropertyName}' -> {error.ErrorMessage}");
    }

    // Export all failed rows and errors as a downloadable CSV manifest
    byte[] errorCsvBytes = result.ExportErrorsToCsv();
    await File.WriteAllBytesAsync("ingestion_errors.csv", errorCsvBytes);
}
else
{
    Console.WriteLine($"Successfully ingested {result.TotalSucceeded:N0} rows in {result.Duration.TotalSeconds:F2}s");
}
```

---

## Next Steps

- Explore [Dependency Injection & ASP.NET Core API Integration](/guide/dependency-injection)
- Learn about [Validation Strategies & Error Manifests](/guide/validation)
- Check out the specific guide for your database in [Database Sinks](/sinks/postgresql)
