# Validation & Error Handling

FastIngest provides built-in integration with **FluentValidation** to ensure data integrity during bulk ingestion. Unlike typical importers that either silently skip bad rows or discard entire imports upon encountering a single error, FastIngest offers two distinct, configurable strategies: **FailFast** and **CollectAndContinue**.

---

## Defining Validation Rules

FastIngest accepts standard FluentValidation `AbstractValidator<TRecord>` classes. Because validators are executed on strongly typed records parsed by the pipeline, you have access to the full suite of FluentValidation rules:

```csharp
using FluentValidation;

public record OrderRecord(
    string OrderId,
    string CustomerEmail,
    decimal TotalAmount,
    DateTime OrderDate,
    string Status);

public class OrderValidator : AbstractValidator<OrderRecord>
{
    public OrderValidator()
    {
        RuleFor(x => x.OrderId)
            .NotEmpty().WithMessage("Order ID cannot be empty.")
            .MaximumLength(32);

        RuleFor(x => x.CustomerEmail)
            .NotEmpty()
            .EmailAddress().WithMessage("A valid email address is required.");

        RuleFor(x => x.TotalAmount)
            .GreaterThan(0).WithMessage("Total amount must be greater than zero.");

        RuleFor(x => x.OrderDate)
            .LessThanOrEqualTo(DateTime.UtcNow).WithMessage("Order date cannot be in the future.");

        RuleFor(x => x.Status)
            .Must(s => s is "PENDING" or "COMPLETED" or "CANCELLED")
            .WithMessage("Invalid order status.");
    }
}
```

---

## Validation Strategies

Configure the validation strategy using `ValidateWith<TValidator>` on the pipeline or `WithErrorStrategy(...)` in a `FastIngestProfile<TRecord>`.

### 1. `ErrorStrategy.FailFast` (Default)

In `FailFast` mode, the pipeline immediately halts execution upon the first validation or conversion error. The target sink aborts the batch, and a `FastIngestValidationException` is thrown.

Use this strategy for financial or strictly transactional pipelines where an import must be 100% valid or completely rejected:

```csharp
try
{
    var result = await FastIngestPipeline<OrderRecord>.Create()
        .FromStream(stream, FileType.Csv)
        .WithMapping(m => { /* ... */ })
        .ValidateWith<OrderValidator>(options =>
        {
            options.ErrorStrategy = ErrorStrategy.FailFast;
        })
        .WriteToPostgresAsync(connection, "orders");
}
catch (FastIngestValidationException ex)
{
    Console.WriteLine($"Ingestion halted at row {ex.RowIndex}!");
    Console.WriteLine($"Column: {ex.ColumnName}");
    Console.WriteLine($"Value: '{ex.AttemptedValue}'");
    Console.WriteLine($"Error: {ex.Message}");
}
```

### 2. `ErrorStrategy.CollectAndContinue`

In `CollectAndContinue` mode, invalid rows are skipped and recorded into an error manifest, while all valid rows proceed through the batching engine and are committed to the database.

Use this strategy for customer data imports, analytics dumps, or self-service spreadsheet uploads where users expect partial imports and an error report:

```csharp
var result = await FastIngestPipeline<OrderRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m => { /* ... */ })
    .ValidateWith<OrderValidator>(options =>
    {
        options.ErrorStrategy = ErrorStrategy.CollectAndContinue;
    })
    .WriteToPostgresAsync(connection, "orders");

Console.WriteLine($"Import complete!");
Console.WriteLine($"Total Rows Read:    {result.TotalProcessed:N0}");
Console.WriteLine($"Successfully Saved: {result.TotalSucceeded:N0}");
Console.WriteLine($"Failed / Skipped:   {result.TotalFailed:N0}");
```

---

## Exporting Error Manifests (CSV)

When rows fail validation in `CollectAndContinue` mode, `result.ExportErrorsToCsv()` generates an RFC 4180 compliant CSV document detailing every rejection:

```csharp
if (!result.IsSuccess)
{
    // Generates a CSV containing: RowIndex, ColumnName, AttemptedValue, ErrorMessage
    byte[] errorCsv = result.ExportErrorsToCsv();

    await File.WriteAllBytesAsync("orders_failed_rows.csv", errorCsv);
}
```

### Error Manifest Output Format

| RowIndex | ColumnName | AttemptedValue | ErrorMessage |
| :--- | :--- | :--- | :--- |
| `142` | `CustomerEmail` | `john-invalid-email` | `A valid email address is required.` |
| `289` | `TotalAmount` | `-45.50` | `Total amount must be greater than zero.` |
| `1204` | `Status` | `UNKNOWN_STATUS` | `Invalid order status.` |

---

## Inspecting Errors Programmatically

You can iterate through `result.Errors` directly in memory:

```csharp
foreach (IngestRowError error in result.Errors)
{
    logger.LogWarning("Line {Row}: Column '{Col}' value '{Val}' rejected: {Reason}",
        error.RowIndex,
        error.ColumnName ?? "Record",
        error.AttemptedValue,
        error.ErrorMessage);
}
```

---

## JSON Lines (NDJSON) Syntax & Deserialization Errors

When ingesting line-delimited JSON (`FileType.JsonLines`), errors can occur at two distinct phases:

1. **Syntax & Schema Deserialization**: A line contains invalid JSON syntax (e.g., missing brackets, unquoted tokens, or type conversion mismatches like a string provided for a numeric property).
2. **Domain Business Validation**: The line deserializes into `TRecord` successfully, but fails rules defined in `IValidator<TRecord>`.

FastIngest seamlessly unifies both categories into the same `IngestRowError` model and error manifest:

| Error Type | `RowIndex` | `ColumnName` | `AttemptedValue` | `ErrorMessage` |
| :--- | :--- | :--- | :--- | :--- |
| **JSON Syntax Error** | `15` | `null` | `{"id": 15, "balance": abc}` | `JSON parsing failed on row 15: The JSON value could not be converted...` |
| **Schema Type Error** | `42` | `$.age` | `{"id": 42, "age": "not_an_int"}` | `JSON parsing failed on row 42: The JSON value could not be converted to System.Int32.` |
| **FluentValidation Error** | `88` | `CustomerEmail` | `not-an-email` | `A valid email address is required.` |

Both `FailFast` and `CollectAndContinue` strategies apply uniformly to JSON syntax and domain validation errors. For more details on JSON Lines streaming, see the [NDJSON / JSON Lines Guide](/guide/json-lines).
