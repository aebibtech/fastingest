using System.Text;

namespace FastIngest.Core.Results;

/// <summary>
/// Represents a parsing or validation error associated with a specific row during ingestion.
/// </summary>
/// <param name="RowIndex">The 1-based index of the offending row in the source file.</param>
/// <param name="ColumnName">The name of the column that caused the error, or null if the error is record-level.</param>
/// <param name="AttemptedValue">The raw string value that failed to convert or validate.</param>
/// <param name="ErrorMessage">A descriptive message detailing why the value or row failed.</param>
public record IngestRowError(long RowIndex, string? ColumnName, string AttemptedValue, string ErrorMessage);

/// <summary>
/// Contains the aggregated summary and diagnostic details of an ingestion pipeline execution.
/// </summary>
/// <param name="TotalProcessed">The total number of rows read from the source stream.</param>
/// <param name="TotalSucceeded">The total number of rows successfully validated and committed to the target sink.</param>
/// <param name="TotalFailed">The total number of rows rejected due to parsing or validation errors.</param>
/// <param name="Errors">The read-only collection of individual row errors collected during execution.</param>
public record IngestResult(long TotalProcessed, long TotalSucceeded, long TotalFailed, IReadOnlyList<IngestRowError> Errors)
{
    /// <summary>
    /// Gets a value indicating whether the ingestion pipeline succeeded without any row failures.
    /// </summary>
    public bool IsSuccess => TotalFailed == 0;

    /// <summary>
    /// Exports all collected row errors as a UTF-8 encoded RFC 4180 compliant CSV byte array.
    /// </summary>
    /// <returns>A byte array containing the CSV document with columns: RowIndex, ColumnName, AttemptedValue, ErrorMessage.</returns>
    public byte[] ExportErrorsToCsv()
    {
        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, Encoding.UTF8, leaveOpen: true))
        {
            // Write RFC 4180 header
            writer.WriteLine("RowIndex,ColumnName,AttemptedValue,ErrorMessage");

            // Write each captured row error with standard CSV escaping
            foreach (var error in Errors)
            {
                writer.WriteLine($"{error.RowIndex},{EscapeCsv(error.ColumnName)},{EscapeCsv(error.AttemptedValue)},{EscapeCsv(error.ErrorMessage)}");
            }
        }
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Escapes field values containing commas, double quotes, or newlines in accordance with RFC 4180.
    /// </summary>
    private static string EscapeCsv(string? value)
    {
        if (string.IsNullOrEmpty(value)) return string.Empty;
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }
        return value;
    }
}
