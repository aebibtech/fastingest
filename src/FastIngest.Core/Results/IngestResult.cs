using System.Text;

namespace FastIngest.Core.Results;

public record IngestRowError(long RowIndex, string? ColumnName, string AttemptedValue, string ErrorMessage);

public record IngestResult(long TotalProcessed, long TotalSucceeded, long TotalFailed, IReadOnlyList<IngestRowError> Errors)
{
    public bool IsSuccess => TotalFailed == 0;

    public byte[] ExportErrorsToCsv()
    {
        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, Encoding.UTF8, leaveOpen: true))
        {
            writer.WriteLine("RowIndex,ColumnName,AttemptedValue,ErrorMessage");
            foreach (var error in Errors)
            {
                writer.WriteLine($"{error.RowIndex},{EscapeCsv(error.ColumnName)},{EscapeCsv(error.AttemptedValue)},{EscapeCsv(error.ErrorMessage)}");
            }
        }
        return memoryStream.ToArray();
    }

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
