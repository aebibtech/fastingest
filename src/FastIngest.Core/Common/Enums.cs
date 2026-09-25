namespace FastIngest.Core.Common;

/// <summary>
/// Specifies the input tabular file format supported by the ingestion pipeline.
/// </summary>
public enum FileType
{
    /// <summary>
    /// Standard Comma-Separated Values (CSV) plain text file format.
    /// </summary>
    Csv,

    /// <summary>
    /// Microsoft Excel OpenXML Spreadsheet (.xlsx) binary format.
    /// </summary>
    Xlsx,

    /// <summary>
    /// Line-delimited JSON (JSON Lines / NDJSON) streaming plain text format.
    /// </summary>
    JsonLines,

    /// <summary>
    /// Alias for <see cref="JsonLines"/>.
    /// </summary>
    Ndjson = JsonLines,

    /// <summary>
    /// Automatically infers the file format based on content inspection or stream headers.
    /// </summary>
    AutoDetect
}

/// <summary>
/// Defines the error handling strategy used during row parsing and validation.
/// </summary>
public enum ErrorStrategy
{
    /// <summary>
    /// Immediately halts pipeline execution upon encountering the first invalid record
    /// and throws a <see cref="Exceptions.FastIngestValidationException"/>.
    /// </summary>
    FailFast,

    /// <summary>
    /// Records the offending row error, skips inserting the invalid record, and continues
    /// processing the remaining rows. The collected errors are accessible in the final result.
    /// </summary>
    CollectAndContinue
}
