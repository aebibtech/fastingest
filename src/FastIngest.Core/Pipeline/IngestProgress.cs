namespace FastIngest.Core.Pipeline;

/// <summary>
/// Provides a snapshot of the pipeline's progress during ingestion execution.
/// </summary>
/// <param name="RowsProcessed">The total number of rows processed from the stream up to this point.</param>
/// <param name="RowsSucceeded">The total number of records successfully written to the sink up to this point.</param>
/// <param name="RowsFailed">The total number of records rejected due to validation or parsing errors up to this point.</param>
/// <param name="PercentComplete">The estimated percentage of completion (0.0 to 100.0) if the stream supports seeking, or null.</param>
public sealed record IngestProgress(
    long RowsProcessed,
    long RowsSucceeded,
    long RowsFailed,
    double? PercentComplete = null
);
