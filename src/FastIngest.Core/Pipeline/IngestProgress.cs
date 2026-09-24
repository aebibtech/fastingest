namespace FastIngest.Core.Pipeline;

public sealed record IngestProgress(
    long RowsProcessed,
    long RowsSucceeded,
    long RowsFailed,
    double? PercentComplete = null
);
