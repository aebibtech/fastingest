using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;

namespace FastIngest.AspNetCore.Workers;

/// <summary>
/// Delegate representing an asynchronous ingestion execution against a given service provider and stream.
/// </summary>
/// <param name="serviceProvider">The scoped service provider.</param>
/// <param name="stream">The file stream to ingest.</param>
/// <param name="onProgress">The progress callback.</param>
/// <param name="cancellationToken">Cancellation token.</param>
/// <returns>A task returning the <see cref="IngestResult"/>.</returns>
public delegate Task<IngestResult> IngestExecutionDelegate(
    IServiceProvider serviceProvider,
    Stream stream,
    Action<IngestProgress> onProgress,
    CancellationToken cancellationToken);

/// <summary>
/// Represents a queued bulk ingestion job to be processed asynchronously by the background worker.
/// </summary>
/// <param name="JobId">The unique identifier for the ingestion job.</param>
/// <param name="TempFilePath">The path to the uploaded file temporarily stored on disk.</param>
/// <param name="OriginalFileName">The original file name submitted by the client.</param>
/// <param name="Handler">The execution delegate that invokes the ingestion pipeline.</param>
public record IngestJob(
    string JobId,
    string TempFilePath,
    string? OriginalFileName,
    IngestExecutionDelegate Handler);
