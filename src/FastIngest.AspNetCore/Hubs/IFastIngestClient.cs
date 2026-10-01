using FastIngest.Core.Pipeline;

namespace FastIngest.AspNetCore.Hubs;

/// <summary>
/// Strongly-typed client contract for real-time FastIngest notifications over SignalR.
/// </summary>
public interface IFastIngestClient
{
    /// <summary>
    /// Invoked when progress is reported during bulk data ingestion.
    /// </summary>
    /// <param name="jobId">The unique identifier of the ingestion job.</param>
    /// <param name="progress">The current ingestion progress telemetry.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReceiveProgress(string jobId, IngestProgress progress);

    /// <summary>
    /// Invoked when bulk data ingestion successfully completes.
    /// </summary>
    /// <param name="jobId">The unique identifier of the ingestion job.</param>
    /// <param name="totalProcessed">Total number of records processed.</param>
    /// <param name="totalSucceeded">Total number of records successfully written.</param>
    /// <param name="totalFailed">Total number of records that failed validation or insertion.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReceiveCompletion(string jobId, long totalProcessed, long totalSucceeded, long totalFailed);

    /// <summary>
    /// Invoked when a fatal error occurs during ingestion.
    /// </summary>
    /// <param name="jobId">The unique identifier of the ingestion job.</param>
    /// <param name="errorMessage">The error message describing the failure.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReceiveError(string jobId, string errorMessage);
}
