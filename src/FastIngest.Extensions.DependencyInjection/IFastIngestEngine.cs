using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;

namespace FastIngest.Extensions.DependencyInjection;

/// <summary>
/// Scoped engine providing dependency-injected ingestion pipeline execution for ASP.NET Core endpoints.
/// </summary>
public interface IFastIngestEngine
{
    /// <summary>
    /// Executes the ingestion pipeline for <typeparamref name="TRecord"/> streaming data from <paramref name="stream"/>
    /// directly to the resolved destination sink.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="stream">The tabular input stream (e.g. uploaded CSV file).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing row counts and validation errors.</returns>
    Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the ingestion pipeline for <typeparamref name="TRecord"/> streaming data from <paramref name="stream"/>
    /// with an optional progress callback.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="stream">The tabular input stream.</param>
    /// <param name="onProgress">Optional callback reporting ingestion progress telemetry.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing row counts and validation errors.</returns>
    Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        Action<IngestProgress>? onProgress,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the ingestion pipeline for <typeparamref name="TRecord"/> streaming data into a custom destination sink.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="stream">The tabular input stream.</param>
    /// <param name="sink">The destination sink instance.</param>
    /// <param name="onProgress">Optional callback reporting ingestion progress telemetry.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing row counts and validation errors.</returns>
    Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        IIngestionSink<TRecord> sink,
        Action<IngestProgress>? onProgress = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the ingestion pipeline for <typeparamref name="TRecord"/> with a target database table name override.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="stream">The tabular input stream.</param>
    /// <param name="tableName">The destination table name override.</param>
    /// <param name="onProgress">Optional callback reporting ingestion progress telemetry.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing row counts and validation errors.</returns>
    Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        string tableName,
        Action<IngestProgress>? onProgress = null,
        CancellationToken cancellationToken = default);
}
