using FastIngest.Core.Common;
using FastIngest.Core.Mapping;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FluentValidation;

namespace FastIngest.Core.Pipeline;

/// <summary>
/// Defines the fluent interface for configuring and executing a high-throughput, constant-memory ingestion pipeline.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public interface IFastIngestPipeline<TRecord>
{
    /// <summary>
    /// Configures the source data stream and format for ingestion.
    /// </summary>
    /// <param name="stream">The readable stream containing tabular data.</param>
    /// <param name="fileType">The format of the input stream (CSV, XLSX, or AutoDetect).</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> FromStream(Stream stream, FileType fileType = FileType.AutoDetect);

    /// <summary>
    /// Configures the property-to-column mappings using a fluent builder callback.
    /// </summary>
    /// <param name="configure">The action to configure the <see cref="ColumnMappingBuilder{TRecord}"/>.</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> WithMapping(Action<ColumnMappingBuilder<TRecord>> configure);

    /// <summary>
    /// Registers a FluentValidation validator to validate each parsed record prior to writing to the sink.
    /// </summary>
    /// <typeparam name="TValidator">The concrete validator type implementing <see cref="IValidator{TRecord}"/> with a parameterless constructor.</typeparam>
    /// <param name="configure">Optional configuration for validation options and error strategies.</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> ValidateWith<TValidator>(Action<ValidationOptions>? configure = null) where TValidator : IValidator<TRecord>, new();

    /// <summary>
    /// Registers a FluentValidation validator instance to validate each parsed record.
    /// </summary>
    /// <param name="validator">The validator instance.</param>
    /// <param name="configure">Optional configuration for validation options and error strategies.</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> ValidateWith(IValidator<TRecord> validator, Action<ValidationOptions>? configure = null);

    /// <summary>
    /// Configures the chunk batch size for buffering and writing to the sink.
    /// </summary>
    /// <param name="batchSize">The maximum number of records per batch flush (defaults to 5,000).</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> WithBatchSize(int batchSize = 5000);

    /// <summary>
    /// Registers a progress callback invoked after every batch flush or pipeline stage update.
    /// </summary>
    /// <param name="callback">The callback receiving <see cref="IngestProgress"/> telemetry.</param>
    /// <returns>The pipeline instance for fluent chaining.</returns>
    IFastIngestPipeline<TRecord> OnProgress(Action<IngestProgress> callback);

    /// <summary>
    /// Retrieves the finalized list of column mappings configured on the pipeline.
    /// </summary>
    /// <returns>The read-only collection of <see cref="ColumnMapping{TRecord}"/>.</returns>
    IReadOnlyList<ColumnMapping<TRecord>> GetMappings();

    /// <summary>
    /// Executes the streaming ingestion pipeline, writing parsed batches into the provided destination sink.
    /// </summary>
    /// <param name="sink">The destination sink implementation (e.g. PostgreSQL COPY or custom sink).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing processed, succeeded, and failed records.</returns>
    Task<IngestResult> WriteToSinkAsync(IIngestionSink<TRecord> sink, CancellationToken cancellationToken = default);
}
