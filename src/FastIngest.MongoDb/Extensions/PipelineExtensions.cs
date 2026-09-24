using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using MongoDB.Driver;

namespace FastIngest.MongoDb.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into MongoDB collections via <see cref="MongoDbBulkSink{TRecord}"/>.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MongoDB collection using an existing <see cref="IMongoCollection{TRecord}"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="collection">The target MongoDB collection.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="BulkWriteOptions"/>.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMongoDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        IMongoCollection<TRecord> collection,
        Action<BulkWriteOptions>? configureOptions = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(collection);

        var options = new BulkWriteOptions { IsOrdered = false };
        configureOptions?.Invoke(options);

        var sink = new MongoDbBulkSink<TRecord>(collection, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MongoDB collection using connection details.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The target database name.</param>
    /// <param name="collectionName">The target collection name.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="BulkWriteOptions"/>.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMongoDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string databaseName,
        string collectionName,
        Action<BulkWriteOptions>? configureOptions = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

        var options = new BulkWriteOptions { IsOrdered = false };
        configureOptions?.Invoke(options);

        var sink = new MongoDbBulkSink<TRecord>(connectionString, databaseName, collectionName, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MongoDB collection using explicit <see cref="BulkWriteOptions"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="collection">The target MongoDB collection.</param>
    /// <param name="options">The explicit bulk write options.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMongoDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        IMongoCollection<TRecord> collection,
        BulkWriteOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(collection);
        ArgumentNullException.ThrowIfNull(options);

        var sink = new MongoDbBulkSink<TRecord>(collection, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MongoDB collection using connection details and explicit <see cref="BulkWriteOptions"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The target database name.</param>
    /// <param name="collectionName">The target collection name.</param>
    /// <param name="options">The explicit bulk write options.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMongoDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string databaseName,
        string collectionName,
        BulkWriteOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        ArgumentNullException.ThrowIfNull(options);

        var sink = new MongoDbBulkSink<TRecord>(connectionString, databaseName, collectionName, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }
}
