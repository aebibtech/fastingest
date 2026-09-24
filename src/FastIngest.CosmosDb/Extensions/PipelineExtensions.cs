using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using Microsoft.Azure.Cosmos;

namespace FastIngest.CosmosDb.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into Azure Cosmos DB containers.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Azure Cosmos DB container using a <see cref="PartitionKey"/> selector.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="container">The target Cosmos DB container configured with bulk execution enabled.</param>
    /// <param name="partitionKeySelector">Delegate extracting the <see cref="PartitionKey"/> for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToCosmosDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        Container container,
        Func<TRecord, PartitionKey> partitionKeySelector,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        var sink = new CosmosDbBulkSink<TRecord>(container, partitionKeySelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Azure Cosmos DB container using a string partition key selector.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="container">The target Cosmos DB container configured with bulk execution enabled.</param>
    /// <param name="partitionKeySelector">Delegate extracting the partition key string value for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToCosmosDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        Container container,
        Func<TRecord, string> partitionKeySelector,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        var sink = new CosmosDbBulkSink<TRecord>(container, partitionKeySelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Azure Cosmos DB container using connection details.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">The destination database name.</param>
    /// <param name="containerName">The destination container name.</param>
    /// <param name="partitionKeySelector">Delegate extracting the <see cref="PartitionKey"/> for each record.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="CosmosClientOptions"/>.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToCosmosDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string databaseName,
        string containerName,
        Func<TRecord, PartitionKey> partitionKeySelector,
        Action<CosmosClientOptions>? configureOptions = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        var options = new CosmosClientOptions();
        configureOptions?.Invoke(options);

        using var sink = new CosmosDbBulkSink<TRecord>(connectionString, databaseName, containerName, partitionKeySelector, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Azure Cosmos DB container using connection details and string partition key selector.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">The destination database name.</param>
    /// <param name="containerName">The destination container name.</param>
    /// <param name="partitionKeySelector">Delegate extracting the partition key string value for each record.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="CosmosClientOptions"/>.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToCosmosDbAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string databaseName,
        string containerName,
        Func<TRecord, string> partitionKeySelector,
        Action<CosmosClientOptions>? configureOptions = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        return await pipeline.WriteToCosmosDbAsync(
            connectionString,
            databaseName,
            containerName,
            record =>
            {
                var pk = partitionKeySelector(record);
                return pk == null ? PartitionKey.Null : new PartitionKey(pk);
            },
            configureOptions,
            ct).ConfigureAwait(false);
    }
}
