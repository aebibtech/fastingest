using FastIngest.Core.Sinks;
using Microsoft.Azure.Cosmos;

namespace FastIngest.CosmosDb;

/// <summary>
/// High-performance bulk ingestion sink persisting batches of records into Azure Cosmos DB
/// using concurrent dispatch with <see cref="CosmosClientOptions.AllowBulkExecution"/> enabled.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public class CosmosDbBulkSink<TRecord> : IIngestionSink<TRecord>, IDisposable
{
    private readonly Container _container;
    private readonly Func<TRecord, PartitionKey> _partitionKeySelector;
    private readonly CosmosClient? _ownedClient;

    /// <summary>
    /// Gets the target Cosmos DB container.
    /// </summary>
    public Container Container => _container;

    /// <summary>
    /// Initializes a new instance of the <see cref="CosmosDbBulkSink{TRecord}"/> class using an existing <see cref="Microsoft.Azure.Cosmos.Container"/>.
    /// </summary>
    /// <param name="container">The Azure Cosmos DB container configured with bulk execution enabled.</param>
    /// <param name="partitionKeySelector">Delegate extracting the <see cref="PartitionKey"/> for each record.</param>
    public CosmosDbBulkSink(
        Container container,
        Func<TRecord, PartitionKey> partitionKeySelector)
    {
        _container = container ?? throw new ArgumentNullException(nameof(container));
        _partitionKeySelector = partitionKeySelector ?? throw new ArgumentNullException(nameof(partitionKeySelector));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CosmosDbBulkSink{TRecord}"/> class using an existing <see cref="Microsoft.Azure.Cosmos.Container"/> and string partition key selector.
    /// </summary>
    /// <param name="container">The Azure Cosmos DB container configured with bulk execution enabled.</param>
    /// <param name="partitionKeySelector">Delegate extracting the partition key string value for each record.</param>
    public CosmosDbBulkSink(
        Container container,
        Func<TRecord, string> partitionKeySelector)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        _container = container;
        _partitionKeySelector = record =>
        {
            var pk = partitionKeySelector(record);
            return pk == null ? PartitionKey.Null : new PartitionKey(pk);
        };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CosmosDbBulkSink{TRecord}"/> class with connection details and bulk execution enabled.
    /// </summary>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">The destination database name.</param>
    /// <param name="containerName">The destination container name.</param>
    /// <param name="partitionKeySelector">Delegate extracting the <see cref="PartitionKey"/> for each record.</param>
    /// <param name="clientOptions">Optional Cosmos client options. <see cref="CosmosClientOptions.AllowBulkExecution"/> will be set to <c>true</c>.</param>
    public CosmosDbBulkSink(
        string connectionString,
        string databaseName,
        string containerName,
        Func<TRecord, PartitionKey> partitionKeySelector,
        CosmosClientOptions? clientOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        var options = clientOptions ?? new CosmosClientOptions();
        options.AllowBulkExecution = true;

        _ownedClient = new CosmosClient(connectionString, options);
        _container = _ownedClient.GetContainer(databaseName, containerName);
        _partitionKeySelector = partitionKeySelector;
    }

    /// <summary>
    /// Writes a batch of records directly into Azure Cosmos DB by dispatching concurrent item creation tasks.
    /// </summary>
    /// <param name="batch">The batch of records to ingest.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The total number of items successfully written.</returns>
    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch == null || batch.Count == 0)
        {
            return 0;
        }

        // Dispatch all items concurrently as un-awaited tasks
        var tasks = batch.Select(item =>
        {
            try
            {
                return _container.CreateItemAsync(item, _partitionKeySelector(item), cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                return Task.FromException<ItemResponse<TRecord>>(ex);
            }
        }).ToList();

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
            return tasks.Count;
        }
        catch (Exception)
        {
            long successes = 0;
            var failures = new List<Exception>();

            foreach (var task in tasks)
            {
                if (task.IsCompletedSuccessfully)
                {
                    successes++;
                }
                else if (task.IsFaulted && task.Exception != null)
                {
                    foreach (var inner in task.Exception.Flatten().InnerExceptions)
                    {
                        failures.Add(inner);
                    }
                }
                else if (task.IsCanceled)
                {
                    failures.Add(new OperationCanceledException(cancellationToken));
                }
            }

            if (failures.Count == 1)
            {
                throw failures[0];
            }

            throw new AggregateException(
                $"Azure Cosmos DB bulk ingestion failed for {failures.Count} of {batch.Count} items. Successful: {successes}.",
                failures);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _ownedClient?.Dispose();
        GC.SuppressFinalize(this);
    }
}
