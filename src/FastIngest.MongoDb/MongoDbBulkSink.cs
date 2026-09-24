using FastIngest.Core.Sinks;
using MongoDB.Driver;

namespace FastIngest.MongoDb;

/// <summary>
/// High-performance MongoDB bulk ingestion sink persisting batches of records via unordered <c>BulkWriteAsync</c>.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the document to persist.</typeparam>
public class MongoDbBulkSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly IMongoCollection<TRecord> _collection;
    private readonly BulkWriteOptions _bulkWriteOptions;

    /// <summary>
    /// Gets the target MongoDB collection.
    /// </summary>
    public IMongoCollection<TRecord> Collection => _collection;

    /// <summary>
    /// Gets the bulk write options applied during ingestion.
    /// </summary>
    public BulkWriteOptions BulkWriteOptions => _bulkWriteOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbBulkSink{TRecord}"/> class using an existing <see cref="IMongoCollection{TRecord}"/>.
    /// </summary>
    /// <param name="collection">The target MongoDB collection.</param>
    /// <param name="bulkWriteOptions">Optional bulk write options. Defaults to unordered writes (<see cref="BulkWriteOptions.IsOrdered"/> = <c>false</c>).</param>
    public MongoDbBulkSink(
        IMongoCollection<TRecord> collection,
        BulkWriteOptions? bulkWriteOptions = null)
    {
        _collection = collection ?? throw new ArgumentNullException(nameof(collection));
        _bulkWriteOptions = bulkWriteOptions ?? new BulkWriteOptions { IsOrdered = false };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbBulkSink{TRecord}"/> class using connection details.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The destination database name.</param>
    /// <param name="collectionName">The destination collection name.</param>
    /// <param name="bulkWriteOptions">Optional bulk write options. Defaults to unordered writes (<see cref="BulkWriteOptions.IsOrdered"/> = <c>false</c>).</param>
    public MongoDbBulkSink(
        string connectionString,
        string databaseName,
        string collectionName,
        BulkWriteOptions? bulkWriteOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

        var client = new MongoClient(connectionString);
        var database = client.GetDatabase(databaseName);
        _collection = database.GetCollection<TRecord>(collectionName);
        _bulkWriteOptions = bulkWriteOptions ?? new BulkWriteOptions { IsOrdered = false };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbBulkSink{TRecord}"/> class using a database instance and collection name.
    /// </summary>
    /// <param name="database">The target MongoDB database.</param>
    /// <param name="collectionName">The destination collection name.</param>
    /// <param name="bulkWriteOptions">Optional bulk write options. Defaults to unordered writes (<see cref="BulkWriteOptions.IsOrdered"/> = <c>false</c>).</param>
    public MongoDbBulkSink(
        IMongoDatabase database,
        string collectionName,
        BulkWriteOptions? bulkWriteOptions = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

        _collection = database.GetCollection<TRecord>(collectionName);
        _bulkWriteOptions = bulkWriteOptions ?? new BulkWriteOptions { IsOrdered = false };
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbBulkSink{TRecord}"/> class using a client instance, database name, and collection name.
    /// </summary>
    /// <param name="client">The MongoDB client.</param>
    /// <param name="databaseName">The destination database name.</param>
    /// <param name="collectionName">The destination collection name.</param>
    /// <param name="bulkWriteOptions">Optional bulk write options. Defaults to unordered writes (<see cref="BulkWriteOptions.IsOrdered"/> = <c>false</c>).</param>
    public MongoDbBulkSink(
        IMongoClient client,
        string databaseName,
        string collectionName,
        BulkWriteOptions? bulkWriteOptions = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

        _collection = client.GetDatabase(databaseName).GetCollection<TRecord>(collectionName);
        _bulkWriteOptions = bulkWriteOptions ?? new BulkWriteOptions { IsOrdered = false };
    }

    /// <summary>
    /// Writes a batch of records directly into MongoDB using <c>BulkWriteAsync</c>.
    /// </summary>
    /// <param name="batch">The batch of records to ingest.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The total number of rows successfully written and committed.</returns>
    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch == null || batch.Count == 0)
        {
            return 0;
        }

        var writes = batch.Select(item => new InsertOneModel<TRecord>(item));
        await _collection.BulkWriteAsync(writes, _bulkWriteOptions, cancellationToken).ConfigureAwait(false);
        return batch.Count;
    }
}
