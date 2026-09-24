using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FastIngest.MongoDb;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace FastIngest.Extensions.DependencyInjection.Sinks;

/// <summary>
/// Destination sink adapter resolving MongoDB collections from registered DI services or FastIngest options.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public class MongoDbIngestionSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly MongoDbBulkSink<TRecord> _sink;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbIngestionSink{TRecord}"/> class.
    /// </summary>
    /// <param name="serviceProvider">The active service provider.</param>
    /// <param name="options">The configured FastIngest options.</param>
    /// <param name="profileRegistry">The profile registry.</param>
    public MongoDbIngestionSink(
        IServiceProvider serviceProvider,
        IOptions<FastIngestOptions> options,
        IFastIngestProfileRegistry profileRegistry)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(profileRegistry);

        // 1. Direct IMongoCollection<TRecord> registration
        var directCollection = serviceProvider.GetService<IMongoCollection<TRecord>>();
        if (directCollection != null)
        {
            _sink = new MongoDbBulkSink<TRecord>(directCollection);
            return;
        }

        var opt = options.Value;
        var profile = profileRegistry.GetProfile<TRecord>();
        var collectionName = profile?.TargetTable ?? opt.MongoCollectionName ?? typeof(TRecord).Name;

        // 2. Direct IMongoDatabase registration
        var directDatabase = serviceProvider.GetService<IMongoDatabase>();
        if (directDatabase != null)
        {
            var coll = directDatabase.GetCollection<TRecord>(collectionName);
            _sink = new MongoDbBulkSink<TRecord>(coll);
            return;
        }

        // 3. IMongoClient registration (or fallback to FastIngestOptions connection string)
        var client = serviceProvider.GetService<IMongoClient>();
        var connectionString = opt.MongoConnectionString ?? opt.DefaultConnectionString;

        if (client == null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    $"No MongoDB connection configured for {typeof(TRecord).Name}. " +
                    "Configure MongoConnectionString or DefaultConnectionString in FastIngestOptions, or register an IMongoClient in DI.");
            }

            client = new MongoClient(connectionString);
        }

        // 4. Resolve database name
        string? databaseName = opt.MongoDatabaseName;
        if (string.IsNullOrWhiteSpace(databaseName) && !string.IsNullOrWhiteSpace(connectionString))
        {
            try
            {
                var mongoUrl = MongoUrl.Create(connectionString);
                databaseName = mongoUrl.DatabaseName;
            }
            catch
            {
                // Ignore parsing errors here; handled by next check
            }
        }

        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException(
                $"MongoDB database name is not configured for {typeof(TRecord).Name}. " +
                "Specify databaseName in AddMongoDbSink(...) or include it in the connection string URL (e.g. 'mongodb://localhost:27017/myDatabase').");
        }

        var database = client.GetDatabase(databaseName);
        var collection = database.GetCollection<TRecord>(collectionName);
        _sink = new MongoDbBulkSink<TRecord>(collection);
    }

    /// <inheritdoc/>
    public Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        return _sink.WriteBatchAsync(batch, cancellationToken);
    }
}
