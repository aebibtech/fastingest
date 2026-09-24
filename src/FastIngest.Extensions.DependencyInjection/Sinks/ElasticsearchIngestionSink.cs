using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using FastIngest.Core.Sinks;
using FastIngest.Elasticsearch;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace FastIngest.Extensions.DependencyInjection.Sinks;

/// <summary>
/// Destination sink adapter resolving Elasticsearch clients and target indices from registered DI services or FastIngest options.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public class ElasticsearchIngestionSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly ElasticsearchBulkSink<TRecord> _sink;

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticsearchIngestionSink{TRecord}"/> class.
    /// </summary>
    /// <param name="serviceProvider">The active service provider.</param>
    /// <param name="options">The configured FastIngest options.</param>
    /// <param name="profileRegistry">The profile registry.</param>
    public ElasticsearchIngestionSink(
        IServiceProvider serviceProvider,
        IOptions<FastIngestOptions> options,
        IFastIngestProfileRegistry profileRegistry)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(profileRegistry);

        var client = serviceProvider.GetService<ElasticsearchClient>();
        var opt = options.Value;

        if (client == null)
        {
            if (string.IsNullOrWhiteSpace(opt.ElasticsearchEndpoint))
            {
                throw new InvalidOperationException(
                    $"No Elasticsearch client or endpoint configured for {typeof(TRecord).Name}. " +
                    "Configure ElasticsearchEndpoint in FastIngestOptions or register an ElasticsearchClient in DI.");
            }

            var settings = new ElasticsearchClientSettings(new Uri(opt.ElasticsearchEndpoint));
            if (!string.IsNullOrWhiteSpace(opt.ElasticsearchApiKey))
            {
                settings.Authentication(new ApiKey(opt.ElasticsearchApiKey));
            }
            client = new ElasticsearchClient(settings);
        }

        var profile = profileRegistry.GetProfile<TRecord>();
        var indexName = profile?.TargetTable ?? opt.ElasticsearchDefaultIndex ?? typeof(TRecord).Name.ToLowerInvariant();

        _sink = new ElasticsearchBulkSink<TRecord>(client, indexName);
    }

    /// <inheritdoc/>
    public Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        return _sink.WriteBatchAsync(batch, cancellationToken);
    }
}
