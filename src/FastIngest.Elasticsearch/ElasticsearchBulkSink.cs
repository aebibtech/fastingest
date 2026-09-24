using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Transport;
using FastIngest.Core.Sinks;

namespace FastIngest.Elasticsearch;

/// <summary>
/// High-performance bulk ingestion sink persisting batches of records into an Elasticsearch index
/// using NDJSON bulk operations via <c>BulkAsync</c>.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing an ingested record.</typeparam>
public class ElasticsearchBulkSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly ElasticsearchClient _client;
    private readonly IndexName _targetIndex;
    private readonly Func<TRecord, Id>? _idSelector;

    /// <summary>
    /// Gets the underlying <see cref="ElasticsearchClient"/> instance.
    /// </summary>
    public ElasticsearchClient Client => _client;

    /// <summary>
    /// Gets the target Elasticsearch index name.
    /// </summary>
    public IndexName TargetIndex => _targetIndex;

    /// <summary>
    /// Gets the optional delegate extracting document ID from each record.
    /// </summary>
    public Func<TRecord, Id>? IdSelector => _idSelector;

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticsearchBulkSink{TRecord}"/> class using an existing <see cref="ElasticsearchClient"/>.
    /// </summary>
    /// <param name="client">The configured <see cref="ElasticsearchClient"/>.</param>
    /// <param name="targetIndex">The target index name.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    public ElasticsearchBulkSink(
        ElasticsearchClient client,
        IndexName targetIndex,
        Func<TRecord, Id>? idSelector = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _targetIndex = targetIndex ?? throw new ArgumentNullException(nameof(targetIndex));
        _idSelector = idSelector;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticsearchBulkSink{TRecord}"/> class using an existing <see cref="ElasticsearchClient"/> and string index name.
    /// </summary>
    /// <param name="client">The configured <see cref="ElasticsearchClient"/>.</param>
    /// <param name="targetIndex">The target index name.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    public ElasticsearchBulkSink(
        ElasticsearchClient client,
        string targetIndex,
        Func<TRecord, Id>? idSelector = null)
        : this(client, (IndexName)(targetIndex ?? throw new ArgumentNullException(nameof(targetIndex))), idSelector)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticsearchBulkSink{TRecord}"/> class with connection details.
    /// </summary>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="targetIndex">The target index name.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    public ElasticsearchBulkSink(
        Uri endpoint,
        IndexName targetIndex,
        string? apiKey = null,
        Func<TRecord, Id>? idSelector = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        _targetIndex = targetIndex ?? throw new ArgumentNullException(nameof(targetIndex));
        _idSelector = idSelector;

        var settings = new ElasticsearchClientSettings(endpoint);
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            settings.Authentication(new ApiKey(apiKey));
        }
        _client = new ElasticsearchClient(settings);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticsearchBulkSink{TRecord}"/> class with connection details and string index name.
    /// </summary>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="targetIndex">The target index name.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    public ElasticsearchBulkSink(
        Uri endpoint,
        string targetIndex,
        string? apiKey = null,
        Func<TRecord, Id>? idSelector = null)
        : this(endpoint, (IndexName)(targetIndex ?? throw new ArgumentNullException(nameof(targetIndex))), apiKey, idSelector)
    {
    }

    /// <summary>
    /// Writes a batch of records directly into Elasticsearch using a bulk request.
    /// </summary>
    /// <param name="batch">The batch of records to ingest.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The total number of records successfully written.</returns>
    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch == null || batch.Count == 0)
        {
            return 0;
        }

        var operations = new List<IBulkOperation>(batch.Count);
        foreach (var item in batch)
        {
            var op = new BulkIndexOperation<TRecord>(item);
            if (_idSelector is not null)
            {
                op.Id = _idSelector(item);
            }
            operations.Add(op);
        }

        var request = new BulkRequest(_targetIndex)
        {
            Operations = operations
        };

        var response = await _client.BulkAsync(request, cancellationToken).ConfigureAwait(false);

        if (response.Errors)
        {
            var failedIndices = new List<int>();
            var errorDetails = new List<string>();

            int index = 0;
            if (response.Items != null)
            {
                foreach (var item in response.Items)
                {
                    if (item.Error != null || item.Status >= 400)
                    {
                        failedIndices.Add(index);
                        var reason = item.Error?.Reason ?? $"Status {item.Status}";
                        errorDetails.Add($"Item at index {index} (id: '{item.Id}'): {reason}");
                    }
                    index++;
                }
            }

            var summary = $"Elasticsearch bulk ingestion reported errors for {failedIndices.Count} item(s). Failed indices: [{string.Join(", ", failedIndices)}]. Details: {string.Join("; ", errorDetails)}";
            throw new IngestionSinkException(summary, failedIndices, errorDetails);
        }

        if (!response.IsValidResponse)
        {
            var errorMsg = response.ElasticsearchServerError?.Error?.Reason
                ?? response.DebugInformation
                ?? "Elasticsearch bulk request failed.";
            throw new IngestionSinkException($"Elasticsearch bulk operation failed: {errorMsg}");
        }

        return batch.Count;
    }
}
