using Elastic.Clients.Elasticsearch;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;

namespace FastIngest.Elasticsearch.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into Elasticsearch indices via <see cref="ElasticsearchBulkSink{TRecord}"/>.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Elasticsearch index using an existing <see cref="ElasticsearchClient"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="client">The configured <see cref="ElasticsearchClient"/> instance.</param>
    /// <param name="indexName">The destination index name.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToElasticsearchAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        ElasticsearchClient client,
        string indexName,
        Func<TRecord, Id>? idSelector = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);

        var sink = new ElasticsearchBulkSink<TRecord>(client, indexName, idSelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Elasticsearch index using an existing <see cref="ElasticsearchClient"/> and <see cref="IndexName"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="client">The configured <see cref="ElasticsearchClient"/> instance.</param>
    /// <param name="indexName">The destination <see cref="IndexName"/>.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToElasticsearchAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        ElasticsearchClient client,
        IndexName indexName,
        Func<TRecord, Id>? idSelector = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(indexName);

        var sink = new ElasticsearchBulkSink<TRecord>(client, indexName, idSelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Elasticsearch index using endpoint details.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="indexName">The destination index name.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToElasticsearchAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        Uri endpoint,
        string indexName,
        string? apiKey = null,
        Func<TRecord, Id>? idSelector = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);

        var sink = new ElasticsearchBulkSink<TRecord>(endpoint, indexName, apiKey, idSelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to an Elasticsearch index using endpoint details and <see cref="IndexName"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="indexName">The destination <see cref="IndexName"/>.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="idSelector">Optional delegate extracting the document <see cref="Id"/> for each record.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToElasticsearchAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        Uri endpoint,
        IndexName indexName,
        string? apiKey = null,
        Func<TRecord, Id>? idSelector = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(indexName);

        var sink = new ElasticsearchBulkSink<TRecord>(endpoint, indexName, apiKey, idSelector);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }
}
