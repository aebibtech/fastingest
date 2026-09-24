using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using MySqlConnector;

namespace FastIngest.MySql.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into MySQL or MariaDB tables via <see cref="MySqlBulkCopy"/>.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MySQL table using a connection string.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <param name="tableName">The destination MySQL table name.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMySqlAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        CancellationToken ct = default)
    {
        return await pipeline.WriteToMySqlAsync(connectionString, tableName, externalTransaction: null, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MySQL table using a connection string and external transaction.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <param name="tableName">The destination MySQL table name.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMySqlAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        MySqlTransaction? externalTransaction,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new MySqlBulkSink<TRecord>(connectionString, tableName, mappings, externalTransaction);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a MySQL table using an existing <see cref="MySqlConnection"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connection">An existing MySQL connection.</param>
    /// <param name="tableName">The destination MySQL table name.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToMySqlAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        MySqlConnection connection,
        string tableName,
        MySqlTransaction? externalTransaction = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new MySqlBulkSink<TRecord>(connection, tableName, mappings, externalTransaction);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }
}
