using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using Microsoft.Data.Sqlite;

namespace FastIngest.Sqlite.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into SQLite tables.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a SQLite table using a connection string.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The SQLite connection string (e.g. "Data Source=app.db").</param>
    /// <param name="tableName">The destination SQLite table name.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToSqliteAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new SqliteBulkSink<TRecord>(connectionString, tableName, mappings);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a SQLite table using an existing <see cref="SqliteConnection"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connection">An existing SQLite connection.</param>
    /// <param name="tableName">The destination SQLite table name.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToSqliteAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        SqliteConnection connection,
        string tableName,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new SqliteBulkSink<TRecord>(connection, tableName, mappings);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }
}
