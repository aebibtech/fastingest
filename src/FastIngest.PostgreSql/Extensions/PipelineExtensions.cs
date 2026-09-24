using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using Npgsql;

namespace FastIngest.PostgreSql.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into PostgreSQL tables via binary COPY.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a PostgreSQL table using an existing <see cref="NpgsqlConnection"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connection">An open or connectable <see cref="NpgsqlConnection"/>.</param>
    /// <param name="tableName">The destination PostgreSQL table name (e.g. "customers" or "public.customers").</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToPostgresAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        NpgsqlConnection connection,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new PostgreSqlSink<TRecord>(connection, tableName, mappings);
        return await pipeline.WriteToSinkAsync(sink, cancellationToken);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a PostgreSQL table by establishing a new connection.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="tableName">The destination PostgreSQL table name (e.g. "customers" or "public.customers").</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToPostgresAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var mappings = pipeline.GetMappings();
        var sink = new PostgreSqlSink<TRecord>(connection, tableName, mappings);
        return await pipeline.WriteToSinkAsync(sink, cancellationToken);
    }
}
