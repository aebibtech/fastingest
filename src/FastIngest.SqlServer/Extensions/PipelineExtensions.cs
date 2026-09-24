using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using Microsoft.Data.SqlClient;

namespace FastIngest.SqlServer.Extensions;

/// <summary>
/// Provides fluent extension methods to stream ingestion pipeline data directly into Microsoft SQL Server tables via <see cref="SqlBulkCopy"/>.
/// </summary>
public static class PipelineExtensions
{
    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a Microsoft SQL Server table using a connection string.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="tableName">The destination SQL Server table name (e.g. "Customers" or "dbo.Customers").</param>
    /// <param name="options">Optional delegate configuring <see cref="SqlBulkCopyOptions"/> flags.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToSqlServerAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        Action<SqlBulkCopyOptions>? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var bulkOptions = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.CheckConstraints;
        options?.Invoke(bulkOptions);

        var mappings = pipeline.GetMappings();
        var sink = new SqlServerBulkSink<TRecord>(connectionString, tableName, mappings, bulkOptions);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a Microsoft SQL Server table using explicit <see cref="SqlBulkCopyOptions"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="tableName">The destination SQL Server table name.</param>
    /// <param name="options">The bulk copy options flags to apply.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToSqlServerAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        string connectionString,
        string tableName,
        SqlBulkCopyOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new SqlServerBulkSink<TRecord>(connectionString, tableName, mappings, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the ingestion pipeline and writes parsed records directly to a Microsoft SQL Server table using an existing <see cref="SqlConnection"/>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="pipeline">The configured ingestion pipeline instance.</param>
    /// <param name="connection">An existing SQL Server connection.</param>
    /// <param name="tableName">The destination SQL Server table name.</param>
    /// <param name="options">The bulk copy options flags to apply. Defaults to <see cref="SqlBulkCopyOptions.Default"/> | <see cref="SqlBulkCopyOptions.CheckConstraints"/>.</param>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IngestResult"/> summarizing the ingestion execution.</returns>
    public static async Task<IngestResult> WriteToSqlServerAsync<TRecord>(
        this IFastIngestPipeline<TRecord> pipeline,
        SqlConnection connection,
        string tableName,
        SqlBulkCopyOptions options = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.CheckConstraints,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var mappings = pipeline.GetMappings();
        var sink = new SqlServerBulkSink<TRecord>(connection, tableName, mappings, options);
        return await pipeline.WriteToSinkAsync(sink, ct).ConfigureAwait(false);
    }
}
