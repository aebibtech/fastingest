using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using Npgsql;

namespace FastIngest.PostgreSql.Extensions;

public static class PipelineExtensions
{
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
