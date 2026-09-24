using System.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Sinks;
using Npgsql;

namespace FastIngest.PostgreSql;

public class PostgreSqlSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly NpgsqlConnection _connection;
    private readonly string _tableName;
    private readonly IReadOnlyList<ColumnMapping<TRecord>> _mappings;

    public PostgreSqlSink(
        NpgsqlConnection connection,
        string tableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = string.IsNullOrWhiteSpace(tableName) ? throw new ArgumentNullException(nameof(tableName)) : tableName;
        _mappings = mappings ?? throw new ArgumentNullException(nameof(mappings));

        if (_mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for PostgreSQL COPY sink.", nameof(mappings));
        }
    }

    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0) return 0;

        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(cancellationToken);
        }

        var qualifiedTable = FormatTableName(_tableName);
        var columns = string.Join(", ", _mappings.Select(m => $"\"{m.ColumnName.Trim('\"')}\""));
        var copyCommand = $"COPY {qualifiedTable} ({columns}) FROM STDIN (FORMAT BINARY)";

        await using var writer = await _connection.BeginBinaryImportAsync(copyCommand, cancellationToken);

        foreach (var record in batch)
        {
            await writer.StartRowAsync(cancellationToken);

            foreach (var mapping in _mappings)
            {
                var value = mapping.Getter(record);
                if (value is null)
                {
                    await writer.WriteNullAsync(cancellationToken);
                }
                else
                {
                    await writer.WriteAsync(value, cancellationToken);
                }
            }
        }

        ulong rowsImported = await writer.CompleteAsync(cancellationToken);
        return (long)rowsImported;
    }

    private static string FormatTableName(string tableName)
    {
        if (tableName.Contains('.'))
        {
            var parts = tableName.Split('.');
            return string.Join(".", parts.Select(p => $"\"{p.Trim('\"')}\""));
        }
        return $"\"{tableName.Trim('\"')}\"";
    }
}
