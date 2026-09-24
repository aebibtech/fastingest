using System.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Sinks;
using Npgsql;

namespace FastIngest.PostgreSql;

/// <summary>
/// High-throughput PostgreSQL bulk ingestion sink leveraging native binary <c>COPY FROM STDIN (FORMAT BINARY)</c>.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public class PostgreSqlSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly NpgsqlConnection _connection;
    private readonly string _tableName;
    private readonly IReadOnlyList<ColumnMapping<TRecord>> _mappings;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgreSqlSink{TRecord}"/> class.
    /// </summary>
    /// <param name="connection">An open or connectable <see cref="NpgsqlConnection"/>.</param>
    /// <param name="tableName">The destination table name (optionally schema-qualified, e.g. "public.customers").</param>
    /// <param name="mappings">The collection of column-to-property mappings for binary column serialization.</param>
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

    /// <summary>
    /// Writes a batch of records directly into PostgreSQL using binary COPY streaming.
    /// </summary>
    /// <param name="batch">The batch of records to ingest.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The total number of rows successfully written and committed.</returns>
    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0) return 0;

        // Ensure database connection is open
        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(cancellationToken);
        }

        // Construct qualified table identifier and quoted column list
        var qualifiedTable = FormatTableName(_tableName);
        var columns = string.Join(", ", _mappings.Select(m => $"\"{m.ColumnName.Trim('\"')}\""));
        var copyCommand = $"COPY {qualifiedTable} ({columns}) FROM STDIN (FORMAT BINARY)";

        // Initiate PostgreSQL binary import stream
        await using var writer = await _connection.BeginBinaryImportAsync(copyCommand, cancellationToken);

        // Stream rows and columns into the binary importer
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

        // Commit binary import operation and return affected row count
        ulong rowsImported = await writer.CompleteAsync(cancellationToken);
        return (long)rowsImported;
    }

    /// <summary>
    /// Formats table names, quoting identifiers and handling schema qualifiers (e.g. "my_schema"."my_table").
    /// </summary>
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
