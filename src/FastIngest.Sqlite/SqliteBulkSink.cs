using System.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Sinks;
using Microsoft.Data.Sqlite;

namespace FastIngest.Sqlite;

/// <summary>
/// High-performance bulk ingestion sink persisting batches of records into SQLite
/// using a batched parameterized transaction loop with WAL mode optimization.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public class SqliteBulkSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly string? _connectionString;
    private readonly SqliteConnection? _connection;
    private readonly string _targetTableName;
    private readonly IReadOnlyList<ColumnMapping<TRecord>> _mappings;
    private readonly Func<TRecord, object?>[] _getters;
    private readonly string _insertSql;

    /// <summary>
    /// Gets the target destination table name.
    /// </summary>
    public string TargetTableName => _targetTableName;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqliteBulkSink{TRecord}"/> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The SQLite connection string (e.g. "Data Source=app.db").</param>
    /// <param name="targetTableName">The destination table name.</param>
    /// <param name="mappings">The collection of column mappings.</param>
    public SqliteBulkSink(
        string connectionString,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for SQLite bulk sink.", nameof(mappings));
        }

        _connectionString = connectionString;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _getters = mappings.Select(m => m.Getter).ToArray();
        _insertSql = BuildInsertSql(targetTableName, mappings);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SqliteBulkSink{TRecord}"/> class using an existing <see cref="SqliteConnection"/>.
    /// </summary>
    /// <param name="connection">An existing SQLite connection.</param>
    /// <param name="targetTableName">The destination table name.</param>
    /// <param name="mappings">The collection of column mappings.</param>
    public SqliteBulkSink(
        SqliteConnection connection,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for SQLite bulk sink.", nameof(mappings));
        }

        _connection = connection;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _getters = mappings.Select(m => m.Getter).ToArray();
        _insertSql = BuildInsertSql(targetTableName, mappings);
    }

    /// <summary>
    /// Writes a batch of records directly into SQLite using a batched parameterized transaction loop.
    /// </summary>
    /// <param name="batch">The batch of records to ingest.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The total number of rows successfully written and committed.</returns>
    public async Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken)
    {
        if (batch == null || batch.Count == 0)
        {
            return 0;
        }

        if (_connection != null)
        {
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            await ConfigurePragmasAsync(_connection, cancellationToken).ConfigureAwait(false);
            return await ExecuteBatchCoreAsync(_connection, batch, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await using var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ConfigurePragmasAsync(connection, cancellationToken).ConfigureAwait(false);
            return await ExecuteBatchCoreAsync(connection, batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task ConfigurePragmasAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        using var pragmaCmd = connection.CreateCommand();
        pragmaCmd.CommandText = "PRAGMA synchronous = NORMAL; PRAGMA journal_mode = WAL;";
        await pragmaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<long> ExecuteBatchCoreAsync(
        SqliteConnection connection,
        IReadOnlyList<TRecord> batch,
        CancellationToken cancellationToken)
    {
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = _insertSql;

        var parameters = new SqliteParameter[_mappings.Count];
        for (int i = 0; i < _mappings.Count; i++)
        {
            var param = command.CreateParameter();
            param.ParameterName = $"@p{i}";
            command.Parameters.Add(param);
            parameters[i] = param;
        }

        long rowsAffected = 0;
        for (int i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            for (int col = 0; col < _mappings.Count; col++)
            {
                var val = _getters[col](record);
                parameters[col].Value = val ?? DBNull.Value;
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            rowsAffected++;
        }

        transaction.Commit();
        return rowsAffected;
    }

    private static string BuildInsertSql(string tableName, IReadOnlyList<ColumnMapping<TRecord>> mappings)
    {
        var escapedTable = FormatTableName(tableName);
        var columns = string.Join(", ", mappings.Select(m => $"\"{m.ColumnName.Trim('\"').Replace("\"", "\"\"")}\""));
        var paramPlaceholders = string.Join(", ", Enumerable.Range(0, mappings.Count).Select(i => $"@p{i}"));

        return $"INSERT INTO {escapedTable} ({columns}) VALUES ({paramPlaceholders});";
    }

    private static string FormatTableName(string tableName)
    {
        if (tableName.Contains('.'))
        {
            var parts = tableName.Split('.');
            return string.Join(".", parts.Select(p => $"\"{p.Trim('\"').Replace("\"", "\"\"")}\""));
        }

        return $"\"{tableName.Trim('\"').Replace("\"", "\"\"")}\"";
    }
}
