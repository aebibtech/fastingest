using System.Data;
using FastIngest.Core.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Sinks;
using MySqlConnector;

namespace FastIngest.MySql;

/// <summary>
/// High-performance bulk ingestion sink persisting batches of records into MySQL or MariaDB
/// via <see cref="MySqlBulkCopy"/> and zero-allocation streaming data readers.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public class MySqlBulkSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly string? _connectionString;
    private readonly MySqlConnection? _connection;
    private readonly string _targetTableName;
    private readonly IReadOnlyList<ColumnMapping<TRecord>> _mappings;
    private readonly MySqlTransaction? _externalTransaction;
    private readonly Func<TRecord, object?>[] _getters;
    private readonly string[] _columnNames;
    private readonly Type[] _columnTypes;

    /// <summary>
    /// Gets the target destination table name.
    /// </summary>
    public string TargetTableName => _targetTableName;

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlBulkSink{TRecord}"/> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <param name="targetTableName">The destination table name (e.g. "customers" or "my_db.customers").</param>
    /// <param name="mappings">The collection of column mappings.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    public MySqlBulkSink(
        string connectionString,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        MySqlTransaction? externalTransaction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for MySQL bulk copy sink.", nameof(mappings));
        }

        _connectionString = connectionString;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _externalTransaction = externalTransaction;

        _getters = mappings.Select(m => m.Getter).ToArray();
        _columnNames = mappings.Select(m => m.ColumnName).ToArray();
        _columnTypes = mappings.Select(m => m.PropertyType).ToArray();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlBulkSink{TRecord}"/> class using an existing <see cref="MySqlConnection"/>.
    /// </summary>
    /// <param name="connection">An existing MySQL connection.</param>
    /// <param name="targetTableName">The destination table name (e.g. "customers" or "my_db.customers").</param>
    /// <param name="mappings">The collection of column mappings.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    public MySqlBulkSink(
        MySqlConnection connection,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        MySqlTransaction? externalTransaction = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for MySQL bulk copy sink.", nameof(mappings));
        }

        _connection = connection;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _externalTransaction = externalTransaction;

        _getters = mappings.Select(m => m.Getter).ToArray();
        _columnNames = mappings.Select(m => m.ColumnName).ToArray();
        _columnTypes = mappings.Select(m => m.PropertyType).ToArray();
    }

    /// <summary>
    /// Writes a batch of records directly into MySQL using <see cref="MySqlBulkCopy"/>.
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

            var bulkCopy = new MySqlBulkCopy(_connection, _externalTransaction);
            return await ExecuteBulkCopyAsync(bulkCopy, batch, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await using var connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            var bulkCopy = new MySqlBulkCopy(connection, _externalTransaction);
            return await ExecuteBulkCopyAsync(bulkCopy, batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<long> ExecuteBulkCopyAsync(
        MySqlBulkCopy bulkCopy,
        IReadOnlyList<TRecord> batch,
        CancellationToken cancellationToken)
    {
        bulkCopy.DestinationTableName = _targetTableName;

        for (int i = 0; i < _columnNames.Length; i++)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, _columnNames[i]));
        }

        using var reader = new BatchDataReader<TRecord>(batch, _getters, _columnNames, _columnTypes);
        var result = await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

        return result.RowsInserted > 0 ? result.RowsInserted : batch.Count;
    }
}
