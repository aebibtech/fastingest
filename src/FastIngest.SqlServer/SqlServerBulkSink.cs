using System.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Sinks;
using FastIngest.SqlServer.Internal;
using Microsoft.Data.SqlClient;

namespace FastIngest.SqlServer;

/// <summary>
/// High-performance bulk ingestion sink persisting batches of records into Microsoft SQL Server or Azure SQL
/// via <see cref="SqlBulkCopy"/> and zero-allocation streaming data readers.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public class SqlServerBulkSink<TRecord> : IIngestionSink<TRecord>
{
    private readonly string? _connectionString;
    private readonly SqlConnection? _connection;
    private readonly string _targetTableName;
    private readonly IReadOnlyList<ColumnMapping<TRecord>> _mappings;
    private readonly SqlBulkCopyOptions _options;
    private readonly SqlTransaction? _externalTransaction;
    private readonly Func<TRecord, object?>[] _getters;
    private readonly string[] _columnNames;
    private readonly Type[] _columnTypes;

    /// <summary>
    /// Gets the target destination table name.
    /// </summary>
    public string TargetTableName => _targetTableName;

    /// <summary>
    /// Gets the bulk copy options applied during ingestion.
    /// </summary>
    public SqlBulkCopyOptions Options => _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlServerBulkSink{TRecord}"/> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="targetTableName">The destination table name (e.g. "Customers" or "dbo.Customers").</param>
    /// <param name="mappings">The collection of column mappings.</param>
    /// <param name="options">The <see cref="SqlBulkCopyOptions"/> flags to use. Defaults to <see cref="SqlBulkCopyOptions.Default"/> | <see cref="SqlBulkCopyOptions.CheckConstraints"/>.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    public SqlServerBulkSink(
        string connectionString,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        SqlBulkCopyOptions options = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.CheckConstraints,
        SqlTransaction? externalTransaction = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for SQL Server bulk copy sink.", nameof(mappings));
        }

        _connectionString = connectionString;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _options = options;
        _externalTransaction = externalTransaction;

        _getters = mappings.Select(m => m.Getter).ToArray();
        _columnNames = mappings.Select(m => m.ColumnName).ToArray();
        _columnTypes = mappings.Select(m => m.PropertyType).ToArray();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlServerBulkSink{TRecord}"/> class using an existing <see cref="SqlConnection"/>.
    /// </summary>
    /// <param name="connection">An existing SQL Server connection.</param>
    /// <param name="targetTableName">The destination table name (e.g. "Customers" or "dbo.Customers").</param>
    /// <param name="mappings">The collection of column mappings.</param>
    /// <param name="options">The <see cref="SqlBulkCopyOptions"/> flags to use. Defaults to <see cref="SqlBulkCopyOptions.Default"/> | <see cref="SqlBulkCopyOptions.CheckConstraints"/>.</param>
    /// <param name="externalTransaction">Optional external transaction under which the bulk copy operates.</param>
    public SqlServerBulkSink(
        SqlConnection connection,
        string targetTableName,
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        SqlBulkCopyOptions options = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.CheckConstraints,
        SqlTransaction? externalTransaction = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTableName);
        ArgumentNullException.ThrowIfNull(mappings);

        if (mappings.Count == 0)
        {
            throw new ArgumentException("At least one column mapping is required for SQL Server bulk copy sink.", nameof(mappings));
        }

        _connection = connection;
        _targetTableName = targetTableName;
        _mappings = mappings;
        _options = options;
        _externalTransaction = externalTransaction;

        _getters = mappings.Select(m => m.Getter).ToArray();
        _columnNames = mappings.Select(m => m.ColumnName).ToArray();
        _columnTypes = mappings.Select(m => m.PropertyType).ToArray();
    }

    /// <summary>
    /// Writes a batch of records directly into Microsoft SQL Server using <see cref="SqlBulkCopy"/>.
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

            using var bulkCopy = new SqlBulkCopy(_connection, _options, _externalTransaction);
            return await ExecuteBulkCopyAsync(bulkCopy, batch, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var bulkCopy = new SqlBulkCopy(connection, _options, _externalTransaction);
            return await ExecuteBulkCopyAsync(bulkCopy, batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<long> ExecuteBulkCopyAsync(
        SqlBulkCopy bulkCopy,
        IReadOnlyList<TRecord> batch,
        CancellationToken cancellationToken)
    {
        bulkCopy.DestinationTableName = _targetTableName;

        foreach (var colName in _columnNames)
        {
            bulkCopy.ColumnMappings.Add(colName, colName);
        }

        using var reader = new BatchDataReader<TRecord>(batch, _getters, _columnNames, _columnTypes);
        await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

        return batch.Count;
    }
}
