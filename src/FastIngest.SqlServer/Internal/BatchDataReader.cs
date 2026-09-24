using FastIngest.Core.Data;

namespace FastIngest.SqlServer.Internal;

/// <summary>
/// High-performance, zero-allocation <see cref="System.Data.Common.DbDataReader"/> adapter wrapping an in-memory batch of records.
/// Avoids intermediate <see cref="System.Data.DataTable"/> allocations when feeding <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
internal sealed class BatchDataReader<TRecord> : FastIngest.Core.Data.BatchDataReader<TRecord>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BatchDataReader{TRecord}"/> class.
    /// </summary>
    /// <param name="records">The batch of records to stream.</param>
    /// <param name="getters">The compiled column getter delegates.</param>
    /// <param name="columnNames">The list of destination column names.</param>
    public BatchDataReader(
        IReadOnlyList<TRecord> records,
        IReadOnlyList<Func<TRecord, object?>> getters,
        IReadOnlyList<string> columnNames)
        : base(records, getters, columnNames)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="BatchDataReader{TRecord}"/> class with optional column types.
    /// </summary>
    /// <param name="records">The batch of records to stream.</param>
    /// <param name="getters">The compiled column getter delegates.</param>
    /// <param name="columnNames">The list of destination column names.</param>
    /// <param name="columnTypes">Optional CLR types of the mapped columns.</param>
    public BatchDataReader(
        IReadOnlyList<TRecord> records,
        IReadOnlyList<Func<TRecord, object?>> getters,
        IReadOnlyList<string> columnNames,
        IReadOnlyList<Type>? columnTypes)
        : base(records, getters, columnNames, columnTypes)
    {
    }
}
