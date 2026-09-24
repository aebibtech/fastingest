using System.Collections;
using System.Data.Common;

namespace FastIngest.Core.Data;

/// <summary>
/// High-performance, zero-allocation <see cref="DbDataReader"/> adapter wrapping an in-memory batch of records.
/// Avoids intermediate <see cref="System.Data.DataTable"/> allocations when feeding bulk copy mechanisms.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public class BatchDataReader<TRecord> : DbDataReader
{
    private readonly IReadOnlyList<TRecord> _records;
    private readonly IReadOnlyList<Func<TRecord, object?>> _getters;
    private readonly IReadOnlyList<string> _columnNames;
    private readonly IReadOnlyList<Type>? _columnTypes;
    private int _currentIndex = -1;
    private bool _isClosed;

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
        : this(records, getters, columnNames, null)
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
    {
        _records = records ?? throw new ArgumentNullException(nameof(records));
        _getters = getters ?? throw new ArgumentNullException(nameof(getters));
        _columnNames = columnNames ?? throw new ArgumentNullException(nameof(columnNames));
        _columnTypes = columnTypes;

        if (_getters.Count != _columnNames.Count)
        {
            throw new ArgumentException(
                $"Getter count ({_getters.Count}) must match column names count ({_columnNames.Count}).",
                nameof(columnNames));
        }

        if (_columnTypes != null && _columnTypes.Count != _columnNames.Count)
        {
            throw new ArgumentException(
                $"Column types count ({_columnTypes.Count}) must match column names count ({_columnNames.Count}).",
                nameof(columnTypes));
        }
    }

    /// <inheritdoc/>
    public override int FieldCount => _columnNames.Count;

    /// <inheritdoc/>
    public override int Depth => 0;

    /// <inheritdoc/>
    public override bool IsClosed => _isClosed;

    /// <inheritdoc/>
    public override int RecordsAffected => -1;

    /// <inheritdoc/>
    public override bool HasRows => _records.Count > 0;

    /// <inheritdoc/>
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc/>
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc/>
    public override bool Read()
    {
        if (_isClosed)
        {
            return false;
        }

        _currentIndex++;
        return _currentIndex < _records.Count;
    }

    /// <inheritdoc/>
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<bool>(cancellationToken);
        }

        return Task.FromResult(Read());
    }

    /// <inheritdoc/>
    public override string GetName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _columnNames[ordinal];
    }

    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        for (int i = 0; i < _columnNames.Count; i++)
        {
            if (string.Equals(_columnNames[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new IndexOutOfRangeException($"Column '{name}' was not found.");
    }

    /// <inheritdoc/>
    public override object GetValue(int ordinal)
    {
        ValidateCurrentRow();
        ValidateOrdinal(ordinal);

        return _getters[ordinal](_records[_currentIndex])!;
    }

    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal)
    {
        ValidateCurrentRow();
        ValidateOrdinal(ordinal);

        var value = GetValue(ordinal);
        return value == null || Convert.IsDBNull(value);
    }

    /// <inheritdoc/>
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<bool>(cancellationToken);
        }

        return Task.FromResult(IsDBNull(ordinal));
    }

    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        ValidateCurrentRow();

        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
        {
            values[i] = GetValue(i) ?? DBNull.Value;
        }
        return count;
    }

    /// <inheritdoc/>
    public override Type GetFieldType(int ordinal)
    {
        ValidateOrdinal(ordinal);

        if (_columnTypes != null)
        {
            return _columnTypes[ordinal];
        }

        if (_currentIndex >= 0 && _currentIndex < _records.Count)
        {
            var value = _getters[ordinal](_records[_currentIndex]);
            if (value != null)
            {
                return value.GetType();
            }
        }

        return typeof(object);
    }

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal)
    {
        return GetFieldType(ordinal).Name;
    }

    /// <inheritdoc/>
    public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);

    /// <inheritdoc/>
    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

    /// <inheritdoc/>
    public override char GetChar(int ordinal) => (char)GetValue(ordinal);

    /// <inheritdoc/>
    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);

    /// <inheritdoc/>
    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);

    /// <inheritdoc/>
    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);

    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    /// <inheritdoc/>
    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);

    /// <inheritdoc/>
    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);

    /// <inheritdoc/>
    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);

    /// <inheritdoc/>
    public override string GetString(int ordinal) => (string)GetValue(ordinal);

    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetBytes is not supported by BatchDataReader.");
    }

    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetChars is not supported by BatchDataReader.");
    }

    /// <inheritdoc/>
    public override T GetFieldValue<T>(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is null or DBNull)
        {
            if (default(T) is null)
            {
                return default!;
            }
            throw new InvalidCastException($"Column '{GetName(ordinal)}' is null and cannot be converted to {typeof(T).Name}.");
        }

        return (T)value;
    }

    /// <inheritdoc/>
    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        return Task.FromResult(GetFieldValue<T>(ordinal));
    }

    /// <inheritdoc/>
    public override bool NextResult() => false;

    /// <inheritdoc/>
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<bool>(cancellationToken);
        }

        return Task.FromResult(false);
    }

    /// <inheritdoc/>
    public override void Close()
    {
        _isClosed = true;
    }

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    private void ValidateCurrentRow()
    {
        if (_currentIndex < 0 || _currentIndex >= _records.Count)
        {
            throw new InvalidOperationException("No current row. Call Read() first.");
        }
    }

    private void ValidateOrdinal(int ordinal)
    {
        if ((uint)ordinal >= (uint)_columnNames.Count)
        {
            throw new IndexOutOfRangeException($"Ordinal {ordinal} is outside the valid range [0, {_columnNames.Count - 1}].");
        }
    }
}
