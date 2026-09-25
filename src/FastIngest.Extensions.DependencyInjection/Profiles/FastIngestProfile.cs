using System.Linq.Expressions;
using FastIngest.Core.Common;
using FastIngest.Core.Mapping;

namespace FastIngest.Extensions.DependencyInjection.Profiles;

/// <summary>
/// Base class for configuring FastIngest mapping and pipeline behavior for <typeparamref name="TRecord"/>.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing an ingested record.</typeparam>
public abstract class FastIngestProfile<TRecord> : IFastIngestProfile<TRecord>
{
    private readonly ColumnMappingBuilder<TRecord> _mappingBuilder = new();
    private IReadOnlyList<ColumnMapping<TRecord>>? _compiledMappings;

    /// <inheritdoc/>
    public Type RecordType => typeof(TRecord);

    /// <inheritdoc/>
    public string? TargetTable { get; protected set; }

    /// <inheritdoc/>
    public string? ConnectionString { get; protected set; }

    /// <inheritdoc/>
    public int BatchSize { get; protected set; } = 5000;

    /// <inheritdoc/>
    public int ChannelCapacity { get; protected set; } = 2;

    /// <inheritdoc/>
    public ErrorStrategy ErrorStrategy { get; protected set; } = ErrorStrategy.FailFast;

    /// <inheritdoc/>
    public FileType FileType { get; protected set; } = FileType.AutoDetect;

    /// <inheritdoc/>
    public System.Text.Json.JsonSerializerOptions? JsonSerializerOptions { get; protected set; }

    /// <summary>
    /// Configures the destination table name for this record profile.
    /// </summary>
    /// <param name="tableName">The PostgreSQL destination table name.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> ToTable(string tableName)
    {
        TargetTable = string.IsNullOrWhiteSpace(tableName) ? throw new ArgumentNullException(nameof(tableName)) : tableName;
        return this;
    }

    /// <summary>
    /// Configures a dedicated connection string override for this record profile.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithConnectionString(string connectionString)
    {
        ConnectionString = string.IsNullOrWhiteSpace(connectionString) ? throw new ArgumentNullException(nameof(connectionString)) : connectionString;
        return this;
    }

    /// <summary>
    /// Configures the batch size for this record profile.
    /// </summary>
    /// <param name="batchSize">The maximum number of rows per batch flush.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithBatchSize(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        }
        BatchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Configures the bounded channel capacity (number of batches in flight) for this record profile.
    /// </summary>
    /// <param name="capacity">The maximum number of batches to buffer in flight concurrently.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithChannelCapacity(int capacity)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "Channel capacity must be greater than zero.");
        }
        ChannelCapacity = capacity;
        return this;
    }

    /// <summary>
    /// Configures the error handling strategy for validation and materialization failures.
    /// </summary>
    /// <param name="errorStrategy">The <see cref="ErrorStrategy"/> to apply.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithErrorStrategy(ErrorStrategy errorStrategy)
    {
        ErrorStrategy = errorStrategy;
        return this;
    }

    /// <summary>
    /// Configures the tabular file format expected by this profile.
    /// </summary>
    /// <param name="fileType">The format of the input file stream.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithFileType(FileType fileType)
    {
        FileType = fileType;
        return this;
    }

    /// <summary>
    /// Configures the custom JSON serialization options used for JSON Lines ingestion.
    /// </summary>
    /// <param name="jsonOptions">The JSON serializer options to apply.</param>
    /// <returns>The current profile instance for fluent chaining.</returns>
    protected FastIngestProfile<TRecord> WithJsonOptions(System.Text.Json.JsonSerializerOptions jsonOptions)
    {
        JsonSerializerOptions = jsonOptions ?? throw new ArgumentNullException(nameof(jsonOptions));
        return this;
    }

    /// <summary>
    /// Maps a record property to a source column by header name using a strongly-typed expression.
    /// </summary>
    /// <typeparam name="TProperty">The property value type.</typeparam>
    /// <param name="propertyExpression">An expression selecting the target property, e.g. <c>x => x.Email</c>.</param>
    /// <param name="columnName">The name of the column in the source data. If omitted, the property name is used.</param>
    /// <returns>The underlying <see cref="ColumnMappingBuilder{TRecord}"/> for fluent chaining.</returns>
    protected ColumnMappingBuilder<TRecord> Map<TProperty>(
        Expression<Func<TRecord, TProperty>> propertyExpression,
        string? columnName = null)
    {
        return _mappingBuilder.Map(propertyExpression, columnName);
    }

    /// <summary>
    /// Maps a record property to a source column by zero-based ordinal index using a strongly-typed expression.
    /// </summary>
    /// <typeparam name="TProperty">The property value type.</typeparam>
    /// <param name="propertyExpression">An expression selecting the target property, e.g. <c>x => x.Id</c>.</param>
    /// <param name="columnIndex">The zero-based index of the column in the source data.</param>
    /// <param name="columnName">Optional column name override for logging and sink destination targeting.</param>
    /// <returns>The underlying <see cref="ColumnMappingBuilder{TRecord}"/> for fluent chaining.</returns>
    protected ColumnMappingBuilder<TRecord> Map<TProperty>(
        Expression<Func<TRecord, TProperty>> propertyExpression,
        int columnIndex,
        string? columnName = null)
    {
        return _mappingBuilder.Map(propertyExpression, columnIndex, columnName);
    }

    /// <summary>
    /// Compiles expressions and caches the resolved column mappings. Subsequent calls return the cached singleton instances.
    /// </summary>
    /// <returns>A read-only collection of <see cref="ColumnMapping{TRecord}"/> items.</returns>
    public IReadOnlyList<ColumnMapping<TRecord>> GetMappings()
    {
        return _compiledMappings ??= _mappingBuilder.Build();
    }
}
