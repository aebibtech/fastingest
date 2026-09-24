using FastIngest.Core.Common;
using FastIngest.Core.Mapping;

namespace FastIngest.Extensions.DependencyInjection.Profiles;

/// <summary>
/// Non-generic interface representing an ingestion profile configuration for a record type.
/// </summary>
public interface IFastIngestProfile
{
    /// <summary>
    /// Gets the CLR type of the target record model.
    /// </summary>
    Type RecordType { get; }

    /// <summary>
    /// Gets the destination database table name (optionally schema-qualified).
    /// </summary>
    string? TargetTable { get; }

    /// <summary>
    /// Gets the specific connection string for this profile, or null to use the default.
    /// </summary>
    string? ConnectionString { get; }

    /// <summary>
    /// Gets the batch size for chunked writing to destination sinks.
    /// </summary>
    int BatchSize { get; }

    /// <summary>
    /// Gets the error handling strategy when encountering invalid rows.
    /// </summary>
    ErrorStrategy ErrorStrategy { get; }

    /// <summary>
    /// Gets the expected tabular file format (CSV, XLSX, or AutoDetect).
    /// </summary>
    FileType FileType { get; }
}

/// <summary>
/// Strongly-typed interface representing an ingestion profile for <typeparamref name="TRecord"/>.
/// </summary>
/// <typeparam name="TRecord">The domain record type.</typeparam>
public interface IFastIngestProfile<TRecord> : IFastIngestProfile
{
    /// <summary>
    /// Returns the pre-compiled column mappings for <typeparamref name="TRecord"/>.
    /// </summary>
    /// <returns>A read-only collection of <see cref="ColumnMapping{TRecord}"/> items.</returns>
    IReadOnlyList<ColumnMapping<TRecord>> GetMappings();
}
