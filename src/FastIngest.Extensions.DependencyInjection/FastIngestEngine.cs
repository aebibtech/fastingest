using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FastIngest.PostgreSql;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace FastIngest.Extensions.DependencyInjection;

/// <summary>
/// Scoped ingestion engine resolving profiles, validators, and sinks from the active DI container.
/// </summary>
public class FastIngestEngine : IFastIngestEngine
{
    private readonly IFastIngestProfileRegistry _profileRegistry;
    private readonly IServiceProvider _serviceProvider;
    private readonly FastIngestOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="FastIngestEngine"/> class.
    /// </summary>
    /// <param name="profileRegistry">The singleton profile cache registry.</param>
    /// <param name="serviceProvider">The active scoped service provider.</param>
    /// <param name="options">The configured FastIngest options.</param>
    public FastIngestEngine(
        IFastIngestProfileRegistry profileRegistry,
        IServiceProvider serviceProvider,
        IOptions<FastIngestOptions> options)
    {
        _profileRegistry = profileRegistry ?? throw new ArgumentNullException(nameof(profileRegistry));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc/>
    public Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        return IngestCoreAsync<TRecord>(stream, customSink: null, tableNameOverride: null, onProgress: null, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        Action<IngestProgress>? onProgress,
        CancellationToken cancellationToken = default)
    {
        return IngestCoreAsync<TRecord>(stream, customSink: null, tableNameOverride: null, onProgress: onProgress, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        IIngestionSink<TRecord> sink,
        Action<IngestProgress>? onProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        return IngestCoreAsync(stream, customSink: sink, tableNameOverride: null, onProgress: onProgress, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IngestResult> IngestAsync<TRecord>(
        Stream stream,
        string tableName,
        Action<IngestProgress>? onProgress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        return IngestCoreAsync<TRecord>(stream, customSink: null, tableNameOverride: tableName, onProgress: onProgress, cancellationToken);
    }

    private async Task<IngestResult> IngestCoreAsync<TRecord>(
        Stream stream,
        IIngestionSink<TRecord>? customSink,
        string? tableNameOverride,
        Action<IngestProgress>? onProgress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);

        // 1. Resolve registered profile from registry or DI
        var profile = _profileRegistry.GetProfile<TRecord>()
            ?? _serviceProvider.GetService<FastIngestProfile<TRecord>>();

        if (profile == null)
        {
            throw new InvalidOperationException(
                $"No FastIngestProfile found for record type '{typeof(TRecord).FullName}'. " +
                $"Ensure a profile inheriting FastIngestProfile<{typeof(TRecord).Name}> is registered in DI.");
        }

        // 2. Retrieve pre-compiled column mappings (cached delegate getters/setters)
        var mappings = profile.GetMappings();

        int batchSize = profile.BatchSize > 0 ? profile.BatchSize : _options.DefaultBatchSize;

        // 3. Construct high-throughput streaming pipeline
        var pipeline = FastIngestPipeline<TRecord>.Create()
            .FromStream(stream, profile.FileType)
            .WithMappings(mappings)
            .WithBatchSize(batchSize);

        if (onProgress != null)
        {
            pipeline.OnProgress(onProgress);
        }

        // 4. Resolve validator from active IServiceProvider if registered
        var validator = _serviceProvider.GetService<IValidator<TRecord>>();
        if (validator != null)
        {
            pipeline.ValidateWith(validator, opt =>
            {
                opt.ErrorStrategy = profile.ErrorStrategy;
            });
        }

        // 5. Resolve destination sink
        if (customSink != null)
        {
            return await pipeline.WriteToSinkAsync(customSink, cancellationToken);
        }

        var diSink = _serviceProvider.GetService<IIngestionSink<TRecord>>();
        if (diSink != null)
        {
            return await pipeline.WriteToSinkAsync(diSink, cancellationToken);
        }

        // 6. Fall back to PostgreSQL sink
        var connectionString = !string.IsNullOrWhiteSpace(profile.ConnectionString)
            ? profile.ConnectionString
            : _options.DefaultConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                $"No connection string configured for {typeof(TRecord).Name}. " +
                $"Configure ConnectionString on FastIngestProfile<{typeof(TRecord).Name}>, " +
                $"specify DefaultConnectionString via AddPostgreSqlSink(...), or register an IIngestionSink<{typeof(TRecord).Name}> in DI.");
        }

        var targetTable = !string.IsNullOrWhiteSpace(tableNameOverride)
            ? tableNameOverride
            : profile.TargetTable;

        if (string.IsNullOrWhiteSpace(targetTable))
        {
            throw new InvalidOperationException(
                $"No target table configured for {typeof(TRecord).Name}. " +
                $"Configure TargetTable on FastIngestProfile<{typeof(TRecord).Name}> or provide a tableName override.");
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var postgresSink = new PostgreSqlSink<TRecord>(connection, targetTable, mappings);
        return await pipeline.WriteToSinkAsync(postgresSink, cancellationToken);
    }
}
