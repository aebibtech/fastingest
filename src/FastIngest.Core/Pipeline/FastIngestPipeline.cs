using System.Globalization;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Channels;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Mapping;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FluentValidation;
using Sylvan.Data.Csv;

namespace FastIngest.Core.Pipeline;

/// <summary>
/// High-throughput, constant-memory streaming ingestion pipeline for .NET.
/// Parses tabular data row-by-row and streams batches directly to destination sinks.
/// </summary>
/// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
public class FastIngestPipeline<TRecord> : IFastIngestPipeline<TRecord>
{
    private Stream? _stream;
    private FileType _fileType = FileType.AutoDetect;
    private readonly ColumnMappingBuilder<TRecord> _mappingBuilder = new();
    private bool _hasCustomMapping;
    private IReadOnlyList<ColumnMapping<TRecord>>? _customMappings;
    private IValidator<TRecord>? _validator;
    private ValidationOptions _validationOptions = new();
    private int _batchSize = 5000;
    private readonly PipelineOptions _pipelineOptions = new();
    private Action<IngestProgress>? _progressCallback;

    /// <summary>
    /// Creates a new fluent pipeline builder instance for <typeparamref name="TRecord"/>.
    /// </summary>
    /// <returns>A new <see cref="FastIngestPipeline{TRecord}"/> instance.</returns>
    public static FastIngestPipeline<TRecord> Create() => new();

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> FromStream(Stream stream, FileType fileType = FileType.AutoDetect)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _fileType = fileType;
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> WithMapping(Action<ColumnMappingBuilder<TRecord>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_mappingBuilder);
        _hasCustomMapping = true;
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> WithMappings(IReadOnlyList<ColumnMapping<TRecord>> mappings)
    {
        _customMappings = mappings ?? throw new ArgumentNullException(nameof(mappings));
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> ValidateWith<TValidator>(Action<ValidationOptions>? configure = null)
        where TValidator : IValidator<TRecord>, new()
    {
        _validator = new TValidator();
        if (configure != null)
        {
            _validationOptions = new ValidationOptions();
            configure(_validationOptions);
        }
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> ValidateWith(IValidator<TRecord> validator, Action<ValidationOptions>? configure = null)
    {
        _validator = validator ?? throw new ArgumentNullException(nameof(validator));
        if (configure != null)
        {
            _validationOptions = new ValidationOptions();
            configure(_validationOptions);
        }
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> WithBatchSize(int batchSize = 5000)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        _batchSize = batchSize;
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> WithChannelCapacity(int capacity = 2)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Channel capacity must be greater than zero.");
        _pipelineOptions.BoundedChannelCapacity = capacity;
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> WithOptions(Action<PipelineOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_pipelineOptions);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestPipeline<TRecord> OnProgress(Action<IngestProgress> callback)
    {
        _progressCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    /// <inheritdoc/>
    public IReadOnlyList<ColumnMapping<TRecord>> GetMappings()
    {
        if (_customMappings != null)
        {
            return _customMappings;
        }

        return _hasCustomMapping ? _mappingBuilder.Build() : ColumnMappingBuilder<TRecord>.CreateDefaultMappings();
    }

    /// <inheritdoc/>
    public async Task<IngestResult> WriteToSinkAsync(IIngestionSink<TRecord> sink, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        if (_stream == null)
        {
            throw new InvalidOperationException("Source stream has not been configured. Call FromStream() before executing the pipeline.");
        }

        var mappings = GetMappings();
        var errors = new List<IngestRowError>();

        long totalProcessed = 0;
        long totalSucceeded = 0;
        long totalFailed = 0;
        long streamPosition = 0;
        long streamLength = _stream.CanSeek ? _stream.Length : 0;

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var ct = linkedCts.Token;

        var channel = Channel.CreateBounded<IReadOnlyList<TRecord>>(new BoundedChannelOptions(_pipelineOptions.BoundedChannelCapacity)
        {
            SingleWriter = _pipelineOptions.SingleWriter,
            SingleReader = _pipelineOptions.SingleReader,
            FullMode = _pipelineOptions.FullMode
        });

        var producerTask = Task.Run(async () =>
        {
            try
            {
                // Initialize zero-allocation Sylvan CsvDataReader over stream
                using var streamReader = new StreamReader(_stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);
                var csvOptions = new CsvDataReaderOptions
                {
                    HasHeaders = true
                };

                using var csvReader = CsvDataReader.Create(streamReader, csvOptions);

                // Map column header names to zero-based ordinals for fast O(1) index lookups
                var headerOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < csvReader.FieldCount; i++)
                {
                    headerOrdinals[csvReader.GetName(i)] = i;
                }

                // Prepare high-performance binder (constructor-based for positional records or property-setter based)
                var binder = CreateRowBinder(mappings, headerOrdinals);

                var batch = new List<TRecord>(_batchSize);

                // Streaming row-by-row iteration without loading whole dataset into memory
                while (await csvReader.ReadAsync(ct))
                {
                    ct.ThrowIfCancellationRequested();
                    Interlocked.Increment(ref totalProcessed);
                    long rowIndex = totalProcessed;

                    if (_stream.CanSeek)
                    {
                        Volatile.Write(ref streamPosition, _stream.Position);
                    }

                    // Attempt to materialize TRecord from current CSV row
                    bool rowMaterialized = binder.TryMaterialize(csvReader, rowIndex, out var record, out var materializationErrors);

                    if (!rowMaterialized)
                    {
                        Interlocked.Increment(ref totalFailed);
                        errors.AddRange(materializationErrors);

                        if (_validationOptions.ErrorStrategy == ErrorStrategy.FailFast)
                        {
                            throw new FastIngestValidationException(
                                $"Materialization failed on row {rowIndex}: {materializationErrors.FirstOrDefault()?.ErrorMessage}",
                                materializationErrors);
                        }

                        continue;
                    }

                    // Execute FluentValidation rules if configured
                    if (_validator != null && record != null)
                    {
                        var validationResult = await _validator.ValidateAsync(record, ct);
                        if (!validationResult.IsValid)
                        {
                            Interlocked.Increment(ref totalFailed);
                            var rowErrors = validationResult.Errors.Select(e =>
                                new IngestRowError(rowIndex, e.PropertyName, e.AttemptedValue?.ToString() ?? string.Empty, e.ErrorMessage)
                            ).ToList();

                            errors.AddRange(rowErrors);

                            if (_validationOptions.ErrorStrategy == ErrorStrategy.FailFast)
                            {
                                throw new FastIngestValidationException(
                                    $"Validation failed on row {rowIndex}: {rowErrors.FirstOrDefault()?.ErrorMessage}",
                                    rowErrors);
                            }

                            continue;
                        }
                    }

                    if (record != null)
                    {
                        batch.Add(record);
                    }

                    // Flush chunk batch when threshold reached
                    if (batch.Count >= _batchSize)
                    {
                        await channel.Writer.WriteAsync(batch, ct);
                        batch = new List<TRecord>(_batchSize);
                    }
                }

                // Flush remaining trailing records
                if (batch.Count > 0)
                {
                    await channel.Writer.WriteAsync(batch, ct);
                }

                if (_stream.CanSeek)
                {
                    Volatile.Write(ref streamPosition, _stream.Length);
                }

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                try { linkedCts.Cancel(); } catch { }
                throw;
            }
        }, ct);

        var consumerTask = Task.Run(async () =>
        {
            long succeeded = 0;
            try
            {
                await foreach (var batch in channel.Reader.ReadAllAsync(ct))
                {
                    long written = await sink.WriteBatchAsync(batch, ct);
                    succeeded += written > 0 ? written : batch.Count;
                    Interlocked.Exchange(ref totalSucceeded, succeeded);

                    double? percent = streamLength > 0
                        ? Math.Min(100.0, ((double)Volatile.Read(ref streamPosition) / streamLength) * 100.0)
                        : null;

                    _progressCallback?.Invoke(new IngestProgress(
                        Interlocked.Read(ref totalProcessed),
                        succeeded,
                        Interlocked.Read(ref totalFailed),
                        percent));
                }

                return succeeded;
            }
            catch (Exception)
            {
                try { linkedCts.Cancel(); } catch { }
                throw;
            }
        }, ct);

        try
        {
            await Task.WhenAll(producerTask, consumerTask);
        }
        catch
        {
            if (cancellationToken.IsCancellationRequested)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            var realExceptions = new List<Exception>();

            if (producerTask.IsFaulted && producerTask.Exception != null)
            {
                realExceptions.AddRange(producerTask.Exception.InnerExceptions.Where(e => e is not OperationCanceledException));
            }

            if (consumerTask.IsFaulted && consumerTask.Exception != null)
            {
                realExceptions.AddRange(consumerTask.Exception.InnerExceptions.Where(e => e is not OperationCanceledException));
            }

            if (realExceptions.Count > 0)
            {
                var validationEx = realExceptions.OfType<FastIngestValidationException>().FirstOrDefault();
                if (validationEx != null)
                {
                    throw validationEx;
                }

                ExceptionDispatchInfo.Capture(realExceptions[0]).Throw();
            }

            throw;
        }

        totalSucceeded = await consumerTask;

        // Final progress report at 100%
        _progressCallback?.Invoke(new IngestProgress(totalProcessed, totalSucceeded, totalFailed, 100.0));

        return new IngestResult(totalProcessed, totalSucceeded, totalFailed, errors);
    }

    /// <summary>
    /// Inspects the target type constructors and properties to generate an optimized binder.
    /// </summary>
    private static IRowBinder<TRecord> CreateRowBinder(
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        Dictionary<string, int> headerOrdinals)
    {
        var recordType = typeof(TRecord);

        // Inspect public constructors (preferring parameterized constructors for positional records)
        var constructors = recordType.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .ToList();

        foreach (var ctor in constructors)
        {
            var parameters = ctor.GetParameters();
            if (parameters.Length == 0) continue;

            var paramBindings = new List<(ParameterInfo Parameter, int Ordinal, string ColumnName)>();
            bool allMatched = true;

            foreach (var param in parameters)
            {
                var matchingMapping = mappings.FirstOrDefault(m =>
                    string.Equals(m.PropertyName, param.Name, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(m.ColumnName, param.Name, StringComparison.OrdinalIgnoreCase));

                int ordinal = -1;
                string colName = param.Name ?? string.Empty;

                if (matchingMapping != null)
                {
                    colName = matchingMapping.ColumnName;
                    if (matchingMapping.ColumnIndex.HasValue)
                    {
                        ordinal = matchingMapping.ColumnIndex.Value;
                    }
                    else if (headerOrdinals.TryGetValue(matchingMapping.ColumnName, out int ord))
                    {
                        ordinal = ord;
                    }
                    else if (headerOrdinals.TryGetValue(matchingMapping.PropertyName, out int propOrd))
                    {
                        ordinal = propOrd;
                    }
                }
                else if (headerOrdinals.TryGetValue(param.Name ?? string.Empty, out int directOrd))
                {
                    ordinal = directOrd;
                }

                if (ordinal >= 0)
                {
                    paramBindings.Add((param, ordinal, colName));
                }
                else
                {
                    allMatched = false;
                    break;
                }
            }

            if (allMatched)
            {
                return new ConstructorRowBinder<TRecord>(ctor, paramBindings);
            }
        }

        // Fallback to property setter binder for mutable POCOs
        var propertyBindings = new List<(ColumnMapping<TRecord> Mapping, int Ordinal)>();
        foreach (var mapping in mappings)
        {
            int ordinal = -1;
            if (mapping.ColumnIndex.HasValue)
            {
                ordinal = mapping.ColumnIndex.Value;
            }
            else if (headerOrdinals.TryGetValue(mapping.ColumnName, out int ord))
            {
                ordinal = ord;
            }
            else if (headerOrdinals.TryGetValue(mapping.PropertyName, out int propOrd))
            {
                ordinal = propOrd;
            }

            if (ordinal >= 0)
            {
                propertyBindings.Add((mapping, ordinal));
            }
        }

        return new PropertySetterRowBinder<TRecord>(propertyBindings);
    }

    /// <summary>
    /// Internal row materialization abstraction.
    /// </summary>
    private interface IRowBinder<T>
    {
        bool TryMaterialize(CsvDataReader reader, long rowIndex, out T? record, out List<IngestRowError> errors);
    }

    /// <summary>
    /// Materializes records using a parameterized constructor (e.g. C# positional records).
    /// </summary>
    private sealed class ConstructorRowBinder<T> : IRowBinder<T>
    {
        private readonly ConstructorInfo _constructor;
        private readonly List<(ParameterInfo Parameter, int Ordinal, string ColumnName)> _bindings;

        public ConstructorRowBinder(ConstructorInfo constructor, List<(ParameterInfo, int, string)> bindings)
        {
            _constructor = constructor;
            _bindings = bindings;
        }

        public bool TryMaterialize(CsvDataReader reader, long rowIndex, out T? record, out List<IngestRowError> errors)
        {
            errors = new List<IngestRowError>();
            var args = new object?[_bindings.Count];

            for (int i = 0; i < _bindings.Count; i++)
            {
                var (param, ordinal, colName) = _bindings[i];
                string? raw = null;
                try
                {
                    if (reader.IsDBNull(ordinal))
                    {
                        raw = null;
                    }
                    else
                    {
                        raw = reader.GetString(ordinal);
                    }

                    args[i] = ConvertValue(raw, param.ParameterType);
                }
                catch (Exception ex)
                {
                    errors.Add(new IngestRowError(rowIndex, colName, raw ?? string.Empty, $"Cannot convert '{raw}' to {param.ParameterType.Name}: {ex.Message}"));
                }
            }

            if (errors.Count > 0)
            {
                record = default;
                return false;
            }

            try
            {
                record = (T)_constructor.Invoke(args);
                return true;
            }
            catch (Exception ex)
            {
                errors.Add(new IngestRowError(rowIndex, null, string.Empty, $"Failed to instantiate record: {ex.InnerException?.Message ?? ex.Message}"));
                record = default;
                return false;
            }
        }
    }

    /// <summary>
    /// Materializes records using parameterless constructor and property setters.
    /// </summary>
    private sealed class PropertySetterRowBinder<T> : IRowBinder<T>
    {
        private readonly List<(ColumnMapping<T> Mapping, int Ordinal)> _bindings;

        public PropertySetterRowBinder(List<(ColumnMapping<T>, int)> bindings)
        {
            _bindings = bindings;
        }

        public bool TryMaterialize(CsvDataReader reader, long rowIndex, out T? record, out List<IngestRowError> errors)
        {
            errors = new List<IngestRowError>();
            T instance;
            try
            {
                instance = Activator.CreateInstance<T>()!;
            }
            catch (Exception ex)
            {
                errors.Add(new IngestRowError(rowIndex, null, string.Empty, $"Failed to instantiate record: {ex.Message}"));
                record = default;
                return false;
            }

            foreach (var (mapping, ordinal) in _bindings)
            {
                if (mapping.Setter == null) continue;

                string? raw = null;
                try
                {
                    if (reader.IsDBNull(ordinal))
                    {
                        raw = null;
                    }
                    else
                    {
                        raw = reader.GetString(ordinal);
                    }

                    var val = ConvertValue(raw, mapping.PropertyType);
                    mapping.Setter(instance, val);
                }
                catch (Exception ex)
                {
                    errors.Add(new IngestRowError(rowIndex, mapping.ColumnName, raw ?? string.Empty, $"Cannot convert '{raw}' to {mapping.PropertyType.Name}: {ex.Message}"));
                }
            }

            if (errors.Count > 0)
            {
                record = default;
                return false;
            }

            record = instance;
            return true;
        }
    }

    /// <summary>
    /// Converts a raw string representation to the target CLR type with invariant culture parsing.
    /// </summary>
    private static object? ConvertValue(string? raw, Type targetType)
    {
        if (raw == null || (string.IsNullOrWhiteSpace(raw) && targetType != typeof(string)))
        {
            if (Nullable.GetUnderlyingType(targetType) != null || !targetType.IsValueType)
            {
                return null;
            }
            throw new FormatException($"Value cannot be empty for non-nullable type {targetType.Name}.");
        }

        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlyingType == typeof(string)) return raw;
        if (underlyingType == typeof(int)) return int.Parse(raw, CultureInfo.InvariantCulture);
        if (underlyingType == typeof(long)) return long.Parse(raw, CultureInfo.InvariantCulture);
        if (underlyingType == typeof(decimal)) return decimal.Parse(raw, CultureInfo.InvariantCulture);
        if (underlyingType == typeof(double)) return double.Parse(raw, CultureInfo.InvariantCulture);
        if (underlyingType == typeof(float)) return float.Parse(raw, CultureInfo.InvariantCulture);
        if (underlyingType == typeof(bool))
        {
            if (bool.TryParse(raw, out bool b)) return b;
            if (raw == "1") return true;
            if (raw == "0") return false;
            return bool.Parse(raw);
        }
        if (underlyingType == typeof(DateTime))
        {
            return DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        }
        if (underlyingType == typeof(DateTimeOffset))
        {
            return DateTimeOffset.Parse(raw, CultureInfo.InvariantCulture);
        }
        if (underlyingType == typeof(Guid)) return Guid.Parse(raw);
        if (underlyingType.IsEnum) return Enum.Parse(underlyingType, raw, ignoreCase: true);

        return Convert.ChangeType(raw, underlyingType, CultureInfo.InvariantCulture);
    }
}
