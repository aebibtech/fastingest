using System.Globalization;
using System.Reflection;
using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Mapping;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FluentValidation;
using Sylvan.Data.Csv;

namespace FastIngest.Core.Pipeline;

public class FastIngestPipeline<TRecord> : IFastIngestPipeline<TRecord>
{
    private Stream? _stream;
    private FileType _fileType = FileType.AutoDetect;
    private readonly ColumnMappingBuilder<TRecord> _mappingBuilder = new();
    private bool _hasCustomMapping;
    private IValidator<TRecord>? _validator;
    private ValidationOptions _validationOptions = new();
    private int _batchSize = 5000;
    private Action<IngestProgress>? _progressCallback;

    public static FastIngestPipeline<TRecord> Create() => new();

    public IFastIngestPipeline<TRecord> FromStream(Stream stream, FileType fileType = FileType.AutoDetect)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _fileType = fileType;
        return this;
    }

    public IFastIngestPipeline<TRecord> WithMapping(Action<ColumnMappingBuilder<TRecord>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_mappingBuilder);
        _hasCustomMapping = true;
        return this;
    }

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

    public IFastIngestPipeline<TRecord> WithBatchSize(int batchSize = 5000)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        _batchSize = batchSize;
        return this;
    }

    public IFastIngestPipeline<TRecord> OnProgress(Action<IngestProgress> callback)
    {
        _progressCallback = callback ?? throw new ArgumentNullException(nameof(callback));
        return this;
    }

    public IReadOnlyList<ColumnMapping<TRecord>> GetMappings()
    {
        return _hasCustomMapping ? _mappingBuilder.Build() : ColumnMappingBuilder<TRecord>.CreateDefaultMappings();
    }

    public async Task<IngestResult> WriteToSinkAsync(IIngestionSink<TRecord> sink, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        if (_stream == null)
        {
            throw new InvalidOperationException("Source stream has not been configured. Call FromStream() before executing the pipeline.");
        }

        var mappings = GetMappings();
        var errors = new List<IngestRowError>();
        var batch = new List<TRecord>(_batchSize);

        long totalProcessed = 0;
        long totalSucceeded = 0;
        long totalFailed = 0;

        using var streamReader = new StreamReader(_stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);
        var csvOptions = new CsvDataReaderOptions
        {
            HasHeaders = true
        };

        using var csvReader = CsvDataReader.Create(streamReader, csvOptions);

        // Header mapping
        var headerOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < csvReader.FieldCount; i++)
        {
            headerOrdinals[csvReader.GetName(i)] = i;
        }

        var binder = CreateRowBinder(mappings, headerOrdinals);

        long streamLength = _stream.CanSeek ? _stream.Length : 0;

        while (await csvReader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            totalProcessed++;
            long rowIndex = totalProcessed;

            bool rowMaterialized = binder.TryMaterialize(csvReader, rowIndex, out var record, out var materializationErrors);

            if (!rowMaterialized)
            {
                totalFailed++;
                errors.AddRange(materializationErrors);

                if (_validationOptions.ErrorStrategy == ErrorStrategy.FailFast)
                {
                    throw new FastIngestValidationException(
                        $"Materialization failed on row {rowIndex}: {materializationErrors.FirstOrDefault()?.ErrorMessage}",
                        materializationErrors);
                }

                continue;
            }

            if (_validator != null && record != null)
            {
                var validationResult = await _validator.ValidateAsync(record, cancellationToken);
                if (!validationResult.IsValid)
                {
                    totalFailed++;
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
                totalSucceeded++;
            }

            if (batch.Count >= _batchSize)
            {
                await sink.WriteBatchAsync(batch, cancellationToken);
                batch.Clear();

                double? percent = streamLength > 0 && _stream.CanSeek
                    ? Math.Min(100.0, ((double)_stream.Position / streamLength) * 100.0)
                    : null;

                _progressCallback?.Invoke(new IngestProgress(totalProcessed, totalSucceeded, totalFailed, percent));
            }
        }

        if (batch.Count > 0)
        {
            await sink.WriteBatchAsync(batch, cancellationToken);
            batch.Clear();
        }

        _progressCallback?.Invoke(new IngestProgress(totalProcessed, totalSucceeded, totalFailed, 100.0));

        return new IngestResult(totalProcessed, totalSucceeded, totalFailed, errors);
    }

    private static IRowBinder<TRecord> CreateRowBinder(
        IReadOnlyList<ColumnMapping<TRecord>> mappings,
        Dictionary<string, int> headerOrdinals)
    {
        var recordType = typeof(TRecord);

        // Find candidate constructors
        var constructors = recordType.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .ToList();

        foreach (var ctor in constructors)
        {
            var parameters = ctor.GetParameters();
            if (parameters.Length == 0) continue;

            // Check if all parameters can be resolved from mappings
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

        // Fallback to property setter binder
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

    private interface IRowBinder<T>
    {
        bool TryMaterialize(CsvDataReader reader, long rowIndex, out T? record, out List<IngestRowError> errors);
    }

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
                instance = Activator.CreateInstance<T>();
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
