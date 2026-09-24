using FastIngest.Core.Common;
using FastIngest.Core.Mapping;
using FastIngest.Core.Results;
using FastIngest.Core.Sinks;
using FluentValidation;

namespace FastIngest.Core.Pipeline;

public interface IFastIngestPipeline<TRecord>
{
    IFastIngestPipeline<TRecord> FromStream(Stream stream, FileType fileType = FileType.AutoDetect);
    IFastIngestPipeline<TRecord> WithMapping(Action<ColumnMappingBuilder<TRecord>> configure);
    IFastIngestPipeline<TRecord> ValidateWith<TValidator>(Action<ValidationOptions>? configure = null) where TValidator : IValidator<TRecord>, new();
    IFastIngestPipeline<TRecord> ValidateWith(IValidator<TRecord> validator, Action<ValidationOptions>? configure = null);
    IFastIngestPipeline<TRecord> WithBatchSize(int batchSize = 5000);
    IFastIngestPipeline<TRecord> OnProgress(Action<IngestProgress> callback);
    IReadOnlyList<ColumnMapping<TRecord>> GetMappings();
    Task<IngestResult> WriteToSinkAsync(IIngestionSink<TRecord> sink, CancellationToken cancellationToken = default);
}
