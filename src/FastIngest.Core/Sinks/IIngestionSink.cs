namespace FastIngest.Core.Sinks;

public interface IIngestionSink<TRecord>
{
    Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken);
}
