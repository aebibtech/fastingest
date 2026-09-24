namespace FastIngest.Core.Sinks;

/// <summary>
/// Defines a destination sink capable of consuming batches of ingested records.
/// </summary>
/// <typeparam name="TRecord">The strongly-typed model representing the parsed row.</typeparam>
public interface IIngestionSink<TRecord>
{
    /// <summary>
    /// Writes a batch of validated records into the underlying destination store.
    /// </summary>
    /// <param name="batch">The read-only collection of records to persist.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous write operation, containing the number of rows successfully written.</returns>
    Task<long> WriteBatchAsync(IReadOnlyList<TRecord> batch, CancellationToken cancellationToken);
}
