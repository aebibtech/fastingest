using System.Threading.Channels;

namespace FastIngest.AspNetCore.Workers;

/// <summary>
/// In-memory bounded queue for dispatching ingestion jobs to background workers.
/// </summary>
public class IngestJobQueue
{
    private readonly Channel<IngestJob> _queue;

    /// <summary>
    /// Initializes a new instance of <see cref="IngestJobQueue"/> with the specified channel capacity.
    /// </summary>
    /// <param name="capacity">The maximum number of jobs that can be queued simultaneously.</param>
    public IngestJobQueue(int capacity = 100)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };
        _queue = Channel.CreateBounded<IngestJob>(options);
    }

    /// <summary>
    /// Asynchronously enqueues an ingestion job.
    /// </summary>
    /// <param name="job">The ingestion job descriptor.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the asynchronous queue operation.</returns>
    public async ValueTask QueueJobAsync(IngestJob job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        await _queue.Writer.WriteAsync(job, cancellationToken);
    }

    /// <summary>
    /// Reads all queued ingestion jobs asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of <see cref="IngestJob"/>.</returns>
    public IAsyncEnumerable<IngestJob> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        return _queue.Reader.ReadAllAsync(cancellationToken);
    }
}
