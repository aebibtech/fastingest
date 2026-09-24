using System.Threading.Channels;

namespace FastIngest.Core.Pipeline;

/// <summary>
/// Configuration options for tuning the concurrent producer-consumer bounded channel within the ingestion pipeline.
/// </summary>
public sealed class PipelineOptions
{
    private int _boundedChannelCapacity = 2;

    /// <summary>
    /// Gets or sets the capacity of the bounded channel (number of batches kept in flight concurrently).
    /// Defaults to 2.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the capacity is less than or equal to zero.</exception>
    public int BoundedChannelCapacity
    {
        get => _boundedChannelCapacity;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Bounded channel capacity must be greater than zero.");
            }
            _boundedChannelCapacity = value;
        }
    }

    /// <summary>
    /// Gets or sets a value indicating whether there is guaranteed to be a single writer writing to the channel.
    /// Defaults to <c>true</c>.
    /// </summary>
    public bool SingleWriter { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether there is guaranteed to be a single reader reading from the channel.
    /// Defaults to <c>true</c>.
    /// </summary>
    public bool SingleReader { get; set; } = true;

    /// <summary>
    /// Gets or sets the backpressure behavior when the bounded channel reaches capacity.
    /// Defaults to <see cref="BoundedChannelFullMode.Wait"/> to guarantee bounded memory without dropping batches.
    /// </summary>
    public BoundedChannelFullMode FullMode { get; set; } = BoundedChannelFullMode.Wait;
}
