namespace FastIngest.Core.Resilience;

/// <summary>
/// Configures optional transient fault handling and retry behavior for batch sink writes.
/// </summary>
public sealed class ResilienceOptions
{
    private int _maxRetryAttempts = 3;
    private TimeSpan _baseDelay = TimeSpan.FromMilliseconds(500);
    private TimeSpan _maxDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets a value indicating whether transient retry is enabled.
    /// Defaults to <c>false</c> (opt-in).
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Gets or sets the maximum number of retry attempts after the first failure.
    /// Defaults to 3.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is less than or equal to 0.</exception>
    public int MaxRetryAttempts
    {
        get => _maxRetryAttempts;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Max retry attempts must be greater than zero.");
            }
            _maxRetryAttempts = value;
        }
    }

    /// <summary>
    /// Gets or sets the base delay between retry attempts.
    /// Exponential backoff doubles this value per attempt.
    /// Defaults to 500 ms.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is negative.</exception>
    public TimeSpan BaseDelay
    {
        get => _baseDelay;
        set
        {
            if (value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Base delay cannot be negative.");
            }
            _baseDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum total delay cap for a single retry sequence.
    /// Defaults to 30 seconds.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is negative.</exception>
    public TimeSpan MaxDelay
    {
        get => _maxDelay;
        set
        {
            if (value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Max delay cannot be negative.");
            }
            _maxDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets whether decorrelated jitter is applied to each backoff delay.
    /// Defaults to <c>true</c> to avoid thundering-herd on distributed clusters.
    /// </summary>
    public bool UseJitter { get; set; } = true;

    /// <summary>
    /// Gets or sets a predicate that classifies an exception as transient (retryable).
    /// When <c>null</c>, a built-in predicate handles common database transient faults:
    /// <list type="bullet">
    ///   <item>Npgsql <c>NpgsqlException</c> with <c>IsTransient = true</c></item>
    ///   <item>SQL Server <c>SqlException</c> with deadlock error numbers (1205, -2, 233, 10054)</item>
    ///   <item><see cref="TimeoutException"/></item>
    ///   <item>MySQL transient connection errors (08xxx SQLState)</item>
    /// </list>
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; set; }
}
