namespace FastIngest.Elasticsearch;

/// <summary>
/// Exception thrown when one or more document operations fail during Elasticsearch bulk ingestion.
/// </summary>
public class IngestionSinkException : Exception
{
    /// <summary>
    /// Gets the list of document indices within the batch that failed during bulk ingestion.
    /// </summary>
    public IReadOnlyList<int> FailedIndices { get; }

    /// <summary>
    /// Gets the list of error messages or diagnostic details for each failed document.
    /// </summary>
    public IReadOnlyList<string> FailedItems { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="IngestionSinkException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message describing the failure.</param>
    public IngestionSinkException(string message)
        : base(message)
    {
        FailedIndices = Array.Empty<int>();
        FailedItems = Array.Empty<string>();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IngestionSinkException"/> class with a specified error message and inner exception.
    /// </summary>
    /// <param name="message">The message describing the failure.</param>
    /// <param name="innerException">The inner exception that caused the failure.</param>
    public IngestionSinkException(string message, Exception innerException)
        : base(message, innerException)
    {
        FailedIndices = Array.Empty<int>();
        FailedItems = Array.Empty<string>();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IngestionSinkException"/> class with failed document indices and error details.
    /// </summary>
    /// <param name="message">The message describing the failure.</param>
    /// <param name="failedIndices">The document indices in the batch that failed.</param>
    /// <param name="failedItems">Optional failure details for each failed item.</param>
    public IngestionSinkException(string message, IReadOnlyList<int> failedIndices, IReadOnlyList<string>? failedItems = null)
        : base(message)
    {
        FailedIndices = failedIndices ?? Array.Empty<int>();
        FailedItems = failedItems ?? Array.Empty<string>();
    }
}
