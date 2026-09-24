using FastIngest.Core.Results;

namespace FastIngest.Core.Exceptions;

/// <summary>
/// Exception thrown when row parsing or validation fails while using the <see cref="Common.ErrorStrategy.FailFast"/> strategy.
/// </summary>
public class FastIngestValidationException : Exception
{
    /// <summary>
    /// Gets the collection of row errors that triggered the exception.
    /// </summary>
    public IReadOnlyList<IngestRowError> Errors { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="FastIngestValidationException"/> class with a custom message and error collection.
    /// </summary>
    /// <param name="message">The message that describes the validation error.</param>
    /// <param name="errors">The list of row errors captured prior to throwing.</param>
    public FastIngestValidationException(string message, IReadOnlyList<IngestRowError> errors)
        : base(message)
    {
        Errors = errors;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FastIngestValidationException"/> class with a single row error.
    /// </summary>
    /// <param name="error">The row error that failed validation.</param>
    public FastIngestValidationException(IngestRowError error)
        : base($"Row {error.RowIndex}: {error.ErrorMessage}")
    {
        Errors = new[] { error };
    }
}
