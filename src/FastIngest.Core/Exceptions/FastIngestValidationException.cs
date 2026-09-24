using FastIngest.Core.Results;

namespace FastIngest.Core.Exceptions;

public class FastIngestValidationException : Exception
{
    public IReadOnlyList<IngestRowError> Errors { get; }

    public FastIngestValidationException(string message, IReadOnlyList<IngestRowError> errors)
        : base(message)
    {
        Errors = errors;
    }

    public FastIngestValidationException(IngestRowError error)
        : base($"Row {error.RowIndex}: {error.ErrorMessage}")
    {
        Errors = new[] { error };
    }
}
