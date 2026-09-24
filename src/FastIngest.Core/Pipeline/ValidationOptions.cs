using FastIngest.Core.Common;

namespace FastIngest.Core.Pipeline;

/// <summary>
/// Configures validation behaviors and failure policies for the ingestion pipeline.
/// </summary>
public sealed class ValidationOptions
{
    /// <summary>
    /// Gets or sets the error handling strategy to apply when record validation or parsing fails.
    /// Defaults to <see cref="ErrorStrategy.FailFast"/>.
    /// </summary>
    public ErrorStrategy ErrorStrategy { get; set; } = ErrorStrategy.FailFast;
}
