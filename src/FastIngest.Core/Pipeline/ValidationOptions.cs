using FastIngest.Core.Common;

namespace FastIngest.Core.Pipeline;

public sealed class ValidationOptions
{
    public ErrorStrategy ErrorStrategy { get; set; } = ErrorStrategy.FailFast;
}
