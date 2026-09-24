namespace FastIngest.Core.Common;

public enum FileType
{
    Csv,
    Xlsx,
    AutoDetect
}

public enum ErrorStrategy
{
    FailFast,
    CollectAndContinue
}
