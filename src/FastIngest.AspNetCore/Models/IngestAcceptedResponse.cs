namespace FastIngest.AspNetCore.Models;

/// <summary>
/// Response payload returned when a bulk data file is accepted for background ingestion.
/// </summary>
/// <param name="JobId">The unique identifier to track the progress of the ingestion job.</param>
/// <param name="Message">Descriptive message indicating the job status.</param>
/// <param name="FileName">The name of the uploaded file.</param>
/// <param name="ByteCount">The size of the uploaded file in bytes.</param>
public record IngestAcceptedResponse(
    string JobId,
    string Message,
    string? FileName,
    long ByteCount);
