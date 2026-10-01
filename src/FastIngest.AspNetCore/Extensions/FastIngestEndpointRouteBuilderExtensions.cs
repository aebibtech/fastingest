using FastIngest.AspNetCore.Hubs;
using FastIngest.AspNetCore.Models;
using FastIngest.AspNetCore.Workers;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace FastIngest.AspNetCore.Extensions;

/// <summary>
/// Route builder extensions for mounting FastIngest minimal API upload endpoints and SignalR hubs.
/// </summary>
public static class FastIngestEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the FastIngest SignalR hub for real-time progress and completion events.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The URL route pattern for the SignalR hub. Defaults to <c>"/hubs/fastingest"</c>.</param>
    /// <returns>A hub endpoint convention builder for chaining further configuration.</returns>
    public static HubEndpointConventionBuilder MapFastIngestHub(
        this IEndpointRouteBuilder endpoints,
        string pattern = "/hubs/fastingest")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapHub<FastIngestHub>(pattern);
    }

    /// <summary>
    /// Maps a POST endpoint that accepts a multipart/form-data file upload, stages the stream to temporary storage,
    /// enqueues the ingestion task for background processing, and returns <c>202 Accepted</c> with the tracking Job ID.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The URL route pattern for the upload endpoint.</param>
    /// <param name="tableName">Optional destination database table name override.</param>
    /// <returns>An endpoint convention builder for chaining further routing configuration.</returns>
    public static RouteHandlerBuilder MapFastIngestUpload<TRecord>(
        this IEndpointRouteBuilder endpoints,
        string pattern,
        string? tableName = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapPost(pattern, async (
            HttpRequest request,
            IFormFile file,
            IngestJobQueue queue,
            CancellationToken cancellationToken) =>
        {
            if (file == null || file.Length == 0)
            {
                return Results.BadRequest(new { message = "No file uploaded or file is empty." });
            }

            var jobId = request.Query["jobId"].FirstOrDefault()
                ?? request.Headers["X-Job-Id"].FirstOrDefault()
                ?? Guid.NewGuid().ToString("N");

            var tempFilePath = Path.Combine(Path.GetTempPath(), $"fastingest_{jobId}_{Path.GetFileName(file.FileName)}");

            await using (var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await file.CopyToAsync(fileStream, cancellationToken);
            }

            var job = new IngestJob(
                JobId: jobId,
                TempFilePath: tempFilePath,
                OriginalFileName: file.FileName,
                Handler: async (sp, stream, onProgress, ct) =>
                {
                    var engine = sp.GetRequiredService<IFastIngestEngine>();
                    if (tableName != null)
                    {
                        return await engine.IngestAsync<TRecord>(stream, tableName, onProgress, ct);
                    }
                    return await engine.IngestAsync<TRecord>(stream, onProgress, ct);
                });

            await queue.QueueJobAsync(job, cancellationToken);

            var response = new IngestAcceptedResponse(
                JobId: jobId,
                Message: "File accepted and ingestion queued for background processing.",
                FileName: file.FileName,
                ByteCount: file.Length);

            return Results.Accepted(uri: null, value: response);
        })
        .DisableAntiforgery();
    }

    /// <summary>
    /// Maps a POST endpoint that accepts a multipart/form-data file upload, stages the stream,
    /// enqueues ingestion to a custom resolved <typeparamref name="TSink"/> in the background, and returns <c>202 Accepted</c>.
    /// </summary>
    /// <typeparam name="TRecord">The model type representing an ingested record.</typeparam>
    /// <typeparam name="TSink">The custom sink type resolved from dependency injection.</typeparam>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="pattern">The URL route pattern for the upload endpoint.</param>
    /// <returns>An endpoint convention builder for chaining further routing configuration.</returns>
    public static RouteHandlerBuilder MapFastIngestUpload<TRecord, TSink>(
        this IEndpointRouteBuilder endpoints,
        string pattern)
        where TSink : IIngestionSink<TRecord>
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapPost(pattern, async (
            HttpRequest request,
            IFormFile file,
            IngestJobQueue queue,
            CancellationToken cancellationToken) =>
        {
            if (file == null || file.Length == 0)
            {
                return Results.BadRequest(new { message = "No file uploaded or file is empty." });
            }

            var jobId = request.Query["jobId"].FirstOrDefault()
                ?? request.Headers["X-Job-Id"].FirstOrDefault()
                ?? Guid.NewGuid().ToString("N");

            var tempFilePath = Path.Combine(Path.GetTempPath(), $"fastingest_{jobId}_{Path.GetFileName(file.FileName)}");

            await using (var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await file.CopyToAsync(fileStream, cancellationToken);
            }

            var job = new IngestJob(
                JobId: jobId,
                TempFilePath: tempFilePath,
                OriginalFileName: file.FileName,
                Handler: async (sp, stream, onProgress, ct) =>
                {
                    var engine = sp.GetRequiredService<IFastIngestEngine>();
                    var sink = sp.GetRequiredService<TSink>();
                    return await engine.IngestAsync(stream, sink, onProgress, ct);
                });

            await queue.QueueJobAsync(job, cancellationToken);

            var response = new IngestAcceptedResponse(
                JobId: jobId,
                Message: "File accepted and ingestion queued for background processing.",
                FileName: file.FileName,
                ByteCount: file.Length);

            return Results.Accepted(uri: null, value: response);
        })
        .DisableAntiforgery();
    }
}
