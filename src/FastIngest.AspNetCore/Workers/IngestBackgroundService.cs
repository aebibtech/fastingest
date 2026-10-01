using FastIngest.AspNetCore.Hubs;
using FastIngest.Core.Pipeline;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FastIngest.AspNetCore.Workers;

/// <summary>
/// Background worker that consumes queued FastIngest ingestion jobs, runs them through the ingestion engine,
/// and broadcasts progress and completion events via SignalR.
/// </summary>
public class IngestBackgroundService : BackgroundService
{
    private readonly IngestJobQueue _queue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<IngestBackgroundService> _logger;
    private readonly IHubContext<FastIngestHub, IFastIngestClient> _hubContext;

    /// <summary>
    /// Initializes a new instance of <see cref="IngestBackgroundService"/>.
    /// </summary>
    /// <param name="queue">The background job queue.</param>
    /// <param name="scopeFactory">The service scope factory for creating scoped engine instances.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="hubContext">The SignalR hub context for client notifications.</param>
    public IngestBackgroundService(
        IngestJobQueue queue,
        IServiceScopeFactory scopeFactory,
        ILogger<IngestBackgroundService> logger,
        IHubContext<FastIngestHub, IFastIngestClient> hubContext)
    {
        _queue = queue ?? throw new ArgumentNullException(nameof(queue));
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("FastIngest background worker started.");

        await foreach (var job in _queue.ReadAllAsync(stoppingToken))
        {
            try
            {
                _logger.LogInformation("Starting processing for FastIngest Job {JobId} (File: {FileName})",
                    job.JobId, job.OriginalFileName ?? "unknown");

                using var scope = _scopeFactory.CreateScope();
                await using var fileStream = File.OpenRead(job.TempFilePath);

                void OnProgress(IngestProgress progress)
                {
                    _ = _hubContext.Clients.Group(job.JobId).ReceiveProgress(job.JobId, progress);
                }

                var result = await job.Handler(scope.ServiceProvider, fileStream, OnProgress, stoppingToken);

                _logger.LogInformation(
                    "Completed FastIngest Job {JobId}. Processed: {Processed}, Succeeded: {Succeeded}, Failed: {Failed}",
                    job.JobId, result.TotalProcessed, result.TotalSucceeded, result.TotalFailed);

                await _hubContext.Clients.Group(job.JobId).ReceiveCompletion(
                    job.JobId,
                    result.TotalProcessed,
                    result.TotalSucceeded,
                    result.TotalFailed);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogWarning("FastIngest Job {JobId} was cancelled due to application shutdown.", job.JobId);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FastIngest Job {JobId} encountered an unhandled error: {ErrorMessage}",
                    job.JobId, ex.Message);

                try
                {
                    await _hubContext.Clients.Group(job.JobId).ReceiveError(job.JobId, ex.Message);
                }
                catch (Exception notifyEx)
                {
                    _logger.LogWarning(notifyEx, "Failed to notify SignalR client of error for Job {JobId}", job.JobId);
                }
            }
            finally
            {
                try
                {
                    if (File.Exists(job.TempFilePath))
                    {
                        File.Delete(job.TempFilePath);
                    }
                }
                catch (Exception cleanupEx)
                {
                    _logger.LogWarning(cleanupEx, "Failed to delete temporary file {TempFilePath} for Job {JobId}",
                        job.TempFilePath, job.JobId);
                }
            }
        }

        _logger.LogInformation("FastIngest background worker stopped.");
    }
}
