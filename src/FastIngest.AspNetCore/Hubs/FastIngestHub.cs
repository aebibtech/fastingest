using Microsoft.AspNetCore.SignalR;

namespace FastIngest.AspNetCore.Hubs;

/// <summary>
/// SignalR Hub for broadcasting FastIngest real-time ingestion progress and events.
/// </summary>
public class FastIngestHub : Hub<IFastIngestClient>
{
    /// <summary>
    /// Adds the calling client connection to the group for the specified ingestion job.
    /// </summary>
    /// <param name="jobId">The unique identifier of the ingestion job.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task JoinJob(string jobId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        await Groups.AddToGroupAsync(Context.ConnectionId, jobId);
    }

    /// <summary>
    /// Removes the calling client connection from the group for the specified ingestion job.
    /// </summary>
    /// <param name="jobId">The unique identifier of the ingestion job.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task LeaveJob(string jobId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, jobId);
    }
}
