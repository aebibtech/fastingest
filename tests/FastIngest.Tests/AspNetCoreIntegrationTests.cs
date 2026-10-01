using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using FastIngest.AspNetCore.Extensions;
using FastIngest.AspNetCore.Hubs;
using FastIngest.AspNetCore.Models;
using FastIngest.AspNetCore.Workers;
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FastIngest.Tests;

public class AspNetCoreIntegrationTests
{
    public record TestRecord(int Id, string Name, decimal Amount);

    public class TestRecordProfile : FastIngestProfile<TestRecord>
    {
        public TestRecordProfile()
        {
            ToTable("test_records");
            WithFileType(FileType.Csv);
            Map(x => x.Id, "id");
            Map(x => x.Name, "name");
            Map(x => x.Amount, "amount");
        }
    }

    public class TestRecordSink : IIngestionSink<TestRecord>
    {
        public List<TestRecord> Records { get; } = new();

        public Task<long> WriteBatchAsync(IReadOnlyList<TestRecord> batch, CancellationToken cancellationToken)
        {
            lock (Records)
            {
                Records.AddRange(batch);
            }
            return Task.FromResult((long)batch.Count);
        }
    }

    [Fact]
    public void AddFastIngestAspNetCore_RegistersRequiredServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddFastIngestAspNetCore();

        var sp = services.BuildServiceProvider();

        Assert.NotNull(sp.GetService<IngestJobQueue>());
        Assert.NotNull(sp.GetService<IFastIngestEngine>());
        Assert.Contains(services, s => s.ImplementationType == typeof(IngestBackgroundService));
    }

    [Fact]
    public async Task MapFastIngestUpload_AcceptsFileAndProcessesInBackground()
    {
        var testSink = new TestRecordSink();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        builder.Services.AddFastIngestAspNetCore(ingest =>
        {
            ingest.RegisterProfile<TestRecordProfile>();
        });
        builder.Services.AddSingleton(testSink);
        builder.Services.AddTransient<IIngestionSink<TestRecord>>(_ => testSink);

        var app = builder.Build();

        app.MapFastIngestHub("/hubs/fastingest");
        app.MapFastIngestUpload<TestRecord>("/api/ingest/test-records");

        await app.StartAsync();

        var client = app.GetTestClient();

        var csvContent = "id,name,amount\n1,Alice,100.50\n2,Bob,200.75\n3,Charlie,300.00\n";
        using var form = new MultipartFormDataContent();
        var fileContent = new ByteArrayContent(Encoding.UTF8.GetBytes(csvContent));
        fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("text/csv");
        form.Add(fileContent, "file", "records.csv");

        var response = await client.PostAsync("/api/ingest/test-records", form);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<IngestAcceptedResponse>();
        Assert.NotNull(payload);
        Assert.False(string.IsNullOrWhiteSpace(payload.JobId));
        Assert.Equal("records.csv", payload.FileName);

        // Wait for background worker to complete
        var maxWait = TimeSpan.FromSeconds(5);
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < maxWait)
        {
            lock (testSink.Records)
            {
                if (testSink.Records.Count == 3)
                {
                    break;
                }
            }
            await Task.Delay(50);
        }

        lock (testSink.Records)
        {
            Assert.Equal(3, testSink.Records.Count);
            Assert.Equal(1, testSink.Records[0].Id);
            Assert.Equal("Alice", testSink.Records[0].Name);
            Assert.Equal(100.50m, testSink.Records[0].Amount);
        }

        await app.StopAsync();
    }

    [Fact]
    public async Task MapFastIngestUpload_WithCustomSink_ProcessesSuccessfully()
    {
        var testSink = new TestRecordSink();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        builder.Services.AddFastIngestAspNetCore(ingest =>
        {
            ingest.RegisterProfile<TestRecordProfile>();
        });
        builder.Services.AddSingleton(testSink);

        var app = builder.Build();

        app.MapFastIngestHub("/hubs/fastingest");
        app.MapFastIngestUpload<TestRecord, TestRecordSink>("/api/ingest/custom-sink");

        await app.StartAsync();

        var client = app.GetTestClient();

        var csvContent = "id,name,amount\n10,David,450.00\n";
        using var form = new MultipartFormDataContent();
        var fileContent = new ByteArrayContent(Encoding.UTF8.GetBytes(csvContent));
        fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("text/csv");
        form.Add(fileContent, "file", "custom.csv");

        var response = await client.PostAsync("/api/ingest/custom-sink", form);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Wait for background worker
        var maxWait = TimeSpan.FromSeconds(5);
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < maxWait)
        {
            lock (testSink.Records)
            {
                if (testSink.Records.Count == 1)
                {
                    break;
                }
            }
            await Task.Delay(50);
        }

        lock (testSink.Records)
        {
            Assert.Single(testSink.Records);
            Assert.Equal(10, testSink.Records[0].Id);
            Assert.Equal("David", testSink.Records[0].Name);
        }

        await app.StopAsync();
    }

    [Fact]
    public async Task MapFastIngestUpload_ReturnsBadRequest_WhenFileIsEmpty()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddFastIngestAspNetCore();

        var app = builder.Build();
        app.MapFastIngestUpload<TestRecord>("/api/ingest/empty");

        await app.StartAsync();
        var client = app.GetTestClient();

        using var form = new MultipartFormDataContent();
        var fileContent = new ByteArrayContent(Array.Empty<byte>());
        form.Add(fileContent, "file", "empty.csv");

        var response = await client.PostAsync("/api/ingest/empty", form);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        await app.StopAsync();
    }

    [Fact]
    public async Task MapFastIngestUpload_StreamsProgressAndCompletionToSignalRClient()
    {
        var testSink = new TestRecordSink();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        builder.Services.AddFastIngestAspNetCore(ingest =>
        {
            ingest.RegisterProfile<TestRecordProfile>();
        });
        builder.Services.AddSingleton(testSink);
        builder.Services.AddTransient<IIngestionSink<TestRecord>>(_ => testSink);

        var app = builder.Build();

        app.MapFastIngestHub("/hubs/fastingest");
        app.MapFastIngestUpload<TestRecord>("/api/ingest/test-records");

        await app.StartAsync();

        var testServer = app.GetTestServer();
        var client = app.GetTestClient();

        var hubConnection = new HubConnectionBuilder()
            .WithUrl("http://localhost/hubs/fastingest", options =>
            {
                options.HttpMessageHandlerFactory = _ => testServer.CreateHandler();
            })
            .Build();

        var progressReceived = new List<IngestProgress>();
        var completionReceived = new TaskCompletionSource<(string JobId, long Processed, long Succeeded, long Failed)>();

        hubConnection.On<string, IngestProgress>("ReceiveProgress", (jobId, progress) =>
        {
            lock (progressReceived)
            {
                progressReceived.Add(progress);
            }
        });

        hubConnection.On<string, long, long, long>("ReceiveCompletion", (jobId, processed, succeeded, failed) =>
        {
            completionReceived.TrySetResult((jobId, processed, succeeded, failed));
        });

        await hubConnection.StartAsync();

        var expectedJobId = Guid.NewGuid().ToString("N");
        await hubConnection.InvokeAsync("JoinJob", expectedJobId);

        // Prepare upload
        var csvContent = "id,name,amount\n1,Alice,100.50\n2,Bob,200.75\n3,Charlie,300.00\n";
        using var form = new MultipartFormDataContent();
        var fileContent = new ByteArrayContent(Encoding.UTF8.GetBytes(csvContent));
        fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("text/csv");
        form.Add(fileContent, "file", "signalr_test.csv");

        var response = await client.PostAsync($"/api/ingest/test-records?jobId={expectedJobId}", form);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<IngestAcceptedResponse>();
        Assert.NotNull(payload);
        Assert.Equal(expectedJobId, payload.JobId);

        // Wait for completion notification
        var completion = await Task.WhenAny(completionReceived.Task, Task.Delay(5000));
        Assert.Equal(completionReceived.Task, completion);

        var result = await completionReceived.Task;
        Assert.Equal(expectedJobId, result.JobId);
        Assert.Equal(3, result.Processed);
        Assert.Equal(3, result.Succeeded);
        Assert.Equal(0, result.Failed);

        await hubConnection.StopAsync();
        await app.StopAsync();
    }
}
