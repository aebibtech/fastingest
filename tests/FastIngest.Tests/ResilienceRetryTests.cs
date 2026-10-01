using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Resilience;
using FastIngest.Core.Sinks;
using Xunit;

namespace FastIngest.Tests;

/// <summary>
/// Mock sink that fails with a designated exception for a specified number of invocations before succeeding.
/// </summary>
public class TransientFaultSink<T> : IIngestionSink<T>
{
    private readonly int _failCount;
    private readonly Exception _exceptionToThrow;

    public int Attempts { get; private set; }
    public List<T> PersistedRecords { get; } = new();

    public TransientFaultSink(int failCount, Exception exceptionToThrow)
    {
        _failCount = failCount;
        _exceptionToThrow = exceptionToThrow;
    }

    public Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Attempts++;

        if (Attempts <= _failCount)
        {
            throw _exceptionToThrow;
        }

        PersistedRecords.AddRange(batch);
        return Task.FromResult((long)batch.Count);
    }
}

public class ResilienceRetryTests
{
    public record TestCustomer(int Id, string Name, decimal Balance);

    private static Stream CreateSampleCsvStream(int rowCount = 10)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Id,Name,Balance");
        for (int i = 1; i <= rowCount; i++)
        {
            sb.AppendLine($"{i},Customer_{i},{100 + i * 10}");
        }
        return new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
    }

    [Fact]
    public async Task Retry_TransientFault_Succeeds_AfterRetry()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(2, new TimeoutException("Database connection timeout."));

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = true;
                options.MaxRetryAttempts = 3;
                options.BaseDelay = TimeSpan.FromMilliseconds(10);
                options.UseJitter = false;
            });

        var result = await pipeline.WriteToSinkAsync(sink);

        Assert.Equal(3, sink.Attempts); // 1 initial + 2 retries
        Assert.Equal(5, result.TotalSucceeded);
        Assert.Equal(5, sink.PersistedRecords.Count);
    }

    [Fact]
    public async Task Retry_ExhaustsMaxAttempts_PropagatesException()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(5, new TimeoutException("Persistent timeout."));

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = true;
                options.MaxRetryAttempts = 2;
                options.BaseDelay = TimeSpan.FromMilliseconds(10);
                options.UseJitter = false;
            });

        var ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await pipeline.WriteToSinkAsync(sink);
        });

        Assert.Equal("Persistent timeout.", ex.Message);
        Assert.Equal(3, sink.Attempts); // 1 initial + 2 retries = 3 attempts total
    }

    [Fact]
    public async Task Retry_NonTransientFault_DoesNotRetry()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(3, new InvalidOperationException("Non-transient schema violation."));

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = true;
                options.MaxRetryAttempts = 3;
                options.BaseDelay = TimeSpan.FromMilliseconds(10);
                options.UseJitter = false;
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await pipeline.WriteToSinkAsync(sink);
        });

        Assert.Equal("Non-transient schema violation.", ex.Message);
        Assert.Equal(1, sink.Attempts); // Failed fast without retrying
    }

    [Fact]
    public async Task Retry_Disabled_NoRetry()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(2, new TimeoutException("Transient timeout"));

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = false; // Disabled
                options.MaxRetryAttempts = 3;
            });

        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await pipeline.WriteToSinkAsync(sink);
        });

        Assert.Equal(1, sink.Attempts);
    }

    [Fact]
    public async Task Retry_CustomPredicate_UsedOverDefault()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(2, new FormatException("Custom retryable format issue."));

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = true;
                options.MaxRetryAttempts = 3;
                options.BaseDelay = TimeSpan.FromMilliseconds(10);
                options.UseJitter = false;
                options.ShouldRetry = ex => ex is FormatException;
            });

        var result = await pipeline.WriteToSinkAsync(sink);

        Assert.Equal(3, sink.Attempts);
        Assert.Equal(5, result.TotalSucceeded);
    }

    [Fact]
    public async Task Retry_Progress_ReportsRetryMessage()
    {
        using var stream = CreateSampleCsvStream(5);
        var sink = new TransientFaultSink<TestCustomer>(1, new TimeoutException("Socket timed out."));

        var progressMessages = new List<string>();

        var pipeline = FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(10)
            .WithResilience(options =>
            {
                options.Enabled = true;
                options.MaxRetryAttempts = 2;
                options.BaseDelay = TimeSpan.FromMilliseconds(10);
                options.UseJitter = false;
            })
            .OnProgress(progress =>
            {
                if (!string.IsNullOrEmpty(progress.Message))
                {
                    progressMessages.Add(progress.Message);
                }
            });

        var result = await pipeline.WriteToSinkAsync(sink);

        Assert.Equal(2, sink.Attempts);
        Assert.Equal(5, result.TotalSucceeded);
        Assert.NotEmpty(progressMessages);
        Assert.Contains(progressMessages, m => m.Contains("Transient fault on attempt"));
    }

    [Fact]
    public void TransientFaultPredicates_Evaluates_Correctly()
    {
        Assert.True(TransientFaultPredicates.IsTransient(new TimeoutException()));
        Assert.False(TransientFaultPredicates.IsTransient(new InvalidOperationException()));
        Assert.False(TransientFaultPredicates.IsTransient(new ArgumentNullException()));
    }

    [Fact]
    public void ResilienceOptions_Validation_ThrowsOnInvalidArguments()
    {
        var options = new ResilienceOptions();

        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxRetryAttempts = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxRetryAttempts = -1);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.BaseDelay = TimeSpan.FromMilliseconds(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxDelay = TimeSpan.FromMilliseconds(-1));
    }
}
