using System.Text;
using System.Threading.Channels;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Sinks;
using Xunit;

namespace FastIngest.Tests;

/// <summary>
/// Slow test sink that introduces an asynchronous delay to simulate I/O backpressure.
/// </summary>
public class DelayedMemorySink<T> : IIngestionSink<T>
{
    private readonly int _delayMs;
    public List<T> Records { get; } = new();
    public int BatchCallCount { get; private set; }

    public DelayedMemorySink(int delayMs = 10)
    {
        _delayMs = delayMs;
    }

    public async Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        await Task.Delay(_delayMs, cancellationToken);
        BatchCallCount++;
        Records.AddRange(batch);
        return batch.Count;
    }
}

/// <summary>
/// Faulting test sink that throws after a designated number of successful batches.
/// </summary>
public class FaultingSink<T> : IIngestionSink<T>
{
    private readonly int _failAfterBatches;
    private readonly Exception _exceptionToThrow;
    public int BatchesWritten { get; private set; }

    public FaultingSink(int failAfterBatches, Exception exceptionToThrow)
    {
        _failAfterBatches = failAfterBatches;
        _exceptionToThrow = exceptionToThrow;
    }

    public Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (BatchesWritten >= _failAfterBatches)
        {
            throw _exceptionToThrow;
        }

        BatchesWritten++;
        return Task.FromResult((long)batch.Count);
    }
}

/// <summary>
/// Tests verifying concurrent producer-consumer pipelining, bounded channels, backpressure, and fault tolerance.
/// </summary>
public class ChannelPipelineTests
{
    [Fact]
    public void PipelineOptions_Should_Have_Expected_Defaults()
    {
        var options = new PipelineOptions();

        Assert.Equal(2, options.BoundedChannelCapacity);
        Assert.True(options.SingleWriter);
        Assert.True(options.SingleReader);
        Assert.Equal(BoundedChannelFullMode.Wait, options.FullMode);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void PipelineOptions_Should_Reject_Invalid_Capacity(int invalidCapacity)
    {
        var options = new PipelineOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => options.BoundedChannelCapacity = invalidCapacity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Pipeline_WithChannelCapacity_Should_Reject_Invalid_Values(int invalidCapacity)
    {
        var pipeline = FastIngestPipeline<TestCustomer>.Create();
        Assert.Throws<ArgumentOutOfRangeException>(() => pipeline.WithChannelCapacity(invalidCapacity));
    }

    [Fact]
    public void Pipeline_WithOptions_Should_Reject_Null_Configure()
    {
        var pipeline = FastIngestPipeline<TestCustomer>.Create();
        Assert.Throws<ArgumentNullException>(() => pipeline.WithOptions(null!));
    }

    [Fact]
    public async Task Pipeline_Should_Stream_Batches_Concurrently_With_Bounded_Channel()
    {
        // 10 records, batch size 2, channel capacity 2
        var sb = new StringBuilder();
        sb.AppendLine("customer_id,email,full_name,balance");
        for (int i = 1; i <= 10; i++)
        {
            sb.AppendLine($"{i},user{i}@example.com,User {i},{i * 10}.00");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
        var sink = new DelayedMemorySink<TestCustomer>(delayMs: 5);
        var progressList = new List<IngestProgress>();

        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "customer_id");
                m.Map(x => x.Email, "email");
                m.Map(x => x.FullName, "full_name");
                m.Map(x => x.Balance, "balance");
            })
            .WithBatchSize(2)
            .WithChannelCapacity(2)
            .OnProgress(p => progressList.Add(p))
            .WriteToSinkAsync(sink);

        Assert.True(result.IsSuccess);
        Assert.Equal(10, result.TotalProcessed);
        Assert.Equal(10, result.TotalSucceeded);
        Assert.Equal(0, result.TotalFailed);
        Assert.Empty(result.Errors);

        Assert.Equal(10, sink.Records.Count);
        Assert.Equal(5, sink.BatchCallCount); // 10 rows / 2 per batch = 5 batches
        Assert.NotEmpty(progressList);
    }

    [Fact]
    public async Task Pipeline_Should_Cancel_Producer_And_Propagate_When_Consumer_Fails()
    {
        // Generate 50 records
        var sb = new StringBuilder();
        sb.AppendLine("customer_id,email,full_name,balance");
        for (int i = 1; i <= 50; i++)
        {
            sb.AppendLine($"{i},user{i}@example.com,User {i},{i * 10}.00");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
        var expectedException = new InvalidOperationException("Simulated database socket failure");
        var faultingSink = new FaultingSink<TestCustomer>(failAfterBatches: 1, expectedException);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await FastIngestPipeline<TestCustomer>.Create()
                .FromStream(stream, FileType.Csv)
                .WithMapping(m =>
                {
                    m.Map(x => x.Id, "customer_id");
                    m.Map(x => x.Email, "email");
                    m.Map(x => x.FullName, "full_name");
                    m.Map(x => x.Balance, "balance");
                })
                .WithBatchSize(5)
                .WithChannelCapacity(2)
                .WriteToSinkAsync(faultingSink);
        });

        Assert.Same(expectedException, thrown);
    }

    [Fact]
    public async Task Pipeline_Should_Abort_Both_Tasks_Immediately_On_FailFast()
    {
        var sb = new StringBuilder();
        sb.AppendLine("customer_id,email,full_name,balance");
        sb.AppendLine("1,valid1@example.com,Valid 1,100.00");
        sb.AppendLine("2,invalid-email,Invalid 2,-50.00"); // fails validation
        for (int i = 3; i <= 30; i++)
        {
            sb.AppendLine($"{i},valid{i}@example.com,Valid {i},{i * 10}.00");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
        var sink = new TestMemorySink<TestCustomer>();

        var ex = await Assert.ThrowsAsync<FastIngestValidationException>(async () =>
        {
            await FastIngestPipeline<TestCustomer>.Create()
                .FromStream(stream, FileType.Csv)
                .WithMapping(m =>
                {
                    m.Map(x => x.Id, "customer_id");
                    m.Map(x => x.Email, "email");
                    m.Map(x => x.FullName, "full_name");
                    m.Map(x => x.Balance, "balance");
                })
                .ValidateWith<TestCustomerValidator>(opt =>
                {
                    opt.ErrorStrategy = ErrorStrategy.FailFast;
                })
                .WithBatchSize(5)
                .WithChannelCapacity(2)
                .WriteToSinkAsync(sink);
        });

        Assert.NotEmpty(ex.Errors);
        Assert.Equal(2, ex.Errors[0].RowIndex);
    }

    [Fact]
    public async Task Pipeline_Should_Respect_External_Cancellation()
    {
        var sb = new StringBuilder();
        sb.AppendLine("customer_id,email,full_name,balance");
        for (int i = 1; i <= 100; i++)
        {
            sb.AppendLine($"{i},user{i}@example.com,User {i},{i * 10}.00");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
        var sink = new DelayedMemorySink<TestCustomer>(delayMs: 50);

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(20);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await FastIngestPipeline<TestCustomer>.Create()
                .FromStream(stream, FileType.Csv)
                .WithMapping(m =>
                {
                    m.Map(x => x.Id, "customer_id");
                    m.Map(x => x.Email, "email");
                    m.Map(x => x.FullName, "full_name");
                    m.Map(x => x.Balance, "balance");
                })
                .WithBatchSize(2)
                .WithChannelCapacity(2)
                .WriteToSinkAsync(sink, cts.Token);
        });
    }

    [Fact]
    public async Task Pipeline_WithOptions_Should_Apply_Channel_Tuning()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,bob@example.com,Bob Jones,200.00
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
        var sink = new TestMemorySink<TestCustomer>();

        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Csv)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "customer_id");
                m.Map(x => x.Email, "email");
                m.Map(x => x.FullName, "full_name");
                m.Map(x => x.Balance, "balance");
            })
            .WithOptions(opt =>
            {
                opt.BoundedChannelCapacity = 3;
                opt.SingleWriter = true;
                opt.SingleReader = true;
                opt.FullMode = BoundedChannelFullMode.Wait;
            })
            .WithBatchSize(1)
            .WriteToSinkAsync(sink);

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(2, sink.BatchCallCount);
    }
}
