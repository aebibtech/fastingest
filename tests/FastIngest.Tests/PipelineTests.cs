using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Sinks;
using FluentValidation;
using Xunit;

namespace FastIngest.Tests;

public record TestCustomer(int Id, string Email, string FullName, decimal Balance);

public class TestCustomerValidator : AbstractValidator<TestCustomer>
{
    public TestCustomerValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.FullName).NotEmpty();
        RuleFor(x => x.Balance).GreaterThanOrEqualTo(0);
    }
}

public class TestMemorySink<T> : IIngestionSink<T>
{
    public List<T> Records { get; } = new();
    public int BatchCallCount { get; private set; }

    public Task<long> WriteBatchAsync(IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        BatchCallCount++;
        Records.AddRange(batch);
        return Task.FromResult((long)batch.Count);
    }
}

public class PipelineTests
{
    [Fact]
    public async Task Pipeline_Should_Ingest_Valid_Csv_Successfully()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,bob@example.com,Bob Jones,200.00
                  3,carol@example.com,Carol White,99.99
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
        var sink = new TestMemorySink<TestCustomer>();
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
            .OnProgress(p => progressList.Add(p))
            .WriteToSinkAsync(sink);

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(3, result.TotalSucceeded);
        Assert.Equal(0, result.TotalFailed);
        Assert.Empty(result.Errors);

        Assert.Equal(3, sink.Records.Count);
        Assert.Equal(2, sink.BatchCallCount); // 2 in first batch, 1 in second batch
        Assert.Equal("alice@example.com", sink.Records[0].Email);
        Assert.Equal(150.50m, sink.Records[0].Balance);

        Assert.NotEmpty(progressList);
    }

    [Fact]
    public async Task Pipeline_Should_CollectAndContinue_On_Invalid_Rows()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,invalid-email,Bob Jones,-50.00
                  3,carol@example.com,Carol White,99.99
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
            .ValidateWith<TestCustomerValidator>(opt =>
            {
                opt.ErrorStrategy = ErrorStrategy.CollectAndContinue;
            })
            .WithBatchSize(5)
            .WriteToSinkAsync(sink);

        Assert.False(result.IsSuccess);
        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(1, result.TotalFailed);
        Assert.NotEmpty(result.Errors);

        var error = result.Errors[0];
        Assert.Equal(2, error.RowIndex);

        var errorCsv = Encoding.UTF8.GetString(result.ExportErrorsToCsv());
        Assert.Contains("RowIndex,ColumnName,AttemptedValue,ErrorMessage", errorCsv);
        Assert.Contains("invalid-email", errorCsv);
    }

    [Fact]
    public async Task Pipeline_Should_Throw_On_FailFast()
    {
        var csv = """
                  customer_id,email,full_name,balance
                  1,alice@example.com,Alice Smith,150.50
                  2,invalid-email,Bob Jones,-50.00
                  3,carol@example.com,Carol White,99.99
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
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
                .WriteToSinkAsync(sink);
        });

        Assert.NotEmpty(ex.Errors);
    }
}
