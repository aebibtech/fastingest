using System.Text;
using System.Text.Json;
using FastIngest.Core.Common;
using FastIngest.Core.Exceptions;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Readers;
using FluentValidation;
using Xunit;

namespace FastIngest.Tests;

/// <summary>
/// Unit and integration tests verifying high-performance line-delimited JSON (NDJSON / JSONL) streaming reader,
/// bounded channel pipelining, and error handling strategies.
/// </summary>
public class JsonLinesStreamReaderTests
{
    [Fact]
    public async Task Reader_Should_Stream_10000_Items_Successfully()
    {
        // Arrange
        const int recordCount = 10_000;
        var sb = new StringBuilder(recordCount * 90);
        for (int i = 1; i <= recordCount; i++)
        {
            sb.Append($"{{\"id\":{i},\"email\":\"user{i}@example.com\",\"fullName\":\"User {i}\",\"balance\":{i * 1.5:F2}}}\n");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));

        // Act
        var reader = new JsonLinesStreamReader<TestCustomer>(stream);
        var readItems = new List<TestCustomer>(recordCount);

        await foreach (var customer in reader.ReadAsync())
        {
            readItems.Add(customer);
        }

        // Assert
        Assert.Equal(recordCount, readItems.Count);
        Assert.Equal(1, readItems[0].Id);
        Assert.Equal("user1@example.com", readItems[0].Email);
        Assert.Equal("User 1", readItems[0].FullName);
        Assert.Equal(1.50m, readItems[0].Balance);

        Assert.Equal(recordCount, readItems[^1].Id);
        Assert.Equal($"user{recordCount}@example.com", readItems[^1].Email);
        Assert.Equal($"User {recordCount}", readItems[^1].FullName);
        Assert.Equal(recordCount * 1.5m, readItems[^1].Balance);
    }

    [Fact]
    public async Task Pipeline_Should_CollectAndContinue_On_Malformed_Row_N()
    {
        // Arrange: 5 rows with row 3 containing syntactically malformed JSON
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "fullName": "Bob Jones", "balance": 200.00}
                     {"id": 3, "email": "carol@example.com", "balance": INVALID_JSON_PAYLOAD
                     {"id": 4, "email": "david@example.com", "fullName": "David Brown", "balance": 350.75}
                     {"id": 5, "email": "eva@example.com", "fullName": "Eva Green", "balance": 400.00}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.JsonLines)
            .ValidateWith<TestCustomerValidator>(opt =>
            {
                opt.ErrorStrategy = ErrorStrategy.CollectAndContinue;
            })
            .WithBatchSize(2)
            .WriteToSinkAsync(sink);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal(5, result.TotalProcessed);
        Assert.Equal(4, result.TotalSucceeded);
        Assert.Equal(1, result.TotalFailed);
        Assert.Single(result.Errors);

        var error = result.Errors[0];
        Assert.Equal(3, error.RowIndex);
        Assert.Contains("INVALID_JSON_PAYLOAD", error.AttemptedValue);
        Assert.Contains("JSON parsing failed", error.ErrorMessage);

        // Verify that the 4 valid records were ingested into sink
        Assert.Equal(4, sink.Records.Count);
        Assert.Equal(1, sink.Records[0].Id);
        Assert.Equal(2, sink.Records[1].Id);
        Assert.Equal(4, sink.Records[2].Id);
        Assert.Equal(5, sink.Records[3].Id);

        // Verify CSV error export contains the error details
        var errorCsv = Encoding.UTF8.GetString(result.ExportErrorsToCsv());
        Assert.Contains("RowIndex,ColumnName,AttemptedValue,ErrorMessage", errorCsv);
        Assert.Contains("INVALID_JSON_PAYLOAD", errorCsv);
    }

    [Fact]
    public async Task Pipeline_Should_FailFast_Immediately_On_Invalid_JSON()
    {
        // Arrange: Row 2 has malformed JSON
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "fullName": malformed_unquoted_string}
                     {"id": 3, "email": "carol@example.com", "fullName": "Carol White", "balance": 99.99}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FastIngestValidationException>(async () =>
        {
            await FastIngestPipeline<TestCustomer>.Create()
                .FromStream(stream, FileType.JsonLines)
                .ValidateWith<TestCustomerValidator>(opt =>
                {
                    opt.ErrorStrategy = ErrorStrategy.FailFast;
                })
                .WriteToSinkAsync(sink);
        });

        Assert.NotEmpty(ex.Errors);
        Assert.Equal(2, ex.Errors[0].RowIndex);
        Assert.Contains("malformed_unquoted_string", ex.Errors[0].AttemptedValue);
    }

    [Fact]
    public async Task Pipeline_Should_Integrate_EndToEnd_Stream_Channel_Sink()
    {
        // Arrange: 100 NDJSON items streamed through bounded channels in batches of 10
        const int totalCount = 100;
        const int batchSize = 10;
        var sb = new StringBuilder();
        for (int i = 1; i <= totalCount; i++)
        {
            sb.AppendLine($"{{\"id\": {i}, \"email\": \"user{i}@example.com\", \"fullName\": \"User {i}\", \"balance\": {i * 10}.00}}");
        }

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
        var sink = new TestMemorySink<TestCustomer>();
        var progressList = new List<IngestProgress>();

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.Ndjson)
            .WithBatchSize(batchSize)
            .WithChannelCapacity(2)
            .OnProgress(p => progressList.Add(p))
            .WriteToSinkAsync(sink);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(totalCount, result.TotalProcessed);
        Assert.Equal(totalCount, result.TotalSucceeded);
        Assert.Equal(0, result.TotalFailed);
        Assert.Empty(result.Errors);

        Assert.Equal(totalCount, sink.Records.Count);
        Assert.Equal(10, sink.BatchCallCount); // 100 / 10 = 10 batches
        Assert.Equal(1, sink.Records[0].Id);
        Assert.Equal("user1@example.com", sink.Records[0].Email);
        Assert.Equal(100, sink.Records[^1].Id);
        Assert.Equal("user100@example.com", sink.Records[^1].Email);
        Assert.Equal(1000.00m, sink.Records[^1].Balance);

        Assert.NotEmpty(progressList);
        Assert.Equal(100.0, progressList[^1].PercentComplete);
    }

    [Fact]
    public async Task Pipeline_Should_AutoDetect_JsonLines_By_Extension()
    {
        // Arrange
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "fullName": "Bob Jones", "balance": 200.00}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act: Pass FileType.AutoDetect with fileName "customers.jsonl"
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, "customers.jsonl", FileType.AutoDetect)
            .WriteToSinkAsync(sink);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(2, sink.Records.Count);
    }

    [Fact]
    public async Task Pipeline_Should_AutoDetect_JsonLines_By_Content_Inspection()
    {
        // Arrange: Stream starts with '{', auto-detect without file name
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "fullName": "Bob Jones", "balance": 200.00}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.AutoDetect)
            .WriteToSinkAsync(sink);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(2, sink.Records.Count);
    }

    [Fact]
    public async Task Reader_Should_Handle_Windows_CRLF_And_Empty_Lines()
    {
        // Arrange: Mix of CRLF and empty lines
        var ndjson = "{\"id\": 1, \"email\": \"alice@example.com\", \"fullName\": \"Alice Smith\", \"balance\": 100.0}\r\n" +
                     "\r\n" +
                     "{\"id\": 2, \"email\": \"bob@example.com\", \"fullName\": \"Bob Jones\", \"balance\": 200.0}\r\n" +
                     "   \r\n" +
                     "{\"id\": 3, \"email\": \"carol@example.com\", \"fullName\": \"Carol White\", \"balance\": 300.0}\r\n";

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.JsonLines)
            .WriteToSinkAsync(sink);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(3, result.TotalSucceeded);
        Assert.Equal(3, sink.Records.Count);
    }

    [Fact]
    public async Task Pipeline_Should_Apply_FluentValidation_To_JsonLines_Records()
    {
        // Arrange: Row 2 has an invalid email and negative balance
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "fullName": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "not-an-email", "fullName": "Bob Jones", "balance": -50.00}
                     {"id": 3, "email": "carol@example.com", "fullName": "Carol White", "balance": 99.99}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.JsonLines)
            .ValidateWith<TestCustomerValidator>(opt =>
            {
                opt.ErrorStrategy = ErrorStrategy.CollectAndContinue;
            })
            .WriteToSinkAsync(sink);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal(1, result.TotalFailed);
        Assert.NotEmpty(result.Errors);
        Assert.Equal(2, result.Errors[0].RowIndex);
        Assert.Equal(2, sink.Records.Count);
    }

    [Fact]
    public async Task Pipeline_Should_Respect_Custom_JsonSerializerOptions()
    {
        // Arrange: Custom JSON options (e.g., snake_case property naming)
        var ndjson = """
                     {"id": 1, "email": "alice@example.com", "full_name": "Alice Smith", "balance": 150.50}
                     {"id": 2, "email": "bob@example.com", "full_name": "Bob Jones", "balance": 200.00}
                     """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        var sink = new TestMemorySink<TestCustomer>();

        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
        };

        // Act
        var result = await FastIngestPipeline<TestCustomer>.Create()
            .FromStream(stream, FileType.JsonLines)
            .WithJsonOptions(options)
            .WriteToSinkAsync(sink);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.Equal("Alice Smith", sink.Records[0].FullName);
        Assert.Equal("Bob Jones", sink.Records[1].FullName);
    }
}
