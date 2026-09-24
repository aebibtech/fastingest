using System.Text;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using FastIngest.Core.Pipeline;
using FastIngest.Elasticsearch;
using FastIngest.Elasticsearch.Extensions;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace FastIngest.Tests;

public record TestElasticRecord(string Id, string Title, decimal Price);

public class ElasticsearchSinkTests
{
    private static ElasticsearchClient CreateMockClient(string responseJson, int statusCode = 200)
    {
        var responseBytes = Encoding.UTF8.GetBytes(responseJson);
        var headers = new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-elastic-product"] = new[] { "Elasticsearch" }
        };
        var invoker = new InMemoryRequestInvoker(responseBytes, statusCode, null, "application/json", headers);
        var settings = new ElasticsearchClientSettings(invoker);
        return new ElasticsearchClient(settings);
    }

    [Fact]
    public void ElasticsearchBulkSink_Constructor_Validation()
    {
        ElasticsearchClient nullClient = null!;
        IndexName nullIndex = null!;
        string nullIndexString = null!;
        Uri nullEndpoint = null!;

        Assert.Throws<ArgumentNullException>(() => new ElasticsearchBulkSink<TestElasticRecord>(nullClient, "test-index"));
        Assert.Throws<ArgumentNullException>(() => new ElasticsearchBulkSink<TestElasticRecord>(CreateMockClient("{}"), nullIndex));
        Assert.Throws<ArgumentNullException>(() => new ElasticsearchBulkSink<TestElasticRecord>(CreateMockClient("{}"), nullIndexString));
        Assert.Throws<ArgumentNullException>(() => new ElasticsearchBulkSink<TestElasticRecord>(nullEndpoint, "test-index"));
        Assert.Throws<ArgumentNullException>(() => new ElasticsearchBulkSink<TestElasticRecord>(new Uri("http://localhost:9200"), nullIndexString));

        var client = CreateMockClient("{}");
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "my-index", r => r.Id);
        Assert.Same(client, sink.Client);
        Assert.Equal("my-index", sink.TargetIndex.ToString());
        Assert.NotNull(sink.IdSelector);

        var sinkWithUri = new ElasticsearchBulkSink<TestElasticRecord>(new Uri("http://localhost:9200"), "my-index", "test-key");
        Assert.NotNull(sinkWithUri.Client);
        Assert.Equal("my-index", sinkWithUri.TargetIndex.ToString());
    }

    [Fact]
    public async Task ElasticsearchBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero()
    {
        var client = CreateMockClient("{}");
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "test-index");

        var result = await sink.WriteBatchAsync(Array.Empty<TestElasticRecord>(), CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task ElasticsearchBulkSink_WriteBatchAsync_Success()
    {
        var successResponse = """
        {
          "took": 10,
          "errors": false,
          "items": [
            { "index": { "_index": "test-index", "_id": "1", "status": 201 } },
            { "index": { "_index": "test-index", "_id": "2", "status": 201 } }
          ]
        }
        """;

        var client = CreateMockClient(successResponse);
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "test-index", r => r.Id);

        var records = new List<TestElasticRecord>
        {
            new("1", "Product A", 19.99m),
            new("2", "Product B", 29.99m)
        };

        var count = await sink.WriteBatchAsync(records, CancellationToken.None);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ElasticsearchBulkSink_WriteBatchAsync_Throws_When_Response_Has_Errors()
    {
        var errorResponse = """
        {
          "took": 15,
          "errors": true,
          "items": [
            { "index": { "_index": "test-index", "_id": "1", "status": 201 } },
            { "index": { "_index": "test-index", "_id": "2", "status": 400, "error": { "type": "mapper_parsing_exception", "reason": "failed to parse field" } } }
          ]
        }
        """;

        var client = CreateMockClient(errorResponse);
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "test-index", r => r.Id);

        var records = new List<TestElasticRecord>
        {
            new("1", "Product A", 19.99m),
            new("2", "Product B", 29.99m)
        };

        var ex = await Assert.ThrowsAsync<IngestionSinkException>(async () =>
        {
            await sink.WriteBatchAsync(records, CancellationToken.None);
        });

        Assert.Contains("reported errors for 1 item(s)", ex.Message);
        Assert.Contains(1, ex.FailedIndices);
        Assert.Single(ex.FailedItems);
        Assert.Contains("failed to parse field", ex.FailedItems[0]);
    }

    [Fact]
    public async Task ElasticsearchBulkSink_WriteBatchAsync_Throws_When_Invalid_Response()
    {
        var errorResponse = """
        {
          "error": {
            "root_cause": [
              { "type": "index_not_found_exception", "reason": "no such index [bad-index]" }
            ],
            "type": "index_not_found_exception",
            "reason": "no such index [bad-index]"
          },
          "status": 404
        }
        """;

        var client = CreateMockClient(errorResponse, 404);
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "bad-index");

        var records = new List<TestElasticRecord> { new("1", "Product A", 19.99m) };

        var ex = await Assert.ThrowsAsync<IngestionSinkException>(async () =>
        {
            await sink.WriteBatchAsync(records, CancellationToken.None);
        });

        Assert.Contains("Elasticsearch bulk operation failed", ex.Message);
    }

    [Fact]
    public async Task PipelineExtensions_WriteToElasticsearchAsync_Validation()
    {
        IFastIngestPipeline<TestElasticRecord>? nullPipeline = null;
        var client = CreateMockClient("{}");

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToElasticsearchAsync(client, "test-index");
        });

        var pipeline = FastIngestPipeline<TestElasticRecord>.Create();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToElasticsearchAsync((ElasticsearchClient)null!, "test-index");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToElasticsearchAsync(client, "");
        });

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToElasticsearchAsync((Uri)null!, "test-index");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToElasticsearchAsync(new Uri("http://localhost:9200"), "");
        });
    }

    [Fact]
    public async Task PipelineExtensions_WriteToElasticsearchAsync_Executes_Pipeline()
    {
        var csv = """
                  Id,Title,Price
                  10,Gadget,49.99
                  20,Widget,99.99
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var responseJson = """
        {
          "took": 5,
          "errors": false,
          "items": [
            { "index": { "_index": "catalog", "_id": "10", "status": 201 } },
            { "index": { "_index": "catalog", "_id": "20", "status": 201 } }
          ]
        }
        """;

        var client = CreateMockClient(responseJson);

        var pipeline = FastIngestPipeline<TestElasticRecord>.Create()
            .FromStream(stream)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "Id");
                m.Map(x => x.Title, "Title");
                m.Map(x => x.Price, "Price");
            });

        var result = await pipeline.WriteToElasticsearchAsync(client, "catalog", r => r.Id);

        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void DependencyInjection_AddElasticsearchSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var endpoint = "http://localhost:9200";

        services.AddFastIngest(builder =>
        {
            builder.AddElasticsearchSink();
            builder.AddElasticsearchSink(endpoint, "my-api-key", "my-default-index");
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(endpoint, options.ElasticsearchEndpoint);
        Assert.Equal("my-api-key", options.ElasticsearchApiKey);
        Assert.Equal("my-default-index", options.ElasticsearchDefaultIndex);

        var client = sp.GetService<ElasticsearchClient>();
        Assert.NotNull(client);
    }

    [Fact]
    public void DependencyInjection_AddElasticsearchSink_With_Client_Registers_Singleton()
    {
        var services = new ServiceCollection();
        var client = CreateMockClient("{}");

        services.AddFastIngest(builder =>
        {
            builder.AddElasticsearchSink(client);
        });

        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetService<ElasticsearchClient>();
        Assert.Same(client, resolved);
    }

    [Fact]
    public void DependencyInjection_AddElasticsearchSink_With_ConfigureSettings_Registers_Client()
    {
        var services = new ServiceCollection();

        services.AddFastIngest(builder =>
        {
            builder.AddElasticsearchSink(settings =>
            {
                // configure custom client settings
            });
        });

        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetService<ElasticsearchClient>();
        Assert.NotNull(resolved);
    }

    [Fact]
    public void FastIngestOptions_AddElasticsearchSink_Sets_Properties()
    {
        var options = new FastIngestOptions();
        options.AddElasticsearchSink("http://localhost:9200", "api-key", "default-idx");

        Assert.Equal("http://localhost:9200", options.ElasticsearchEndpoint);
        Assert.Equal("api-key", options.ElasticsearchApiKey);
        Assert.Equal("default-idx", options.ElasticsearchDefaultIndex);

        Assert.Throws<ArgumentNullException>(() => options.AddElasticsearchSink(""));
    }

    [Fact]
    public async Task ElasticsearchBulkSink_WriteBatchAsync_NullBatch_Returns_Zero()
    {
        var client = CreateMockClient("{}");
        var sink = new ElasticsearchBulkSink<TestElasticRecord>(client, "test-index");

        var result = await sink.WriteBatchAsync(null!, CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task PipelineExtensions_WriteToElasticsearchAsync_WithIndexName_Executes_Pipeline()
    {
        var csv = "Id,Title,Price\n1,Alpha,10.0\n";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var responseJson = """
        {
          "took": 2,
          "errors": false,
          "items": [
            { "index": { "_index": "products", "_id": "1", "status": 201 } }
          ]
        }
        """;

        var client = CreateMockClient(responseJson);

        var pipeline = FastIngestPipeline<TestElasticRecord>.Create()
            .FromStream(stream)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "Id");
                m.Map(x => x.Title, "Title");
                m.Map(x => x.Price, "Price");
            });

        var result = await pipeline.WriteToElasticsearchAsync(client, (IndexName)"products", r => r.Id);

        Assert.Equal(1, result.TotalProcessed);
        Assert.Equal(1, result.TotalSucceeded);
    }

    [Fact]
    public async Task DependencyInjection_ElasticsearchIngestionSink_Resolves_From_DI_And_Ingests()
    {
        var csv = "Id,Title,Price\n1,Product,15.5\n";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var responseJson = """
        {
          "took": 3,
          "errors": false,
          "items": [
            { "index": { "_index": "test-elastic-index", "_id": "1", "status": 201 } }
          ]
        }
        """;

        var client = CreateMockClient(responseJson);

        var services = new ServiceCollection();
        services.AddFastIngest(builder =>
        {
            builder.AddElasticsearchSink(client);
            builder.RegisterProfile<TestElasticProfile>();
        });

        using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var result = await engine.IngestAsync<TestElasticRecord>(stream);

        Assert.Equal(1, result.TotalProcessed);
        Assert.Equal(1, result.TotalSucceeded);
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void DependencyInjection_AddElasticsearchSink_OnServiceCollection_Registers_Client()
    {
        var services = new ServiceCollection();
        var client = CreateMockClient("{}");
        services.AddElasticsearchSink(client);

        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetService<ElasticsearchClient>();
        Assert.Same(client, resolved);
    }
}

public class TestElasticProfile : FastIngestProfile<TestElasticRecord>
{
    public TestElasticProfile()
    {
        ToTable("test-elastic-index");
        Map(x => x.Id, "Id");
        Map(x => x.Title, "Title");
        Map(x => x.Price, "Price");
    }
}
