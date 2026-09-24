using System.Net;
using FastIngest.Core.Pipeline;
using FastIngest.CosmosDb;
using FastIngest.CosmosDb.Extensions;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Options;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace FastIngest.Tests;

public record TestCosmosRecord(string Id, string Category, decimal Value);

public class CosmosDbSinkTests
{
    [Fact]
    public void CosmosDbBulkSink_Constructor_Validation()
    {
        Container nullContainer = null!;
        Func<TestCosmosRecord, PartitionKey> nullSelector = null!;
        Func<TestCosmosRecord, string> nullStringSelector = null!;

        Assert.Throws<ArgumentNullException>(() => new CosmosDbBulkSink<TestCosmosRecord>(nullContainer, r => new PartitionKey(r.Category)));
        Assert.Throws<ArgumentNullException>(() => new CosmosDbBulkSink<TestCosmosRecord>(Substitute.For<Container>(), nullSelector));
        Assert.Throws<ArgumentNullException>(() => new CosmosDbBulkSink<TestCosmosRecord>(nullContainer, r => r.Category));
        Assert.Throws<ArgumentNullException>(() => new CosmosDbBulkSink<TestCosmosRecord>(Substitute.For<Container>(), nullStringSelector));

        // Connection string constructor validation
        Assert.Throws<ArgumentException>(() => new CosmosDbBulkSink<TestCosmosRecord>("", "db", "container", r => new PartitionKey(r.Category)));
        Assert.Throws<ArgumentException>(() => new CosmosDbBulkSink<TestCosmosRecord>("conn", "", "container", r => new PartitionKey(r.Category)));
        Assert.Throws<ArgumentException>(() => new CosmosDbBulkSink<TestCosmosRecord>("conn", "db", "", r => new PartitionKey(r.Category)));
        Assert.Throws<ArgumentNullException>(() => new CosmosDbBulkSink<TestCosmosRecord>("conn", "db", "container", (Func<TestCosmosRecord, PartitionKey>)null!));

        // Valid constructor with Container
        var container = Substitute.For<Container>();
        var sink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => new PartitionKey(r.Category));
        Assert.Same(container, sink.Container);

        // Valid constructor with string partitionKey
        var stringSink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => r.Category);
        Assert.Same(container, stringSink.Container);
    }

    [Fact]
    public async Task CosmosDbBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero()
    {
        var container = Substitute.For<Container>();
        var sink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => new PartitionKey(r.Category));

        var result = await sink.WriteBatchAsync(Array.Empty<TestCosmosRecord>(), CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task CosmosDbBulkSink_WriteBatchAsync_Dispatches_Items_Concurrently()
    {
        var container = Substitute.For<Container>();
        var response = Substitute.For<ItemResponse<TestCosmosRecord>>();

        container.CreateItemAsync(
            Arg.Any<TestCosmosRecord>(),
            Arg.Any<PartitionKey?>(),
            Arg.Any<ItemRequestOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var sink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => new PartitionKey(r.Category));

        var records = new List<TestCosmosRecord>
        {
            new("1", "A", 10m),
            new("2", "B", 20m),
            new("3", "A", 30m)
        };

        var count = await sink.WriteBatchAsync(records, CancellationToken.None);
        Assert.Equal(3, count);

        // Verify each item was dispatched
        await container.Received(1).CreateItemAsync(
            Arg.Is<TestCosmosRecord>(r => r.Id == "1"),
            Arg.Is<PartitionKey?>(pk => pk.HasValue && pk.Value.Equals(new PartitionKey("A"))),
            cancellationToken: Arg.Any<CancellationToken>());

        await container.Received(1).CreateItemAsync(
            Arg.Is<TestCosmosRecord>(r => r.Id == "2"),
            Arg.Is<PartitionKey?>(pk => pk.HasValue && pk.Value.Equals(new PartitionKey("B"))),
            cancellationToken: Arg.Any<CancellationToken>());

        await container.Received(1).CreateItemAsync(
            Arg.Is<TestCosmosRecord>(r => r.Id == "3"),
            Arg.Is<PartitionKey?>(pk => pk.HasValue && pk.Value.Equals(new PartitionKey("A"))),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CosmosDbBulkSink_WriteBatchAsync_Should_Throw_When_Item_Fails()
    {
        var container = Substitute.For<Container>();

        var cosmosException = new CosmosException("Conflict", HttpStatusCode.Conflict, 0, "activity-1", 1.0);

        container.CreateItemAsync(
            Arg.Any<TestCosmosRecord>(),
            Arg.Any<PartitionKey?>(),
            Arg.Any<ItemRequestOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<TestCosmosRecord>>(cosmosException));

        var sink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => new PartitionKey(r.Category));

        var records = new List<TestCosmosRecord> { new("1", "A", 10m) };

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
        {
            await sink.WriteBatchAsync(records, CancellationToken.None);
        });

        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
    }

    [Fact]
    public async Task CosmosDbBulkSink_WriteBatchAsync_Should_Wrap_Multiple_Failures()
    {
        var container = Substitute.For<Container>();

        container.CreateItemAsync(
            Arg.Any<TestCosmosRecord>(),
            Arg.Any<PartitionKey?>(),
            Arg.Any<ItemRequestOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<TestCosmosRecord>>(
                new CosmosException("Rate limited", HttpStatusCode.TooManyRequests, 0, "act-2", 0)));

        var sink = new CosmosDbBulkSink<TestCosmosRecord>(container, r => new PartitionKey(r.Category));

        var records = new List<TestCosmosRecord>
        {
            new("1", "A", 10m),
            new("2", "B", 20m)
        };

        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await sink.WriteBatchAsync(records, CancellationToken.None);
        });

        Assert.Contains("Cosmos DB bulk ingestion failed", ex.Message);
        Assert.Equal(2, ex.InnerExceptions.Count);
    }

    [Fact]
    public void DependencyInjection_AddCosmosDbSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var connStr = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=dummykey==;";

        services.AddFastIngest(builder =>
        {
            builder.AddCosmosDbSink();
            builder.AddCosmosDbSink(connStr, "testdb", "testcontainer");
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.CosmosConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
        Assert.Equal("testdb", options.CosmosDatabaseName);
        Assert.Equal("testcontainer", options.CosmosContainerName);
    }

    [Fact]
    public void DependencyInjection_AddCosmosDbSink_With_Container_Registers_Singleton()
    {
        var services = new ServiceCollection();
        var container = Substitute.For<Container>();

        services.AddFastIngest(builder =>
        {
            builder.AddCosmosDbSink(container);
        });

        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetService<Container>();
        Assert.Same(container, resolved);
    }

    [Fact]
    public void FastIngestOptions_AddCosmosDbSink_Sets_Properties()
    {
        var options = new FastIngestOptions();
        options.AddCosmosDbSink("connStr", "myDb", "myContainer");

        Assert.Equal("connStr", options.CosmosConnectionString);
        Assert.Equal("myDb", options.CosmosDatabaseName);
        Assert.Equal("myContainer", options.CosmosContainerName);

        Assert.Throws<ArgumentNullException>(() => options.AddCosmosDbSink(""));
    }

    [Fact]
    public async Task PipelineExtensions_WriteToCosmosDbAsync_Validates_Arguments()
    {
        IFastIngestPipeline<TestCosmosRecord>? nullPipeline = null;
        var container = Substitute.For<Container>();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToCosmosDbAsync(container, r => new PartitionKey(r.Category));
        });

        var pipeline = FastIngestPipeline<TestCosmosRecord>.Create();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToCosmosDbAsync((Container)null!, r => new PartitionKey(r.Category));
        });

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToCosmosDbAsync(container, (Func<TestCosmosRecord, PartitionKey>)null!);
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToCosmosDbAsync("", "db", "container", r => new PartitionKey(r.Category));
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToCosmosDbAsync("conn", "", "container", r => new PartitionKey(r.Category));
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToCosmosDbAsync("conn", "db", "", r => new PartitionKey(r.Category));
        });
    }
}
