using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Pipeline;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Builder;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FastIngest.MongoDb;
using FastIngest.MongoDb.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using NSubstitute;
using Xunit;

namespace FastIngest.Tests;

public record TestMongoRecord(int Id, string Name, decimal Amount);

public class TestMongoRecordProfile : FastIngestProfile<TestMongoRecord>
{
    public TestMongoRecordProfile()
    {
        ToTable("mongo_records");
        WithBatchSize(5);
        WithFileType(FileType.Csv);

        Map(x => x.Id, "Id");
        Map(x => x.Name, "Name");
        Map(x => x.Amount, "Amount");
    }
}

public class MongoDbSinkTests
{
    [Fact]
    public void MongoDbBulkSink_Constructor_Validation()
    {
        // Null collection
        Assert.Throws<ArgumentNullException>(() => new MongoDbBulkSink<TestMongoRecord>((IMongoCollection<TestMongoRecord>)null!));

        // Invalid connection string, database name, or collection name
        Assert.Throws<ArgumentException>(() => new MongoDbBulkSink<TestMongoRecord>("", "db", "coll"));
        Assert.Throws<ArgumentException>(() => new MongoDbBulkSink<TestMongoRecord>("mongodb://localhost:27017", "", "coll"));
        Assert.Throws<ArgumentException>(() => new MongoDbBulkSink<TestMongoRecord>("mongodb://localhost:27017", "db", ""));

        // Invalid database / client overloads
        Assert.Throws<ArgumentNullException>(() => new MongoDbBulkSink<TestMongoRecord>((IMongoDatabase)null!, "coll"));
        Assert.Throws<ArgumentException>(() => new MongoDbBulkSink<TestMongoRecord>(Substitute.For<IMongoDatabase>(), " "));
        Assert.Throws<ArgumentNullException>(() => new MongoDbBulkSink<TestMongoRecord>((IMongoClient)null!, "db", "coll"));
    }

    [Fact]
    public void MongoDbBulkSink_Defaults_BulkWriteOptions_To_Unordered()
    {
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();
        var sink = new MongoDbBulkSink<TestMongoRecord>(mockCollection);

        Assert.NotNull(sink.BulkWriteOptions);
        Assert.False(sink.BulkWriteOptions.IsOrdered);
        Assert.Same(mockCollection, sink.Collection);
    }

    [Fact]
    public void MongoDbBulkSink_Preserves_Custom_BulkWriteOptions()
    {
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();
        var customOptions = new BulkWriteOptions { IsOrdered = true, BypassDocumentValidation = true };
        var sink = new MongoDbBulkSink<TestMongoRecord>(mockCollection, customOptions);

        Assert.Same(customOptions, sink.BulkWriteOptions);
        Assert.True(sink.BulkWriteOptions.IsOrdered);
        Assert.True(sink.BulkWriteOptions.BypassDocumentValidation);
    }

    [Fact]
    public async Task MongoDbBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero_Without_Calling_Collection()
    {
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();
        var sink = new MongoDbBulkSink<TestMongoRecord>(mockCollection);

        var result = await sink.WriteBatchAsync(Array.Empty<TestMongoRecord>(), CancellationToken.None);
        Assert.Equal(0, result);

        await mockCollection.DidNotReceiveWithAnyArgs().BulkWriteAsync(default!, default!, default);
    }

    [Fact]
    public async Task MongoDbBulkSink_WriteBatchAsync_Maps_To_InsertOneModel_And_Calls_BulkWriteAsync()
    {
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();
        var sink = new MongoDbBulkSink<TestMongoRecord>(mockCollection);

        var records = new List<TestMongoRecord>
        {
            new(1, "Alpha", 10.5m),
            new(2, "Beta", 20.0m)
        };

        IEnumerable<WriteModel<TestMongoRecord>>? capturedWrites = null;
        BulkWriteOptions? capturedOptions = null;

        mockCollection.BulkWriteAsync(
            Arg.Do<IEnumerable<WriteModel<TestMongoRecord>>>(w => capturedWrites = w.ToList()),
            Arg.Do<BulkWriteOptions>(opt => capturedOptions = opt),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BulkWriteResult<TestMongoRecord>>(null!));

        var writtenCount = await sink.WriteBatchAsync(records, CancellationToken.None);

        Assert.Equal(2, writtenCount);
        Assert.NotNull(capturedWrites);
        Assert.NotNull(capturedOptions);
        Assert.False(capturedOptions.IsOrdered);

        var writeList = capturedWrites.ToList();
        Assert.Equal(2, writeList.Count);

        var insert1 = Assert.IsType<InsertOneModel<TestMongoRecord>>(writeList[0]);
        Assert.Equal(1, insert1.Document.Id);
        Assert.Equal("Alpha", insert1.Document.Name);

        var insert2 = Assert.IsType<InsertOneModel<TestMongoRecord>>(writeList[1]);
        Assert.Equal(2, insert2.Document.Id);
        Assert.Equal("Beta", insert2.Document.Name);
    }

    [Fact]
    public async Task PipelineExtensions_WriteToMongoDbAsync_Validates_Arguments()
    {
        IFastIngestPipeline<TestMongoRecord>? nullPipeline = null;
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToMongoDbAsync(mockCollection);
        });

        var pipeline = FastIngestPipeline<TestMongoRecord>.Create();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToMongoDbAsync((IMongoCollection<TestMongoRecord>)null!);
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToMongoDbAsync("", "db", "coll");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToMongoDbAsync("mongodb://localhost:27017", "", "coll");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToMongoDbAsync("mongodb://localhost:27017", "db", "");
        });
    }

    [Fact]
    public async Task PipelineExtensions_WriteToMongoDbAsync_Streams_Records_Successfully()
    {
        var csv = """
                  Id,Name,Amount
                  1,Alpha,10.5
                  2,Beta,20.0
                  3,Gamma,30.0
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();

        var writtenBatches = new List<List<WriteModel<TestMongoRecord>>>();
        mockCollection.BulkWriteAsync(
            Arg.Do<IEnumerable<WriteModel<TestMongoRecord>>>(w => writtenBatches.Add(w.ToList())),
            Arg.Any<BulkWriteOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BulkWriteResult<TestMongoRecord>>(null!));

        var pipeline = FastIngestPipeline<TestMongoRecord>.Create()
            .FromStream(stream, FileType.Csv)
            .WithBatchSize(2)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "Id");
                m.Map(x => x.Name, "Name");
                m.Map(x => x.Amount, "Amount");
            });

        var result = await pipeline.WriteToMongoDbAsync(
            mockCollection,
            opt => opt.BypassDocumentValidation = true);

        Assert.Equal(3, result.TotalProcessed);
        Assert.Equal(3, result.TotalSucceeded);
        Assert.Equal(0, result.TotalFailed);
        Assert.True(result.IsSuccess);
        Assert.Equal(2, writtenBatches.Count); // Batch 1: 2 items, Batch 2: 1 item
    }

    [Fact]
    public void DependencyInjection_AddMongoDbSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var connStr = "mongodb://localhost:27017/analytics";

        services.AddFastIngest(builder =>
        {
            builder.AddMongoDbSink();
            builder.AddMongoDbSink(connStr, "analytics");
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.MongoConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
        Assert.Equal("analytics", options.MongoDatabaseName);
    }

    [Fact]
    public void DependencyInjection_AddMongoDbSink_On_ServiceCollection_Configures_Options()
    {
        var services = new ServiceCollection();
        var connStr = "mongodb://cluster0.example.com:27017";

        services.AddMongoDbSink(connStr, "stagingDb");

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.MongoConnectionString);
        Assert.Equal("stagingDb", options.MongoDatabaseName);
    }

    [Fact]
    public void FastIngestOptions_AddMongoDbSink_Sets_Properties()
    {
        var options = new FastIngestOptions();
        options.AddMongoDbSink("mongodb://localhost:27017/testDb", "testDb");

        Assert.Equal("mongodb://localhost:27017/testDb", options.MongoConnectionString);
        Assert.Equal("mongodb://localhost:27017/testDb", options.DefaultConnectionString);
        Assert.Equal("testDb", options.MongoDatabaseName);

        Assert.Throws<ArgumentNullException>(() => options.AddMongoDbSink(""));
    }

    [Fact]
    public async Task DependencyInjection_Engine_Ingests_With_Configured_MongoClient()
    {
        var csv = """
                  Id,Name,Amount
                  1,Alpha,10.5
                  2,Beta,20.0
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var mockClient = Substitute.For<IMongoClient>();
        var mockDb = Substitute.For<IMongoDatabase>();
        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();

        mockClient.GetDatabase("test_db").Returns(mockDb);
        mockDb.GetCollection<TestMongoRecord>("mongo_records").Returns(mockCollection);

        mockCollection.BulkWriteAsync(
            Arg.Any<IEnumerable<WriteModel<TestMongoRecord>>>(),
            Arg.Any<BulkWriteOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BulkWriteResult<TestMongoRecord>>(null!));

        var services = new ServiceCollection();
        services.AddFastIngest(builder =>
        {
            builder.AddMongoDbSink(mockClient, "test_db");
            builder.RegisterProfile<TestMongoRecordProfile>();
        });

        using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var result = await engine.IngestAsync<TestMongoRecord>(stream);

        Assert.Equal(2, result.TotalProcessed);
        Assert.Equal(2, result.TotalSucceeded);
        Assert.True(result.IsSuccess);

        await mockCollection.Received(1).BulkWriteAsync(
            Arg.Any<IEnumerable<WriteModel<TestMongoRecord>>>(),
            Arg.Is<BulkWriteOptions>(opt => !opt.IsOrdered),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DependencyInjection_Engine_Ingests_With_Direct_IMongoCollection()
    {
        var csv = """
                  Id,Name,Amount
                  100,DirectColl,99.99
                  """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var mockCollection = Substitute.For<IMongoCollection<TestMongoRecord>>();
        mockCollection.BulkWriteAsync(
            Arg.Any<IEnumerable<WriteModel<TestMongoRecord>>>(),
            Arg.Any<BulkWriteOptions>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BulkWriteResult<TestMongoRecord>>(null!));

        var services = new ServiceCollection();
        services.AddSingleton(mockCollection);
        services.AddFastIngest(builder =>
        {
            builder.AddMongoDbSink();
            builder.RegisterProfile<TestMongoRecordProfile>();
        });

        using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();

        var engine = scope.ServiceProvider.GetRequiredService<IFastIngestEngine>();
        var result = await engine.IngestAsync<TestMongoRecord>(stream);

        Assert.Equal(1, result.TotalProcessed);
        Assert.Equal(1, result.TotalSucceeded);
        Assert.True(result.IsSuccess);

        await mockCollection.Received(1).BulkWriteAsync(
            Arg.Any<IEnumerable<WriteModel<TestMongoRecord>>>(),
            Arg.Any<BulkWriteOptions>(),
            Arg.Any<CancellationToken>());
    }
}
