using FastIngest.Benchmarks.EfCore;
using FastIngest.Benchmarks.Fixtures;
using FastIngest.Benchmarks.Models;
using FastIngest.Core.Pipeline;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace FastIngest.Tests;

/// <summary>
/// Verifies the correctness of benchmark dataset generation and EF Core context model configurations.
/// </summary>
public class BenchmarkFixturesTests
{
    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    public void DataGenerator_Should_Generate_Expected_Record_Count(int count)
    {
        var records = DataGenerator.GenerateRecords(count);

        Assert.Equal(count, records.Count);
        Assert.All(records, r =>
        {
            Assert.True(r.Id > 0);
            Assert.StartsWith("SKU-", r.Sku);
            Assert.Contains("@", r.Email);
            Assert.True(r.Price > 0);
            Assert.InRange(r.Quantity, 1, 100);
            Assert.Equal(DateTimeKind.Utc, r.CreatedAt.Kind);
        });
    }

    [Fact]
    public void DataGenerator_Should_Generate_Parseable_Csv_Stream()
    {
        const int count = 500;
        using var stream = DataGenerator.GenerateCsvStream(count);

        Assert.True(stream.Length > 0);
        Assert.Equal(0, stream.Position);

        var parsed = DataGenerator.ParseCsvStream(stream);

        Assert.Equal(count, parsed.Count);
        Assert.Equal(1, parsed[0].Id);
        Assert.Equal(count, parsed[^1].Id);
        Assert.StartsWith("SKU-", parsed[0].Sku);
        Assert.Contains("@", parsed[0].Email);
        Assert.True(parsed[0].Price > 0);
        Assert.Equal(DateTimeKind.Utc, parsed[0].CreatedAt.Kind);
    }

    [Fact]
    public async Task FastIngestPipeline_Should_Stream_Benchmark_Csv_To_Sink()
    {
        const int count = 250;
        using var stream = DataGenerator.GenerateCsvStream(count);
        var sink = new TestMemorySink<CustomerRecord>();

        var result = await FastIngestPipeline<CustomerRecord>.Create()
            .FromStream(stream)
            .WithMapping(m => m
                .Map(x => x.Id, "id")
                .Map(x => x.Sku, "sku")
                .Map(x => x.Email, "email")
                .Map(x => x.Price, "price")
                .Map(x => x.Quantity, "quantity")
                .Map(x => x.CreatedAt, "created_at"))
            .WithBatchSize(50)
            .WriteToSinkAsync(sink);

        Assert.Equal(count, result.TotalProcessed);
        Assert.Equal(count, result.TotalSucceeded);
        Assert.Equal(0, result.TotalFailed);
        Assert.Equal(count, sink.Records.Count);
        Assert.Equal(5, sink.BatchCallCount); // 250 / 50 = 5 batches
    }

    [Fact]
    public void BenchmarkDbContext_Should_Map_CustomerRecord_To_Expected_Table_And_Columns()
    {
        var options = new DbContextOptionsBuilder<BenchmarkDbContext>()
            .UseNpgsql("Host=localhost;Database=test;Username=test;Password=test")
            .Options;

        using var context = new BenchmarkDbContext(options);
        var entityType = context.Model.FindEntityType(typeof(CustomerRecord));

        Assert.NotNull(entityType);
        Assert.Equal("benchmark_customers", entityType.GetTableName());

        var idProp = entityType.FindProperty(nameof(CustomerRecord.Id));
        Assert.NotNull(idProp);
        Assert.Equal("id", idProp.GetColumnName());

        var skuProp = entityType.FindProperty(nameof(CustomerRecord.Sku));
        Assert.NotNull(skuProp);
        Assert.Equal("sku", skuProp.GetColumnName());

        var emailProp = entityType.FindProperty(nameof(CustomerRecord.Email));
        Assert.NotNull(emailProp);
        Assert.Equal("email", emailProp.GetColumnName());

        var priceProp = entityType.FindProperty(nameof(CustomerRecord.Price));
        Assert.NotNull(priceProp);
        Assert.Equal("price", priceProp.GetColumnName());

        var qtyProp = entityType.FindProperty(nameof(CustomerRecord.Quantity));
        Assert.NotNull(qtyProp);
        Assert.Equal("quantity", qtyProp.GetColumnName());

        var createdAtProp = entityType.FindProperty(nameof(CustomerRecord.CreatedAt));
        Assert.NotNull(createdAtProp);
        Assert.Equal("created_at", createdAtProp.GetColumnName());
    }
}
