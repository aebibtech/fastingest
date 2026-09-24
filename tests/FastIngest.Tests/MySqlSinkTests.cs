using FastIngest.Core.Mapping;
using FastIngest.Core.Pipeline;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.MySql;
using FastIngest.MySql.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Xunit;

namespace FastIngest.Tests;

public record TestMySqlRecord(int Id, string? Name, decimal Amount, DateTime CreatedAt);

public class MySqlSinkTests
{
    private static IReadOnlyList<ColumnMapping<TestMySqlRecord>> CreateTestMappings()
    {
        var builder = new ColumnMappingBuilder<TestMySqlRecord>();
        builder.Map(x => x.Id, "id");
        builder.Map(x => x.Name, "name");
        builder.Map(x => x.Amount, "amount");
        builder.Map(x => x.CreatedAt, "created_at");
        return builder.Build();
    }

    [Fact]
    public void MySqlBulkSink_Constructor_Validation()
    {
        var mappings = CreateTestMappings();

        // Null/empty connectionString
        Assert.Throws<ArgumentException>(() => new MySqlBulkSink<TestMySqlRecord>("", "table", mappings));
        Assert.Throws<ArgumentException>(() => new MySqlBulkSink<TestMySqlRecord>("   ", "table", mappings));

        // Null/empty table
        Assert.Throws<ArgumentException>(() => new MySqlBulkSink<TestMySqlRecord>("Server=localhost", "", mappings));
        Assert.Throws<ArgumentException>(() => new MySqlBulkSink<TestMySqlRecord>("Server=localhost", "   ", mappings));

        // Null/empty mappings
        Assert.Throws<ArgumentNullException>(() => new MySqlBulkSink<TestMySqlRecord>("Server=localhost", "table", null!));
        Assert.Throws<ArgumentException>(() => new MySqlBulkSink<TestMySqlRecord>("Server=localhost", "table", Array.Empty<ColumnMapping<TestMySqlRecord>>()));

        // Valid constructor with connection string
        var sink = new MySqlBulkSink<TestMySqlRecord>("Server=localhost;Database=test;User=root;", "my_table", mappings);
        Assert.Equal("my_table", sink.TargetTableName);

        // Valid constructor with MySqlConnection
        using var conn = new MySqlConnection("Server=localhost;Database=test;User=root;");
        var connSink = new MySqlBulkSink<TestMySqlRecord>(conn, "my_table_conn", mappings);
        Assert.Equal("my_table_conn", connSink.TargetTableName);

        // Null MySqlConnection
        Assert.Throws<ArgumentNullException>(() => new MySqlBulkSink<TestMySqlRecord>((MySqlConnection)null!, "table", mappings));
    }

    [Fact]
    public async Task MySqlBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero()
    {
        var mappings = CreateTestMappings();
        var sink = new MySqlBulkSink<TestMySqlRecord>("Server=dummy;Database=dummy;", "test_table", mappings);

        var result = await sink.WriteBatchAsync(Array.Empty<TestMySqlRecord>(), CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public void DependencyInjection_AddMySqlSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Server=mysql.example.com;Database=FastIngestDb;User Id=root;Password=secret;";

        services.AddFastIngest(builder =>
        {
            builder.AddMySqlSink();
            builder.AddMySqlSink(connStr);
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.MySqlConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void DependencyInjection_AddMySqlSink_On_ServiceCollection_Configures_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Server=mysql.example.com;Database=FastIngestDb;User Id=root;Password=secret;";

        services.AddMySqlSink(connStr);

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.MySqlConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void FastIngestOptions_AddMySqlSink_Sets_ConnectionString()
    {
        var options = new FastIngestOptions();
        options.AddMySqlSink("Server=localhost;Database=FastIngest;");
        Assert.Equal("Server=localhost;Database=FastIngest;", options.MySqlConnectionString);
        Assert.Equal("Server=localhost;Database=FastIngest;", options.DefaultConnectionString);

        Assert.Throws<ArgumentNullException>(() => options.AddMySqlSink(""));
    }

    [Fact]
    public async Task PipelineExtensions_WriteToMySqlAsync_Validates_Arguments()
    {
        IFastIngestPipeline<TestMySqlRecord>? nullPipeline = null;
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToMySqlAsync("conn", "table");
        });

        var pipeline = FastIngestPipeline<TestMySqlRecord>.Create();
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToMySqlAsync("", "table");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToMySqlAsync("conn", "");
        });

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToMySqlAsync((MySqlConnection)null!, "table");
        });
    }
}
