using System.Text;
using FastIngest.Core.Common;
using FastIngest.Core.Mapping;
using FastIngest.Core.Pipeline;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Sqlite;
using FastIngest.Sqlite.Extensions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace FastIngest.Tests;

public record TestSqliteRecord(int Id, string? Name, decimal Amount, DateTime CreatedAt);

public class SqliteSinkTests
{
    private static IReadOnlyList<ColumnMapping<TestSqliteRecord>> CreateTestMappings()
    {
        var builder = new ColumnMappingBuilder<TestSqliteRecord>();
        builder.Map(x => x.Id, "id");
        builder.Map(x => x.Name, "name");
        builder.Map(x => x.Amount, "amount");
        builder.Map(x => x.CreatedAt, "created_at");
        return builder.Build();
    }

    [Fact]
    public void SqliteBulkSink_Constructor_Validation()
    {
        var mappings = CreateTestMappings();

        // Null/empty connectionString
        Assert.Throws<ArgumentException>(() => new SqliteBulkSink<TestSqliteRecord>("", "table", mappings));
        Assert.Throws<ArgumentException>(() => new SqliteBulkSink<TestSqliteRecord>("   ", "table", mappings));

        // Null/empty table
        Assert.Throws<ArgumentException>(() => new SqliteBulkSink<TestSqliteRecord>("Data Source=:memory:", "", mappings));
        Assert.Throws<ArgumentException>(() => new SqliteBulkSink<TestSqliteRecord>("Data Source=:memory:", "   ", mappings));

        // Null/empty mappings
        Assert.Throws<ArgumentNullException>(() => new SqliteBulkSink<TestSqliteRecord>("Data Source=:memory:", "table", null!));
        Assert.Throws<ArgumentException>(() => new SqliteBulkSink<TestSqliteRecord>("Data Source=:memory:", "table", Array.Empty<ColumnMapping<TestSqliteRecord>>()));

        // Valid constructor with connection string
        var sink = new SqliteBulkSink<TestSqliteRecord>("Data Source=test.db", "customers", mappings);
        Assert.Equal("customers", sink.TargetTableName);

        // Valid constructor with SqliteConnection
        using var conn = new SqliteConnection("Data Source=:memory:");
        var connSink = new SqliteBulkSink<TestSqliteRecord>(conn, "customers", mappings);
        Assert.Equal("customers", connSink.TargetTableName);

        // Null SqliteConnection
        Assert.Throws<ArgumentNullException>(() => new SqliteBulkSink<TestSqliteRecord>((SqliteConnection)null!, "table", mappings));
    }

    [Fact]
    public async Task SqliteBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero()
    {
        var mappings = CreateTestMappings();
        using var conn = new SqliteConnection("Data Source=:memory:");
        var sink = new SqliteBulkSink<TestSqliteRecord>(conn, "customers", mappings);

        var result = await sink.WriteBatchAsync(Array.Empty<TestSqliteRecord>(), CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task SqliteBulkSink_Should_Ingest_Records_Into_Database()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Create target table
        using (var createCmd = connection.CreateCommand())
        {
            createCmd.CommandText = """
                CREATE TABLE customers (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    amount NUMERIC,
                    created_at TEXT
                );
                """;
            await createCmd.ExecuteNonQueryAsync();
        }

        var mappings = CreateTestMappings();
        var sink = new SqliteBulkSink<TestSqliteRecord>(connection, "customers", mappings);

        var now = DateTime.UtcNow;
        var records = new List<TestSqliteRecord>
        {
            new(1, "Alice", 100.50m, now),
            new(2, "Bob", 200.75m, now),
            new(3, null, 0.0m, now)
        };

        var written = await sink.WriteBatchAsync(records, CancellationToken.None);
        Assert.Equal(3, written);

        // Verify records in SQLite table
        using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT COUNT(*) FROM customers;";
        var count = Convert.ToInt64(await queryCmd.ExecuteScalarAsync());
        Assert.Equal(3, count);

        queryCmd.CommandText = "SELECT name, amount FROM customers WHERE id = 3;";
        using var reader = await queryCmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.True(await reader.IsDBNullAsync(0));
        Assert.Equal(0.0m, reader.GetDecimal(1));
    }

    [Fact]
    public async Task PipelineExtensions_WriteToSqliteAsync_Executes_Full_Pipeline()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        using (var createCmd = connection.CreateCommand())
        {
            createCmd.CommandText = """
                CREATE TABLE customers (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    amount NUMERIC,
                    created_at TEXT
                );
                """;
            await createCmd.ExecuteNonQueryAsync();
        }

        var csv = """
            id,name,amount,created_at
            10,Alice,12.34,2026-01-01T00:00:00Z
            20,Bob,56.78,2026-01-02T00:00:00Z
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

        var pipeline = FastIngestPipeline<TestSqliteRecord>.Create()
            .FromStream(stream, FileType.Csv)
            .WithMapping(m =>
            {
                m.Map(x => x.Id, "id");
                m.Map(x => x.Name, "name");
                m.Map(x => x.Amount, "amount");
                m.Map(x => x.CreatedAt, "created_at");
            });

        var result = await pipeline.WriteToSqliteAsync(connection, "customers");

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.TotalSucceeded);

        using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT COUNT(*) FROM customers;";
        var count = Convert.ToInt64(await queryCmd.ExecuteScalarAsync());
        Assert.Equal(2, count);
    }

    [Fact]
    public void DependencyInjection_AddSqliteSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Data Source=fastingest.db;";

        services.AddFastIngest(builder =>
        {
            builder.AddSqliteSink();
            builder.AddSqliteSink(connStr);
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.SqliteConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void DependencyInjection_AddSqliteSink_On_ServiceCollection_Configures_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Data Source=fastingest.db;";

        services.AddSqliteSink(connStr);

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.SqliteConnectionString);
        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void FastIngestOptions_AddSqliteSink_Sets_ConnectionString()
    {
        var options = new FastIngestOptions();
        options.AddSqliteSink("Data Source=app.db;");
        Assert.Equal("Data Source=app.db;", options.SqliteConnectionString);
        Assert.Equal("Data Source=app.db;", options.DefaultConnectionString);

        Assert.Throws<ArgumentNullException>(() => options.AddSqliteSink(""));
    }

    [Fact]
    public async Task PipelineExtensions_WriteToSqliteAsync_Validates_Arguments()
    {
        IFastIngestPipeline<TestSqliteRecord>? nullPipeline = null;
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToSqliteAsync("conn", "table");
        });

        var pipeline = FastIngestPipeline<TestSqliteRecord>.Create();
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToSqliteAsync("", "table");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToSqliteAsync("conn", "");
        });

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await pipeline.WriteToSqliteAsync((SqliteConnection)null!, "table");
        });
    }
}
