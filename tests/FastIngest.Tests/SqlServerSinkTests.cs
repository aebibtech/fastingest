using System.Data;
using FastIngest.Core.Mapping;
using FastIngest.Core.Pipeline;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Builder;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FastIngest.SqlServer;
using FastIngest.SqlServer.Extensions;
using FastIngest.SqlServer.Internal;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace FastIngest.Tests;

public record TestSqlRecord(int Id, string? Name, decimal Amount, DateTime CreatedAt);

public class SqlServerSinkTests
{
    private static (IReadOnlyList<Func<TestSqlRecord, object?>> Getters, IReadOnlyList<string> ColumnNames, IReadOnlyList<Type> Types) CreateTestGetters()
    {
        var getters = new Func<TestSqlRecord, object?>[]
        {
            r => r.Id,
            r => r.Name,
            r => r.Amount,
            r => r.CreatedAt
        };

        var columnNames = new[] { "Id", "Name", "Amount", "CreatedAt" };
        var types = new[] { typeof(int), typeof(string), typeof(decimal), typeof(DateTime) };

        return (getters, columnNames, types);
    }

    [Fact]
    public void BatchDataReader_Should_Read_Records_And_Advance_Indices()
    {
        var records = new List<TestSqlRecord>
        {
            new(1, "Alice", 10.5m, DateTime.UtcNow),
            new(2, "Bob", 20.0m, DateTime.UtcNow)
        };

        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.Equal(4, reader.FieldCount);
        Assert.True(reader.HasRows);
        Assert.Equal(0, reader.Depth);
        Assert.False(reader.IsClosed);
        Assert.Equal(-1, reader.RecordsAffected);

        // Before Read, accessing values should throw
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));

        // Row 1
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal("Alice", reader.GetString(1));
        Assert.Equal(10.5m, reader.GetDecimal(2));
        Assert.False(reader.IsDBNull(1));

        // Row 2
        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
        Assert.Equal("Bob", reader.GetString(1));
        Assert.Equal(20.0m, reader.GetDecimal(2));

        // End of stream
        Assert.False(reader.Read());
        Assert.False(reader.Read());
    }

    [Fact]
    public async Task BatchDataReader_ReadAsync_Should_Respect_Cancellation()
    {
        var records = new List<TestSqlRecord>
        {
            new(1, "Alice", 10.5m, DateTime.UtcNow)
        };

        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await reader.ReadAsync(cts.Token);
        });
    }

    [Fact]
    public void BatchDataReader_Should_Handle_Null_Values_Correctly()
    {
        var records = new List<TestSqlRecord>
        {
            new(1, null, 0m, DateTime.UtcNow)
        };

        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetValue(0));
        Assert.Null(reader.GetValue(1));
        Assert.True(reader.IsDBNull(1));
        Assert.False(reader.IsDBNull(0));
    }

    [Fact]
    public void BatchDataReader_Should_Find_Ordinal_Case_Insensitively()
    {
        var records = new List<TestSqlRecord> { new(1, "Test", 5m, DateTime.UtcNow) };
        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal(0, reader.GetOrdinal("ID"));
        Assert.Equal(1, reader.GetOrdinal("name"));
        Assert.Equal(2, reader.GetOrdinal("AMOUNT"));
        Assert.Equal(3, reader.GetOrdinal("CreatedAt"));

        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("NonExistent"));
    }

    [Fact]
    public void BatchDataReader_GetName_And_FieldType()
    {
        var records = new List<TestSqlRecord> { new(1, "Test", 5m, DateTime.UtcNow) };
        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.Equal("Id", reader.GetName(0));
        Assert.Equal("Name", reader.GetName(1));
        Assert.Equal(typeof(int), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));
        Assert.Equal("Int32", reader.GetDataTypeName(0));
        Assert.Equal("String", reader.GetDataTypeName(1));

        Assert.Throws<IndexOutOfRangeException>(() => reader.GetName(99));
        Assert.Throws<IndexOutOfRangeException>(() => reader.GetFieldType(99));
    }

    [Fact]
    public void BatchDataReader_GetValues_Should_Populate_Array()
    {
        var now = DateTime.UtcNow;
        var records = new List<TestSqlRecord> { new(42, "Bob", 99.99m, now) };
        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.True(reader.Read());
        var values = new object[4];
        int count = reader.GetValues(values);

        Assert.Equal(4, count);
        Assert.Equal(42, values[0]);
        Assert.Equal("Bob", values[1]);
        Assert.Equal(99.99m, values[2]);
        Assert.Equal(now, values[3]);
    }

    [Fact]
    public void BatchDataReader_Indexers_Work()
    {
        var records = new List<TestSqlRecord> { new(7, "Seven", 77.7m, DateTime.UtcNow) };
        var (getters, columnNames, types) = CreateTestGetters();
        using var reader = new BatchDataReader<TestSqlRecord>(records, getters, columnNames, types);

        Assert.True(reader.Read());
        Assert.Equal(7, reader[0]);
        Assert.Equal("Seven", reader["Name"]);
    }

    [Fact]
    public void SqlServerBulkSink_Validation()
    {
        var builder = new ColumnMappingBuilder<TestSqlRecord>();
        builder.Map(x => x.Id);
        var mappings = builder.Build();

        // Null/empty connectionString
        Assert.Throws<ArgumentException>(() => new SqlServerBulkSink<TestSqlRecord>("", "table", mappings));
        // Null/empty table
        Assert.Throws<ArgumentException>(() => new SqlServerBulkSink<TestSqlRecord>("Server=localhost", " ", mappings));
        // Empty mappings
        Assert.Throws<ArgumentException>(() => new SqlServerBulkSink<TestSqlRecord>("Server=localhost", "table", Array.Empty<ColumnMapping<TestSqlRecord>>()));

        // Valid constructor
        var sink = new SqlServerBulkSink<TestSqlRecord>(
            "Server=localhost;Database=test;Integrated Security=true;TrustServerCertificate=true",
            "TestRecords",
            mappings,
            SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock);

        Assert.Equal("TestRecords", sink.TargetTableName);
        Assert.Equal(SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock, sink.Options);
    }

    [Fact]
    public async Task SqlServerBulkSink_WriteBatchAsync_EmptyBatch_Returns_Zero()
    {
        var builder = new ColumnMappingBuilder<TestSqlRecord>();
        builder.Map(x => x.Id);
        var mappings = builder.Build();

        var sink = new SqlServerBulkSink<TestSqlRecord>(
            "Server=dummy;Database=dummy;",
            "TestRecords",
            mappings);

        // An empty batch should return 0 without opening or attempting connection
        var result = await sink.WriteBatchAsync(Array.Empty<TestSqlRecord>(), CancellationToken.None);
        Assert.Equal(0, result);
    }

    [Fact]
    public void DependencyInjection_AddSqlServerSink_Configures_Builder_And_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Server=tcp:sql.example.com;Database=FastIngestDb;User Id=sa;Password=secret;";

        services.AddFastIngest(builder =>
        {
            builder.AddSqlServerSink();
            builder.AddSqlServerSink(connStr);
        });

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void DependencyInjection_AddSqlServerSink_On_ServiceCollection_Configures_Options()
    {
        var services = new ServiceCollection();
        var connStr = "Server=tcp:sql.example.com;Database=FastIngestDb;User Id=sa;Password=secret;";

        services.AddSqlServerSink(connStr);

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;

        Assert.Equal(connStr, options.DefaultConnectionString);
    }

    [Fact]
    public void FastIngestOptions_AddSqlServerSink_Sets_DefaultConnectionString()
    {
        var options = new FastIngestOptions();
        options.AddSqlServerSink("Server=localhost;Database=FastIngest;");
        Assert.Equal("Server=localhost;Database=FastIngest;", options.DefaultConnectionString);

        Assert.Throws<ArgumentNullException>(() => options.AddSqlServerSink(""));
    }

    [Fact]
    public async Task PipelineExtensions_WriteToSqlServerAsync_Validates_Arguments()
    {
        IFastIngestPipeline<TestSqlRecord>? nullPipeline = null;
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await nullPipeline!.WriteToSqlServerAsync("conn", "table");
        });

        var pipeline = FastIngestPipeline<TestSqlRecord>.Create();
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToSqlServerAsync("", "table");
        });

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await pipeline.WriteToSqlServerAsync("conn", "");
        });
    }
}
