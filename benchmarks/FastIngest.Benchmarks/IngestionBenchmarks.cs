using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FastIngest.Benchmarks.EfCore;
using FastIngest.Benchmarks.Fixtures;
using FastIngest.Benchmarks.Models;
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Sylvan.Data.Csv;
using Testcontainers.PostgreSql;

namespace FastIngest.Benchmarks;

/// <summary>
/// Benchmark suite comparing FastIngest streaming binary COPY with EF Core 9 naive and batched ingestion.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class IngestionBenchmarks
{
    private PostgreSqlContainer? _container;
    private string _connectionString = string.Empty;
    private MemoryStream _csvStream = null!;
    private BenchmarkDbContext _context = null!;
    private NpgsqlConnection _cleanupConnection = null!;

    /// <summary>
    /// Parameterized row counts for benchmark iterations.
    /// </summary>
    [Params(25_000, 100_000)]
    public int RowCount;

    /// <summary>
    /// Global setup: Provisions PostgreSQL instance, creates schema, and pre-generates CSV data.
    /// </summary>
    [GlobalSetup]
    public async Task GlobalSetup()
    {
        // 1. Establish PostgreSQL connection string from environment or Testcontainers
        string? envConn = Environment.GetEnvironmentVariable("FASTINGEST_BENCHMARK_CONNECTION_STRING")
            ?? Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

        if (!string.IsNullOrWhiteSpace(envConn))
        {
            _connectionString = envConn;
        }
        else
        {
            try
            {
                _container = new PostgreSqlBuilder()
                    .WithImage("postgres:16-alpine")
                    .WithDatabase("benchmark_db")
                    .WithUsername("postgres")
                    .WithPassword("postgres")
                    .Build();

                await _container.StartAsync();
                _connectionString = _container.GetConnectionString();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Unable to start PostgreSQL Testcontainer. Ensure Docker is running, or specify a connection string via " +
                    "the FASTINGEST_BENCHMARK_CONNECTION_STRING environment variable.", ex);
            }
        }

        // 2. Initialize schema
        await using (var conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS benchmark_customers (
                    id BIGINT,
                    sku TEXT,
                    email TEXT,
                    price NUMERIC,
                    quantity INT,
                    created_at TIMESTAMPTZ
                );";
            await cmd.ExecuteNonQueryAsync();
        }

        // 3. Pre-generate CSV stream into memory (excludes file I/O from benchmark loop)
        _csvStream = DataGenerator.GenerateCsvStream(RowCount);

        // 4. Initialize EF Core DbContext
        var options = new DbContextOptionsBuilder<BenchmarkDbContext>()
            .UseNpgsql(_connectionString)
            .Options;
        _context = new BenchmarkDbContext(options);

        // 5. Open persistent connection for rapid cleanup operations
        _cleanupConnection = new NpgsqlConnection(_connectionString);
        await _cleanupConnection.OpenAsync();
    }

    /// <summary>
    /// Iteration setup: Resets stream position and clears EF Core change tracker.
    /// </summary>
    [IterationSetup]
    public void IterationSetup()
    {
        _csvStream.Position = 0;
        _context.ChangeTracker.Clear();
    }

    /// <summary>
    /// Iteration cleanup: Truncates destination table to ensure clean state for next iteration.
    /// </summary>
    [IterationCleanup]
    public void IterationCleanup()
    {
        using var cmd = _cleanupConnection.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE benchmark_customers;";
        cmd.ExecuteNonQuery();

        _context.ChangeTracker.Clear();
    }

    /// <summary>
    /// Global cleanup: Disposes EF context, stream, connections, and container.
    /// </summary>
    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_cleanupConnection != null)
        {
            await _cleanupConnection.DisposeAsync();
        }

        if (_context != null)
        {
            await _context.DisposeAsync();
        }

        if (_csvStream != null)
        {
            await _csvStream.DisposeAsync();
        }

        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }

    /// <summary>
    /// Naive EF Core 9 baseline: parses all rows into memory and executes AddRangeAsync + SaveChangesAsync.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task EfCore_Naive()
    {
        _csvStream.Position = 0;
        _context.ChangeTracker.AutoDetectChangesEnabled = true;

        var records = DataGenerator.ParseCsvStream(_csvStream);
        await _context.AddRangeAsync(records);
        await _context.SaveChangesAsync();
    }

    /// <summary>
    /// Batched EF Core 9: reads in chunks of 1,000 rows with AutoDetectChanges disabled and saves per batch.
    /// </summary>
    [Benchmark]
    public async Task EfCore_Batched()
    {
        _csvStream.Position = 0;
        _context.ChangeTracker.AutoDetectChangesEnabled = false;

        using var reader = new StreamReader(_csvStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);
        using var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { HasHeaders = true });

        int idOrd = csv.GetOrdinal("id");
        int skuOrd = csv.GetOrdinal("sku");
        int emailOrd = csv.GetOrdinal("email");
        int priceOrd = csv.GetOrdinal("price");
        int qtyOrd = csv.GetOrdinal("quantity");
        int createdAtOrd = csv.GetOrdinal("created_at");

        var batch = new List<CustomerRecord>(1000);
        while (csv.Read())
        {
            batch.Add(new CustomerRecord
            {
                Id = csv.GetInt64(idOrd),
                Sku = csv.GetString(skuOrd),
                Email = csv.GetString(emailOrd),
                Price = csv.GetDecimal(priceOrd),
                Quantity = csv.GetInt32(qtyOrd),
                CreatedAt = csv.GetDateTime(createdAtOrd)
            });

            if (batch.Count >= 1000)
            {
                _context.AddRange(batch);
                await _context.SaveChangesAsync();
                _context.ChangeTracker.Clear();
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            _context.AddRange(batch);
            await _context.SaveChangesAsync();
            _context.ChangeTracker.Clear();
            batch.Clear();
        }
    }

    /// <summary>
    /// FastIngest streaming pipeline: streams rows directly into PostgreSQL using binary COPY.
    /// </summary>
    [Benchmark]
    public async Task FastIngest_Pipeline()
    {
        _csvStream.Position = 0;

        await FastIngestPipeline<CustomerRecord>.Create()
            .FromStream(_csvStream)
            .WithMapping(m => m
                .Map(x => x.Id, "id")
                .Map(x => x.Sku, "sku")
                .Map(x => x.Email, "email")
                .Map(x => x.Price, "price")
                .Map(x => x.Quantity, "quantity")
                .Map(x => x.CreatedAt, "created_at"))
            .WithBatchSize(5000)
            .WriteToPostgresAsync(_connectionString, "benchmark_customers");
    }
}
