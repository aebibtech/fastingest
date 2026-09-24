using System.Globalization;
using System.Text;
using Bogus;
using FastIngest.Benchmarks.Models;
using Sylvan.Data.Csv;

namespace FastIngest.Benchmarks.Fixtures;

/// <summary>
/// High-performance synthetic data generator for ingestion benchmarks.
/// Uses Bogus to produce realistic datasets with parameterized row counts.
/// </summary>
public static class DataGenerator
{
    public const int DefaultSeed = 42;

    /// <summary>
    /// Generates a collection of in-memory <see cref="CustomerRecord"/> instances.
    /// </summary>
    /// <param name="rowCount">Number of rows to generate.</param>
    /// <param name="seed">Optional seed for deterministic reproducibility.</param>
    /// <returns>A list of generated records.</returns>
    public static List<CustomerRecord> GenerateRecords(int rowCount, int seed = DefaultSeed)
    {
        Randomizer.Seed = new Random(seed);

        var faker = new Faker<CustomerRecord>()
            .RuleFor(c => c.Sku, f => $"SKU-{f.Random.AlphaNumeric(8).ToUpperInvariant()}")
            .RuleFor(c => c.Email, f => f.Internet.Email())
            .RuleFor(c => c.Price, f => Math.Round(f.Random.Decimal(1.99m, 999.99m), 2))
            .RuleFor(c => c.Quantity, f => f.Random.Int(1, 100))
            .RuleFor(c => c.CreatedAt, f => DateTime.SpecifyKind(f.Date.Recent(30), DateTimeKind.Utc));

        var list = new List<CustomerRecord>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            var record = faker.Generate();
            record.Id = i + 1;
            list.Add(record);
        }

        return list;
    }

    /// <summary>
    /// Generates a CSV UTF-8 byte array containing the specified number of customer records.
    /// </summary>
    /// <param name="rowCount">Number of rows to generate.</param>
    /// <param name="seed">Optional seed for deterministic reproducibility.</param>
    /// <returns>Byte array containing full CSV content.</returns>
    public static byte[] GenerateCsvBytes(int rowCount, int seed = DefaultSeed)
    {
        using var stream = GenerateCsvStream(rowCount, seed);
        return stream.ToArray();
    }

    /// <summary>
    /// Generates a seekable <see cref="MemoryStream"/> of CSV data for the specified row count.
    /// </summary>
    /// <param name="rowCount">Number of rows to generate.</param>
    /// <param name="seed">Optional seed for deterministic reproducibility.</param>
    /// <returns>A memory stream positioned at the beginning (offset 0).</returns>
    public static MemoryStream GenerateCsvStream(int rowCount, int seed = DefaultSeed)
    {
        Randomizer.Seed = new Random(seed);
        var random = new Random(seed);

        var stream = new MemoryStream();
        using (var writer = new StreamWriter(stream, Encoding.UTF8, bufferSize: 65536, leaveOpen: true))
        {
            // CSV Header matching database columns
            writer.WriteLine("id,sku,email,price,quantity,created_at");

            var baseDate = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Utc);

            for (long i = 1; i <= rowCount; i++)
            {
                long id = i;
                string sku = $"SKU-{random.Next(10000000, 99999999)}";
                string email = $"customer_{id}_{random.Next(1000, 9999)}@benchmark-domain.test";
                decimal price = Math.Round((decimal)(random.NextDouble() * 998.0 + 1.99), 2);
                int quantity = random.Next(1, 101);
                var createdAt = baseDate.AddSeconds(i);

                writer.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"{id},{sku},{email},{price:F2},{quantity},{createdAt:O}"));
            }

            writer.Flush();
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Parses a CSV stream into strongly-typed <see cref="CustomerRecord"/> objects using Sylvan's zero-allocation reader.
    /// </summary>
    /// <param name="stream">The CSV data stream.</param>
    /// <returns>Materialized list of customer records.</returns>
    public static List<CustomerRecord> ParseCsvStream(Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);
        using var csv = CsvDataReader.Create(reader, new CsvDataReaderOptions { HasHeaders = true });

        int idOrd = csv.GetOrdinal("id");
        int skuOrd = csv.GetOrdinal("sku");
        int emailOrd = csv.GetOrdinal("email");
        int priceOrd = csv.GetOrdinal("price");
        int qtyOrd = csv.GetOrdinal("quantity");
        int createdAtOrd = csv.GetOrdinal("created_at");

        var records = new List<CustomerRecord>();
        while (csv.Read())
        {
            records.Add(new CustomerRecord
            {
                Id = csv.GetInt64(idOrd),
                Sku = csv.GetString(skuOrd),
                Email = csv.GetString(emailOrd),
                Price = csv.GetDecimal(priceOrd),
                Quantity = csv.GetInt32(qtyOrd),
                CreatedAt = csv.GetDateTime(createdAtOrd)
            });
        }

        return records;
    }
}
