namespace FastIngest.Benchmarks.Models;

/// <summary>
/// Benchmark model representing a customer product record.
/// </summary>
public class CustomerRecord
{
    public long Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    public DateTime CreatedAt { get; set; }
}
