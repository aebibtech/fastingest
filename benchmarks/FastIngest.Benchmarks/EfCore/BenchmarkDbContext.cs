using FastIngest.Benchmarks.Models;
using Microsoft.EntityFrameworkCore;

namespace FastIngest.Benchmarks.EfCore;

/// <summary>
/// Entity Framework Core DbContext mapping <see cref="CustomerRecord"/> to the PostgreSQL benchmark table.
/// </summary>
public class BenchmarkDbContext : DbContext
{
    public BenchmarkDbContext(DbContextOptions<BenchmarkDbContext> options)
        : base(options)
    {
    }

    public DbSet<CustomerRecord> Customers => Set<CustomerRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<CustomerRecord>(entity =>
        {
            entity.ToTable("benchmark_customers");
            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id)
                .HasColumnName("id")
                .ValueGeneratedNever();

            entity.Property(e => e.Sku)
                .HasColumnName("sku")
                .IsRequired();

            entity.Property(e => e.Email)
                .HasColumnName("email")
                .IsRequired();

            entity.Property(e => e.Price)
                .HasColumnName("price")
                .HasColumnType("numeric");

            entity.Property(e => e.Quantity)
                .HasColumnName("quantity");

            entity.Property(e => e.CreatedAt)
                .HasColumnName("created_at")
                .HasColumnType("timestamp with time zone");
        });
    }
}
