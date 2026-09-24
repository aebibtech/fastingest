# FastIngest BenchmarkDotNet Suite

A dedicated, reproducible performance benchmark suite for **FastIngest** built on **BenchmarkDotNet**, comparing throughput, execution latency, and memory allocations against Entity Framework Core 9.

---

## Benchmark Comparisons

The benchmark suite measures three distinct ingestion methodologies over identical synthetic customer datasets:

1. **`EfCore_Naive` (Baseline)**:
   - Full in-memory parsing into `List<CustomerRecord>`.
   - `_context.AddRangeAsync(records)` with default change tracking (`AutoDetectChangesEnabled = true`).
   - Single atomic `_context.SaveChangesAsync()`.
2. **`EfCore_Batched`**:
   - Streaming row-by-row reading in chunks of 1,000 records.
   - `ChangeTracker.AutoDetectChangesEnabled = false`.
   - `AddRange` and `SaveChangesAsync()` per 1,000-row batch, clearing change tracking after each save.
3. **`FastIngest_Pipeline`**:
   - Zero-allocation streaming row parsing via Sylvan CSV reader.
   - Direct streaming binary `COPY ... FROM STDIN (FORMAT BINARY)` using `NpgsqlBinaryImporter`.
   - Tuned batch sizing (5,000 rows) with constant memory footprint.

---

## Hardware & Environment Configurations

Benchmarks are executed under:
- **Framework**: `net9.0`
- **Diagnosers**: `[MemoryDiagnoser]` capturing Gen 0/1/2 GC collections and total allocated bytes.
- **Parameters**: `RowCount` = `25_000` and `100_000` rows.
- **Exporters**: `MarkdownExporter.GitHub` (outputs tables directly into `BenchmarkDotNet.Artifacts/results/`).

---

## Running the Benchmarks

### Option A: Using Local Docker / Testcontainers (Default)
Ensure Docker is running locally. The suite automatically provisions an isolated PostgreSQL 16 Alpine container:

```bash
./benchmarks/run-benchmarks.sh
```

Or via `dotnet`:
```bash
dotnet run -c Release --project benchmarks/FastIngest.Benchmarks/FastIngest.Benchmarks.csproj
```

### Option B: Using an Existing PostgreSQL Instance
Point to an existing PostgreSQL database by setting `FASTINGEST_BENCHMARK_CONNECTION_STRING`:

```bash
export FASTINGEST_BENCHMARK_CONNECTION_STRING="Host=localhost;Port=5432;Database=benchmark_db;Username=postgres;Password=postgres"
./benchmarks/run-benchmarks.sh
```

### Option C: Passing Custom BenchmarkDotNet Arguments
You can pass any standard BenchmarkDotNet flags:

```bash
# Run only FastIngest pipeline
./benchmarks/run-benchmarks.sh --filter *FastIngest_Pipeline*

# Dry run (1 iteration for sanity check)
./benchmarks/run-benchmarks.sh --job dry
```

---

## Output Artifacts

When execution finishes, BenchmarkDotNet writes GitHub-flavored Markdown tables to:
```
BenchmarkDotNet.Artifacts/results/FastIngest.Benchmarks.IngestionBenchmarks-report-github.md
```
These tables can be directly embedded into `README.md` and `docs/benchmarks/performance.md`.
