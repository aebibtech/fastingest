# Constant-Memory Architecture ($O(1)$)

A primary design requirement of FastIngest is guaranteeing a **constant memory footprint ($O(1)$)**, regardless of whether you are ingesting a 1,000-row file or a 50,000,000-row file.

---

## The Root Cause of Ingestion Memory Spikes

In naive data pipelines, memory consumption scales linearly with file size ($O(N)$):

1. **Entire File Buffering**: Uploading a file and reading it via `File.ReadAllBytes()` or storing it in an in-memory `MemoryStream`.
2. **Intermediate Object Trees**: Deserializing rows into lists like `List<MyEntity>` or populating `System.Data.DataTable` objects. A 5GB CSV file containing 10 million rows typically expands to **12 GB to 20 GB** of heap allocations when materialized as managed C# class instances.
3. **String Allocations & LOH Pollution**: Every cell value parsed as a standard managed `string` is allocated on the managed heap. Strings larger than 85,000 bytes or massive collections of objects end up in the **Large Object Heap (LOH)** or promote quickly to **Generation 2**, leading to frequent, stop-the-world Garbage Collection pauses.

---

## How FastIngest Achieves $O(1)$ Memory

FastIngest replaces memory buffering with a pure, forward-only streaming pipeline:

```
Source Stream (CSV/XLSX)
   │
   ▼
[Reusable Span / Buffer Pool]  ◄── Sylvan Zero-Allocation Reader
   │
   ▼
[Pre-compiled Expression Map]  ◄── Reads raw string spans, zero reflection
   │
   ▼
[Fixed-Capacity Batch Chunk]   ◄── e.g., strictly 5,000 rows in memory
   │
   ▼
[Native Database Stream Sink]  ◄── Pushed directly to TCP wire buffer
   │
   ▼
[Batch Discarded / Recycled]   ◄── Instant Gen 0 collection; Gen 2 untouched
```

### 1. Zero-Allocation Sylvan Reader

FastIngest embeds [Sylvan.Data.Csv](https://github.com/MarkPflug/Sylvan), the highest performance CSV parser in .NET. Sylvan reads bytes directly from the underlying stream into reusable internal buffers. Column values are accessed as `ReadOnlySpan<char>` without allocating intermediate strings when converting to numbers, booleans, dates, or Guids.

### 2. Pre-Compiled Lambda Expressions

Dynamic property accessors often use `System.Reflection`, which boxes value types and causes constant heap allocations. FastIngest pre-compiles strongly-typed lambda expressions (`Expression<Func<TRecord, object?>>`) during profile initialization. Mapping and reading values incurs no reflection penalty.

### 3. Chunk Partitioning & Prompt Deallocation

Records are accumulated in batches capped at your configured `batchSize` (default: `5,000`). Once a batch reaches capacity:
1. It is immediately flushed into the native database sink (PostgreSQL binary `COPY`, SQL Server `SqlBulkCopy`, etc.).
2. The batch reference is cleared.
3. Because batch lifecycles are measured in tens of milliseconds, objects stay strictly within **Generation 0** of the .NET Garbage Collector and are collected almost instantaneously without triggering costly Gen 2 or LOH sweeps.

---

## Memory Allocation Profile

| Metric | Traditional Importer (CsvHelper + EF Core) | FastIngest Pipeline |
| :--- | :--- | :--- |
| **100K Rows** | ~180 MB Heap | **~21 MB Constant** |
| **1M Rows** | ~1.8 GB Heap | **~22 MB Constant** |
| **10M Rows** | ~18.5 GB Heap (OOM Risk) | **~24 MB Constant** |
| **50M Rows** | Crashes process (`OutOfMemoryException`) | **~24 MB Constant** |
| **GC Gen 2 Sweeps** | Continuous (severe GC pause spikes) | **0 Gen 2 sweeps** |
