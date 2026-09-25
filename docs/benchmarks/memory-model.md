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
Source Stream (CSV / NDJSON / XLSX)
   │
   ▼
[Reusable Span / Buffer Pool]  ◄── Sylvan CSV Reader or PipeReader NDJSON Slicer
   │
   ▼
[Zero-Reflection Binders]      ◄── Pre-compiled Lambdas (CSV) or Utf8JsonReader (NDJSON)
   │
   ▼
[System.Threading.Channels]    ◄── Bounded channel (e.g., 2 batches in flight)
   │                           ◄── BoundedChannelFullMode.Wait enforces backpressure
   ▼
[Native Database Stream Sink]  ◄── Pushed directly to TCP wire buffer
   │
   ▼
[Batch Discarded / Recycled]   ◄── Instant Gen 0 collection; Gen 2 untouched
```

### 1. Zero-Allocation Sylvan CSV Reader

FastIngest embeds [Sylvan.Data.Csv](https://github.com/MarkPflug/Sylvan), the highest performance CSV parser in .NET. Sylvan reads bytes directly from the underlying stream into reusable internal buffers. Column values are accessed as `ReadOnlySpan<char>` without allocating intermediate strings when converting to numbers, booleans, dates, or Guids.

### 2. Zero-Allocation PipeReader & Utf8JsonReader for JSON Lines

For line-delimited JSON (`.jsonl` / `.ndjson`), FastIngest utilizes `System.IO.Pipelines.PipeReader` to slice lines directly out of pooled `ReadOnlySequence<byte>` buffers without materializing intermediate line strings on the heap. Each line is immediately passed to `System.Text.Json.Utf8JsonReader` as a raw byte sequence, deserializing straight into domain models. Empty or whitespace-only lines are skipped with zero allocations, and memory buffers are returned to the pipeline's memory pool immediately.

### 3. Pre-Compiled Lambda Expressions

Dynamic property accessors often use `System.Reflection`, which boxes value types and causes constant heap allocations. FastIngest pre-compiles strongly-typed lambda expressions (`Expression<Func<TRecord, object?>>`) during profile initialization. Mapping and reading values incurs no reflection penalty.

### 4. Bounded Channel Buffering & Backpressure

Records are accumulated in batches capped at your configured `batchSize` (default: `5,000`). Batches are posted to a bounded `System.Threading.Channels.Channel<IReadOnlyList<TRecord>>` configured with `BoundedChannelFullMode.Wait`:
1. Even when CPU row parsing outpaces database socket writes, the producer task pauses at `WriteAsync` whenever the channel reaches capacity (default: 2 batches).
2. The maximum number of records held in memory is strictly bounded by $(\text{ChannelCapacity} + 1) \times \text{BatchSize}$. For a 5,000-row batch with capacity 2, at most 15,000 rows can exist in flight at any given moment, preserving true $O(1)$ memory guarantees regardless of total file size.
3. Once written to the database sink, batch references are dropped immediately, remaining within **Generation 0** of the .NET Garbage Collector and avoiding Gen 2 or Large Object Heap (LOH) pollution.

---

## Memory Allocation Profile

| Metric | Traditional Importer (CsvHelper + EF Core) | FastIngest Pipeline |
| :--- | :--- | :--- |
| **100K Rows** | ~180 MB Heap | **~21 MB Constant** |
| **1M Rows** | ~1.8 GB Heap | **~22 MB Constant** |
| **10M Rows** | ~18.5 GB Heap (OOM Risk) | **~24 MB Constant** |
| **50M Rows** | Crashes process (`OutOfMemoryException`) | **~24 MB Constant** |
| **GC Gen 2 Sweeps** | Continuous (severe GC pause spikes) | **0 Gen 2 sweeps** |
