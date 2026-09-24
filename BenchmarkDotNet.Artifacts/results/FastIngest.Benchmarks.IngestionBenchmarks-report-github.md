```

BenchmarkDotNet v0.15.8, macOS 27.0 (26A428) [Darwin 27.0.0]
Apple M4, 1 CPU, 10 logical and 10 physical cores
.NET SDK 10.0.203
  [Host] : .NET 9.0.15 (9.0.15, 9.0.1526.17522), Arm64 RyuJIT armv8.0-a
  Dry    : .NET 9.0.15 (9.0.15, 9.0.1526.17522), Arm64 RyuJIT armv8.0-a

Job=Dry  IterationCount=1  LaunchCount=1  
RunStrategy=ColdStart  UnrollFactor=1  WarmupCount=1  

```
| Method              | RowCount | Mean       | Error | Ratio | Rank | Gen0        | Gen1       | Gen2       | Allocated | Alloc Ratio |
|-------------------- |--------- |-----------:|------:|------:|-----:|------------:|-----------:|-----------:|----------:|------------:|
| FastIngest_Pipeline | 25000    |   143.2 ms |    NA |  0.14 |    1 |   2000.0000 |  1000.0000 |          - |  18.64 MB |        0.08 |
| EfCore_Naive        | 25000    | 1,029.0 ms |    NA |  1.00 |    2 |  25000.0000 |  9000.0000 |  2000.0000 | 222.15 MB |        1.00 |
| EfCore_Batched      | 25000    | 1,190.8 ms |    NA |  1.16 |    3 |  26000.0000 | 12000.0000 |  3000.0000 |  210.1 MB |        0.95 |
|                     |          |            |       |       |      |             |            |            |           |             |
| FastIngest_Pipeline | 100000   |   439.6 ms |    NA |  0.16 |    1 |  10000.0000 |  4000.0000 |  1000.0000 |  73.59 MB |        0.08 |
| EfCore_Batched      | 100000   | 2,442.1 ms |    NA |  0.91 |    2 | 107000.0000 | 53000.0000 | 17000.0000 | 825.17 MB |        0.94 |
| EfCore_Naive        | 100000   | 2,691.6 ms |    NA |  1.00 |    3 |  95000.0000 | 32000.0000 |  3000.0000 | 876.09 MB |        1.00 |
