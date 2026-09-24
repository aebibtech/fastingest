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
| FastIngest_Pipeline | 25000    |   178.7 ms |    NA |  0.17 |    1 |   2000.0000 |  1000.0000 |          - |  18.44 MB |        0.08 |
| EfCore_Batched      | 25000    | 1,033.0 ms |    NA |  0.97 |    2 |  25000.0000 | 12000.0000 |  3000.0000 | 207.63 MB |        0.94 |
| EfCore_Naive        | 25000    | 1,067.0 ms |    NA |  1.00 |    3 |  25000.0000 |  9000.0000 |  2000.0000 | 219.95 MB |        1.00 |
|                     |          |            |       |       |      |             |            |            |           |             |
| FastIngest_Pipeline | 100000   |   496.6 ms |    NA |  0.17 |    1 |   9000.0000 |  3000.0000 |          - |  72.81 MB |        0.08 |
| EfCore_Batched      | 100000   | 2,592.3 ms |    NA |  0.91 |    2 | 107000.0000 | 53000.0000 | 17000.0000 | 825.18 MB |        0.94 |
| EfCore_Naive        | 100000   | 2,843.0 ms |    NA |  1.00 |    3 |  95000.0000 | 32000.0000 |  3000.0000 | 876.09 MB |        1.00 |
