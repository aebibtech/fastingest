
BenchmarkDotNet v0.15.8, macOS 27.0 (26A428) [Darwin 27.0.0]
Apple M4, 1 CPU, 10 logical and 10 physical cores
.NET SDK 10.0.203
  [Host]     : .NET 9.0.15 (9.0.15, 9.0.1526.17522), Arm64 RyuJIT armv8.0-a
  Job-CNUJVU : .NET 9.0.15 (9.0.15, 9.0.1526.17522), Arm64 RyuJIT armv8.0-a

InvocationCount=1  UnrollFactor=1  

 Method              | RowCount | Mean        | Error     | StdDev    | Ratio | RatioSD | Rank | Gen0        | Gen1       | Gen2       | Allocated | Alloc Ratio |
-------------------- |--------- |------------:|----------:|----------:|------:|--------:|-----:|------------:|-----------:|-----------:|----------:|------------:|
 FastIngest_Pipeline | 25000    |    31.27 ms |  0.938 ms |  2.735 ms |  0.08 |    0.01 |    1 |   2000.0000 |  1000.0000 |          - |  18.44 MB |        0.08 |
 EfCore_Batched      | 25000    |   366.33 ms |  7.064 ms |  6.938 ms |  0.96 |    0.02 |    2 |  25000.0000 | 12000.0000 |  3000.0000 | 206.46 MB |        0.94 |
 EfCore_Naive        | 25000    |   382.72 ms |  7.274 ms |  6.804 ms |  1.00 |    0.02 |    2 |  25000.0000 |  9000.0000 |  2000.0000 |  218.8 MB |        1.00 |
                     |          |             |           |           |       |         |      |             |            |            |           |             |
 FastIngest_Pipeline | 100000   |   116.47 ms |  2.318 ms |  5.685 ms |  0.07 |    0.00 |    1 |   9000.0000 |  3000.0000 |          - |  72.81 MB |        0.08 |
 EfCore_Batched      | 100000   | 1,512.14 ms | 21.495 ms | 20.106 ms |  0.92 |    0.01 |    2 | 107000.0000 | 53000.0000 | 17000.0000 | 825.18 MB |        0.94 |
 EfCore_Naive        | 100000   | 1,637.89 ms | 15.489 ms | 14.488 ms |  1.00 |    0.01 |    3 |  95000.0000 | 32000.0000 |  3000.0000 | 876.08 MB |        1.00 |
