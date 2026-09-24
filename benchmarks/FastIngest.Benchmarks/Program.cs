using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;

namespace FastIngest.Benchmarks;

/// <summary>
/// Benchmark entrypoint for FastIngest performance suite.
/// Configures GitHub markdown exporter to output comparison tables for documentation.
/// </summary>
public static class Program
{
    public static void Main(string[] args)
    {
        var config = DefaultConfig.Instance;

        var effectiveArgs = args.Length > 0 && !args.Any(a => a.StartsWith("-f", StringComparison.OrdinalIgnoreCase) || a.StartsWith("--filter", StringComparison.OrdinalIgnoreCase))
            ? [.. args, "--filter", "*"]
            : args;

        if (effectiveArgs.Length > 0)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(effectiveArgs, config);
        }
        else
        {
            BenchmarkRunner.Run<IngestionBenchmarks>(config);
        }

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine(" FastIngest Benchmark Run Finished");
        Console.WriteLine(" Markdown reports generated in: BenchmarkDotNet.Artifacts/results/");
        Console.WriteLine(" Table output is formatted for README.md and docs/benchmarks/performance.md");
        Console.WriteLine("================================================================================");
    }
}
