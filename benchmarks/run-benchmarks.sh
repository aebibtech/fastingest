#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCHMARK_PROJ="${SCRIPT_DIR}/FastIngest.Benchmarks/FastIngest.Benchmarks.csproj"
ARTIFACTS_DIR="${ROOT_DIR}/BenchmarkDotNet.Artifacts/results"

echo "================================================================================"
echo " Starting FastIngest Benchmark Suite"
echo " Configuration: Release (-c Release)"
echo " Target Project: benchmarks/FastIngest.Benchmarks"
echo "================================================================================"

# Build project in Release mode
dotnet build -c Release "${BENCHMARK_PROJ}"

# Run benchmarks passing any forwarded arguments
dotnet run -c Release --no-build --project "${BENCHMARK_PROJ}" -- "$@"

echo ""
echo "================================================================================"
echo " Benchmark Run Completed Successfully"
echo " GitHub Markdown exports available in:"
echo " ${ARTIFACTS_DIR}"
echo " Markdown tables can be copied directly to README.md and docs/benchmarks/performance.md"
echo "================================================================================"
