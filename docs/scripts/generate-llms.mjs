import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const docsDir = path.resolve(__dirname, '..')
const publicDir = path.resolve(docsDir, 'public')

const pages = [
  {
    section: 'Getting Started',
    title: 'Introduction',
    file: 'guide/introduction.md',
    url: '/guide/introduction',
    description: 'Architecture overview, O(1) constant-memory model, producer-consumer design, and comparison with traditional ingestion bottlenecks.'
  },
  {
    section: 'Getting Started',
    title: 'Quickstart Guide',
    file: 'guide/getting-started.md',
    url: '/guide/getting-started',
    description: '5-minute setup guide covering installation, record definitions, FluentValidation rules, pipeline builder execution, and inspecting IngestResult.'
  },
  {
    section: 'Getting Started',
    title: 'NDJSON / JSON Lines Streaming',
    file: 'guide/json-lines.md',
    url: '/guide/json-lines',
    description: 'Zero-allocation streaming ingestion for line-delimited JSON using PipeReader and Utf8JsonReader with automatic format detection.'
  },
  {
    section: 'Getting Started',
    title: 'Dependency Injection & ASP.NET Core',
    file: 'guide/dependency-injection.md',
    url: '/guide/dependency-injection',
    description: 'Registering FastIngest in ASP.NET Core IServiceCollection, using IFastIngestEngine, ingestion profiles, and background workers.'
  },
  {
    section: 'Getting Started',
    title: 'Validation & Error Handling',
    file: 'guide/validation.md',
    url: '/guide/validation',
    description: 'FluentValidation integration, FailFast vs CollectAndContinue strategies, error manifests, and exporting error CSVs.'
  },
  {
    section: 'Database Sinks',
    title: 'PostgreSQL Sink (Native Binary COPY)',
    file: 'sinks/postgresql.md',
    url: '/sinks/postgresql',
    description: 'Native binary COPY FROM STDIN protocol via Npgsql BeginBinaryImportAsync at 180,000+ rows/sec with zero SQL parsing.'
  },
  {
    section: 'Database Sinks',
    title: 'SQL Server Sink (SqlBulkCopy)',
    file: 'sinks/sql-server.md',
    url: '/sinks/sql-server',
    description: 'Streaming SqlBulkCopy backed by custom BatchDataReader<TRecord> at 140,000+ rows/sec with zero DataTable materialization.'
  },
  {
    section: 'Database Sinks',
    title: 'MySQL / MariaDB Sink',
    file: 'sinks/mysql.md',
    url: '/sinks/mysql',
    description: 'High-speed MySqlBulkCopy with local infile and batched multi-row transactional fallback at 110,000+ rows/sec.'
  },
  {
    section: 'Database Sinks',
    title: 'SQLite Sink (WAL Batch)',
    file: 'sinks/sqlite.md',
    url: '/sinks/sqlite',
    description: 'Transactional batch inserts optimized for Write-Ahead Logging (WAL) mode at 95,000+ rows/sec.'
  },
  {
    section: 'Database Sinks',
    title: 'MongoDB Sink (BulkWrite)',
    file: 'sinks/mongodb.md',
    url: '/sinks/mongodb',
    description: 'Unordered BulkWriteAsync using InsertOneModel<TRecord> batches at 85,000+ docs/sec.'
  },
  {
    section: 'Database Sinks',
    title: 'Azure Cosmos DB Sink',
    file: 'sinks/cosmosdb.md',
    url: '/sinks/cosmosdb',
    description: 'Concurrent point-write dispatching with AllowBulkExecution at 35,000+ docs/sec.'
  },
  {
    section: 'Database Sinks',
    title: 'Elasticsearch Sink',
    file: 'sinks/elasticsearch.md',
    url: '/sinks/elasticsearch',
    description: 'Bulk API chunking with raw NDJSON payload streaming at 65,000+ docs/sec.'
  },
  {
    section: 'Architecture & Benchmarks',
    title: 'Memory Model & Bounded Channels',
    file: 'benchmarks/memory-model.md',
    url: '/benchmarks/memory-model',
    description: 'Deep dive into bounded System.Threading.Channels, backpressure mechanics, Gen 0/1/2 GC analysis, and O(1) memory guarantees.'
  },
  {
    section: 'Architecture & Benchmarks',
    title: 'Performance Benchmarks',
    file: 'benchmarks/performance.md',
    url: '/benchmarks/performance',
    description: 'Comprehensive BenchmarkDotNet results across 100K, 1M, and 10M rows comparing FastIngest against EF Core and Dapper.'
  }
]

function cleanMarkdown(rawContent, pageUrl, baseUrl) {
  let content = rawContent

  // 1. Remove YAML frontmatter
  content = content.replace(/^---[\s\S]*?---\n*/, '')

  // 2. Convert VitePress code-group blocks
  content = content.replace(/::: code-group\n([\s\S]*?):::/g, (match, inner) => {
    return inner.trim()
  })

  // 3. Format code block headers like ```bash [PostgreSQL] -> ```bash\n# [PostgreSQL]
  content = content.replace(/```([a-zA-Z0-9_-]+)\s+\[(.*?)\]/g, (match, lang, label) => {
    const commentPrefix = ['bash', 'sh', 'zsh', 'yaml', 'yml', 'dockerfile'].includes(lang.toLowerCase()) ? '#' : '//'
    return '```' + lang + '\n' + commentPrefix + ' ' + label
  })

  // 4. Remove remaining generic VitePress containers like ::: tip or :::
  content = content.replace(/:::\s*[a-zA-Z0-9_-]*\s*\n?/g, '')

  // 5. Convert VitePress alert callouts to standard Markdown blockquotes
  content = content.replace(/^>\s*\[!TIP\]\s*/gm, '> **Tip:** ')
  content = content.replace(/^>\s*\[!NOTE\]\s*/gm, '> **Note:** ')
  content = content.replace(/^>\s*\[!IMPORTANT\]\s*/gm, '> **Important:** ')
  content = content.replace(/^>\s*\[!WARNING\]\s*/gm, '> **Warning:** ')
  content = content.replace(/^>\s*\[!CAUTION\]\s*/gm, '> **Caution:** ')

  // 6. Remove HTML layout wrapper elements
  content = content.replace(/<div[^>]*>/g, '')
  content = content.replace(/<\/div>/g, '')

  // 7. Normalize internal relative root links like (/guide/xyz) to canonical URLs
  content = content.replace(/\]\((\/[a-zA-Z0-9_/-]+)\)/g, (match, relPath) => {
    return `](${baseUrl}${relPath})`
  })

  // 8. Collapse excessive blank lines
  content = content.replace(/\n{3,}/g, '\n\n')

  return content.trim()
}

export function generateLlms({
  baseUrl = 'https://fastingest.aebibtech.com',
  outDirs = [publicDir]
} = {}) {
  // Ensure directories exist
  for (const dir of outDirs) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true })
    }
  }

  // --- Generate llms.txt ---
  let llmsTxt = `# FastIngest

> FastIngest is a zero-allocation, high-throughput, constant-memory (O(1)) bulk ingestion pipeline for .NET 9+. It streams massive CSV, Excel, and line-delimited JSON (NDJSON / .jsonl) datasets directly into native database bulk protocols—including PostgreSQL binary COPY, SQL Server SqlBulkCopy, MySQL BulkCopy, SQLite WAL batching, MongoDB unordered BulkWrite, Azure Cosmos DB, and Elasticsearch—with bounded channel backpressure, pre-compiled zero-reflection mapping, and FluentValidation error manifests.

FastIngest solves the problem of high memory consumption and GC freezes when importing multi-million row datasets. Memory usage remains constant (~20-35 MB) regardless of file size (100 rows or 50M+ rows).

- Documentation: ${baseUrl}
- Full Context Documentation: ${baseUrl}/llms-full.txt
- GitHub Repository: https://github.com/aebibtech/fastingest
- NuGet Package: https://www.nuget.org/packages/FastIngest.Core

## Quickstart Example

\`\`\`csharp
using FastIngest.Core.Pipeline;
using FastIngest.PostgreSql.Extensions;
using Npgsql;

await using var stream = File.OpenRead("customers.csv");
await using var connection = new NpgsqlConnection(connectionString);
await connection.OpenAsync();

var result = await FastIngestPipeline<CustomerRecord>.Create()
    .FromStream(stream, FileType.Csv)
    .WithMapping(m =>
    {
        m.Map(x => x.Id, "customer_id");
        m.Map(x => x.Email, "email");
        m.Map(x => x.FullName, "full_name");
        m.Map(x => x.Balance, "balance");
    })
    .ValidateWith<CustomerValidator>(opt => opt.ErrorStrategy = ErrorStrategy.CollectAndContinue)
    .WithBatchSize(10_000)
    .WithChannelCapacity(2)
    .WriteToPostgresAsync(connection, "customers");

Console.WriteLine($"Ingested {result.TotalSucceeded:N0} rows in {result.Duration.TotalSeconds:F1}s!");
\`\`\`
`

  // Group pages by section
  const sections = {}
  for (const page of pages) {
    if (!sections[page.section]) {
      sections[page.section] = []
    }
    sections[page.section].push(page)
  }

  for (const [sectionName, pageList] of Object.entries(sections)) {
    llmsTxt += `\n## ${sectionName}\n\n`
    for (const page of pageList) {
      llmsTxt += `- [${page.title}](${baseUrl}${page.url}): ${page.description}\n`
    }
  }

  llmsTxt += `\n## Packages & Installation

- \`FastIngest.Core\`: Core pipeline abstractions, streaming readers (CSV, JSON Lines), channel coordination, and validation.
- \`FastIngest.PostgreSql\`: Native binary COPY sink for PostgreSQL.
- \`FastIngest.SqlServer\`: SqlBulkCopy streaming sink for Microsoft SQL Server.
- \`FastIngest.MySql\`: BulkCopy sink for MySQL and MariaDB.
- \`FastIngest.Sqlite\`: Parameterized batch WAL sink for SQLite.
- \`FastIngest.MongoDb\`: Unordered BulkWrite sink for MongoDB.
- \`FastIngest.CosmosDb\`: Bulk executor sink for Azure Cosmos DB.
- \`FastIngest.Elasticsearch\`: Bulk API sink for Elasticsearch.
- \`FastIngest.Extensions.DependencyInjection\`: Dependency injection extensions, IFastIngestEngine, and profile registry for ASP.NET Core.

## Optional & Deep Dive

- [Full Consolidated Context](${baseUrl}/llms-full.txt): Complete documentation concatenated into a single plain-text Markdown file for LLM context windows and RAG systems.
- [Sitemap](${baseUrl}/sitemap.xml): Complete XML sitemap of all documentation pages.
`

  // --- Generate llms-full.txt ---
  let llmsFullTxt = `# FastIngest — Full Documentation Context

> FastIngest is a zero-allocation, high-throughput, constant-memory (O(1)) bulk ingestion pipeline for .NET 9+.
> This document consolidates all guides, database sinks, and architectural benchmarks into a single machine-readable document for LLM context windows, AI coding assistants, and semantic retrieval systems.

- Website: ${baseUrl}
- Index: ${baseUrl}/llms.txt
- GitHub: https://github.com/aebibtech/fastingest
- NuGet: https://www.nuget.org/packages/FastIngest.Core

---

# Table of Contents
`

  for (const page of pages) {
    llmsFullTxt += `- [${page.section} > ${page.title}](${baseUrl}${page.url})\n`
  }

  llmsFullTxt += `\n---\n\n`

  for (const page of pages) {
    const filePath = path.resolve(docsDir, page.file)
    if (!fs.existsSync(filePath)) {
      console.warn(`[generate-llms] Warning: File not found: ${filePath}`)
      continue
    }

    const raw = fs.readFileSync(filePath, 'utf-8')
    const cleaned = cleanMarkdown(raw, page.url, baseUrl)

    llmsFullTxt += `# ${page.section}: ${page.title}\n`
    llmsFullTxt += `Source: ${baseUrl}${page.url}\n\n`
    llmsFullTxt += cleaned
    llmsFullTxt += `\n\n---\n\n`
  }

  // Write files to all output directories
  for (const dir of outDirs) {
    fs.writeFileSync(path.resolve(dir, 'llms.txt'), llmsTxt, 'utf-8')
    fs.writeFileSync(path.resolve(dir, 'llms-full.txt'), llmsFullTxt, 'utf-8')
    console.log(`[generate-llms] Successfully wrote llms.txt and llms-full.txt to ${dir}`)
  }

  return { llmsTxt, llmsFullTxt }
}

// Execute directly if run as a script
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const distDir = path.resolve(docsDir, '.vitepress', 'dist')
  const outDirs = [publicDir]
  if (fs.existsSync(distDir)) {
    outDirs.push(distDir)
  }

  generateLlms({ outDirs })
}
