import { defineConfig } from 'vitepress'
import { generateLlms } from '../scripts/generate-llms.mjs'

// Target platform detection:
// - Cloudflare Pages sets CF_PAGES=1 during build, or DOCS_ENV can be set to 'cloudflare'.
// - GitHub Pages uses GITHUB_PAGES=true, DOCS_ENV='github', or GITHUB_ACTIONS (when not targeting Cloudflare).
// - Local dev / preview defaults to root base ('/').
const isCloudflare = Boolean(process.env.CF_PAGES) || process.env.DOCS_ENV === 'cloudflare'
const isGitHubPages = !isCloudflare && (Boolean(process.env.GITHUB_PAGES) || process.env.DOCS_ENV === 'github' || Boolean(process.env.GITHUB_ACTIONS))

const base = process.env.DOCS_BASE || (isGitHubPages ? '/fastingest/' : '/')
const hostname = process.env.DOCS_HOSTNAME || (isGitHubPages ? 'https://aebibtech.github.io/fastingest' : 'https://fastingest.aebibtech.com')

export default defineConfig({
  title: 'FastIngest',
  description: 'High-throughput, constant-memory bulk ingestion pipeline for .NET',
  base,
  cleanUrls: true,
  lastUpdated: true,
  sitemap: {
    hostname
  },

  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: `${base.replace(/\/$/, '')}/logo.svg` }],
    ['meta', { name: 'theme-color', content: '#3eaf7c' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:locale', content: 'en' }],
    ['meta', { property: 'og:title', content: 'FastIngest | High-Throughput Bulk Ingestion for .NET' }],
    ['meta', { property: 'og:site_name', content: 'FastIngest' }],
    ['meta', { property: 'og:description', content: 'Constant-memory bulk ingestion pipeline for .NET with support for PostgreSQL, SQL Server, MySQL, SQLite, MongoDB, Cosmos DB, and Elasticsearch.' }],
    // LLM Discoverability
    ['link', { rel: 'alternate', type: 'text/markdown', href: `${base.replace(/\/$/, '')}/llms.txt`, title: 'LLM Documentation (llms.txt)' }],
    ['link', { rel: 'alternate', type: 'text/markdown', href: `${base.replace(/\/$/, '')}/llms-full.txt`, title: 'Full LLM Documentation (llms-full.txt)' }],
    [
      'script',
      { type: 'application/ld+json' },
      JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareSourceCode',
        name: 'FastIngest',
        description: 'Zero-allocation, constant-memory bulk ingestion pipeline for .NET streaming CSV, Excel, and JSON Lines directly into PostgreSQL, SQL Server, MySQL, SQLite, MongoDB, Cosmos DB, and Elasticsearch.',
        programmingLanguage: {
          '@type': 'ComputerLanguage',
          name: 'C#',
          version: '12.0 / .NET 9'
        },
        codeRepository: 'https://github.com/aebibtech/fastingest',
        documentation: 'https://fastingest.aebibtech.com',
        license: 'https://opensource.org/licenses/MIT',
        author: {
          '@type': 'Person',
          name: 'Paul Camano',
          url: 'https://github.com/aebibtech'
        }
      })
    ]
  ],

  themeConfig: {
    siteTitle: 'FastIngest',
    logo: '/logo.svg',

    search: {
      provider: 'local'
    },

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Sinks', link: '/sinks/postgresql' },
      { text: 'Benchmarks', link: '/benchmarks/performance' },
      {
        text: 'LLM Docs',
        items: [
          { text: 'llms.txt (Index)', link: '/llms.txt', target: '_blank' },
          { text: 'llms-full.txt (Full Context)', link: '/llms-full.txt', target: '_blank' }
        ]
      },
      { text: 'NuGet', link: 'https://www.nuget.org/packages/FastIngest.Core' }
    ],

    sidebar: [
      {
        text: 'Getting Started',
        collapsed: false,
        items: [
          { text: 'Introduction', link: '/guide/introduction' },
          { text: 'Quickstart', link: '/guide/getting-started' },
          { text: 'NDJSON / JSON Lines', link: '/guide/json-lines' },
          { text: 'Dependency Injection', link: '/guide/dependency-injection' },
          { text: 'Validation & Errors', link: '/guide/validation' }
        ]
      },
      {
        text: 'Database Sinks',
        collapsed: false,
        items: [
          { text: 'PostgreSQL (Binary COPY)', link: '/sinks/postgresql' },
          { text: 'SQL Server (SqlBulkCopy)', link: '/sinks/sql-server' },
          { text: 'MySQL / MariaDB', link: '/sinks/mysql' },
          { text: 'SQLite (WAL Batch)', link: '/sinks/sqlite' },
          { text: 'MongoDB (BulkWrite)', link: '/sinks/mongodb' },
          { text: 'Azure Cosmos DB', link: '/sinks/cosmosdb' },
          { text: 'Elasticsearch', link: '/sinks/elasticsearch' }
        ]
      },
      {
        text: 'Architecture & Benchmarks',
        collapsed: false,
        items: [
          { text: 'Memory Model', link: '/benchmarks/memory-model' },
          { text: 'Performance Benchmarks', link: '/benchmarks/performance' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/aebibtech/fastingest' }
    ],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2026 Paul Abib Camano (Aebibtech)'
    },

    docFooter: {
      prev: 'Previous Page',
      next: 'Next Page'
    }
  },

  markdown: {
    lineNumbers: true,
    theme: {
      light: 'github-light',
      dark: 'github-dark'
    }
  },

  buildEnd: async (siteConfig) => {
    generateLlms({
      baseUrl: hostname,
      outDirs: [siteConfig.outDir]
    })
  }
})
