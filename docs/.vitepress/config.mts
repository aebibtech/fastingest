import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'FastIngest',
  description: 'High-throughput, constant-memory bulk ingestion pipeline for .NET',
  base: process.env.GITHUB_ACTIONS ? '/fastingest/' : '/',
  cleanUrls: true,
  lastUpdated: true,

  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/logo.svg' }],
    ['meta', { name: 'theme-color', content: '#3eaf7c' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:locale', content: 'en' }],
    ['meta', { property: 'og:title', content: 'FastIngest | High-Throughput Bulk Ingestion for .NET' }],
    ['meta', { property: 'og:site_name', content: 'FastIngest' }],
    ['meta', { property: 'og:description', content: 'Constant-memory bulk ingestion pipeline for .NET with support for PostgreSQL, SQL Server, MySQL, SQLite, MongoDB, Cosmos DB, and Elasticsearch.' }]
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
      { text: 'NuGet', link: 'https://www.nuget.org/packages/FastIngest.Core' }
    ],

    sidebar: [
      {
        text: 'Getting Started',
        collapsed: false,
        items: [
          { text: 'Introduction', link: '/guide/introduction' },
          { text: 'Quickstart', link: '/guide/getting-started' },
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
  }
})
