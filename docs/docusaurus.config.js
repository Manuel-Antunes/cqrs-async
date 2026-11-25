// @ts-check
const { execSync } = require('node:child_process');
const { themes: prismThemes } = require('prism-react-renderer');

/** The GitHub repository: REPO_URL, or the `origin` remote. */
function repoUrl() {
  if (process.env.REPO_URL) return process.env.REPO_URL;
  try {
    const remote = execSync('git remote get-url origin', { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString()
      .trim();
    return remote.replace(/^git@github\.com:/, 'https://github.com/').replace(/\.git$/, '');
  } catch {
    return '';
  }
}
const repo = repoUrl();

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'cqrs-async',
  tagline: 'CQRS and event sourcing for Rust, resolved at compile time.',
  favicon: 'img/favicon.svg',

  // GitHub Pages serves a project site from https://<user>.github.io/<repo>/;
  // the deploy workflow sets both.
  url: process.env.SITE_URL || 'http://localhost:3000',
  baseUrl: process.env.BASE_URL || '/',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'warn',

  markdown: {
    // The docs are plain Markdown (`Appender<E>` in prose is not JSX):
    // only .mdx files are parsed as MDX.
    format: 'detect',
    hooks: { onBrokenMarkdownLinks: 'throw' },
  },

  i18n: { defaultLocale: 'en', locales: ['en'] },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: 'content',
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
          editUrl: repo ? `${repo}/edit/main/docs/` : undefined,
        },
        blog: false,
        theme: { customCss: './src/css/custom.css' },
      }),
    ],
  ],

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        docsRouteBasePath: '/',
        docsDir: 'content',
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: { defaultMode: 'dark', respectPrefersColorScheme: true },
      navbar: {
        title: 'cqrs-async',
        logo: { alt: 'cqrs_async', src: 'img/logo.svg' },
        items: [
          { type: 'docSidebar', sidebarId: 'guide', position: 'left', label: 'Guide' },
          { type: 'docSidebar', sidebarId: 'docs', position: 'left', label: 'Docs' },
          { to: '/docs/performance', position: 'left', label: 'Performance' },
          ...(repo ? [{ href: repo, label: 'GitHub', position: 'right' }] : []),
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Learn',
            items: [
              { label: 'Build a blog', to: '/guide' },
              { label: 'Architecture', to: '/docs/architecture' },
              { label: 'Patterns', to: '/docs/patterns' },
            ],
          },
          {
            title: 'Reference',
            items: [
              { label: 'Unit of work', to: '/docs/unit-of-work' },
              { label: 'Event processing', to: '/docs/event-processing' },
              { label: 'Storage', to: '/docs/storage' },
              { label: 'Performance', to: '/docs/performance' },
            ],
          },
          ...(repo ? [{ title: 'More', items: [{ label: 'GitHub', href: repo }] }] : []),
        ],
        copyright: 'cqrs-async contributors. MIT license. Rust 1.89+ (1.94 with PostgreSQL), edition 2024.',
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['rust', 'toml', 'bash', 'sql', 'json'],
      },
    }),
};

module.exports = config;
