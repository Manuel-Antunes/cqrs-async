# Documentation site

The [Docusaurus](https://docusaurus.io) site of the library: a landing page
(`src/pages/index.js`) and the documentation (`content/`, Markdown).

```text
content/docs/    introduction, architecture, patterns and the reference
content/guide/   the guide: a blog, step by step
sidebars.js      the two sidebars (Guide, Docs)
static/img/      the crab logo, drawn by scripts/logo.py
benchmarks/      raw benchmark logs of the migration (not part of the site)
```

```text
npm install
npm start          # live preview on http://localhost:3000
npm run build      # static site in build/ (fails on broken links)
```

Plain `.md` files are Markdown; use `.mdx` when a page needs React components.
`.github/workflows/pages.yml` publishes the site to GitHub Pages on every push
to `main` that touches `docs/` (once: Settings, Pages, Source: GitHub Actions).
