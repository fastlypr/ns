# Page Tracker (HTML / RSS) — Standalone URL Discovery

This adds a standalone tracker that discovers article URLs from tracked pages (or RSS/Atom feeds) and writes new URLs into `to scrape/` so your existing workflow can process them with your main scraper.

## What It Does

- Fetches each configured tracking page (starting with `https://ceoweekly.com/business/`)
- Extracts article URLs from the page content
- Follows pagination (when detected) up to `maxPages`
- Deduplicates URLs using:
  - Your existing database (`history.db`) via the existing schema (`scraped_urls`)
  - A local state file (`.page_tracker_state.json`) to avoid re-queuing the same URLs repeatedly
- Writes newly discovered URLs into `to scrape/page_<name>_<date>_<timestamp>.txt`

## Install / Run

From the project directory:

```bash
node rss.js
```

Use a custom config file:

```bash
node rss.js --config /home/ubuntu/NS/page_tracker.config.json
```

Watch mode (runs every 24 hours):

```bash
node rss.js --watch
```

Custom watch interval:

```bash
node rss.js --watch --interval-ms 3600000
```

## Configure Multiple Tracking Pages

Edit `page_tracker.config.json` and add entries under `trackedPages`.

### HTML tracking page example

```json
{
  "name": "ceoweekly_business",
  "type": "html",
  "startUrl": "https://ceoweekly.com/business/",
  "maxPages": 20,
  "includePathRegex": ["^/[^/]+/?$", "^/business/[^/]+/?$"],
  "excludePathRegex": ["^/business/?$", "^/business/page/\\d+/?$"]
}
```

### RSS/Atom example

```json
{
  "name": "example_feed",
  "type": "rss",
  "startUrl": "https://example.com/feed/"
}
```

## How It Integrates With Your Current Workflow

- It does not modify or run your existing scrapers.
- It only queues URLs into `to scrape/`.
- You continue scraping with your existing flow (e.g., your main scraper option that processes files from `to scrape/`).

## Run Tests

Node 20+ has a built-in test runner:

```bash
node --test page_tracker.integration.test.js
```
