import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { pathToFileURL } from 'url';

function projectRootFromThisFile() {
    return path.dirname(new URL(import.meta.url).pathname);
}

test('extracts article links from HTML within <article>', async () => {
    const root = projectRootFromThisFile();
    const trackerUrl = pathToFileURL(path.join(root, 'rss.js')).href;
    const tracker = await import(trackerUrl);

    const pageUrl = 'https://ceoweekly.com/business/';
    const html = `
      <html><body>
        <article>
          <h2><a href="https://ceoweekly.com/pete-rose-web-a-thon/">A</a></h2>
          <a href="/pete-rose-web-a-thon/?utm_source=x#frag">Dup</a>
        </article>
        <article>
          <a href="/business/">Category</a>
          <a href="/business/page/2/">NextIndex</a>
        </article>
        <a href="https://twitter.com/some">Ignore external</a>
      </body></html>
    `;

    const links = tracker.extractArticleLinksFromHtml(html, pageUrl, {
        sameHostOnly: true,
        keepQueryString: false,
        excludedExtensions: ['.png', '.jpg', '.pdf'],
        includePathRegex: ['^/[^/]+/?$'],
        excludePathRegex: ['^/business/?$', '^/business/page/\\d+/?$']
    });

    assert.deepEqual(links, ['https://ceoweekly.com/pete-rose-web-a-thon']);
});

test('detects next page URL from rel=next', async () => {
    const root = projectRootFromThisFile();
    const trackerUrl = pathToFileURL(path.join(root, 'rss.js')).href;
    const tracker = await import(trackerUrl);

    const pageUrl = 'https://ceoweekly.com/business/';
    const html = `<html><head><link rel="next" href="/business/page/2/" /></head><body></body></html>`;
    const next = tracker.extractNextPageUrlFromHtml(html, pageUrl);
    assert.equal(next, 'https://ceoweekly.com/business/page/2');
});

test('extracts published date from Elementor post date', async () => {
    const root = projectRootFromThisFile();
    const trackerUrl = pathToFileURL(path.join(root, 'rss.js')).href;
    const tracker = await import(trackerUrl);

    const pageUrl = 'https://ceoweekly.com/business/';
    const html = `
      <html><body>
        <article class="elementor-post">
          <h3 class="elementor-post__title">
            <a href="https://ceoweekly.com/paul-davis-restoration-of-charleston-announces-full-service-people-first-approach-to-property-recovery/">
              Title
            </a>
          </h3>
          <div class="elementor-post__meta-data">
            <span class="elementor-post-date">January 9, 2026</span>
          </div>
        </article>
      </body></html>
    `;

    const entries = tracker.extractArticleEntriesFromHtml(html, pageUrl, {
        sameHostOnly: true,
        keepQueryString: false,
        excludedExtensions: ['.png', '.jpg', '.pdf'],
        includePathRegex: ['^/[^/]+/?$'],
        excludePathRegex: ['^/business/?$', '^/business/page/\\d+/?$']
    });

    assert.equal(entries.length, 1);
    assert.equal(
        entries[0].url,
        'https://ceoweekly.com/paul-davis-restoration-of-charleston-announces-full-service-people-first-approach-to-property-recovery'
    );
    assert.ok(entries[0].publishedAt);
    assert.equal(entries[0].publishedAt.slice(0, 10), '2026-01-09');
});

test('writes queue file into "to scrape" without touching existing results', async () => {
    const root = projectRootFromThisFile();
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ns-page-tracker-'));
    const prevCwd = process.cwd();
    process.chdir(tmp);

    try {
        const trackerUrl = pathToFileURL(path.join(root, 'rss.js')).href;
        await import(trackerUrl);

        const toScrapeDir = path.join(tmp, 'to scrape');
        const rssDir = path.join(tmp, 'rss');
        assert.equal(fs.existsSync(toScrapeDir), false);
        assert.equal(fs.existsSync(rssDir), false);

        const filePath = path.join(tmp, 'to scrape', `page_test_${Date.now()}.txt`);
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        fs.writeFileSync(filePath, 'https://example.com/a\n');

        assert.equal(fs.existsSync(path.join(tmp, 'result')), false);
        assert.equal(fs.existsSync(toScrapeDir), true);
        assert.equal(fs.existsSync(rssDir), false);
    } finally {
        process.chdir(prevCwd);
    }
});
