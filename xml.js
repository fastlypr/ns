import { gotScraping } from 'got-scraping';
import fs from 'fs';
import path from 'path';
import readlineSync from 'readline-sync';
import zlib from 'zlib';
import http from 'http';
import https from 'https';
import { promisify } from 'util';
import { normalizeUrl, getAllScrapedUrls, getAllQueuedUrls, enqueueUrls, makeBatchId, saveDiscoveredSitemap, getAllSitemaps, updateSitemapScanDate } from './db.js';

const gunzip = promisify(zlib.gunzip);

// Configuration
// Per-request timeout. Dead sitemaps blocked the crawl for ~90s (30s × 2 retries);
// 12s + 1 retry caps the wait at ~24s and rarely kills healthy fetches.
const TIMEOUT = 12000;
// Fetch fan-out. Sitemap fetching is pure I/O so high concurrency is safe;
// the real limit is the remote server, not our CPU.
const CONCURRENCY = 10;            // child-sitemap fan-out inside one tree
const ROOT_SITEMAP_CONCURRENCY = 5; // root sitemap parallelism across a bulk/rescan run

// Shared keep-alive agents. Reusing TCP+TLS connections across the many
// fetches to the same host (every child sitemap, plus their children)
// eliminates the handshake round-trip per request — typically the biggest
// single win on a deep tree.
const httpAgent  = new http.Agent({  keepAlive: true, maxSockets: 50 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 50, rejectUnauthorized: false });

// Matches every <loc>…</loc> entry, tolerating attributes, CDATA, and whitespace.
// Sitemap XML is a flat list of <loc> tags; a full HTML/XML parser would just
// be a much more expensive way to do the same string extraction.
const LOC_RE = /<loc\b[^>]*>([\s\S]*?)<\/loc>/gi;
const CDATA_RE = /<!\[CDATA\[([\s\S]*?)\]\]>/;

function extractLocs(xml) {
    const out = [];
    let m;
    LOC_RE.lastIndex = 0;
    while ((m = LOC_RE.exec(xml)) !== null) {
        let val = m[1];
        const cdata = CDATA_RE.exec(val);
        if (cdata) val = cdata[1];
        val = val.trim();
        if (val) out.push(val);
    }
    return out;
}

// Extensions to ignore (images, assets)
const IGNORED_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.pdf', '.css', '.js', '.json'];
const NON_ARTICLE_SEGMENTS = new Set([
    'about',
    'account',
    'author',
    'authors',
    'category',
    'categories',
    'contact',
    'feed',
    'home',
    'index',
    'login',
    'page',
    'privacy',
    'privacy-policy',
    'search',
    'signup',
    'tag',
    'tags',
    'terms',
    'topic',
    'topics',
    'wp-json'
]);

function scoreSitemapCandidateUrl(urlStr) {
    try {
        const parsed = new URL(urlStr);
        const lowerHost = parsed.hostname.toLowerCase();
        const lowerPath = parsed.pathname.toLowerCase();
        const segments = lowerPath.split('/').filter(Boolean);

        if (IGNORED_EXTENSIONS.some(ext => lowerPath.endsWith(ext))) return -10;
        if (lowerHost.startsWith('miro.') || lowerHost.startsWith('cdn.') || lowerHost.startsWith('images.')) return -10;
        if (segments.length === 0) return -10;
        if (segments.some(segment => NON_ARTICLE_SEGMENTS.has(segment))) return -6;

        const lastSegment = segments[segments.length - 1] || '';
        let score = 0;

        if (segments.length >= 2) score += 2;
        if (lastSegment.includes('-')) score += 2;
        if (lastSegment.length >= 16) score += 1;
        if (/\d{4}/.test(lowerPath)) score += 1;
        if (/[a-f0-9]{8,}/.test(lastSegment)) score += 1;

        return score;
    } catch {
        return -10;
    }
}

function isLikelyArticleUrl(urlStr) {
    return scoreSitemapCandidateUrl(urlStr) >= 2;
}

/**
 * Fetches a sitemap URL and returns its body as a UTF-8 string (handling gzip
 * transparently for .gz payloads). Uses a shared keep-alive agent so repeated
 * fetches to the same host don't pay a fresh TLS handshake each time.
 */
async function fetchContent(url) {
    const isGzip = url.endsWith('.gz');
    try {
        // For gzipped sitemaps we need the raw buffer; for everything else we
        // let got-scraping return a decoded string (cheaper — no Buffer copy
        // and no manual UTF-8 conversion on our side).
        const response = await gotScraping({
            url,
            timeout: { request: TIMEOUT },
            retry: { limit: 1 },
            responseType: isGzip ? 'buffer' : 'text',
            http2: false,                          // avoids "SETTINGS frame" failures on some hosts
            agent: { http: httpAgent, https: httpsAgent },
            https: { rejectUnauthorized: false },
            headerGeneratorOptions: { devices: ['desktop'], locales: ['en-US'] }
        });

        if (!response.body) return null;

        if (isGzip) {
            const buf = response.body;
            const hasMagic = buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b;
            if (!hasMagic) return buf.toString('utf-8'); // already decompressed by got
            try {
                return (await gunzip(buf)).toString('utf-8');
            } catch (err) {
                console.error(`❌ Decompression failed for ${url}: ${err.message}`);
                return null;
            }
        }

        return response.body;
    } catch (error) {
        console.error(`❌ Error fetching ${url}: ${error.message}`);
        return null;
    }
}

/**
 * Classify a <loc> entry. Kept as a tight function so the hot loop in
 * `processSitemapContent` stays branch-predictable.
 */
function classifyLoc(link) {
    const lower = link.toLowerCase();
    if (IGNORED_EXTENSIONS.some(ext => lower.endsWith(ext))) return 'skip';
    // wp-content paths are almost always assets; keep them only if they
    // look like a sitemap (some plugins place sitemaps there).
    if (lower.includes('wp-content') && !lower.includes('sitemap')) return 'skip';
    if (lower.includes('sitemap') || lower.endsWith('.xml') || lower.endsWith('.xml.gz')) return 'sitemap';
    return 'article';
}

/**
 * Parse one sitemap payload, fold its <loc> entries into the visited/articleUrls
 * sets, and return any newly-found child sitemap URLs. The caller is
 * responsible for scheduling those children — which lets the worker pool keep
 * every slot saturated instead of waiting on the slowest sibling in a chunk.
 */
function processSitemapContent(url, content, articleUrls, saveChildSitemap) {
    const foundUrls = extractLocs(content);
    const children = [];
    for (const link of foundUrls) {
        const kind = classifyLoc(link);
        if (kind === 'skip') continue;
        if (kind === 'sitemap') {
            children.push(link);
            saveChildSitemap(link, url);
        } else {
            articleUrls.add(link);
        }
    }
    return { foundCount: foundUrls.length, children };
}

/**
 * Flat worker-pool crawler. Replaces the old recursive chunked version that
 * had a head-of-line-blocking bug (each chunk waited on its slowest fetch
 * before any worker could start on the next chunk).
 *
 * - `pending` is the global queue of sitemap URLs yet to fetch.
 * - N workers pull atomically from `pending`; when a fetch yields child
 *   sitemaps, they're pushed back onto `pending` and picked up immediately
 *   by whichever worker becomes free next.
 * - Child-sitemap DB writes are batched into a single transaction per
 *   parent page to minimize SQLite fsync overhead.
 */
async function crawlSitemapTree(rootUrls, { visited, articleUrls, concurrency = CONCURRENCY } = {}) {
    const pending = [];
    for (const u of rootUrls) {
        if (u && !visited.has(u)) {
            visited.add(u);
            pending.push(u);
        }
    }
    if (pending.length === 0) return;

    const fetchAndProcess = async (url) => {
        const content = await fetchContent(url);
        if (!content) return;

        // Batch every child-sitemap discovery from this payload into one
        // transaction. Individual INSERT OR IGNOREs fsync per row otherwise.
        const childBuffer = [];
        const { foundCount, children } = processSitemapContent(
            url,
            content,
            articleUrls,
            (child, parent) => childBuffer.push([child, parent])
        );

        if (childBuffer.length > 0) {
            // Fire and forget is fine here — write is synchronous better-sqlite3.
            try {
                for (const [child, parent] of childBuffer) saveDiscoveredSitemap(child, parent);
            } catch { /* per-row ignores duplicates; swallow cumulative errors */ }
        }

        if (foundCount > 0) {
            const articleCount = foundCount - children.length;
            if (children.length > 0) {
                console.log(`📂 ${url} → ${children.length} child sitemap(s), ${articleCount} article(s)`);
            } else {
                console.log(`📄 ${url} → ${articleCount} article(s)`);
            }
        }

        // Enqueue newly-discovered sitemaps (dedupe via `visited`).
        for (const child of children) {
            if (!visited.has(child)) {
                visited.add(child);
                pending.push(child);
            }
        }
    };

    // Active-worker counter so workers don't exit prematurely. The queue
    // grows as sitemaps discover children — a naive `while (pending.length)`
    // would see the queue briefly drained (while N in-flight fetches are
    // still hunting for children) and cause all workers to quit before the
    // tree is fully traversed. A worker only exits when the queue is empty
    // AND no siblings are still fetching (i.e. nobody can add more work).
    let active = 0;
    const worker = async () => {
        while (true) {
            if (pending.length === 0) {
                if (active === 0) return;
                // Yield to the event loop so in-flight fetches can push new
                // children before we re-check.
                await new Promise(r => setImmediate(r));
                continue;
            }
            const next = pending.shift();
            if (!next) continue;
            active++;
            try { await fetchAndProcess(next); }
            catch (err) { console.error(`❌ Worker error on ${next}: ${err.message}`); }
            finally { active--; }
        }
    };

    // Always spawn the full worker count — workers idle-wait when the
    // queue is temporarily empty, so initial queue size doesn't cap fan-out.
    await Promise.all(Array.from({ length: concurrency }, worker));
}

/**
 * Back-compat wrapper for the old recursive API. Callers still pass a single
 * URL + shared `visited`/`articleUrls` sets; we just fan the single URL out
 * through the new worker pool.
 */
async function scrapeSitemapRecursive(url, visited = new Set(), articleUrls = new Set()) {
    await crawlSitemapTree([url], { visited, articleUrls });
}

/**
 * Loads the set of already scraped URLs from the database.
 * URLs stored in `scraped_urls` are already normalized by db.js, so we skip the
 * re-normalize pass (previously ~100ms per 100k rows for no functional gain).
 */
function loadProcessedUrls() {
    return new Set(getAllScrapedUrls());
}

function loadQueuedUrls() {
    // Source of truth: DB queue (already-normalized URLs). Legacy
    // `to scrape/*.txt` files are also folded in for pre-migration deployments.
    const knownQueuedUrls = new Set(getAllQueuedUrls());
    const toScrapeDir = path.join(process.cwd(), 'to scrape');

    if (!fs.existsSync(toScrapeDir)) {
        return knownQueuedUrls;
    }

    try {
        const files = fs.readdirSync(toScrapeDir).filter(file => file.endsWith('.txt') || file.endsWith('.csv'));
        for (const file of files) {
            const filePath = path.join(toScrapeDir, file);
            const content = fs.readFileSync(filePath, 'utf8');
            const matches = content.match(/https?:\/\/[^\s"'<>`]+/g) || [];

            matches.forEach(match => {
                const cleaned = String(match).trim().replace(/[)\].,;:!?]+$/g, '');
                if (cleaned) {
                    knownQueuedUrls.add(normalizeUrl(cleaned));
                }
            });
        }
    } catch (error) {
        console.error(`⚠️  Failed to load queued URLs from to scrape: ${error.message}`);
    }

    return knownQueuedUrls;
}

/**
 * Rescans all saved sitemaps for new URLs.
 */
export async function rescanSavedSitemaps(targetDomain = null, options = {}) {
    const { onProgress } = options;
    const emitProgress = async (payload) => {
        if (typeof onProgress === 'function') {
            await onProgress(payload);
        }
    };
    const startedAt = Date.now();

    const sitemaps = getAllSitemaps();
    const summary = {
        target: targetDomain && String(targetDomain).toLowerCase() !== 'all' ? targetDomain : 'all',
        totalRootSitemaps: 0,
        processedRootSitemaps: 0,
        skippedRootSitemaps: 0,
        uniqueSitemapsVisited: 0,
        newUrlsFound: 0,
        newUrlsSaved: 0,
        filePath: null,
        elapsedMs: 0
    };

    if (sitemaps.length === 0) {
        console.log("⚠️  No saved sitemaps found in database.");
        summary.elapsedMs = Date.now() - startedAt;
        await emitProgress({ stage: 'complete', summary: { ...summary } });
        return summary;
    }

    // Extract unique domains
    const domains = new Set();
    sitemaps.forEach(url => {
        try {
            const hostname = new URL(url).hostname;
            domains.add(hostname);
        } catch (e) {}
    });
    
    let sitemapsToScan = [];
    
    if (targetDomain) {
        if (targetDomain.toLowerCase() === 'all') {
            sitemapsToScan = sitemaps;
            console.log(`\n🔄 Rescanning ALL ${sitemaps.length} saved sitemaps (via argument)...`);
        } else {
            sitemapsToScan = sitemaps.filter(url => url.includes(targetDomain));
            console.log(`\n🔄 Rescanning sitemaps for domain: ${targetDomain} (${sitemapsToScan.length} found) (via argument)...`);
        }
    } else {
        const domainList = Array.from(domains).sort();
        
        console.log('\n👉 Select Domain to Rescan:');
        console.log('0. All Domains');
        domainList.forEach((domain, index) => {
            console.log(`${index + 1}. ${domain}`);
        });
        
        let choiceIndex = -1;
        try {
            const input = readlineSync.question(`\nChoice (0-${domainList.length}): `);
            choiceIndex = parseInt(input, 10);
        } catch(e) {
            choiceIndex = -1;
        }

        if (choiceIndex === 0) {
            sitemapsToScan = sitemaps;
            console.log(`\n🔄 Rescanning ALL ${sitemaps.length} saved sitemaps...`);
        } else if (choiceIndex > 0 && choiceIndex <= domainList.length) {
            const selectedDomain = domainList[choiceIndex - 1];
            summary.target = selectedDomain;
            sitemapsToScan = sitemaps.filter(url => url.includes(selectedDomain));
            console.log(`\n🔄 Rescanning sitemaps for domain: ${selectedDomain} (${sitemapsToScan.length} found)...`);
        } else {
            console.log("❌ Invalid choice or cancelled.");
            summary.elapsedMs = Date.now() - startedAt;
            await emitProgress({ stage: 'complete', summary: { ...summary } });
            return summary;
        }
    }

    summary.totalRootSitemaps = sitemapsToScan.length;

    if (sitemapsToScan.length === 0) {
        console.log("⚠️  No matching saved sitemaps found.");
        summary.elapsedMs = Date.now() - startedAt;
        await emitProgress({ stage: 'complete', summary: { ...summary } });
        return summary;
    }

    summary.elapsedMs = Date.now() - startedAt;
    await emitProgress({ stage: 'start', summary: { ...summary } });
    
    const allNewUrls = new Set();
    const visited = new Set();
    const processedUrls = loadProcessedUrls();
    const queuedUrls = loadQueuedUrls();

    const processRootSitemap = async (sitemapUrl) => {
        if (visited.has(sitemapUrl)) {
            summary.skippedRootSitemaps++;
            summary.elapsedMs = Date.now() - startedAt;
            await emitProgress({
                stage: 'progress',
                currentSitemapUrl: sitemapUrl,
                summary: { ...summary, uniqueSitemapsVisited: visited.size, newUrlsFound: allNewUrls.size }
            });
            return;
        }

        console.log(`\n🔎 Checking: ${sitemapUrl}`);
        const articleUrls = new Set();

        summary.elapsedMs = Date.now() - startedAt;
        await emitProgress({
            stage: 'scanning',
            currentSitemapUrl: sitemapUrl,
            summary: { ...summary, uniqueSitemapsVisited: visited.size, newUrlsFound: allNewUrls.size }
        });
        
        // Pass shared 'visited' set to prevent infinite loops and redundant scrapes
        await scrapeSitemapRecursive(sitemapUrl, visited, articleUrls);
        updateSitemapScanDate(sitemapUrl);
        summary.processedRootSitemaps++;
        summary.uniqueSitemapsVisited = visited.size;
        
        // Filter new ones
        for (const url of articleUrls) {
            const normalizedUrl = normalizeUrl(url);
            if (!normalizedUrl || !isLikelyArticleUrl(normalizedUrl)) {
                continue;
            }

            if (!processedUrls.has(normalizedUrl) && !queuedUrls.has(normalizedUrl) && !allNewUrls.has(normalizedUrl)) {
                allNewUrls.add(normalizedUrl);
            }
        }

        summary.newUrlsFound = allNewUrls.size;
        summary.elapsedMs = Date.now() - startedAt;
        await emitProgress({
            stage: 'progress',
            currentSitemapUrl: sitemapUrl,
            summary: { ...summary }
        });
    };

    // Root-level worker pool. Each root sitemap spawns its own internal
    // `crawlSitemapTree` pool too, but we cap how many root trees run in
    // parallel to avoid overwhelming a single server with hundreds of
    // simultaneous sockets.
    {
        let rootIdx = 0;
        const rootWorker = async () => {
            while (rootIdx < sitemapsToScan.length) {
                const i = rootIdx++;
                try { await processRootSitemap(sitemapsToScan[i]); }
                catch (err) { console.error(`Root sitemap error: ${err.message}`); }
            }
        };
        const n = Math.min(ROOT_SITEMAP_CONCURRENCY, sitemapsToScan.length);
        await Promise.all(Array.from({ length: n }, rootWorker));
    }

    if (allNewUrls.size > 0) {
        console.log(`\n🎉 Found ${allNewUrls.size} NEW URLs across all sitemaps!`);

        const newUrlsToSave = Array.from(allNewUrls);
        const source = `xml_rescan:${summary.target}`;
        const batchId = makeBatchId('xml_rescan', summary.target || 'all');
        const { inserted } = enqueueUrls(newUrlsToSave, { source, batchId });
        console.log(`\n💾 Enqueued ${inserted} NEW URLs to DB queue (batch ${batchId})`);
        summary.newUrlsSaved = inserted;
        summary.batchId = batchId;
    } else {
        console.log(`\n✅ No new URLs found in any sitemap.`);
    }

    summary.elapsedMs = Date.now() - startedAt;
    await emitProgress({ stage: 'complete', summary: { ...summary } });
    return summary;
}

/**
 * Scrapes a sitemap URL (recursive).
 * This is an exported function for external use (e.g., Telegram bot).
 * @param {string} sitemapUrl - The sitemap URL to start scraping from.
 * @returns {Promise<object>} - Object containing stats and new URLs saved.
 */
export async function runSitemapScraper(sitemapUrl) {
    if (!sitemapUrl) {
        console.log("No URL provided for sitemap scrape.");
        return { totalSitemapsVisited: 0, totalUrlsFound: 0, newUrlsSaved: 0, filePath: null };
    }

    // Save the root sitemap too
    saveDiscoveredSitemap(sitemapUrl, 'manual_entry');

    const articleUrls = new Set();
    const visited = new Set();

    console.log(`\n🚀 Starting scrape of ${sitemapUrl}...`);
    await scrapeSitemapRecursive(sitemapUrl, visited, articleUrls);

    console.log(`\n✅ Scrape Complete!`);
    console.log(`   Total Sitemaps Visited: ${visited.size}`);
    console.log(`   Total URLs Found: ${articleUrls.size}`);

    let newUrlsSaved = 0;
    let filePath = null;
    const processedUrls = loadProcessedUrls();
    const queuedUrls = loadQueuedUrls();

    if (articleUrls.size > 0) {
        // Set-based dedupe: `.includes()` on an Array is O(n) so the old
        // filter was O(n²) and choked on sitemaps with 20k+ URLs.
        const newUrlSet = new Set();
        for (const url of articleUrls) {
            const normalizedUrl = normalizeUrl(url);
            if (!normalizedUrl || !isLikelyArticleUrl(normalizedUrl)) continue;
            if (processedUrls.has(normalizedUrl) || queuedUrls.has(normalizedUrl)) continue;
            newUrlSet.add(normalizedUrl);
        }
        const newUrls = Array.from(newUrlSet);

        console.log(`   ----------------------------------------`);
        console.log(`   Duplicate (Already Scraped): ${articleUrls.size - newUrls.length}`);
        console.log(`   New URLs to Process: ${newUrls.length}`);
        console.log(`   ----------------------------------------`);

        if (newUrls.length > 0) {
            const domain = new URL(sitemapUrl).hostname.replace('www.', '');
            const batchId = makeBatchId('xml_single', domain);
            const { inserted } = enqueueUrls(newUrls, { source: `xml_single:${domain}`, batchId });
            newUrlsSaved = inserted;
            filePath = null; // kept for legacy callers
            console.log(`\n💾 Enqueued ${newUrlsSaved} NEW URLs to DB queue (batch ${batchId})`);
        } else {
            console.log(`\n🎉 All found URLs have already been scraped! Nothing new to save.`);
        }
    } else {
        console.log("\n⚠️  No URLs found. The sitemap might be empty or blocked.");
    }

    return { totalSitemapsVisited: visited.size, totalUrlsFound: articleUrls.size, newUrlsSaved, filePath };
}

async function runSitemapScraperCLI() {
    console.log("========================================");
    console.log("   XML Sitemap Scraper (Robust)");
    console.log("========================================");

    const sitemapUrl = readlineSync.question('\n👉 Enter the Sitemap URL (e.g., https://example.com/sitemap.xml): ');
    
    if (!sitemapUrl) {
        console.log("No URL provided. Exiting.");
        return;
    }
    
    const { newUrlsSaved, filePath } = await runSitemapScraper(sitemapUrl);

    if (newUrlsSaved > 0) {
        console.log(`\n💾 Saved ${newUrlsSaved} NEW URLs to: ${filePath}`);
        console.log(`   (You can now use Option 3 in the main scraper to process this file)`);
    } else {
        console.log(`\n🎉 All found URLs have already been scraped! Nothing new to save.`);
    }
}

/**
 * Bulk scrapes sitemaps from sitemaps.txt
 */
export async function runBulkSitemapScraper() {
    const filePath = path.join(process.cwd(), 'sitemaps.txt');
    if (!fs.existsSync(filePath)) {
        console.log("❌ sitemaps.txt not found! Please create it and add domains or full sitemap URLs (one per line).");
        return;
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));

    if (lines.length === 0) {
        console.log("⚠️  sitemaps.txt is empty.");
        return;
    }

    console.log(`\n🚀 Found ${lines.length} entries in sitemaps.txt. Starting bulk scrape...`);

    const processedUrls = loadProcessedUrls();
    const queuedUrls = loadQueuedUrls();

    const processSitemapEntry = async (line) => {
        let sitemapUrl = line;
        
        // 1. Add Protocol if missing
        if (!sitemapUrl.startsWith('http')) {
            sitemapUrl = `https://${sitemapUrl}`;
        }
        
        // 2. Add /sitemap.xml if it looks like a root domain
        try {
            const u = new URL(sitemapUrl);
            if (u.pathname === '/' || u.pathname === '') {
                sitemapUrl = new URL('/sitemap.xml', u.href).href;
            }
        } catch (e) {
            console.log(`❌ Invalid URL/Domain: ${line}`);
            return;
        }

        console.log(`\n========================================`);
        console.log(`   Processing: ${sitemapUrl}`);
        console.log(`========================================`);
        
        saveDiscoveredSitemap(sitemapUrl, 'bulk_entry');
        
        const visited = new Set();
        const articleUrls = new Set();
        
        await scrapeSitemapRecursive(sitemapUrl, visited, articleUrls);
        
        // Save results
        if (articleUrls.size > 0) {
             const newUrlSet = new Set();
             for (const url of articleUrls) {
                 const normalizedUrl = normalizeUrl(url);
                 if (!normalizedUrl || !isLikelyArticleUrl(normalizedUrl)) continue;
                 if (processedUrls.has(normalizedUrl) || queuedUrls.has(normalizedUrl)) continue;
                 newUrlSet.add(normalizedUrl);
             }
             const newUrls = Array.from(newUrlSet);


             if (newUrls.length > 0) {
                const domain = new URL(sitemapUrl).hostname.replace(/^www\./, '');
                const batchId = makeBatchId('xml_bulk', domain);
                const { inserted } = enqueueUrls(newUrls, { source: `xml_bulk:${domain}`, batchId });
                console.log(`\n💾 Enqueued ${inserted} NEW URLs to DB queue (batch ${batchId})`);
             } else {
                 console.log(`\n✅ All found URLs are duplicates.`);
             }
        } else {
            console.log(`\n⚠️  No URLs found for ${sitemapUrl}`);
        }
    };

    {
        let idx = 0;
        const worker = async () => {
            while (idx < lines.length) {
                const i = idx++;
                try { await processSitemapEntry(lines[i]); }
                catch (err) { console.error(`Bulk sitemap error: ${err.message}`); }
            }
        };
        const n = Math.min(ROOT_SITEMAP_CONCURRENCY, lines.length);
        await Promise.all(Array.from({ length: n }, worker));
    }
    console.log('\n✅ Bulk Sitemap Scrape Complete!');
}

// Check if running directly
import { fileURLToPath } from 'url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    runSitemapScraperCLI();
}

export { runSitemapScraperCLI };
