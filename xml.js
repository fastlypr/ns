import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';
import fs from 'fs';
import path from 'path';
import readlineSync from 'readline-sync';
import zlib from 'zlib';
import { promisify } from 'util';
import { urlExists, getHistoryCount, saveDiscoveredSitemap, getAllSitemaps, updateSitemapScanDate } from './db.js';

const gunzip = promisify(zlib.gunzip);

// Configuration
const TIMEOUT = 30000; // 30 seconds
const CONCURRENCY = 5; // Parallel sitemap fetching

// Extensions to ignore (images, assets)
const IGNORED_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.pdf', '.css', '.js', '.json'];

/**
 * Fetches a URL and returns its body (handling gzip if necessary).
 */
async function fetchContent(url) {
    try {
        console.log(`Trying to fetch: ${url}`);
        
        // Determine if it's likely a binary file (gzip)
        const isGzip = url.endsWith('.gz');

        const response = await gotScraping({
            url: url,
            timeout: { request: TIMEOUT },
            retry: { limit: 2 },
            responseType: 'buffer', // Get raw buffer to handle gzip manually if needed
            http2: false, // FORCE HTTP/1.1 - Fixes "Session closed without receiving a SETTINGS frame"
            https: { rejectUnauthorized: false }, // Fix for SSL/Origin mismatch errors
            headerGeneratorOptions: {
                devices: ['desktop'],
                locales: ['en-US'],
            }
        });

        if (!response.body) return null;

        let content = response.body;

        // Check for GZIP magic number (1f 8b)
        // We do NOT rely on the extension alone because 'got' might have already decompressed it
        // based on the Content-Encoding header.
        const hasGzipMagicNumber = content.length > 2 && content[0] === 0x1f && content[1] === 0x8b;

        if (hasGzipMagicNumber) {
            console.log(`📦 Decompressing gzip: ${url}`);
            try {
                content = await gunzip(content);
            } catch (err) {
                console.error(`❌ Decompression failed for ${url}: ${err.message}`);
                // Fallback: It might be that the check failed or something else. 
                // If it fails, we return null, or we could try to return the raw buffer if it looks like text.
                return null;
            }
        }

        return content.toString('utf-8');

    } catch (error) {
        console.error(`❌ Error fetching ${url}: ${error.message}`);
        return null;
    }
}

/**
 * Recursively scrapes sitemaps.
 * @param {string} url - The initial sitemap URL.
 * @param {Set} visited - To prevent infinite loops.
 * @param {Set} articleUrls - To store found article URLs.
 */
async function scrapeSitemapRecursive(url, visited = new Set(), articleUrls = new Set()) {
    if (visited.has(url)) return;
    visited.add(url);

    const content = await fetchContent(url);
    if (!content) return;

    // Parse XML
    const $ = cheerio.load(content, { xmlMode: true });
    
    // Find all <loc> tags (handles namespaces implicitly in cheerio xml mode usually, 
    // but we can use a broad selector to be safe)
    const locs = $('loc, url > loc, sitemap > loc');
    
    const foundUrls = [];
    locs.each((i, el) => {
        const txt = $(el).text().trim();
        if (txt) foundUrls.push(txt);
    });

    console.log(`🔍 Found ${foundUrls.length} <loc> entries in ${url}`);

    const childSitemaps = [];

    for (const link of foundUrls) {
        // Filter out junk
        const lowerLink = link.toLowerCase();
        if (IGNORED_EXTENSIONS.some(ext => lowerLink.endsWith(ext))) continue;
        if (lowerLink.includes('wp-content') && !lowerLink.includes('sitemap')) continue; // Skip assets but keep sitemaps

        // Check if it's a sitemap
        if (lowerLink.includes('sitemap') || lowerLink.endsWith('.xml') || lowerLink.endsWith('.xml.gz')) {
            childSitemaps.push(link);
            saveDiscoveredSitemap(link, url); // Save to DB for future monitoring
        } else {
            // It's an article/page
            articleUrls.add(link);
        }
    }

    if (childSitemaps.length > 0) {
        console.log(`📂 Found ${childSitemaps.length} child sitemaps. Processing...`);
        
        // Process in chunks
        const chunks = [];
        for (let i = 0; i < childSitemaps.length; i += CONCURRENCY) {
            chunks.push(childSitemaps.slice(i, i + CONCURRENCY));
        }

        for (const chunk of chunks) {
            await Promise.allSettled(chunk.map(childUrl => scrapeSitemapRecursive(childUrl, visited, articleUrls)));
        }
    } else {
        console.log(`📄 Added ${foundUrls.length - childSitemaps.length} potential articles from ${url}`);
    }
}

/**
 * Loads the set of already scraped URLs from the database.
 */
function loadProcessedUrls() {
    // This is now redundant for filtering but kept for the final stats count if needed,
    // though we check DB one by one for exactness. 
    // We can just return an empty set and let the DB check handle it in the loop.
    return new Set(); 
}

/**
 * Rescans all saved sitemaps for new URLs.
 */
export async function rescanSavedSitemaps(targetDomain = null) {
    const sitemaps = getAllSitemaps();
    if (sitemaps.length === 0) {
        console.log("⚠️  No saved sitemaps found in database.");
        return;
    }

    // Extract unique domains
    const domains = new Set();
    sitemaps.forEach(url => {
        try {
            const hostname = new URL(url).hostname;
            domains.add(hostname);
        } catch (e) {}
    });
    
    const domainList = Array.from(domains).sort();
    
    console.log('\n👉 Select Domain to Rescan:');
    console.log('0. All Domains');
    domainList.forEach((domain, index) => {
        console.log(`${index + 1}. ${domain}`);
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
            sitemapsToScan = sitemaps.filter(url => url.includes(selectedDomain));
            console.log(`\n🔄 Rescanning sitemaps for domain: ${selectedDomain} (${sitemapsToScan.length} found)...`);
        } else {
            console.log("❌ Invalid choice or cancelled.");
            return;
        }
    }
    
    const allNewUrls = new Set();
    const visited = new Set();
    
    // We process them sequentially to be safe, or we could do concurrency.
    // Given sitemaps can be large, sequential or low concurrency is better.
    for (const sitemapUrl of sitemapsToScan) {
        // Skip if already processed in this session (e.g., was a child of a previous sitemap)
        if (visited.has(sitemapUrl)) continue;

        console.log(`\n🔎 Checking: ${sitemapUrl}`);
        const articleUrls = new Set();
        
        // Pass shared 'visited' set to prevent infinite loops and redundant scrapes
        await scrapeSitemapRecursive(sitemapUrl, visited, articleUrls);
        updateSitemapScanDate(sitemapUrl);
        
        // Filter new ones
        for (const url of articleUrls) {
            if (!urlExists(url)) {
                allNewUrls.add(url);
            }
        }
    }

    if (allNewUrls.size > 0) {
        console.log(`\n🎉 Found ${allNewUrls.size} NEW URLs across all sitemaps!`);
        
        const toScrapeDir = path.join(process.cwd(), 'to scrape');
        if (!fs.existsSync(toScrapeDir)) fs.mkdirSync(toScrapeDir);

        const filename = `rescan_updates_${Date.now()}.txt`;
        const filePath = path.join(toScrapeDir, filename);

        fs.writeFileSync(filePath, Array.from(allNewUrls).join('\n'));
        console.log(`\n� Saved new URLs to: to scrape/${filename}`);
    } else {
        console.log(`\n✅ No new URLs found in any sitemap.`);
    }
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

    if (articleUrls.size > 0) {
        const newUrls = [];
        for (const url of articleUrls) {
            if (!urlExists(url)) {
                newUrls.push(url);
            }
        }

        console.log(`   ----------------------------------------`);
        console.log(`   Duplicate (Already Scraped): ${articleUrls.size - newUrls.length}`);
        console.log(`   New URLs to Process: ${newUrls.length}`);
        console.log(`   ----------------------------------------`);

        if (newUrls.length > 0) {
            const toScrapeDir = path.join(process.cwd(), 'to scrape');
            if (!fs.existsSync(toScrapeDir)) fs.mkdirSync(toScrapeDir);

            const domain = new URL(sitemapUrl).hostname.replace('www.', '');
            const filename = `sitemap_${domain}_${Date.now()}.txt`;
            filePath = path.join(toScrapeDir, filename);

            fs.writeFileSync(filePath, newUrls.join('\n'));
            newUrlsSaved = newUrls.length;
            console.log(`\n💾 Saved ${newUrlsSaved} NEW URLs to: to scrape/${filename}`);
            console.log(`   (You can now use Option 3 in the main scraper to process this file)`);
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

    for (const line of lines) {
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
            continue;
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
             const newUrls = [];
             for (const url of articleUrls) {
                 if (!urlExists(url)) newUrls.push(url);
             }
             
             if (newUrls.length > 0) {
                const toScrapeDir = path.join(process.cwd(), 'to scrape');
                if (!fs.existsSync(toScrapeDir)) fs.mkdirSync(toScrapeDir);

                const domain = new URL(sitemapUrl).hostname.replace(/^www\./, '');
                const filename = `sitemap_${domain}_${Date.now()}.txt`;
                const filePath = path.join(toScrapeDir, filename);

                fs.writeFileSync(filePath, newUrls.join('\n'));
                console.log(`\n💾 Saved ${newUrls.length} NEW URLs to: to scrape/${filename}`);
             } else {
                 console.log(`\n✅ All found URLs are duplicates.`);
             }
        } else {
            console.log(`\n⚠️  No URLs found for ${sitemapUrl}`);
        }
    }
    console.log('\n✅ Bulk Sitemap Scrape Complete!');
}

// Check if running directly
import { fileURLToPath } from 'url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    runSitemapScraperCLI();
}

export { runSitemapScraperCLI };
