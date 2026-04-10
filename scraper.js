import { gotScraping } from 'got-scraping';
import { HttpProxyAgent, HttpsProxyAgent } from 'hpagent';
import * as cheerio from 'cheerio';
import readlineSync from 'readline-sync';
import fs from 'fs';
import path from 'path';
import { runSitemapScraper, rescanSavedSitemaps, runBulkSitemapScraper, runSitemapScraperCLI } from './xml.js';
import { logScrapeResult, urlExists, getHistoryCount, getUrlsByStatus, exportUrlsByStatus, getDomainVariable, saveSocialLead, removeQueuedUrl } from './db.js';

// Configuration
const TIMEOUT = 15000; // 15 seconds
const DEFAULT_CONCURRENCY_LIMIT = 20;
let CONCURRENCY_LIMIT = DEFAULT_CONCURRENCY_LIMIT; // High concurrency for speed
let PROXY_LIST = [];
let CRAWLBASE_TOKEN = '';
let PROXY_MODE = 'AUTO';
let scraperInitialized = false;
const PROXY_MODE_FILE = path.join(process.cwd(), 'proxy_mode.txt');
const PROXY_LIST_FILE = path.join(process.cwd(), 'proxies.txt');
const CRAWLBASE_TOKEN_FILE = path.join(process.cwd(), 'crawlbase_token.txt');
const ARTICLE_URL_IGNORED_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.pdf', '.xml', '.rss', '.atom', '.json', '.js', '.css', '.ico', '.mp4', '.mp3', '.txt', '.csv', '.zip'];
const ARTICLE_URL_EXCLUDED_SEGMENTS = new Set([
    'about',
    'account',
    'author',
    'authors',
    'cart',
    'category',
    'checkout',
    'contact',
    'feed',
    'followers',
    'login',
    'm',
    'page',
    'privacy',
    'product',
    'products',
    'search',
    'shop',
    'signin',
    'signup',
    'sitemap',
    'subpage',
    'tag',
    'tags',
    'terms',
    'wp-admin',
    'wp-content'
]);

function normalizeProxyMode(mode) {
    const normalized = String(mode || '').trim().toUpperCase();
    if (normalized === 'DIRECT_ONLY' || normalized === 'WEBSHARE_ONLY' || normalized === 'CRAWLBASE_ONLY' || normalized === 'AUTO') {
        return normalized;
    }
    return 'AUTO';
}

function loadSavedProxyMode() {
    if (!fs.existsSync(PROXY_MODE_FILE)) {
        return null;
    }

    try {
        return normalizeProxyMode(fs.readFileSync(PROXY_MODE_FILE, 'utf8'));
    } catch (error) {
        return null;
    }
}

function saveProxyMode(mode) {
    try {
        fs.writeFileSync(PROXY_MODE_FILE, `${normalizeProxyMode(mode)}\n`);
    } catch (error) {
        console.error(`Failed to save proxy mode: ${error.message}`);
    }
}

/**
 * Loads proxies from proxies.txt and Crawlbase token
 */
function loadProxies() {
    PROXY_LIST = [];
    CRAWLBASE_TOKEN = '';
    CONCURRENCY_LIMIT = DEFAULT_CONCURRENCY_LIMIT;

    // Load Crawlbase Token
    if (fs.existsSync(CRAWLBASE_TOKEN_FILE)) {
        CRAWLBASE_TOKEN = fs.readFileSync(CRAWLBASE_TOKEN_FILE, 'utf8').trim();
        if (CRAWLBASE_TOKEN) console.log(`\n🔑 Crawlbase Token Loaded`);
    }

    if (fs.existsSync(PROXY_LIST_FILE)) {
        const content = fs.readFileSync(PROXY_LIST_FILE, 'utf8');
        PROXY_LIST = content.split('\n')
            .map(line => line.trim())
            .filter(line => line && !line.startsWith('#'))
            .map(line => {
                // Handle IP:PORT:USER:PASS format
                const parts = line.split(':');
                if (parts.length === 4) {
                    return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
                }
                // Handle standard IP:PORT
                if (!line.startsWith('http')) {
                    return `http://${line}`;
                }
                return line;
            });
        
        if (PROXY_LIST.length > 0) {
            console.log(`\n🛡️  Loaded ${PROXY_LIST.length} proxies from proxies.txt`);
            // Auto-adjust concurrency: 5 threads per proxy, but at least 20
            CONCURRENCY_LIMIT = Math.max(20, PROXY_LIST.length * 5);
            console.log(`⚡ Speed adjusted: Concurrency set to ${CONCURRENCY_LIMIT}`);
        }
    }
}

/**
 * Initializes scraper runtime settings once for non-interactive callers.
 */
export function initializeScraper(options = {}) {
    PROXY_MODE = normalizeProxyMode(options.proxyMode || loadSavedProxyMode() || PROXY_MODE);
    if (!scraperInitialized) {
        loadProxies();
        scraperInitialized = true;
    }
}

export function getProxyMode() {
    ensureScraperInitialized();
    return PROXY_MODE;
}

export function setProxyMode(mode) {
    PROXY_MODE = normalizeProxyMode(mode);
    saveProxyMode(PROXY_MODE);
    if (!scraperInitialized) {
        initializeScraper({ proxyMode: PROXY_MODE });
    }
    return PROXY_MODE;
}

function normalizeProxyText(rawText = '') {
    const normalized = String(rawText || '').trim();
    if (normalized.toUpperCase() === 'CLEAR') {
        return '';
    }

    return normalized
        .split('\n')
        .map(line => line.trim())
        .filter(line => line && !line.startsWith('#'))
        .join('\n');
}

function reloadProxySettings() {
    loadProxies();
    scraperInitialized = true;
}

export function getProxyConfigSummary() {
    ensureScraperInitialized();
    return {
        mode: PROXY_MODE,
        webshareCount: PROXY_LIST.length,
        crawlbaseConfigured: Boolean(CRAWLBASE_TOKEN)
    };
}

export function updateWebshareProxyList(rawText) {
    const normalized = normalizeProxyText(rawText);

    if (normalized) {
        fs.writeFileSync(PROXY_LIST_FILE, `${normalized}\n`);
    } else if (fs.existsSync(PROXY_LIST_FILE)) {
        fs.unlinkSync(PROXY_LIST_FILE);
    }

    reloadProxySettings();
    return getProxyConfigSummary();
}

export function updateCrawlbaseToken(rawToken) {
    const normalized = String(rawToken || '').trim();
    const shouldClear = normalized.toUpperCase() === 'CLEAR' || normalized.length === 0;

    if (!shouldClear) {
        fs.writeFileSync(CRAWLBASE_TOKEN_FILE, `${normalized}\n`);
    } else if (fs.existsSync(CRAWLBASE_TOKEN_FILE)) {
        fs.unlinkSync(CRAWLBASE_TOKEN_FILE);
    }

    reloadProxySettings();
    return getProxyConfigSummary();
}

export async function testProxyProvider(provider) {
    ensureScraperInitialized();

    const normalizedProvider = String(provider || '').trim().toUpperCase();
    let agent;
    let providerLabel = '';

    if (normalizedProvider === 'WEBSHARE') {
        if (PROXY_LIST.length === 0) {
            return { success: false, provider: 'Webshare', message: 'No Webshare proxies configured' };
        }

        agent = getRandomProxyAgent();
        providerLabel = 'Webshare';
    } else if (normalizedProvider === 'CRAWLBASE') {
        if (!CRAWLBASE_TOKEN) {
            return { success: false, provider: 'Crawlbase', message: 'No Crawlbase token configured' };
        }

        const proxyUrl = `http://${CRAWLBASE_TOKEN}@smartproxy.crawlbase.com:8012`;
        agent = {
            http: new HttpProxyAgent({ proxy: proxyUrl }),
            https: new HttpsProxyAgent({ proxy: proxyUrl })
        };
        providerLabel = 'Crawlbase';
    } else {
        return { success: false, provider: normalizedProvider, message: 'Unsupported proxy provider' };
    }

    try {
        const response = await gotScraping('https://api.ipify.org?format=json', {
            agent,
            timeout: { request: 10000 },
            retry: { limit: 0 }
        });

        let ip = '';
        try {
            ip = JSON.parse(response.body || '{}').ip || '';
        } catch (error) {
            ip = '';
        }

        return {
            success: true,
            provider: providerLabel,
            ip,
            message: ip ? `Connected via ${ip}` : 'Connection successful'
        };
    } catch (error) {
        return {
            success: false,
            provider: providerLabel,
            message: error.message
        };
    }
}

function ensureScraperInitialized() {
    if (!scraperInitialized) {
        initializeScraper();
    }
}

function normalizeCandidateArticleUrl(rawUrl) {
    try {
        const cleaned = String(rawUrl || '').trim().replace(/[)\].,;:!?]+$/g, '');
        const parsed = new URL(cleaned);
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return null;
        parsed.hash = '';
        parsed.search = '';
        parsed.username = '';
        parsed.password = '';
        let normalized = parsed.toString();
        if (normalized.endsWith('/') && parsed.pathname !== '/') {
            normalized = normalized.slice(0, -1);
        }
        return normalized;
    } catch {
        return null;
    }
}

function scoreArticleUrl(urlStr) {
    try {
        const parsed = new URL(urlStr);
        const lowerHost = parsed.hostname.toLowerCase();
        const lowerPath = parsed.pathname.toLowerCase();
        const segments = lowerPath.split('/').filter(Boolean);

        if (ARTICLE_URL_IGNORED_EXTENSIONS.some(ext => lowerPath.endsWith(ext))) return -10;
        if (lowerHost.startsWith('miro.') || lowerHost.startsWith('cdn.') || lowerHost.startsWith('images.')) return -10;
        if (segments.length === 0) return -10;
        if (segments.some(segment => ARTICLE_URL_EXCLUDED_SEGMENTS.has(segment))) return -6;

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

export function extractSmartArticleUrls(rawText) {
    const matches = String(rawText || '').match(/https?:\/\/[^\s"'<>`]+/g) || [];
    const scoredUrls = new Map();

    matches.forEach(match => {
        const normalized = normalizeCandidateArticleUrl(match);
        if (!normalized) return;

        const score = scoreArticleUrl(normalized);
        const existing = scoredUrls.get(normalized);
        if (existing === undefined || score > existing) {
            scoredUrls.set(normalized, score);
        }
    });

    const articleLikeUrls = Array.from(scoredUrls.entries())
        .filter(([, score]) => score >= 2)
        .map(([url]) => url);

    if (articleLikeUrls.length > 0) {
        return articleLikeUrls;
    }

    return Array.from(scoredUrls.keys());
}

/**
 * Gets a proxy agent configuration for a random proxy.
 */
function getRandomProxyAgent() {
    if (PROXY_LIST.length === 0) return undefined;
    
    const proxyUrl = PROXY_LIST[Math.floor(Math.random() * PROXY_LIST.length)];
    // Normalize proxy URL
    const formattedProxy = proxyUrl.startsWith('http') ? proxyUrl : `http://${proxyUrl}`;
    
    return {
        http: new HttpProxyAgent({ proxy: formattedProxy }),
        https: new HttpsProxyAgent({ proxy: formattedProxy })
    };
}

class ProxyManager {
    constructor() {
        this.domainStrategies = new Map(); // domain -> 'DIRECT' | 'WEBSHARE' | 'CRAWLBASE'
        this.domainStats = new Map();
    }

    getStrategy(url) {
        try {
            // Respect Global Proxy Mode
            if (PROXY_MODE === 'DIRECT_ONLY') return 'DIRECT';
            if (PROXY_MODE === 'WEBSHARE_ONLY') return 'WEBSHARE';
            if (PROXY_MODE === 'CRAWLBASE_ONLY') return 'CRAWLBASE';

            const domain = new URL(url).hostname.replace(/^www\./, '');
            if (!this.domainStrategies.has(domain)) {
                // Default Strategy Hierarchy: DIRECT -> WEBSHARE -> CRAWLBASE
                this.domainStrategies.set(domain, 'DIRECT');
            }
            return this.domainStrategies.get(domain);
        } catch (e) {
            return 'DIRECT';
        }
    }

    getAgent(url) {
        const strategy = this.getStrategy(url);
        
        if (strategy === 'WEBSHARE') {
            return getRandomProxyAgent();
        } else if (strategy === 'CRAWLBASE' && CRAWLBASE_TOKEN) {
             const proxyUrl = `http://${CRAWLBASE_TOKEN}@smartproxy.crawlbase.com:8012`;
             return {
                http: new HttpProxyAgent({ proxy: proxyUrl }),
                https: new HttpsProxyAgent({ proxy: proxyUrl })
            };
        }
        // DIRECT (undefined agent)
        return undefined;
    }

    reportSuccess(url) {
        try {
            const domain = new URL(url).hostname.replace(/^www\./, '');
            const stats = this.getStats(domain);
            stats.successes++;
            stats.consecutiveFailures = 0;
        } catch (e) {}
    }

    reportFailure(url, statusCode) {
        try {
            // Disable auto-upgrade if mode is fixed
            if (PROXY_MODE !== 'AUTO') return false;

            const domain = new URL(url).hostname.replace(/^www\./, '');
            const stats = this.getStats(domain);
            stats.failures++;
            stats.consecutiveFailures++;
            
            const currentStrategy = this.getStrategy(url);
            
            // Upgrade Logic: If > 2 consecutive failures or blocking status
            if (stats.consecutiveFailures >= 2 || statusCode === 403 || statusCode === 429) {
                if (currentStrategy === 'DIRECT') {
                    if (PROXY_LIST.length > 0) {
                        console.log(`\n⚠️  Blocking detected on ${domain}. Upgrading to WEBSHARE proxies.`);
                        this.domainStrategies.set(domain, 'WEBSHARE');
                        stats.consecutiveFailures = 0;
                        return true; // Indicate upgrade happened (trigger retry)
                    } else if (CRAWLBASE_TOKEN) {
                        console.log(`\n⚠️  Blocking detected on ${domain}. Upgrading to CRAWLBASE.`);
                        this.domainStrategies.set(domain, 'CRAWLBASE');
                        stats.consecutiveFailures = 0;
                        return true;
                    }
                } else if (currentStrategy === 'WEBSHARE') {
                    if (CRAWLBASE_TOKEN) {
                        console.log(`\n⚠️  Blocking detected on ${domain} (Webshare failed). Upgrading to CRAWLBASE.`);
                        this.domainStrategies.set(domain, 'CRAWLBASE');
                        stats.consecutiveFailures = 0;
                        return true;
                    }
                }
            }
        } catch (e) {}
        return false;
    }
    
    getStats(domain) {
        if (!this.domainStats.has(domain)) {
            this.domainStats.set(domain, { successes: 0, failures: 0, consecutiveFailures: 0 });
        }
        return this.domainStats.get(domain);
    }
}

const proxyManager = new ProxyManager();

// Social Media Regex Patterns
const SOCIAL_PATTERNS = {
    instagram: /https?:\/\/(www\.)?instagram\.com\/[a-zA-Z0-9_.]+/i,
    linkedin: /https?:\/\/(www\.)?linkedin\.com\/(in|company)\/[a-zA-Z0-9%-]+/i,
    twitter: /https?:\/\/(www\.)?(twitter\.com|x\.com)\/[a-zA-Z0-9_]+/i,
    facebook: /https?:\/\/(www\.)?facebook\.com\/[a-zA-Z0-9.]+/i,
    youtube: /https?:\/\/(www\.)?youtube\.com\/(channel|c|user|@)[a-zA-Z0-9_-]+/i,
    tiktok: /https?:\/\/(www\.)?tiktok\.com\/@[a-zA-Z0-9_.]+/i,
    pinterest: /https?:\/\/(www\.)?pinterest\.com\/[a-zA-Z0-9_]+/i,
    github: /https?:\/\/(www\.)?github\.com\/[a-zA-Z0-9_-]+/i
};

const SOCIAL_HOSTS = [
    'instagram.com',
    'linkedin.com',
    'twitter.com',
    'x.com',
    'facebook.com',
    'youtube.com',
    'youtu.be',
    'tiktok.com',
    'pinterest.com',
    'github.com'
];

const normalizeExtractedLink = (inputUrl) => {
    try {
        const parsed = new URL(inputUrl);
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
            return '';
        }

        parsed.hash = '';
        parsed.search = '';
        parsed.username = '';
        parsed.password = '';

        let normalized = parsed.toString();
        if (normalized.endsWith('/') && parsed.pathname !== '/') {
            normalized = normalized.slice(0, -1);
        }

        return normalized;
    } catch {
        return '';
    }
};

const isWebsiteLeadUrl = (candidateUrl, sourceUrl) => {
    try {
        const parsed = new URL(candidateUrl);
        const source = new URL(sourceUrl);
        const candidateHost = parsed.hostname.toLowerCase().replace(/^www\./, '');
        const sourceHost = source.hostname.toLowerCase().replace(/^www\./, '');
        const lowerPath = parsed.pathname.toLowerCase();

        if (candidateHost === sourceHost) return false;
        if (SOCIAL_HOSTS.some(host => candidateHost === host || candidateHost.endsWith(`.${host}`))) return false;
        if (ARTICLE_URL_IGNORED_EXTENSIONS.some(ext => lowerPath.endsWith(ext))) return false;

        return true;
    } catch {
        return false;
    }
};

/**
 * Scrapes a single article URL for social media links.
 * @param {string} url - The URL of the article to scrape.
 * @returns {Promise<Object>} - Object containing { source_url, socials, error }.
 */
async function scrapeSocialLinks(url) {
    if (!url) return { source_url: '', socials: [], error: 'Empty URL' };
    
    // Clean URL (remove trailing colons or whitespace)
    url = url.trim().replace(/[:]+$/, '');
    
    // basic protocol check
    if (!url.startsWith('http')) {
        url = 'https://' + url;
    }

    // console.log(`\n🔍 Analyzing: ${url} ...`); // Reduced logging for bulk
    
    let attempts = 0;
    const maxAttempts = 2; // Allow 1 retry for strategy upgrade

    while (attempts < maxAttempts) {
        attempts++;
        try {
            const agent = proxyManager.getAgent(url);
            
            // 'got-scraping' automatically manages headers, TLS fingerprints, and HTTP/2
            const response = await gotScraping({
                url: url,
                timeout: { request: TIMEOUT },
                retry: { limit: 1 }, // Reduced internal retry, we handle strategy retry
                http2: false,
                agent: agent
            });

            const html = response.body;
            proxyManager.reportSuccess(url);

            const $ = cheerio.load(html);
        
            // Check for Cloudflare/Anti-bot blocking pages
            const pageTitle = $('title').text().toLowerCase();
            if (pageTitle.includes('access denied') || 
                pageTitle.includes('attention required') || 
                pageTitle.includes('just a moment') ||
                pageTitle.includes('security check')) {
                throw new Error('Blocked by Cloudflare/Anti-bot');
            }

            const socials = [];
            const uniqueLinks = new Set();

            // Find all anchor tags with href attributes
            $('a[href]').each((_, element) => {
                const href = $(element).attr('href');
                if (!href) return;

                // Skip generic share links
                if (href.includes('sharer.php') || href.includes('twitter.com/share') || href.includes('intent/tweet')) {
                    return;
                }

                let fullUrl = '';
                try {
                    fullUrl = href.startsWith('http') ? href : new URL(href, url).href;
                } catch (e) {
                    return;
                }

                let matchedSocial = false;

                for (const [platform, regex] of Object.entries(SOCIAL_PATTERNS)) {
                    if (regex.test(fullUrl)) {
                        try {
                            const normalizedHref = normalizeExtractedLink(fullUrl);
                            if (!normalizedHref) return;

                            if (!uniqueLinks.has(normalizedHref)) {
                                uniqueLinks.add(normalizedHref);
                                socials.push({
                                    platform: platform,
                                    link: normalizedHref,
                                    category: 'link'
                                });
                            }
                            matchedSocial = true;
                        } catch (e) {
                            // skip invalid urls
                        }
                    }
                }

                if (!matchedSocial && isWebsiteLeadUrl(fullUrl, url)) {
                    const normalizedWebsiteUrl = normalizeExtractedLink(fullUrl);
                    if (normalizedWebsiteUrl && !uniqueLinks.has(normalizedWebsiteUrl)) {
                        uniqueLinks.add(normalizedWebsiteUrl);
                        socials.push({
                            platform: 'website',
                            link: normalizedWebsiteUrl,
                            category: 'website'
                        });
                    }
                }
            });

            // 2. Scan text content for specific Instagram handles
            
            // Cleanup DOM to ensure clean text extraction
            $('script, style, noscript, iframe, svg').remove();
            
            // Add whitespace to prevent concatenation (e.g. "credit @user" + "instagram" -> "@userinstagram")
            $('br').replaceWith(' ');
            $('div, p, h1, h2, h3, h4, h5, h6, li, span, a, strong, em, b, i').after(' '); 
            
            const bodyText = $('body').text().replace(/\s+/g, ' ');
            
            // Define patterns to capture
            const instagramPatterns = [
                /instagram\s+@([a-zA-Z0-9_.]+)/gi,
                /(?:follow|following)\s+(?:us\s+on|on)?\s*instagram\s+@([a-zA-Z0-9_.]+)/gi,
                /IG\s+@([a-zA-Z0-9_.]+)/gi
            ];

            // Define patterns to exclude (Credit lines)
            // Matches: "credit @user", "credit: @user", "credit to @user", "credit instagram @user"
            const creditRegex = /credit(?:s)?\s*(?::|to)?\s*(?:instagram\s+)?@([a-zA-Z0-9_.]+)/gi;
            const excludedHandles = new Set();
            
            let cMatch;
            while ((cMatch = creditRegex.exec(bodyText)) !== null) {
                let h = cMatch[1];
                if (h.endsWith('.')) h = h.slice(0, -1);
                excludedHandles.add(h.toLowerCase());
            }
            
            for (const pattern of instagramPatterns) {
                let match;
                // Reset regex state just in case
                pattern.lastIndex = 0;
                
                while ((match = pattern.exec(bodyText)) !== null) {
                    let handle = match[1];
                    
                    // Remove trailing period (end of sentence)
                    if (handle.endsWith('.')) handle = handle.slice(0, -1);
                    
                    // Basic filtering
                    if (handle.length < 2) continue;
                    if (handle.includes('@')) continue; 
                    
                    // Check if this handle is in the exclusion list
                    if (excludedHandles.has(handle.toLowerCase())) {
                        continue;
                    }

                    // Construct and Normalize URL
                    let cleanHref = `https://www.instagram.com/${handle}`;
                    
                    if (!uniqueLinks.has(cleanHref)) {
                        uniqueLinks.add(cleanHref);
                        socials.push({
                            platform: 'instagram',
                            link: cleanHref,
                            category: 'text'
                        });
                    }
                }
            }

            return { source_url: url, socials: socials, error: null };

        } catch (error) {
            let statusCode = error.response ? error.response.statusCode : 0;
            const upgraded = proxyManager.reportFailure(url, statusCode);
            
            if (upgraded && attempts < maxAttempts) {
                console.log(`   🔄 Retrying ${url} with new proxy strategy...`);
                continue; // Retry loop with new agent
            }

            let errorMsg = error.message;
            if (statusCode === 403) {
                errorMsg = "Access Denied (403)";
            }
            // Return failure only if retries exhausted
            if (attempts >= maxAttempts) {
                 return { source_url: url, socials: [], error: errorMsg };
            }
        }
    }
    return { source_url: url, socials: [], error: 'Max attempts reached' };
}

/**
 * Extracts the username from an Instagram URL.
 */
const extractUsername = (urlStr) => {
    try {
        if (!urlStr) return '';
        const urlObj = new URL(urlStr.startsWith('http') ? urlStr : `https://${urlStr}`);
        if (urlObj.hostname.includes('instagram.com')) {
            const parts = urlObj.pathname.split('/').filter(p => p.length > 0);
            if (parts.length > 0) {
                return parts[0].toLowerCase();
            }
        }
        return '';
    } catch (e) {
        return '';
    }
};

/**
 * Saves results to CSV files organized by domain and platform.
 * Structure: /result/<Domain>/<Platform>.csv
 */
function saveResults(result) {
    if (!result || !result.socials || result.socials.length === 0) return;

    const resultDir = path.join(process.cwd(), 'result');
    if (!fs.existsSync(resultDir)) fs.mkdirSync(resultDir);

    // Format timestamp: "Feb 4" (User requested format)
    // But user asked for "date feb 4". Let's stick to a standard readable format or exactly as asked.
    // "date feb 4" implies something like "MMM D".
    const now = new Date();
    const month = now.toLocaleString('en-US', { month: 'short' });
    const day = now.getDate();
    const timestamp = `${month} ${day}`;

    const parseCsvLine = (line) => {
        const values = [];
        let current = '';
        let inQuotes = false;

        for (let i = 0; i < line.length; i++) {
            const char = line[i];

            if (char === '"') {
                if (inQuotes && line[i + 1] === '"') {
                    current += '"';
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (char === ',' && !inQuotes) {
                values.push(current);
                current = '';
                continue;
            }

            current += char;
        }

        values.push(current);
        return values;
    };

    const escapeCsvCell = (value) => {
        const stringValue = value === null || value === undefined ? '' : String(value);
        if (/[",\n]/.test(stringValue)) {
            return `"${stringValue.replace(/"/g, '""')}"`;
        }
        return stringValue;
    };

    const serializeCsvLine = (values) => values.map(escapeCsvCell).join(',');

    const ensureCsvColumn = (filePath, columnName) => {
        if (!fs.existsSync(filePath)) return;

        const content = fs.readFileSync(filePath, 'utf8');
        if (!content.trim()) return;

        const lines = content.split('\n');
        const header = parseCsvLine(lines[0]);
        if (header.includes(columnName)) return;

        header.push(columnName);
        const updatedLines = lines.map((line, index) => {
            if (!line.trim()) return line;
            if (index === 0) return serializeCsvLine(header);
            const values = parseCsvLine(line);
            values.push('');
            return serializeCsvLine(values);
        });

        fs.writeFileSync(filePath, updatedLines.join('\n'));
    };

    // Consolidated CSV Path
    const allResultsPath = path.join(resultDir, 'all_results.csv');
    if (!fs.existsSync(allResultsPath)) {
        fs.writeFileSync(allResultsPath, 'Timestamp,Domain,Platform,Source URL,Social Link,Username,Category,Domain Variable\n');
    } else {
        ensureCsvColumn(allResultsPath, 'Domain Variable');
    }

    try {
        const urlObj = new URL(result.source_url);
        let domain = urlObj.hostname.replace(/^www\./, '');
        const domainVariable = getDomainVariable(domain);
        
        const domainDir = path.join(resultDir, domain);
        if (!fs.existsSync(domainDir)) fs.mkdirSync(domainDir);
        
        result.socials.forEach(social => {
            const platform = social.platform.toLowerCase();
            const fileName = `${platform}.csv`;
            const filePath = path.join(domainDir, fileName);
            
            // Check duplicates based on USERNAME (if Instagram) or Link
            let existingUsernames = new Set();
            let existingLinks = new Set();
            
            if (fs.existsSync(filePath)) {
                ensureCsvColumn(filePath, 'Domain Variable');
                const content = fs.readFileSync(filePath, 'utf8');
                content.split('\n').forEach(line => {
                    const parts = parseCsvLine(line);
                    // Format changed to include Timestamp at the end or beginning?
                    // User said: "save results with timestamp in csv file"
                    // Existing header: Source URL,Social Link,Username,Category
                    // We should append Timestamp column or prepend it.
                    // Let's Append it to maintain backward compatibility if possible, or Prepend.
                    // Actually, modifying existing structure might break 'removeLineFromFile' if we rely on it?
                    // No, removeLineFromFile works on source files, not result files.
                    
                    // Let's assume we add it as the last column for minimal disruption, OR as first.
                    // "like date feb 4"
                    
                    // Let's check where the link is. 
                    // Old format: Source, Link, Username, Category
                    // New format: Source, Link, Username, Category, Timestamp
                    
                    if (parts.length >= 2) {
                        const link = parts[1].trim();
                        existingLinks.add(link);
                        
                        if (parts.length >= 3 && parts[2].trim()) {
                            existingUsernames.add(parts[2].trim().toLowerCase());
                        } else if (platform === 'instagram') {
                            const uname = extractUsername(link);
                            if (uname) existingUsernames.add(uname);
                        }
                    }
                });
            } else {
                fs.writeFileSync(filePath, 'Source URL,Social Link,Username,Category,Timestamp,Domain Variable\n');
            }
            
            let shouldSave = false;
            let username = '';
            const category = social.category || 'link';

            if (platform === 'instagram') {
                username = extractUsername(social.link);
                if (username && !existingUsernames.has(username)) {
                    shouldSave = true;
                } else if (!username && !existingLinks.has(social.link)) {
                    shouldSave = true;
                }
            } else {
                if (!existingLinks.has(social.link)) {
                    shouldSave = true;
                }
            }

            saveSocialLead({
                domain,
                platform,
                sourceUrl: result.source_url,
                socialLink: social.link,
                username,
                category,
                domainVariable
            });

            if (shouldSave) {
                const cleanSource = result.source_url.includes(',') ? `"${result.source_url}"` : result.source_url;
                const cleanLink = social.link.includes(',') ? `"${social.link}"` : social.link;
                const cleanUser = username;
                
                // 1. Save to Domain Specific File
                fs.appendFileSync(filePath, `${cleanSource},${cleanLink},${cleanUser},${category},${timestamp},${escapeCsvCell(domainVariable)}\n`);
                console.log(`   💾 Saved to result/${domain}/${fileName}`);

                // 2. Save to Consolidated File (Append Only)
                // Header: Timestamp,Domain,Platform,Source URL,Social Link,Username,Category,Domain Variable
                const csvLine = `${timestamp},${domain},${platform},${cleanSource},${cleanLink},${cleanUser},${category},${escapeCsvCell(domainVariable)}\n`;
                fs.appendFileSync(allResultsPath, csvLine);
            }
        });

    } catch (e) {
        console.error(`   ❌ Error saving CSV: ${e.message}`);
    }
}

/**
 * Removes a specific URL from the source file.
 */
function removeLineFromFile(filePath, urlToRemove) {
    if (!filePath || !fs.existsSync(filePath)) return;
    try {
        const normalizedTarget = normalizeCandidateArticleUrl(urlToRemove);
        const content = fs.readFileSync(filePath, 'utf8');
        const lines = content.split('\n');
        const newLines = lines.filter(line => {
            const trimmed = line.trim();
            if (!trimmed) return true; // Keep empty lines?
            
            // Exact match
            if (trimmed === urlToRemove.trim()) return false;
            
            // CSV check
            if (trimmed.includes(',')) {
                const parts = trimmed.split(',');
                if (parts.some(p => p.trim() === urlToRemove.trim())) return false;
            }

            if (!normalizedTarget) return true;

            const normalizedLineUrl = normalizeCandidateArticleUrl(trimmed);
            if (normalizedLineUrl && normalizedLineUrl === normalizedTarget) {
                return false;
            }

            if (trimmed.includes(',')) {
                const parts = trimmed.split(',');
                if (parts.some(part => normalizeCandidateArticleUrl(part.trim()) === normalizedTarget)) {
                    return false;
                }
            }
            
            return true;
        });
        
        if (newLines.length < lines.length) {
            // Check if file is now effectively empty (only whitespace)
            const remainingContent = newLines.join('\n').trim();
            if (!remainingContent) {
                try {
                    if (fs.existsSync(filePath)) {
                        fs.unlinkSync(filePath);
                        console.log(`🗑️  Deleted empty file: ${path.basename(filePath)}`);
                    }
                } catch (err) {
                    console.error(`Error deleting file: ${err.message}`);
                }
            } else {
                fs.writeFileSync(filePath, newLines.join('\n'));
            }
        }
    } catch (e) {
        console.error(`Error updating source file: ${e.message}`);
    }
}

/**
 * Main scraping runner with worker pool
 */
export async function runScraper(urls, sourceFilePath = null, force = false, options = {}) {
    ensureScraperInitialized();
    const { onProgress } = options;
    const startedAt = Date.now();
    const linkedinLeadSet = new Set();
    const instagramLeadSet = new Set();
    const progress = {
        totalUrls: urls.length,
        processedUrls: 0,
        successCount: 0,
        failedCount: 0,
        noResultCount: 0,
        skippedCount: 0,
        linkedinLeadCount: 0,
        instagramLeadCount: 0,
        linkedinLeadKeys: [],
        instagramLeadKeys: [],
        currentUrl: '',
        elapsedMs: 0,
        sourceFilePath
    };

    const emitProgress = async (stage = 'progress', extra = {}) => {
        progress.elapsedMs = Date.now() - startedAt;
        if (typeof onProgress === 'function') {
            await onProgress({
                stage,
                ...progress,
                ...extra
            });
        }
    };

    console.log(`\n🚀 Processing ${urls.length} URL(s) with parallel limit of ${CONCURRENCY_LIMIT}...`);
    await emitProgress('start');

    // Worker function to process a single URL
    const processUrl = async (rawUrl) => {
        if (!rawUrl.trim()) return;
        
        // Clean URL (remove trailing colons or whitespace)
        let url = rawUrl.trim().replace(/[:]+$/, '');

        // Add protocol if missing (for consistent DB checking)
        if (!url.startsWith('http')) {
            url = 'https://' + url;
        }
        progress.currentUrl = url;

        // Check if already scraped (unless forced)
        if (!force && urlExists(url)) {
            console.log(`⏭️  Skipping (Already Scraped): ${url}`);
            // Still remove from source file if it was already processed
            if (sourceFilePath) {
                removeLineFromFile(sourceFilePath, rawUrl);
            }
            removeQueuedUrl(url);
            progress.processedUrls++;
            progress.skippedCount++;
            await emitProgress('progress', { currentUrl: url, lastStatus: 'Skipped' });
            return;
        }
        
        const result = await scrapeSocialLinks(url);
        
        // Remove from file immediately after processing (success or fail)
        if (sourceFilePath) {
            removeLineFromFile(sourceFilePath, rawUrl);
        }
        removeQueuedUrl(url);
        
        if (result.error) {
            logScrapeResult(url, 'Failed', result.error);
            console.log(`❌ Failed: ${url} (${result.error})`);
            progress.processedUrls++;
            progress.failedCount++;
            await emitProgress('progress', { currentUrl: url, lastStatus: 'Failed' });
            return;
        }
        
        console.log(`\n----------------------------------------`);
        console.log(`🔗 Source: ${result.source_url}`);
        
        if (result.socials.length > 0) {
            result.socials.forEach((social) => {
                if (!social || !social.link) return;

                if (social.platform === 'linkedin') {
                    linkedinLeadSet.add(social.link.toLowerCase());
                }

                if (social.platform === 'instagram') {
                    const instagramUsername = extractUsername(social.link);
                    instagramLeadSet.add((instagramUsername || social.link).toLowerCase());
                }
            });

            progress.linkedinLeadCount = linkedinLeadSet.size;
            progress.instagramLeadCount = instagramLeadSet.size;
            progress.linkedinLeadKeys = Array.from(linkedinLeadSet);
            progress.instagramLeadKeys = Array.from(instagramLeadSet);
            logScrapeResult(url, 'Success', `Found ${result.socials.length} links`);
            console.log(`✅ Found ${result.socials.length} links:`);
            result.socials.forEach(item => {
                console.log(`   - [${item.platform.toUpperCase()}] ${item.link}`);
            });
            
            // Save to CSV immediately
            saveResults(result);
            progress.processedUrls++;
            progress.successCount++;
            await emitProgress('progress', { currentUrl: url, lastStatus: 'Success' });
        } else {
            logScrapeResult(url, 'No_Result', 'No links found');
            console.log('⚠️  No links found (Marked as No Result).');
            progress.processedUrls++;
            progress.noResultCount++;
            await emitProgress('progress', { currentUrl: url, lastStatus: 'No_Result' });
        }
    };

    // Use an atomic index instead of shifting from a copied array (Memory Optimization)
    let currentIndex = 0;
    const activeWorkers = [];

    // Worker loop
    const worker = async () => {
        while (currentIndex < urls.length) {
            const url = urls[currentIndex++];
            if (url) await processUrl(url);
        }
    };

    // Start initial workers (up to limit)
    const numWorkers = Math.min(urls.length, CONCURRENCY_LIMIT);
    for (let i = 0; i < numWorkers; i++) {
        activeWorkers.push(worker());
    }

    await Promise.all(activeWorkers);
    
    console.log('\n========================================');
    await emitProgress('complete');
    return { ...progress, elapsedMs: Date.now() - startedAt };
}

/**
 * Scrapes a single URL and logs the result.
 * This is an exported function for external use (e.g., Telegram bot).
 * @param {string} url - The URL to scrape.
 */
export async function scrapeSingleUrlAndProcess(url, options = {}) {
    if (!url) {
        console.log("No URL provided for single scrape.");
        return;
    }
    return runScraper([url], null, false, options);
}

/**
 * Scrapes URLs from a specified input file (e.g., input.txt).
 * This is an exported function for external use (e.g., Telegram bot).
 * @param {string} filePath - The path to the input file containing URLs.
 */
export async function scrapeUrlsFromInputFile(filePath, options = {}) {
    if (!fs.existsSync(filePath)) {
        console.log(`Error: Input file not found at ${filePath}`);
        return;
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const fileUrls = extractSmartArticleUrls(content);

    if (fileUrls.length > 0) {
        console.log(`Processing ${fileUrls.length} URLs from ${filePath}...`);
        return runScraper(fileUrls, filePath, false, options);
    } else {
        console.log(`No valid URLs found in ${filePath}`);
    }
}

/**
 * Processes all text/csv files in the 'to scrape' folder.
 * This is an exported function for external use (e.g., Telegram bot).
 * @param {string} toScrapeDir - The path to the 'to scrape' directory.
 */
export async function processToScrapeFolder(toScrapeDir, options = {}) {
    const { onProgress } = options;
    const emitProgress = async (payload) => {
        if (typeof onProgress === 'function') {
            await onProgress(payload);
        }
    };

    try {
        const files = fs.readdirSync(toScrapeDir).filter(f => f.endsWith('.txt') || f.endsWith('.csv'));
        if (files.length === 0) {
            console.log('No files found in "to scrape" folder.');
            return;
        }

        const startedAt = Date.now();
        const fileEntries = [];

        for (const file of files) {
            const filePath = path.join(toScrapeDir, file);
            const content = fs.readFileSync(filePath, 'utf8');
            const urls = extractSmartArticleUrls(content);

            if (urls.length > 0) {
                fileEntries.push({ file, filePath, urls });
            } else {
                console.log(`Deleting empty file: ${file}`);
                fs.unlinkSync(filePath);
            }
        }

        if (fileEntries.length === 0) {
            console.log('No valid URLs found in "to scrape" folder.');
            return;
        }

        const summary = {
            folderName: path.basename(toScrapeDir),
            totalFiles: fileEntries.length,
            processedFiles: 0,
            currentFile: fileEntries[0].file,
            totalUrls: fileEntries.reduce((total, entry) => total + entry.urls.length, 0),
            processedUrls: 0,
            successCount: 0,
            failedCount: 0,
            noResultCount: 0,
            skippedCount: 0,
            linkedinLeadCount: 0,
            instagramLeadCount: 0,
            currentUrl: '',
            elapsedMs: 0
        };

        console.log(`Found ${fileEntries.length} files in "to scrape" folder.`);
        await emitProgress({ stage: 'start', summary: { ...summary } });

        let completedUrls = 0;
        let completedSuccess = 0;
        let completedFailed = 0;
        let completedNoResult = 0;
        let completedSkipped = 0;
        const completedLinkedinLeadSet = new Set();
        const completedInstagramLeadSet = new Set();

        for (const entry of fileEntries) {
            summary.currentFile = entry.file;
            summary.elapsedMs = Date.now() - startedAt;
            await emitProgress({ stage: 'file_start', summary: { ...summary } });

            const fileSummary = await runScraper(entry.urls, entry.filePath, false, {
                onProgress: async (progress) => {
                    summary.currentFile = entry.file;
                    summary.processedUrls = completedUrls + (progress.processedUrls || 0);
                    summary.successCount = completedSuccess + (progress.successCount || 0);
                    summary.failedCount = completedFailed + (progress.failedCount || 0);
                    summary.noResultCount = completedNoResult + (progress.noResultCount || 0);
                    summary.skippedCount = completedSkipped + (progress.skippedCount || 0);
                    summary.linkedinLeadCount = new Set([
                        ...completedLinkedinLeadSet,
                        ...((progress.linkedinLeadKeys || []).map(key => key.toLowerCase()))
                    ]).size;
                    summary.instagramLeadCount = new Set([
                        ...completedInstagramLeadSet,
                        ...((progress.instagramLeadKeys || []).map(key => key.toLowerCase()))
                    ]).size;
                    summary.currentUrl = progress.currentUrl || '';
                    summary.elapsedMs = Date.now() - startedAt;
                    await emitProgress({ stage: progress.stage || 'progress', summary: { ...summary } });
                }
            });

            if (fileSummary) {
                completedUrls += fileSummary.processedUrls || 0;
                completedSuccess += fileSummary.successCount || 0;
                completedFailed += fileSummary.failedCount || 0;
                completedNoResult += fileSummary.noResultCount || 0;
                completedSkipped += fileSummary.skippedCount || 0;
                (fileSummary.linkedinLeadKeys || []).forEach(key => completedLinkedinLeadSet.add(key.toLowerCase()));
                (fileSummary.instagramLeadKeys || []).forEach(key => completedInstagramLeadSet.add(key.toLowerCase()));
            }

            summary.processedFiles += 1;
            summary.processedUrls = completedUrls;
            summary.successCount = completedSuccess;
            summary.failedCount = completedFailed;
            summary.noResultCount = completedNoResult;
            summary.skippedCount = completedSkipped;
            summary.linkedinLeadCount = completedLinkedinLeadSet.size;
            summary.instagramLeadCount = completedInstagramLeadSet.size;
            summary.currentUrl = '';
            summary.elapsedMs = Date.now() - startedAt;

            await emitProgress({ stage: 'file_complete', summary: { ...summary } });
        }

        await emitProgress({ stage: 'complete', summary: { ...summary } });
        return { ...summary };
    } catch (e) {
        console.error(`Error processing 'to scrape' folder: ${e.message}`);
    }
}

/**
 * Retries URLs that previously failed or had no result.
 * This is an exported function for external use (e.g., Telegram bot).
 */
export async function retryFailedAndNoResultUrls() {
    const failedUrls = getUrlsByStatus('Failed');
    const noResultUrls = getUrlsByStatus('No_Result');
    const urlsToRetry = [...new Set([...failedUrls, ...noResultUrls])];

    if (urlsToRetry.length > 0) {
        console.log(`\nRetrying ${urlsToRetry.length} Failed & No Result URLs...`);
        await runScraper(urlsToRetry, null, true); // Force retry
    } else {
        console.log('No failed or no-result URLs to retry.');
    }
}

async function main() {
    console.log("========================================");
    console.log("   Stealth Social Media Scraper (Ultimate)");
    console.log("========================================");

    // Ask for Proxy Mode
    console.log('\nProxy Configuration:');
    console.log('1. Auto (Direct -> Webshare -> Crawlbase) [Default]');
    console.log('2. Direct Only (No Proxies)');
    console.log('3. Webshare Proxies Only');
    console.log('4. Crawlbase Only');
    
    const proxyChoice = readlineSync.question('👉 Select Proxy Mode (1-4) [1]: ');
    if (proxyChoice === '2') PROXY_MODE = 'DIRECT_ONLY';
    else if (proxyChoice === '3') PROXY_MODE = 'WEBSHARE_ONLY';
    else if (proxyChoice === '4') PROXY_MODE = 'CRAWLBASE_ONLY';
    else PROXY_MODE = 'AUTO';

    initializeScraper({ proxyMode: PROXY_MODE });
    
    console.log(`🔒 Proxy Mode set to: ${PROXY_MODE}`);

    // Show History Count
    try {
        const historyCount = getHistoryCount();
        console.log(`📊 Database History: ${historyCount} URLs processed`);
    } catch (e) {
        console.log(`⚠️  Database not ready: ${e.message}`);
    }

    // Ensure 'to scrape' folder exists
    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) {
        fs.mkdirSync(toScrapeDir);
        console.log(`Created directory: ${toScrapeDir}`);
    }

    // Daemon Mode from Command Line (Backwards Compatibility)
    const args = process.argv.slice(2);
    if (args.includes('--daemon') || args.includes('--watch')) {
        console.log('\n👻 Daemon Mode Activated via Flag');
        await runDaemonMode(toScrapeDir);
        return; // Exit main loop if daemon starts
    }

    while (true) {
        console.log('\n========================================');
        console.log('   MAIN MENU');
        console.log('========================================');
        console.log('1. 🚀 Scrape Articles');
        console.log('2. 🗺️ Sitemap Tools');
        console.log('3. 📊 Manage Results');
        console.log('4. 👻 Start Daemon Mode');
        console.log('5. ❌ Exit');
        
        const choice = readlineSync.question('\n👉 Choose an option (1-5): ');
        
        if (choice === '5' || choice.toLowerCase() === 'exit') {
            console.log('Goodbye! 👋');
            break;
        }

        let urls = [];
        const toScrapeDir = path.join(process.cwd(), 'to scrape');

        // --- OPTION 1: SCRAPE ARTICLES ---
        if (choice === '1') {
            console.log('\n--- 🚀 Scrape Articles ---');
            console.log('1. Paste a single URL');
            console.log('2. Paste multiple URLs (one per line)');
            console.log('3. Scrape URLs from input.txt');
            console.log('4. Process files from "to scrape" folder');
            console.log('5. Back to Main Menu');

            const subChoice = readlineSync.question('\n👉 Choice (1-5): ');

            if (subChoice === '1') {
                const url = readlineSync.question('Enter URL: ');
                if (url) {
                    await scrapeSingleUrlAndProcess(url);
                }
            } else if (subChoice === '2') {
                console.log('Enter URLs (end with empty line):');
                const batchUrls = [];
                while (true) {
                    const u = readlineSync.question('> ');
                    if (!u) break;
                    batchUrls.push(u);
                }
                if (batchUrls.length > 0) {
                    await runScraper(batchUrls, null, false);
                }
            } else if (subChoice === '3') {
                const inputFile = path.join(process.cwd(), 'input.txt');
                await scrapeUrlsFromInputFile(inputFile);
            } else if (subChoice === '4') {
                await processToScrapeFolder(toScrapeDir);
            }
        }

        // --- OPTION 2: SITEMAP TOOLS ---
        else if (choice === '2') {
            console.log('\n--- 🗺️ Sitemap Tools ---');
            console.log('1. Scan New XML Sitemap (Recursive)');
            console.log('2. Bulk Scrape from sitemaps.txt');
            console.log('3. Rescan Saved Sitemaps for New URLs');
            console.log('4. Back to Main Menu');

            const subChoice = readlineSync.question('\n👉 Choice (1-4): ');

            if (subChoice === '1') {
                await runSitemapScraperCLI();
            } else if (subChoice === '2') {
                await runBulkSitemapScraper();
            } else if (subChoice === '3') {
                await rescanSavedSitemaps();
            }
        }

        // --- OPTION 3: MANAGE RESULTS ---
        else if (choice === '3') {
            console.log('\n--- 📊 Manage Results ---');
            console.log('1. Export URLs (Success/Failed/etc)');
            console.log('2. Retry Failed & No Result URLs');
            console.log('3. Back to Main Menu');

            const subChoice = readlineSync.question('\n👉 Choice (1-3): ');

            if (subChoice === '1') {
                console.log('\n👉 Select Export Status:');
                console.log('1. Success');
                console.log('2. Failed');
                console.log('3. No Result');
                console.log('4. All');
                
                const exportChoice = readlineSync.question('Choice (1-4): ');
                let status = '';

                switch(exportChoice) {
                    case '1': status = 'Success'; break;
                    case '2': status = 'Failed'; break;
                    case '3': status = 'No Result'; break;
                    case '4': status = 'All'; break;
                    default: console.log('Invalid choice.'); break;
                }

                if (status) {
                    const exportedFilePath = exportUrlsByStatus(status);
                    if (exportedFilePath) {
                        console.log(`Exported to: ${exportedFilePath}`);
                    }
                }
            } else if (subChoice === '2') {
                await retryFailedAndNoResultUrls();
            }
        }

        // --- OPTION 4: DAEMON MODE ---
        else if (choice === '4') {
            await runDaemonMode(toScrapeDir);
        }
    }
}

async function runDaemonMode(toScrapeDir) {
    console.log('\n👻 Daemon Mode Activated');
    console.log('   Watching "to scrape" folder for new files...');
    
    if (!fs.existsSync(toScrapeDir)) fs.mkdirSync(toScrapeDir);

    while (true) {
        try {
            const files = fs.readdirSync(toScrapeDir).filter(f => f.endsWith('.txt') || f.endsWith('.csv'));
            
            if (files.length > 0) {
                console.log(`\n📂 Found ${files.length} file(s). Processing...`);
                for (const file of files) {
                    const filePath = path.join(toScrapeDir, file);
                    const content = fs.readFileSync(filePath, 'utf8');
                    const extractedUrls = [];
                    
                    content.split('\n').forEach(line => {
                        const trimmed = line.trim();
                        if (trimmed && trimmed.startsWith('http')) extractedUrls.push(trimmed);
                        else if (trimmed.includes(',')) {
                            const parts = trimmed.split(',');
                            const u = parts.find(p => p.trim().startsWith('http'));
                            if (u) extractedUrls.push(u.trim());
                        }
                    });

                    if (extractedUrls.length > 0) {
                        // In Daemon mode, we usually don't force, unless logic requires it. 
                        // But new files implies new requests.
                        // However, if they are duplicates, we might want to skip?
                        // Let's assume standard behavior (no force) but log it.
                        await runScraper(extractedUrls, filePath, false);
                    } else {
                        fs.unlinkSync(filePath);
                    }
                }
            }
        } catch (e) {
            console.error(`Daemon Error: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 30000));
    }
}

import { fileURLToPath } from 'url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    main();
}
