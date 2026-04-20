import fs from 'fs';
import path from 'path';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';
import { urlExists, enqueueUrls, makeBatchId } from './db.js';

function isoNow() {
    return new Date().toISOString();
}

function log(level, message, extra = undefined) {
    const prefix = `[${isoNow()}] [${level}]`;
    if (extra !== undefined) {
        console.log(prefix, message, extra);
        return;
    }
    console.log(prefix, message);
}

function safeMkdir(dirPath) {
    if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function readJsonFileIfExists(filePath, fallback) {
    try {
        if (!fs.existsSync(filePath)) return fallback;
        const raw = fs.readFileSync(filePath, 'utf8');
        return JSON.parse(raw);
    } catch {
        return fallback;
    }
}

function writeJsonFileAtomic(filePath, value) {
    const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
    fs.renameSync(tmp, filePath);
}

function compileRegexList(patterns) {
    if (!Array.isArray(patterns) || patterns.length === 0) return [];
    return patterns.map((p) => new RegExp(p));
}

function escapeRegex(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeUrl(inputUrl, baseUrl, keepQueryString = false) {
    const u = new URL(inputUrl, baseUrl);
    u.hash = '';
    if (!keepQueryString) u.search = '';
    u.username = '';
    u.password = '';
    const normalized = u.toString();
    if (normalized.endsWith('/') && u.pathname !== '/') {
        return normalized.slice(0, -1);
    }
    return normalized;
}

function isHttpUrl(urlStr) {
    try {
        const u = new URL(urlStr);
        return u.protocol === 'http:' || u.protocol === 'https:';
    } catch {
        return false;
    }
}

function pathHasIgnoredExtension(urlStr, excludedExtensions) {
    try {
        const u = new URL(urlStr);
        const lowerPath = u.pathname.toLowerCase();
        return excludedExtensions.some((ext) => lowerPath.endsWith(ext));
    } catch {
        return true;
    }
}

function matchesAny(regexList, value) {
    return regexList.some((r) => r.test(value));
}

function parsePublishedDateToIso(dateStr) {
    const s = String(dateStr || '').trim();
    if (!s) return null;
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (ymd) {
        return `${ymd[1]}-${ymd[2]}-${ymd[3]}T00:00:00.000Z`;
    }

    const monthDayYear = s.match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
    if (monthDayYear) {
        const monthName = monthDayYear[1].toLowerCase();
        const day = Number(monthDayYear[2]);
        const year = Number(monthDayYear[3]);
        const months = {
            january: 0,
            february: 1,
            march: 2,
            april: 3,
            may: 4,
            june: 5,
            july: 6,
            august: 7,
            september: 8,
            october: 9,
            november: 10,
            december: 11
        };
        const monthIndex = months[monthName];
        if (monthIndex !== undefined && Number.isFinite(day) && Number.isFinite(year)) {
            return new Date(Date.UTC(year, monthIndex, day)).toISOString();
        }
    }

    const ms = Date.parse(s);
    if (!Number.isFinite(ms)) return null;
    return new Date(ms).toISOString();
}

function escapeCsvCell(value) {
    const s = value === null || value === undefined ? '' : String(value);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
}

function formatPublishedAtShort(isoString) {
    if (!isoString) return '';
    const d = new Date(isoString);
    if (!Number.isFinite(d.getTime())) return '';
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const m = months[d.getUTCMonth()];
    const day = d.getUTCDate();
    const year = d.getUTCFullYear();
    return `${m} ${day} ${year}`;
}

function formatScrapedAt(date = new Date()) {
    return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(date);
}

function trimTrailingSlash(p) {
    if (p === '/') return '/';
    return p.endsWith('/') ? p.slice(0, -1) : p;
}

const MEDIUM_RESERVED_FIRST_SEGMENTS = new Set([
    'about',
    'feed',
    'jobs-at-medium',
    'm',
    'me',
    'members-only',
    'notifications',
    'p',
    'search',
    'signin',
    'tag',
    'topics'
]);

function getMediumPublicationFeedInfo(pageUrl) {
    try {
        const parsed = new URL(pageUrl);
        if (parsed.hostname !== 'medium.com') return null;

        const segments = parsed.pathname.split('/').filter(Boolean);
        if (segments.length !== 1) return null;

        const publicationSlug = segments[0];
        if (!publicationSlug || MEDIUM_RESERVED_FIRST_SEGMENTS.has(publicationSlug.toLowerCase())) {
            return null;
        }

        return {
            publicationSlug,
            originalUrl: normalizeUrl(pageUrl, pageUrl, true),
            feedUrl: `https://medium.com/feed/${publicationSlug}`
        };
    } catch {
        return null;
    }
}

function mergeTrackedRows(byUrl, rows) {
    for (const row of rows) {
        if (!row?.url) continue;
        const existing = byUrl.get(row.url);
        if (!existing) {
            byUrl.set(row.url, {
                url: row.url,
                publishedAt: row.publishedAt || null,
                sourcePaths: new Set(Array.from(row.sourcePaths ?? []).filter(Boolean))
            });
            continue;
        }

        if (!existing.publishedAt && row.publishedAt) {
            existing.publishedAt = row.publishedAt;
        }

        for (const sourcePath of row.sourcePaths ?? []) {
            if (sourcePath) existing.sourcePaths.add(sourcePath);
        }
    }
}

function collectNewUrlsFromRows(rows, seenSet, newlyDiscovered) {
    for (const row of rows) {
        const link = row?.url;
        if (!link) continue;
        if (seenSet.has(link)) continue;
        if (urlExists(link)) {
            seenSet.add(link);
            continue;
        }
        newlyDiscovered.push(link);
        seenSet.add(link);
    }
}

function extractFeedRowsFromXml(xml, startUrl, keepQueryString, sourcePath) {
    const $ = cheerio.load(xml, { xmlMode: true });
    const rssItems = $('item')
        .map((_, el) => {
            const link = ($(el).find('link').first().text() || '').trim();
            const pubDate = ($(el).find('pubDate').first().text() || '').trim();
            const dcDate = ($(el).find('dc\\:date').first().text() || '').trim();
            const publishedAt = parsePublishedDateToIso(pubDate) || parsePublishedDateToIso(dcDate) || null;
            return { link, publishedAt };
        })
        .get()
        .filter((x) => x.link);

    const atomEntries = $('entry')
        .map((_, el) => {
            const link = ($(el).find('link[rel="alternate"]').first().attr('href') || '').trim();
            const published = ($(el).find('published').first().text() || '').trim();
            const updated = ($(el).find('updated').first().text() || '').trim();
            const publishedAt = parsePublishedDateToIso(published) || parsePublishedDateToIso(updated) || null;
            return { link, publishedAt };
        })
        .get()
        .filter((x) => x.link);

    const rows = [];
    for (const item of [...rssItems, ...atomEntries]) {
        try {
            rows.push({
                url: normalizeUrl(item.link, startUrl, keepQueryString),
                publishedAt: item.publishedAt,
                sourcePaths: new Set([sourcePath])
            });
        } catch {
        }
    }

    return rows;
}

function buildMediumPublicationArticleRegex(publicationSlug) {
    const escapedSlug = escapeRegex(publicationSlug);
    return new RegExp(`https?:\\/\\/medium\\.com\\/${escapedSlug}\\/[a-z0-9-]+-[a-f0-9]{8,}`, 'gi');
}

function extractMediumJsonRows(rawText, publicationSlug, sourcePath) {
    const cleaned = String(rawText || '')
        .replace(/^\]\)\}while\(1\);<\/x>/, '')
        .replace(/\\\//g, '/');
    const matches = cleaned.match(buildMediumPublicationArticleRegex(publicationSlug)) || [];
    const uniqueRows = new Map();

    for (const matchedUrl of matches) {
        try {
            const normalized = normalizeUrl(matchedUrl, matchedUrl, false);
            if (!uniqueRows.has(normalized)) {
                uniqueRows.set(normalized, {
                    url: normalized,
                    publishedAt: null,
                    sourcePaths: new Set([sourcePath])
                });
            }
        } catch {
        }
    }

    return Array.from(uniqueRows.values());
}

async function processMediumPublicationTrackedPage(entry, configDefaults, state, mediumFeedInfo) {
    const name = entry.name || entry.startUrl;
    const startUrl = entry.startUrl;
    const outputBase = entry.outputFileBase || outputBaseFromStartUrl(startUrl);
    const sourcePath = trimTrailingSlash(new URL(startUrl).pathname || '/');

    const timeoutMs = entry.timeoutMs ?? configDefaults.timeoutMs ?? 30000;
    const retryLimit = entry.retryLimit ?? configDefaults.retryLimit ?? 2;
    const maxPages = entry.maxPages ?? configDefaults.maxPages ?? 10;
    const sameHostOnly = entry.sameHostOnly ?? configDefaults.sameHostOnly ?? true;
    const keepQueryString = entry.keepQueryString ?? configDefaults.keepQueryString ?? false;
    const excludedExtensions = entry.excludedExtensions ?? configDefaults.excludedExtensions ?? [];
    const aggressiveLinkExtraction = entry.aggressiveLinkExtraction ?? configDefaults.aggressiveLinkExtraction ?? false;

    const includePathRegex = (Array.isArray(entry.includePathRegex) && entry.includePathRegex.length > 0)
        ? entry.includePathRegex
        : [`^/${escapeRegex(mediumFeedInfo.publicationSlug)}/.+-[a-f0-9]{8,}$`];
    const excludePathRegex = [
        ...(configDefaults.excludePathRegex ?? []),
        ...(entry.excludePathRegex ?? []),
        '^/$',
        '^/about(?:/.*)?$',
        '^/feed(?:/.*)?$',
        '^/followers(?:/.*)?$',
        '^/jobs-at-medium(?:/.*)?$',
        '^/m(?:/.*)?$',
        '^/notifications(?:/.*)?$',
        '^/p/[a-f0-9]+$',
        '^/search(?:/.*)?$',
        '^/signin(?:/.*)?$',
        '^/tag(?:/.*)?$',
        `^/${escapeRegex(mediumFeedInfo.publicationSlug)}/about(?:/.*)?$`,
        `^/${escapeRegex(mediumFeedInfo.publicationSlug)}/followers(?:/.*)?$`,
        `^/${escapeRegex(mediumFeedInfo.publicationSlug)}/subpage(?:/.*)?$`
    ];

    const seenSet = new Set(state.seen?.[name] ?? []);
    const newlyDiscovered = [];
    const byUrl = new Map();

    try {
        log('INFO', `Fetching Medium publication feed: ${mediumFeedInfo.feedUrl}`);
        const feedXml = await fetchText(mediumFeedInfo.feedUrl, { timeoutMs, retryLimit });
        const feedRows = extractFeedRowsFromXml(feedXml, mediumFeedInfo.feedUrl, keepQueryString, sourcePath);
        mergeTrackedRows(byUrl, feedRows);
        collectNewUrlsFromRows(feedRows, seenSet, newlyDiscovered);
    } catch (err) {
        log('ERROR', `Failed to fetch Medium feed ${mediumFeedInfo.feedUrl}`, err?.message || String(err));
    }

    try {
        const mediumJsonUrl = `${startUrl}${startUrl.includes('?') ? '&' : '?'}format=json`;
        log('INFO', `Fetching Medium publication JSON: ${mediumJsonUrl}`);
        const rawJson = await fetchText(mediumJsonUrl, { timeoutMs, retryLimit });
        const jsonRows = extractMediumJsonRows(rawJson, mediumFeedInfo.publicationSlug, sourcePath);
        mergeTrackedRows(byUrl, jsonRows);
        collectNewUrlsFromRows(jsonRows, seenSet, newlyDiscovered);
    } catch (err) {
        log('ERROR', `Failed to fetch Medium JSON ${startUrl}`, err?.message || String(err));
    }

    let currentUrl = startUrl;
    for (let i = 0; i < maxPages; i++) {
        log('INFO', `Fetching Medium publication page: ${currentUrl}`);
        let html;
        try {
            html = await fetchText(currentUrl, { timeoutMs, retryLimit });
        } catch (err) {
            log('ERROR', `Failed to fetch ${currentUrl}`, err?.message || String(err));
            break;
        }

        let entries;
        try {
            entries = extractArticleEntriesFromHtml(html, currentUrl, {
                sameHostOnly,
                keepQueryString,
                excludedExtensions,
                includePathRegex,
                excludePathRegex,
                aggressiveLinkExtraction
            });
        } catch (err) {
            log('ERROR', `Failed to parse ${currentUrl}`, err?.message || String(err));
            break;
        }

        mergeTrackedRows(byUrl, entries.map((entryRow) => ({
            ...entryRow,
            sourcePaths: new Set([sourcePath])
        })));
        collectNewUrlsFromRows(entries, seenSet, newlyDiscovered);

        const nextUrl = extractNextPageUrlFromHtml(html, currentUrl);
        if (!nextUrl || nextUrl === currentUrl) {
            break;
        }
        currentUrl = nextUrl;
    }

    state.seen[name] = Array.from(seenSet).slice(-50000);

    const queuePath = writeQueueFile(name, newlyDiscovered);
    if (queuePath) {
        log('INFO', `Queued ${newlyDiscovered.length} new URL(s)`, queuePath);
    } else {
        log('INFO', `No new URLs found for ${name}`);
    }

    return {
        outputBase,
        rows: Array.from(byUrl.values()),
        newUrlsCount: newlyDiscovered.length,
        queuePath,
        sourceName: name,
        sourceUrl: startUrl,
        trackedPageId: entry.id ?? null
    };
}

function outputBaseFromStartUrl(startUrl) {
    const host = new URL(startUrl).hostname.replace(/^www\./, '');
    const parts = host.split('.').filter(Boolean);
    const base = parts.length >= 2 ? parts.slice(0, -1).join('.') : host;
    return `${base}_rss`;
}

function normalizeTrackedPageType(type) {
    const normalized = String(type || 'html').trim().toLowerCase();
    return normalized === 'rss' || normalized === 'atom' ? 'rss' : 'html';
}

export function buildTrackedPageEntry(entry) {
    const startUrl = String(entry?.startUrl || entry?.url || '').trim();
    const type = normalizeTrackedPageType(entry?.type || 'html');
    const outputFileBase = String(entry?.outputFileBase || entry?.outputBase || entry?.output_base || outputBaseFromStartUrl(startUrl)).trim();
    const name = String(entry?.name || `${outputFileBase}:${trimTrailingSlash(new URL(startUrl).pathname || '/')}`).trim();

    return {
        ...entry,
        name,
        type,
        startUrl,
        outputFileBase,
        aggressiveLinkExtraction: entry?.aggressiveLinkExtraction ?? true
    };
}

export async function extractTrackedPageUrlsFromSitemap(sitemapUrl, options = {}) {
    const { timeoutMs = 30000, retryLimit = 2, maxNestedSitemaps = 20 } = options;
    const visited = new Set();
    const discoveredUrls = new Set();

    const fetchSitemap = async (url, depth = 0) => {
        if (visited.has(url)) return;
        visited.add(url);

        const xml = await fetchText(url, { timeoutMs, retryLimit });
        const $ = cheerio.load(xml, { xmlMode: true });

        const pageUrls = $('url > loc')
            .map((_, el) => ($(el).text() || '').trim())
            .get()
            .filter(Boolean);

        if (pageUrls.length > 0) {
            for (const rawUrl of pageUrls) {
                try {
                    discoveredUrls.add(normalizeUrl(rawUrl, url, false));
                } catch {
                }
            }
            return;
        }

        if (depth >= 1) {
            return;
        }

        const nestedSitemaps = $('sitemap > loc')
            .map((_, el) => ($(el).text() || '').trim())
            .get()
            .filter(Boolean)
            .slice(0, maxNestedSitemaps);

        for (const nestedUrl of nestedSitemaps) {
            try {
                await fetchSitemap(normalizeUrl(nestedUrl, url, true), depth + 1);
            } catch {
            }
        }
    };

    await fetchSitemap(String(sitemapUrl || '').trim(), 0);
    return Array.from(discoveredUrls).sort((a, b) => a.localeCompare(b));
}

function parseTrackedLine(line) {
    const trimmed = line.trim();
    if (!trimmed) return null;
    if (trimmed.startsWith('#')) return null;
    if (/^outputfilebase\s*,\s*type\s*,\s*starturl/i.test(trimmed)) return null;

    let outputFileBase;
    let type;
    let startUrl;

    if (trimmed.includes(',')) {
        const parts = trimmed.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length === 1) {
            startUrl = parts[0];
            type = 'html';
            outputFileBase = outputBaseFromStartUrl(startUrl);
        } else if (parts.length === 2) {
            outputFileBase = parts[0];
            startUrl = parts[1];
            type = 'html';
        } else {
            outputFileBase = parts[0];
            type = parts[1];
            startUrl = parts.slice(2).join(',');
        }
    } else {
        const parts = trimmed.split(/\s+/).filter(Boolean);
        if (parts.length === 1) {
            startUrl = parts[0];
            type = 'html';
            outputFileBase = outputBaseFromStartUrl(startUrl);
        } else {
            outputFileBase = parts[0];
            startUrl = parts.slice(1).join(' ');
            type = 'html';
        }
    }

    try {
        const u = new URL(startUrl);
        if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    } catch {
        return null;
    }

    const name = `${outputFileBase}:${trimTrailingSlash(new URL(startUrl).pathname || '/')}`;
    return buildTrackedPageEntry({ name, type, startUrl, outputFileBase, aggressiveLinkExtraction: true });
}

function loadTrackedPagesFromUrlsFile(filePath) {
    const raw = fs.readFileSync(filePath, 'utf8');
    const lines = raw.split('\n');
    const out = [];

    for (const line of lines) {
        const parsed = parseTrackedLine(line);
        if (!parsed) continue;
        out.push(parsed);
    }

    return out;
}

function parseCsvFirstCell(line) {
    const s = String(line || '');
    if (!s) return '';
    if (s[0] !== '"') {
        const idx = s.indexOf(',');
        return (idx === -1 ? s : s.slice(0, idx)).trim();
    }

    let i = 1;
    let out = '';
    while (i < s.length) {
        const ch = s[i];
        if (ch === '"') {
            const next = s[i + 1];
            if (next === '"') {
                out += '"';
                i += 2;
                continue;
            }
            break;
        }
        out += ch;
        i++;
    }
    return out.trim();
}

function writeDomainCsv({ outputBase, rows, scrapedAt }) {
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const rssDir = path.join(process.cwd(), 'rss');
    safeMkdir(rssDir);
    const filePath = path.join(rssDir, `${slugifyName(outputBase)}.csv`);

    const header = 'url,published_at_short,scraped_at,source_paths';
    const existingUrls = new Set();
    let hasFile = false;
    if (fs.existsSync(filePath)) {
        hasFile = true;
        try {
            const content = fs.readFileSync(filePath, 'utf8');
            const lines = content.split('\n');
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (!line) continue;
                existingUrls.add(parseCsvFirstCell(line));
            }
        } catch {
        }
    }

    const newLines = [];
    if (!hasFile) newLines.push(header);

    for (const row of rows) {
        if (!row?.url) continue;
        if (existingUrls.has(row.url)) continue;
        existingUrls.add(row.url);

        const publishedAtShort = formatPublishedAtShort(row.publishedAt || '');
        const sourcePaths = Array.from(row.sourcePaths ?? []).filter(Boolean).sort().join('|');
        newLines.push(
            `${escapeCsvCell(row.url)},${escapeCsvCell(publishedAtShort)},${escapeCsvCell(scrapedAt)},${escapeCsvCell(sourcePaths)}`
        );
    }

    if (newLines.length === 0 || (newLines.length === 1 && newLines[0] === header)) return null;
    if (hasFile) {
        fs.appendFileSync(filePath, `\n${newLines.join('\n')}`.replace(/^\n+/, '\n'));
    } else {
        fs.writeFileSync(filePath, newLines.join('\n'));
    }
    return filePath;
}

function extractPublishedAtFromArticleNode($, articleNode) {
    const timeEl = $(articleNode).find('time[datetime]').first();
    if (timeEl.length > 0) {
        const dt = (timeEl.attr('datetime') || '').trim();
        const iso = parsePublishedDateToIso(dt);
        if (iso) return iso;
    }

    const timeTextEl = $(articleNode).find('time').first();
    if (timeTextEl.length > 0) {
        const txt = (timeTextEl.text() || '').trim();
        const iso = parsePublishedDateToIso(txt);
        if (iso) return iso;
    }

    const dateText = $(articleNode)
        .find('.elementor-post-date, .date, .post-date, .entry-date, .published, .meta-date, .jeg_meta_date')
        .first()
        .text()
        .trim();
    const iso = parsePublishedDateToIso(dateText);
    if (iso) return iso;

    return null;
}

function collectJsonValues(obj, out) {
    if (obj === null || obj === undefined) return;
    if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean') return;
    if (Array.isArray(obj)) {
        for (const item of obj) collectJsonValues(item, out);
        return;
    }
    if (typeof obj !== 'object') return;

    const possibleUrls = [];
    if (typeof obj.url === 'string') possibleUrls.push(obj.url);
    if (typeof obj['@id'] === 'string') possibleUrls.push(obj['@id']);
    if (typeof obj.mainEntityOfPage === 'string') possibleUrls.push(obj.mainEntityOfPage);
    if (typeof obj.mainEntityOfPage === 'object' && typeof obj.mainEntityOfPage['@id'] === 'string') {
        possibleUrls.push(obj.mainEntityOfPage['@id']);
    }
    if (typeof obj.item === 'string') possibleUrls.push(obj.item);
    if (typeof obj.item === 'object' && typeof obj.item.url === 'string') possibleUrls.push(obj.item.url);
    if (typeof obj.item === 'object' && typeof obj.item['@id'] === 'string') possibleUrls.push(obj.item['@id']);

    let publishedAt = null;
    if (typeof obj.datePublished === 'string') publishedAt = parsePublishedDateToIso(obj.datePublished);
    if (!publishedAt && typeof obj.dateCreated === 'string') publishedAt = parsePublishedDateToIso(obj.dateCreated);
    if (!publishedAt && typeof obj.dateModified === 'string') publishedAt = parsePublishedDateToIso(obj.dateModified);

    for (const u of possibleUrls) {
        if (u) out.push({ url: String(u), publishedAt });
    }

    for (const k of Object.keys(obj)) {
        collectJsonValues(obj[k], out);
    }
}

function extractEntriesFromJsonLd($, pageUrl, keepQueryString) {
    const out = [];
    const scripts = $('script[type="application/ld+json"], script#ld-json, script[id="__NEXT_DATA__"]');
    scripts.each((_, el) => {
        const raw = ($(el).text() || '').trim();
        if (!raw) return;
        try {
            const parsed = JSON.parse(raw);
            const collected = [];
            collectJsonValues(parsed, collected);
            for (const c of collected) {
                let abs;
                try {
                    abs = normalizeUrl(c.url, pageUrl, keepQueryString);
                } catch {
                    continue;
                }
                out.push({ url: abs, publishedAt: c.publishedAt });
            }
        } catch {
        }
    });
    return out;
}

export function extractArticleEntriesFromHtml(html, pageUrl, options) {
    const {
        sameHostOnly,
        keepQueryString,
        excludedExtensions,
        includePathRegex,
        excludePathRegex,
        aggressiveLinkExtraction = false
    } = options;

    const includeList = compileRegexList(includePathRegex);
    const excludeList = compileRegexList(excludePathRegex);

    const pageHost = new URL(pageUrl).host;
    const $ = cheerio.load(html);

    const candidates = [];
    $('a[href]').each((_, el) => {
        const rawHref = $(el).attr('href');
        if (!rawHref) return;
        if (rawHref.startsWith('mailto:') || rawHref.startsWith('tel:') || rawHref.startsWith('javascript:')) return;

        let absolute;
        try {
            absolute = normalizeUrl(rawHref, pageUrl, keepQueryString);
        } catch {
            return;
        }

        if (!isHttpUrl(absolute)) return;
        if (sameHostOnly) {
            const host = new URL(absolute).host;
            if (host !== pageHost) return;
        }
        if (pathHasIgnoredExtension(absolute, excludedExtensions)) return;

        const u = new URL(absolute);
        const pathName = u.pathname;
        if (excludeList.length > 0 && matchesAny(excludeList, pathName)) return;
        if (includeList.length > 0 && !matchesAny(includeList, pathName)) return;

        const article = $(el).closest('article');
        const isInsideArticle = article.length > 0;
        if (!isInsideArticle && includeList.length === 0 && !aggressiveLinkExtraction) return;

        if (
            aggressiveLinkExtraction &&
            includeList.length === 0 &&
            $(el).closest('nav, header, footer, aside').length > 0
        ) {
            return;
        }

        const publishedAt = isInsideArticle ? extractPublishedAtFromArticleNode($, article) : null;
        candidates.push({ url: absolute, publishedAt });
    });

    candidates.push(...extractEntriesFromJsonLd($, pageUrl, keepQueryString));

    const byUrl = new Map();
    for (const c of candidates) {
        const existing = byUrl.get(c.url);
        if (!existing) {
            byUrl.set(c.url, c);
            continue;
        }
        if (!existing.publishedAt && c.publishedAt) {
            byUrl.set(c.url, c);
        }
    }

    return Array.from(byUrl.values());
}

export function extractNextPageUrlFromHtml(html, currentUrl) {
    const $ = cheerio.load(html);
    const relNext = $('a[rel="next"]').attr('href') || $('link[rel="next"]').attr('href');
    if (relNext) return normalizeUrl(relNext, currentUrl, true);

    const ariaNext = $('a[aria-label*="Next"], a[aria-label*="next"]').first().attr('href');
    if (ariaNext) return normalizeUrl(ariaNext, currentUrl, true);

    const classNext = $('a.next, a.next.page-numbers, li.next a').first().attr('href');
    if (classNext) return normalizeUrl(classNext, currentUrl, true);

    const textNext = $('a')
        .filter((_, el) => ($(el).text() || '').trim().toLowerCase() === 'next')
        .first()
        .attr('href');
    if (textNext) return normalizeUrl(textNext, currentUrl, true);

    return null;
}

export function extractArticleLinksFromHtml(html, pageUrl, options) {
    return extractArticleEntriesFromHtml(html, pageUrl, options).map((e) => e.url);
}

async function fetchText(url, { timeoutMs, retryLimit }) {
    const response = await gotScraping({
        url,
        timeout: { request: timeoutMs },
        retry: { limit: retryLimit },
        http2: false
    });
    return response.body;
}

function loadConfig(configPath) {
    const raw = fs.readFileSync(configPath, 'utf8');
    return JSON.parse(raw);
}

function getDefaultConfigPath() {
    return path.join(process.cwd(), 'page_tracker.config.json');
}

function getStatePath() {
    return path.join(process.cwd(), '.page_tracker_state.json');
}

function loadState() {
    return readJsonFileIfExists(getStatePath(), { seen: {} });
}

function saveState(state) {
    writeJsonFileAtomic(getStatePath(), state);
}

function slugifyName(name) {
    return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

/**
 * Enqueue newly-discovered tracked-page URLs into the DB work queue.
 * Returns a "locator" (the batch id) so callers can log where the URLs live.
 * Kept the name `writeQueueFile` for call-site compatibility; it no longer
 * touches the `to scrape/` directory — that path is deprecated.
 */
function writeQueueFile(name, urls) {
    if (!Array.isArray(urls) || urls.length === 0) return null;
    const slug = slugifyName(name) || 'tracked_page';
    const batchId = makeBatchId('page', slug);
    const { inserted } = enqueueUrls(urls, { source: `page:${slug}`, batchId });
    if (inserted === 0) return null;
    return batchId;
}

async function processHtmlTrackedPage(entry, configDefaults, state) {
    const name = entry.name || entry.startUrl;
    const startUrl = entry.startUrl;
    const outputBase = entry.outputFileBase || outputBaseFromStartUrl(startUrl);
    const sourcePath = trimTrailingSlash(new URL(startUrl).pathname || '/');
    const mediumFeedInfo = getMediumPublicationFeedInfo(startUrl);

    if (mediumFeedInfo) {
        return processMediumPublicationTrackedPage(entry, configDefaults, state, mediumFeedInfo);
    }

    const timeoutMs = entry.timeoutMs ?? configDefaults.timeoutMs ?? 30000;
    const retryLimit = entry.retryLimit ?? configDefaults.retryLimit ?? 2;
    const maxPages = entry.maxPages ?? configDefaults.maxPages ?? 10;
    const sameHostOnly = entry.sameHostOnly ?? configDefaults.sameHostOnly ?? true;
    const keepQueryString = entry.keepQueryString ?? configDefaults.keepQueryString ?? false;
    const excludedExtensions = entry.excludedExtensions ?? configDefaults.excludedExtensions ?? [];
    const aggressiveLinkExtraction = entry.aggressiveLinkExtraction ?? configDefaults.aggressiveLinkExtraction ?? false;

    const includePathRegex = entry.includePathRegex ?? [];
    const excludePathRegex = [...(configDefaults.excludePathRegex ?? []), ...(entry.excludePathRegex ?? [])];

    const seenSet = new Set(state.seen?.[name] ?? []);
    const newlyDiscovered = [];
    const extractedRows = [];

    let currentUrl = startUrl;
    for (let i = 0; i < maxPages; i++) {
        log('INFO', `Fetching tracked page: ${currentUrl}`);
        let html;
        try {
            html = await fetchText(currentUrl, { timeoutMs, retryLimit });
        } catch (err) {
            log('ERROR', `Failed to fetch ${currentUrl}`, err?.message || String(err));
            break;
        }

        let entries;
        try {
            entries = extractArticleEntriesFromHtml(html, currentUrl, {
                sameHostOnly,
                keepQueryString,
                excludedExtensions,
                includePathRegex,
                excludePathRegex,
                aggressiveLinkExtraction
            });
        } catch (err) {
            log('ERROR', `Failed to parse ${currentUrl}`, err?.message || String(err));
            break;
        }

        extractedRows.push(...entries);
        for (const { url: link } of entries) {
            if (seenSet.has(link)) continue;
            if (urlExists(link)) {
                seenSet.add(link);
                continue;
            }
            newlyDiscovered.push(link);
            seenSet.add(link);
        }

        const nextUrl = extractNextPageUrlFromHtml(html, currentUrl);
        if (!nextUrl) break;
        if (nextUrl === currentUrl) break;
        currentUrl = nextUrl;
    }

    state.seen[name] = Array.from(seenSet).slice(-50000);

    const queuePath = writeQueueFile(name, newlyDiscovered);
    if (queuePath) {
        log('INFO', `Queued ${newlyDiscovered.length} new URL(s)`, queuePath);
    } else {
        log('INFO', `No new URLs found for ${name}`);
    }

    const byUrl = new Map();
    for (const row of extractedRows) {
        const existing = byUrl.get(row.url);
        if (!existing) {
            byUrl.set(row.url, { url: row.url, publishedAt: row.publishedAt, sourcePaths: new Set([sourcePath]) });
            continue;
        }
        if (!existing.publishedAt && row.publishedAt) {
            existing.publishedAt = row.publishedAt;
        }
        existing.sourcePaths.add(sourcePath);
    }

    return {
        outputBase,
        rows: Array.from(byUrl.values()),
        newUrlsCount: newlyDiscovered.length,
        queuePath,
        sourceName: name,
        sourceUrl: startUrl,
        trackedPageId: entry.id ?? null
    };
}

async function processRssTrackedPage(entry, configDefaults, state) {
    const name = entry.name || entry.startUrl;
    const startUrl = entry.startUrl;
    const originalTrackedUrl = entry.originalTrackedUrl || startUrl;
    const outputBase = entry.outputFileBase || outputBaseFromStartUrl(startUrl);
    const sourcePath = entry.sourcePathOverride || trimTrailingSlash(new URL(originalTrackedUrl).pathname || '/');

    const timeoutMs = entry.timeoutMs ?? configDefaults.timeoutMs ?? 30000;
    const retryLimit = entry.retryLimit ?? configDefaults.retryLimit ?? 2;
    const keepQueryString = entry.keepQueryString ?? configDefaults.keepQueryString ?? false;

    const seenSet = new Set(state.seen?.[name] ?? []);
    const newlyDiscovered = [];

    log('INFO', `Fetching RSS/Atom feed: ${startUrl}`);
    let xml;
    try {
        xml = await fetchText(startUrl, { timeoutMs, retryLimit });
    } catch (err) {
        log('ERROR', `Failed to fetch feed ${startUrl}`, err?.message || String(err));
        return;
    }

    let rssRows = [];
    try {
        rssRows = extractFeedRowsFromXml(xml, startUrl, keepQueryString, sourcePath);
    } catch (err) {
        log('ERROR', `Failed to parse feed ${startUrl}`, err?.message || String(err));
        return;
    }

    collectNewUrlsFromRows(rssRows, seenSet, newlyDiscovered);

    state.seen[name] = Array.from(seenSet).slice(-50000);
    const queuePath = writeQueueFile(name, newlyDiscovered);
    if (queuePath) {
        log('INFO', `Queued ${newlyDiscovered.length} new URL(s)`, queuePath);
    } else {
        log('INFO', `No new URLs found for ${name}`);
    }

    return {
        outputBase,
        rows: rssRows,
        newUrlsCount: newlyDiscovered.length,
        queuePath,
        sourceName: name,
        sourceUrl: originalTrackedUrl,
        trackedPageId: entry.id ?? null
    };
}

async function runTrackedEntries({ tracked, configDefaults, state, scrapedAt, concurrency, onProgress }) {
    const startedAt = Date.now();
    const emitProgress = async (payload) => {
        if (typeof onProgress === 'function') {
            await onProgress(payload);
        }
    };

    const aggregatedByOutput = new Map();
    const queue = [...tracked];
    const summary = {
        totalSources: tracked.length,
        processedSources: 0,
        currentName: tracked[0]?.name || '',
        currentUrl: tracked[0]?.startUrl || '',
        newUrlsFound: 0,
        queuedFiles: 0,
        errors: 0,
        elapsedMs: 0,
        sourceSummaries: []
    };

    summary.elapsedMs = Date.now() - startedAt;
    await emitProgress({ stage: 'start', summary: { ...summary } });

    const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
        while (queue.length > 0) {
            const entry = queue.shift();
            if (!entry) return;
            const type = (entry.type || 'html').toLowerCase();
            summary.currentName = entry.name || entry.startUrl;
            summary.currentUrl = entry.startUrl || '';
            summary.elapsedMs = Date.now() - startedAt;
            await emitProgress({ stage: 'scanning', summary: { ...summary } });
            try {
                let result = null;
                if (type === 'rss' || type === 'atom') {
                    result = await processRssTrackedPage(entry, configDefaults, state);
                    if (result?.outputBase) {
                        if (!aggregatedByOutput.has(result.outputBase)) aggregatedByOutput.set(result.outputBase, new Map());
                        const map = aggregatedByOutput.get(result.outputBase);
                        for (const row of result.rows || []) {
                            const existing = map.get(row.url);
                            if (!existing) {
                                map.set(row.url, row);
                                continue;
                            }
                            if (!existing.publishedAt && row.publishedAt) existing.publishedAt = row.publishedAt;
                            for (const p of row.sourcePaths ?? []) existing.sourcePaths?.add?.(p);
                        }
                    }
                } else {
                    result = await processHtmlTrackedPage(entry, configDefaults, state);
                    if (result?.outputBase) {
                        if (!aggregatedByOutput.has(result.outputBase)) aggregatedByOutput.set(result.outputBase, new Map());
                        const map = aggregatedByOutput.get(result.outputBase);
                        for (const row of result.rows || []) {
                            const existing = map.get(row.url);
                            if (!existing) {
                                map.set(row.url, row);
                                continue;
                            }
                            if (!existing.publishedAt && row.publishedAt) existing.publishedAt = row.publishedAt;
                            for (const p of row.sourcePaths ?? []) existing.sourcePaths?.add?.(p);
                        }
                    }
                }

                summary.newUrlsFound += result?.newUrlsCount || 0;
                summary.queuedFiles += result?.queuePath ? 1 : 0;
                summary.sourceSummaries.push({
                    trackedPageId: result?.trackedPageId ?? entry.id ?? null,
                    name: result?.sourceName || entry.name || entry.startUrl,
                    startUrl: result?.sourceUrl || entry.startUrl,
                    status: 'success',
                    newUrlsCount: result?.newUrlsCount || 0,
                    queuePath: result?.queuePath || null
                });
            } catch (err) {
                log('ERROR', `Unhandled error for ${entry.name || entry.startUrl}`, err?.message || String(err));
                summary.errors += 1;
                summary.sourceSummaries.push({
                    trackedPageId: entry.id ?? null,
                    name: entry.name || entry.startUrl,
                    startUrl: entry.startUrl,
                    status: 'error',
                    newUrlsCount: 0,
                    queuePath: null,
                    error: err?.message || String(err)
                });
            } finally {
                summary.processedSources += 1;
                summary.elapsedMs = Date.now() - startedAt;
                await emitProgress({ stage: 'progress', summary: { ...summary } });
            }
        }
    });

    await Promise.all(workers);
    for (const [outputBase, map] of aggregatedByOutput.entries()) {
        const filePath = writeDomainCsv({ outputBase, rows: Array.from(map.values()), scrapedAt });
        if (filePath) {
            log('INFO', `Saved ${map.size} URL(s)`, filePath);
        }
    }
    saveState(state);
    summary.elapsedMs = Date.now() - startedAt;
    await emitProgress({ stage: 'complete', summary: { ...summary } });
    return summary;
}

export async function runTrackedPagesOnce({ configPath = getDefaultConfigPath(), trackedPages = null, urlsFile = null, onProgress = null, concurrencyOverride = null } = {}) {
    const hasConfig = fs.existsSync(configPath);
    const config = hasConfig ? loadConfig(configPath) : {};
    const configDefaults = config.defaults ?? {};
    const tracked = trackedPages
        ? trackedPages.map((entry) => entry?.startUrl ? buildTrackedPageEntry(entry) : buildTrackedPageEntry({
            ...entry,
            startUrl: entry?.url || entry?.startUrl,
            outputBase: entry?.output_base || entry?.outputBase || entry?.outputFileBase
        }))
        : (urlsFile
            ? loadTrackedPagesFromUrlsFile(urlsFile)
            : (Array.isArray(config.trackedPages) ? config.trackedPages.map((entry) => buildTrackedPageEntry(entry)) : []));

    if (tracked.length === 0) {
        log('ERROR', 'No trackedPages found in config.');
        return {
            totalSources: 0,
            processedSources: 0,
            currentName: '',
            currentUrl: '',
            newUrlsFound: 0,
            queuedFiles: 0,
            errors: 1,
            elapsedMs: 0,
            sourceSummaries: []
        };
    }

    const state = loadState();
    if (!state.seen) state.seen = {};

    const scrapedAt = formatScrapedAt();
    const concurrency = concurrencyOverride ?? (urlsFile ? 1 : Math.max(1, Number(config.concurrency ?? 2)));

    return runTrackedEntries({ tracked, configDefaults, state, scrapedAt, concurrency, onProgress });
}

async function runOnce({ configPath, urlsFile }) {
    const summary = await runTrackedPagesOnce({ configPath, urlsFile });
    if (summary.totalSources === 0) {
        process.exitCode = 1;
    }
}

function parseArgs(argv) {
    const out = {
        configPath: getDefaultConfigPath(),
        urlsFile: null,
        watch: false,
        intervalMs: 24 * 60 * 60 * 1000
    };
    for (let i = 2; i < argv.length; i++) {
        const a = argv[i];
        if (a === '--config' && argv[i + 1]) {
            out.configPath = path.resolve(argv[i + 1]);
            i++;
            continue;
        }
        if (a === '--urls-file' && argv[i + 1]) {
            out.urlsFile = path.resolve(argv[i + 1]);
            i++;
            continue;
        }
        if (a === '--watch') {
            out.watch = true;
            continue;
        }
        if (a === '--interval-ms' && argv[i + 1]) {
            out.intervalMs = Math.max(1000, Number(argv[i + 1]));
            i++;
            continue;
        }
    }
    return out;
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function main() {
    const args = parseArgs(process.argv);
    const configPath = args.configPath;
    if (!fs.existsSync(configPath)) {
        log('ERROR', `Config file not found: ${configPath}`);
        process.exitCode = 1;
        return;
    }

    if (!args.urlsFile) {
        const defaultUrlsFile = path.join(process.cwd(), 'rss_sources.txt');
        if (fs.existsSync(defaultUrlsFile)) {
            args.urlsFile = defaultUrlsFile;
            log('INFO', `Using URLs file: ${args.urlsFile}`);
        }
    }

    if (args.urlsFile && !fs.existsSync(args.urlsFile)) {
        log('ERROR', `URLs file not found: ${args.urlsFile}`);
        process.exitCode = 1;
        return;
    }

    if (!args.watch) {
        await runOnce({ configPath, urlsFile: args.urlsFile });
        return;
    }

    while (true) {
        await runOnce({ configPath, urlsFile: args.urlsFile });
        log('INFO', `Sleeping for ${args.intervalMs}ms`);
        await sleep(args.intervalMs);
    }
}

import { fileURLToPath } from 'url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    main();
}
