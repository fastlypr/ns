import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DB_PATH = path.join(process.cwd(), 'history.db');
const db = new Database(DB_PATH);

/** Path to the SQLite file (used by the Telegram backup/restore feature). */
export const DB_FILE_PATH = DB_PATH;

/**
 * Close the SQLite connection. Used by the Telegram restore flow before
 * swapping `history.db` with an uploaded file. Caller is expected to
 * exit the process afterwards (systemd will restart with the new DB).
 */
export function closeDatabase() {
    try {
        db.close();
    } catch (err) {
        console.error(`Database close error: ${err.message}`);
    }
}

// Initialize Database
db.exec(`
    CREATE TABLE IF NOT EXISTS scraped_urls (
        url TEXT PRIMARY KEY,
        status TEXT,
        message TEXT,
        timestamp TEXT,
        no_unique_profile INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_status ON scraped_urls(status);

    CREATE TABLE IF NOT EXISTS discovered_sitemaps (
        url TEXT PRIMARY KEY,
        parent_url TEXT,
        last_scanned TEXT
    );
    CREATE TABLE IF NOT EXISTS tracked_pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL UNIQUE,
        type TEXT NOT NULL DEFAULT 'html',
        output_base TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT,
        last_run_at TEXT,
        last_status TEXT,
        last_new_urls INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    CREATE TABLE IF NOT EXISTS domain_variables (
        domain TEXT PRIMARY KEY,
        variable TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS queued_urls (
        url TEXT PRIMARY KEY,
        source TEXT,
        batch_id TEXT,
        status TEXT NOT NULL DEFAULT 'Queued',
        discovered_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_queued_urls_updated_at ON queued_urls(updated_at);
    -- NOTE: indexes on batch_id/status are created *after* the ALTER TABLE
    -- migration below, because existing installs may not yet have those
    -- columns when this CREATE TABLE IF NOT EXISTS is a no-op.
    CREATE TABLE IF NOT EXISTS social_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain TEXT NOT NULL,
        platform TEXT NOT NULL,
        lead_key TEXT NOT NULL,
        source_url TEXT NOT NULL,
        social_link TEXT NOT NULL,
        username TEXT,
        category TEXT,
        domain_variable TEXT,
        discovered_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(domain, platform, lead_key)
    );
    CREATE INDEX IF NOT EXISTS idx_social_leads_domain_platform ON social_leads(domain, platform);
`);

// Add new column to scraped_urls if it doesn't exist (Migration)
try {
    const columns = db.prepare("PRAGMA table_info(scraped_urls)").all();
    if (!columns.some(c => c.name === 'no_unique_profile')) {
        db.exec("ALTER TABLE scraped_urls ADD COLUMN no_unique_profile INTEGER DEFAULT 0");
    }
} catch (e) {
    // Column likely already exists
}

// Migration: add batch_id / status columns to queued_urls if missing (older installs)
try {
    const qcols = db.prepare("PRAGMA table_info(queued_urls)").all();
    if (!qcols.some(c => c.name === 'batch_id')) {
        db.exec("ALTER TABLE queued_urls ADD COLUMN batch_id TEXT");
    }
    if (!qcols.some(c => c.name === 'status')) {
        db.exec("ALTER TABLE queued_urls ADD COLUMN status TEXT NOT NULL DEFAULT 'Queued'");
    }
    db.exec("CREATE INDEX IF NOT EXISTS idx_queued_urls_batch_status ON queued_urls(batch_id, status)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_queued_urls_status ON queued_urls(status)");
} catch (e) {
    // Columns likely already exist
}

// Migration: add source_type column to scraped_urls and queued_urls.
// Tags how a URL was originally discovered ('page_tracker' | 'sitemap' |
// 'manual' | 'legacy'). Powers the source-aware coverage / no-result
// exports added on top of this. Existing rows backfill to 'legacy'
// because we can't reverse-engineer their origin from history alone.
try {
    const sCols = db.prepare("PRAGMA table_info(scraped_urls)").all();
    if (!sCols.some(c => c.name === 'source_type')) {
        db.exec("ALTER TABLE scraped_urls ADD COLUMN source_type TEXT NOT NULL DEFAULT 'legacy'");
    }
    db.exec("CREATE INDEX IF NOT EXISTS idx_scraped_urls_source_type ON scraped_urls(source_type)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_scraped_urls_status_source ON scraped_urls(status, source_type)");

    const qCols2 = db.prepare("PRAGMA table_info(queued_urls)").all();
    if (!qCols2.some(c => c.name === 'source_type')) {
        db.exec("ALTER TABLE queued_urls ADD COLUMN source_type TEXT");
    }
} catch (e) {
    // Column likely already exists
}

// Migration: add domain column to scraped_urls so coverage / no-result
// exports can filter by domain efficiently. Backfilled at startup
// (see backfillScrapedUrlsDomain below). With ~500k+ rows this is a
// one-shot cost; afterwards every domain query is index-driven.
try {
    const sCols = db.prepare("PRAGMA table_info(scraped_urls)").all();
    if (!sCols.some(c => c.name === 'domain')) {
        db.exec("ALTER TABLE scraped_urls ADD COLUMN domain TEXT");
    }
    db.exec("CREATE INDEX IF NOT EXISTS idx_scraped_urls_domain ON scraped_urls(domain)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_scraped_urls_status_domain ON scraped_urls(status, domain)");
} catch (e) {
    // Column likely already exists
}

// Performance: the Coverage Export EXISTS / NOT EXISTS checks join
// social_leads on (source_url, platform). This index turns each
// check from O(n) full-table scan into O(log n).
try {
    db.exec("CREATE INDEX IF NOT EXISTS idx_social_leads_source_url_platform ON social_leads(source_url, platform)");
} catch (e) {
    // Index likely already exists
}

// Prepared Statements for Speed
// NB: when REPLACE-ing an existing row we must NOT clobber a known
// source_type / domain with NULL. The two upsert paths below preserve
// any existing values via COALESCE.
const insertStmt = db.prepare(`
    INSERT INTO scraped_urls (url, status, message, timestamp, no_unique_profile, source_type, domain)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url) DO UPDATE SET
        status = excluded.status,
        message = excluded.message,
        timestamp = excluded.timestamp,
        no_unique_profile = excluded.no_unique_profile,
        source_type = COALESCE(scraped_urls.source_type, excluded.source_type, 'legacy'),
        domain = COALESCE(scraped_urls.domain, excluded.domain)
`);
const insertSitemapStmt = db.prepare('INSERT OR IGNORE INTO discovered_sitemaps (url, parent_url, last_scanned) VALUES (?, ?, ?)');
const getAllSitemapsStmt = db.prepare('SELECT url FROM discovered_sitemaps');
const updateSitemapDateStmt = db.prepare('UPDATE discovered_sitemaps SET last_scanned = ? WHERE url = ?');

const checkStmt = db.prepare('SELECT 1 FROM scraped_urls WHERE url = ?');
const getAllScrapedUrlsStmt = db.prepare('SELECT url FROM scraped_urls');
const getAllQueuedUrlsStmt = db.prepare('SELECT url FROM queued_urls');
const deleteQueuedUrlStmt = db.prepare('DELETE FROM queued_urls WHERE url = ?');
const upsertQueuedUrlStmt = db.prepare(`
    INSERT INTO queued_urls (url, source, batch_id, status, source_type, discovered_at, updated_at)
    VALUES (?, ?, ?, 'Queued', ?, ?, ?)
    ON CONFLICT(url) DO UPDATE SET
        source = excluded.source,
        batch_id = COALESCE(excluded.batch_id, queued_urls.batch_id),
        status = CASE WHEN queued_urls.status = 'Done' THEN queued_urls.status ELSE 'Queued' END,
        source_type = COALESCE(queued_urls.source_type, excluded.source_type),
        updated_at = excluded.updated_at
`);
const getQueuedByBatchStmt = db.prepare(`
    SELECT url FROM queued_urls
    WHERE batch_id = ? AND status = 'Queued'
    ORDER BY discovered_at ASC
`);
const getQueuedAllStmt = db.prepare(`
    SELECT url FROM queued_urls
    WHERE status = 'Queued'
    ORDER BY discovered_at ASC
`);
const setQueueStatusStmt = db.prepare(`
    UPDATE queued_urls SET status = ?, updated_at = ? WHERE url = ?
`);
const listBatchesStmt = db.prepare(`
    SELECT
        COALESCE(batch_id, '') AS batch_id,
        COALESCE(source, '')   AS source,
        COUNT(*)                                          AS total,
        SUM(CASE WHEN status = 'Queued'   THEN 1 ELSE 0 END) AS queued,
        SUM(CASE WHEN status = 'Scraping' THEN 1 ELSE 0 END) AS scraping,
        SUM(CASE WHEN status = 'Done'     THEN 1 ELSE 0 END) AS done,
        SUM(CASE WHEN status = 'Failed'   THEN 1 ELSE 0 END) AS failed,
        MIN(discovered_at)                                AS first_seen,
        MAX(updated_at)                                   AS last_updated
    FROM queued_urls
    GROUP BY batch_id, source
    ORDER BY MAX(updated_at) DESC
`);
const deleteBatchStmt = db.prepare('DELETE FROM queued_urls WHERE batch_id = ?');
const deleteDoneStmt  = db.prepare("DELETE FROM queued_urls WHERE status = 'Done'");
const countQueuedBatchStmt = db.prepare("SELECT COUNT(*) AS n FROM queued_urls WHERE batch_id = ? AND status = 'Queued'");
const countQueuedAllStmt   = db.prepare("SELECT COUNT(*) AS n FROM queued_urls WHERE status = 'Queued'");
const getCountStmt = db.prepare('SELECT COUNT(*) as count FROM scraped_urls');
const getSitemapCountStmt = db.prepare('SELECT COUNT(*) as count FROM discovered_sitemaps');
const insertTrackedPageStmt = db.prepare(`
    INSERT OR REPLACE INTO tracked_pages (
        id,
        url,
        type,
        output_base,
        enabled,
        created_at,
        last_run_at,
        last_status,
        last_new_urls
    ) VALUES (
        COALESCE((SELECT id FROM tracked_pages WHERE url = ?), NULL),
        ?, ?, ?, COALESCE((SELECT enabled FROM tracked_pages WHERE url = ?), 1),
        COALESCE((SELECT created_at FROM tracked_pages WHERE url = ?), ?),
        COALESCE((SELECT last_run_at FROM tracked_pages WHERE url = ?), NULL),
        COALESCE((SELECT last_status FROM tracked_pages WHERE url = ?), NULL),
        COALESCE((SELECT last_new_urls FROM tracked_pages WHERE url = ?), 0)
    )
`);
const getTrackedPagesStmt = db.prepare('SELECT * FROM tracked_pages ORDER BY enabled DESC, url ASC');
const getTrackedPageByIdStmt = db.prepare('SELECT * FROM tracked_pages WHERE id = ?');
const updateTrackedPageEnabledStmt = db.prepare('UPDATE tracked_pages SET enabled = ? WHERE id = ?');
const deleteTrackedPageStmt = db.prepare('DELETE FROM tracked_pages WHERE id = ?');
const getDomainVariableStmt = db.prepare('SELECT variable FROM domain_variables WHERE domain = ?');
const upsertDomainVariableStmt = db.prepare('INSERT OR REPLACE INTO domain_variables (domain, variable, updated_at) VALUES (?, ?, ?)');
const upsertSocialLeadStmt = db.prepare(`
    INSERT INTO social_leads (
        domain,
        platform,
        lead_key,
        source_url,
        social_link,
        username,
        category,
        domain_variable,
        discovered_at,
        updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(domain, platform, lead_key) DO UPDATE SET
        source_url = excluded.source_url,
        social_link = excluded.social_link,
        username = excluded.username,
        category = excluded.category,
        domain_variable = excluded.domain_variable,
        updated_at = excluded.updated_at
`);
const updateTrackedPageRunResultStmt = db.prepare(`
    UPDATE tracked_pages
    SET last_run_at = ?, last_status = ?, last_new_urls = ?
    WHERE id = ?
`);
const getSettingStmt = db.prepare('SELECT value FROM app_settings WHERE key = ?');
const upsertSettingStmt = db.prepare('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)');

/**
 * Normalizes a URL into a canonical form used as the DB primary key.
 * Handles: http/https, www vs non-www, hostname case, fragments, query strings,
 * credentials, default ports, and trailing slashes.
 * Path case is preserved (most web servers treat paths as case-sensitive).
 * Falls back to the legacy behavior (trailing-slash strip) if URL parsing fails.
 * @param {string} url
 * @returns {string}
 */
export function normalizeUrl(url) {
    if (!url) return '';
    const raw = String(url).trim();
    if (!raw) return '';

    try {
        const parsed = new URL(raw);
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
            // Non-http(s) schemes (ftp:, file:, mailto:, etc.) — keep as-is, just trim trailing slash
            return raw.replace(/\/$/, '');
        }

        // Normalize scheme to https for dedup (same article whether served over http or https)
        parsed.protocol = 'https:';
        // Lowercase hostname + strip leading www. (canonical host form)
        parsed.hostname = parsed.hostname.toLowerCase().replace(/^www\./, '');
        // Drop fragment (client-side only, same page)
        parsed.hash = '';
        // Drop query string (matches pre-existing main behavior)
        parsed.search = '';
        // Strip any embedded credentials
        parsed.username = '';
        parsed.password = '';
        // Drop default ports
        if ((parsed.protocol === 'https:' && parsed.port === '443') ||
            (parsed.protocol === 'http:' && parsed.port === '80')) {
            parsed.port = '';
        }

        let normalized = parsed.toString();
        // Strip trailing slash unless the path is just "/"
        if (normalized.endsWith('/') && parsed.pathname !== '/') {
            normalized = normalized.slice(0, -1);
        }
        return normalized;
    } catch {
        // Malformed URL — preserve legacy behavior so we don't silently drop entries
        return raw.replace(/\/$/, '');
    }
}

function normalizeDomain(domain) {
    return String(domain || '').trim().toLowerCase().replace(/^www\./, '');
}

function normalizeSocialLink(url) {
    try {
        const parsed = new URL(String(url || '').trim());
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
            return normalizeUrl(String(url || '').trim()).toLowerCase();
        }

        parsed.hash = '';
        parsed.search = '';
        parsed.username = '';
        parsed.password = '';

        let normalized = parsed.toString();
        if (normalized.endsWith('/') && parsed.pathname !== '/') {
            normalized = normalized.slice(0, -1);
        }

        return normalized.toLowerCase();
    } catch {
        return normalizeUrl(String(url || '').trim()).toLowerCase();
    }
}

function buildSocialLeadKey(platform, socialLink, username = '') {
    const normalizedPlatform = String(platform || '').trim().toLowerCase();
    const normalizedUsername = String(username || '').trim().toLowerCase();

    if (normalizedPlatform === 'instagram' && normalizedUsername) {
        return `${normalizedPlatform}:${normalizedUsername}`;
    }

    return `${normalizedPlatform}:${normalizeSocialLink(socialLink)}`;
}

function parseCsvLine(line) {
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
}

function normalizeStatus(status) {
    const raw = String(status || '').trim();
    const normalized = raw.toLowerCase().replace(/[-_\s]+/g, ' ').trim();

    if (normalized === 'no result') {
        return 'No_Result';
    }
    if (normalized === 'success') {
        return 'Success';
    }
    if (normalized === 'failed') {
        return 'Failed';
    }
    if (normalized === 'all') return 'All';
    return raw;
}

/**
 * Checks if a URL has already been processed.
 * @param {string} url 
 * @returns {boolean}
 */
export function urlExists(url) {
    if (!url) return false;
    const normalized = normalizeUrl(url);
    return checkStmt.get(normalized) !== undefined;
}

/**
 * Extract the bare domain from a URL ('https://www.foo.com/x' -> 'foo.com').
 * Lowercased and stripped of `www.` so it matches what social_leads.domain
 * stores (and what the user types in filters).
 */
export function extractDomain(url) {
    try {
        const u = new URL(url);
        return u.hostname.toLowerCase().replace(/^www\./, '') || null;
    } catch {
        return null;
    }
}

/**
 * Look up an existing scraped_urls row's source_type so subsequent
 * writes preserve it. Returns null if the row doesn't exist yet.
 */
const getSourceTypeStmt = db.prepare('SELECT source_type FROM scraped_urls WHERE url = ?');

/**
 * Look up the queued_urls row's source_type for a URL — used by the
 * scraper worker when it doesn't know the source itself (e.g. when a
 * "Run All" picks up a URL that was queued by RSS/page-tracker hours
 * earlier).
 */
const getQueuedSourceTypeStmt = db.prepare('SELECT source_type FROM queued_urls WHERE url = ?');

/**
 * Logs a scraping result.
 *
 * @param {string} url
 * @param {string} status
 * @param {string} message
 * @param {number} [profileCount=0]
 * @param {string} [sourceType] — 'page_tracker' | 'sitemap' | 'manual'
 *                                (defaults to looking up the queue row,
 *                                falling back to 'manual')
 */
export function logScrapeResult(url, status, message, profileCount = 0, sourceType = null) {
    if (!url) return;
    const normalized = normalizeUrl(url);
    const normalizedStatus = normalizeStatus(status);
    const domain = extractDomain(normalized);

    // Resolve source_type with this priority:
    //   1. Caller-provided sourceType (most authoritative)
    //   2. Existing scraped_urls.source_type if this URL was scraped before
    //      (don't downgrade a 'page_tracker' row to 'manual' on rescrape)
    //   3. queued_urls.source_type if URL came from the queue
    //   4. 'manual' fallback
    let resolvedSource = sourceType;
    if (!resolvedSource) {
        const existing = getSourceTypeStmt.get(normalized);
        if (existing?.source_type && existing.source_type !== 'legacy') {
            resolvedSource = existing.source_type;
        }
    }
    if (!resolvedSource) {
        const queued = getQueuedSourceTypeStmt.get(normalized);
        if (queued?.source_type) {
            resolvedSource = queued.source_type;
        }
    }
    if (!resolvedSource) resolvedSource = 'manual';

    try {
        insertStmt.run(
            normalized,
            normalizedStatus || status,
            message,
            new Date().toISOString(),
            profileCount,
            resolvedSource,
            domain
        );
    } catch (err) {
        console.error(`Database Error (Log): ${err.message}`);
    }
}

/**
 * Gets the total count of processed URLs.
 */
export function getHistoryCount() {
    return getCountStmt.get().count;
}

export function getAllScrapedUrls() {
    return getAllScrapedUrlsStmt.all().map(row => row.url);
}

export function getAllQueuedUrls() {
    return getAllQueuedUrlsStmt.all().map(row => row.url);
}

/**
 * Build a deterministic batch id: "{source}:{identifier}:{timestamp}".
 * Safe against collisions since timestamp is ms-precision + random suffix.
 */
export function makeBatchId(source = 'unknown', identifier = '') {
    const s = String(source || 'unknown').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').slice(0, 40) || 'unknown';
    const i = String(identifier || '').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').slice(0, 60);
    const ts = Date.now();
    const rand = Math.random().toString(36).slice(2, 6);
    return i ? `${s}:${i}:${ts}-${rand}` : `${s}:${ts}-${rand}`;
}

/**
 * Map a free-form `source` string to one of our four canonical
 * source_type tags. Used as a fallback when the caller hasn't
 * explicitly passed `sourceType`.
 *
 *   page:*, rss:*           -> 'page_tracker'
 *   xml_*, sitemap:*        -> 'sitemap'
 *   file:*, paste:*, manual -> 'manual'
 *   anything else           -> 'manual'
 */
export function inferSourceType(source) {
    const s = String(source || '').toLowerCase();
    if (s.startsWith('page:') || s.startsWith('rss:') || s.startsWith('page_tracker')) {
        return 'page_tracker';
    }
    if (s.startsWith('xml') || s.startsWith('sitemap')) {
        return 'sitemap';
    }
    return 'manual';
}

/**
 * Enqueue URLs into the work queue. Deduplicates within the input and against
 * any existing rows (existing rows stay 'Done' if already scraped, otherwise reset to 'Queued').
 *
 * @param {string[]} urls
 * @param {{ source?: string, batchId?: string, sourceType?: string }} [opts]
 *        sourceType defaults to inferring from `source` (see inferSourceType).
 * @returns {{ inserted: number, batchId: string, skippedAlreadyScraped: number }}
 */
export function enqueueUrls(urls, { source = 'unknown', batchId = null, sourceType = null } = {}) {
    const normalizedSource = String(source || 'unknown').trim() || 'unknown';
    const resolvedSourceType = sourceType || inferSourceType(normalizedSource);
    const bid = batchId || makeBatchId(normalizedSource);
    const now = new Date().toISOString();
    const uniqueUrls = Array.from(new Set(
        (Array.isArray(urls) ? urls : [])
            .map(url => normalizeUrl(String(url || '').trim()))
            .filter(Boolean)
    ));

    if (uniqueUrls.length === 0) {
        return { inserted: 0, batchId: bid, skippedAlreadyScraped: 0 };
    }

    // Skip URLs that are already in `scraped_urls` — they've been processed
    // and should not be re-queued. Otherwise RSS pollers would keep re-adding
    // the same URLs after `markQueueDone` deletes them from the queue.
    const fresh = uniqueUrls.filter(u => !urlExists(u));
    const skippedAlreadyScraped = uniqueUrls.length - fresh.length;

    if (fresh.length === 0) {
        return { inserted: 0, batchId: bid, skippedAlreadyScraped };
    }

    const tx = db.transaction((rows) => {
        for (const url of rows) {
            upsertQueuedUrlStmt.run(url, normalizedSource, bid, resolvedSourceType, now, now);
        }
    });

    try {
        tx(fresh);
        return { inserted: fresh.length, batchId: bid, skippedAlreadyScraped };
    } catch (err) {
        console.error(`Database Error (Enqueue URLs): ${err.message}`);
        return { inserted: 0, batchId: bid, skippedAlreadyScraped };
    }
}

/**
 * Returns URLs in 'Queued' status, optionally filtered by batch.
 * @param {{ batchId?: string|null }} [opts]
 * @returns {string[]}
 */
export function getQueuedUrls({ batchId = null } = {}) {
    if (batchId) return getQueuedByBatchStmt.all(batchId).map(r => r.url);
    return getQueuedAllStmt.all().map(r => r.url);
}

/**
 * List distinct batches with per-status counts. Used by the bot UI.
 */
export function listQueueBatches() {
    return listBatchesStmt.all();
}

/** Update a queue row's lifecycle status. */
export function setQueueUrlStatus(url, status) {
    const normalized = normalizeUrl(String(url || '').trim());
    if (!normalized) return;
    try {
        setQueueStatusStmt.run(status, new Date().toISOString(), normalized);
    } catch (err) {
        console.error(`Database Error (Queue Status): ${err.message}`);
    }
}

export const markQueueScraping = (url) => setQueueUrlStatus(url, 'Scraping');
/**
 * A scraped-or-skipped URL no longer needs to live in the queue — its outcome
 * is already persisted in `scraped_urls`. Delete the row so the queue view
 * stays clean and "Run All Queued" doesn't re-pick it up.
 */
export const markQueueDone = (url) => {
    const normalized = normalizeUrl(String(url || '').trim());
    if (!normalized) return;
    try {
        deleteQueuedUrlStmt.run(normalized);
    } catch (err) {
        console.error(`Database Error (Queue Done delete): ${err.message}`);
    }
};
export const markQueueFailed   = (url) => setQueueUrlStatus(url, 'Failed');

/** Count remaining queued urls, optionally by batch. */
export function countQueued(batchId = null) {
    if (batchId) return countQueuedBatchStmt.get(batchId)?.n || 0;
    return countQueuedAllStmt.get()?.n || 0;
}

/** Delete all rows in a batch (used on cleanup / cancellation). */
export function deleteQueueBatch(batchId) {
    if (!batchId) return 0;
    try {
        return deleteBatchStmt.run(batchId).changes;
    } catch (err) {
        console.error(`Database Error (Delete Batch): ${err.message}`);
        return 0;
    }
}

/** Purge all rows that finished (status='Done'). */
export function purgeDoneFromQueue() {
    try {
        return deleteDoneStmt.run().changes;
    } catch (err) {
        console.error(`Database Error (Purge Done): ${err.message}`);
        return 0;
    }
}

/**
 * One-shot cleanup: delete queue rows whose URL is already represented in
 * `scraped_urls`. We do this in two passes:
 *   1. Fast SQL pass — exact-match join. Catches the bulk in milliseconds.
 *   2. Normalization pass — re-runs every remaining queue URL through
 *      normalizeUrl() and checks scraped_urls. This catches legacy queue
 *      rows whose stored form has drifted from current normalizeUrl
 *      output (e.g. trailing slash, embedded www., upper-case host).
 *
 * Returns the total number of rows deleted.
 */
export function purgeAlreadyScrapedFromQueue() {
    let removed = 0;
    // Pass 1: fast exact-match SQL.
    try {
        const res = db.prepare(`
            DELETE FROM queued_urls
            WHERE url IN (SELECT url FROM scraped_urls)
        `).run();
        removed += res.changes;
    } catch (err) {
        console.error(`Database Error (Purge Already Scraped, pass 1): ${err.message}`);
    }

    // Pass 2: fuzzy match via normalizeUrl. Iterates remaining queue rows.
    // Uses a single transaction for the deletes so it's still fast on big queues.
    try {
        const remaining = db.prepare('SELECT url FROM queued_urls').all();
        if (remaining.length > 0) {
            const toDelete = [];
            for (const row of remaining) {
                const norm = normalizeUrl(row.url);
                // If either the stored form or the re-normalized form is in scraped_urls, drop it.
                if (norm !== row.url && checkStmt.get(norm)) {
                    toDelete.push(row.url);
                }
            }
            if (toDelete.length > 0) {
                const tx = db.transaction((urls) => {
                    for (const u of urls) deleteQueuedUrlStmt.run(u);
                });
                tx(toDelete);
                removed += toDelete.length;
            }
        }
    } catch (err) {
        console.error(`Database Error (Purge Already Scraped, pass 2): ${err.message}`);
    }

    return removed;
}

/**
 * Re-normalize every URL in `queued_urls`. If a row's url is not the
 * canonical normalized form, replace it (or drop it if a row with the
 * canonical form already exists). Returns { rewritten, dropped }.
 *
 * Intended as a one-shot startup migration. After it runs, the cheap
 * SQL `WHERE url IN (SELECT url FROM scraped_urls)` join works correctly
 * because both tables now use the same canonical form.
 */
export function normalizeQueuedUrls() {
    let rewritten = 0;
    let dropped = 0;
    try {
        const all = db.prepare('SELECT url, source, batch_id, status, discovered_at, updated_at FROM queued_urls').all();
        const tx = db.transaction((rows) => {
            for (const row of rows) {
                const norm = normalizeUrl(row.url);
                if (!norm || norm === row.url) continue;
                // Drop the legacy row first.
                deleteQueuedUrlStmt.run(row.url);
                // If the normalized form already exists, we're done with this URL.
                const exists = db.prepare('SELECT 1 FROM queued_urls WHERE url = ?').get(norm);
                if (exists) {
                    dropped++;
                    continue;
                }
                // Otherwise reinsert with the canonical url.
                upsertQueuedUrlStmt.run(
                    norm,
                    row.source || 'unknown',
                    row.batch_id || null,
                    row.discovered_at || new Date().toISOString(),
                    row.updated_at || new Date().toISOString()
                );
                rewritten++;
            }
        });
        tx(all);
    } catch (err) {
        console.error(`Database Error (Normalize Queue): ${err.message}`);
    }
    return { rewritten, dropped };
}

export function removeQueuedUrl(url) {
    const normalized = normalizeUrl(String(url || '').trim());
    if (!normalized) return;

    try {
        deleteQueuedUrlStmt.run(normalized);
    } catch (err) {
        console.error(`Database Error (Remove Queued URL): ${err.message}`);
    }
}

/**
 * Retrieves URLs based on their status.
 * @param {string} status - The status to filter by (e.g., 'Failed', 'No_Data').
 * @returns {Array<string>} - List of URLs.
 */
export function getUrlsByStatus(status) {
    const normalizedStatus = normalizeStatus(status);
    let stmt;
    if (normalizedStatus === 'All') {
        stmt = db.prepare('SELECT url FROM scraped_urls');
        const rows = stmt.all();
        return rows.map(row => row.url);
    } else {
        stmt = db.prepare('SELECT url FROM scraped_urls WHERE status = ?');
        const rows = stmt.all(normalizedStatus);
        return rows.map(row => row.url);
    }
}

/**
 * Exports URLs with a specific status to a text file.
 * @param {string} status - The status to export.
 * @param {string} filename - The output filename.
 */
export function exportUrlsByStatus(status) {
    const urls = getUrlsByStatus(status);
    const safeStatus = normalizeStatus(status) || String(status || '').trim();
    if (urls.length === 0) {
        console.log(`No URLs found for status: ${safeStatus}`);
        return null;
    }
    
    const timestamp = new Date().toISOString().replace(/:/g, '-').split('.')[0];
    const filename = `exported_urls_${safeStatus.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_${timestamp}.txt`;
    const outputPath = path.join(process.cwd(), 'result', filename);

    // Ensure result directory exists
    const resultDir = path.join(process.cwd(), 'result');
    if (!fs.existsSync(resultDir)) {
        fs.mkdirSync(resultDir);
    }

    fs.writeFileSync(outputPath, urls.join('\n'));
    console.log(`✅ Exported ${urls.length} URLs with status '${safeStatus}' to ${outputPath}`);
    return outputPath;
}

/**
 * Saves a discovered child sitemap.
 * @param {string} url 
 * @param {string} parentUrl 
 */
export function saveDiscoveredSitemap(url, parentUrl) {
    try {
        insertSitemapStmt.run(url, parentUrl, null);
    } catch (err) {
        console.error(`Database Error (Sitemap): ${err.message}`);
    }
}

/**
 * Retrieves all saved sitemaps.
 * @returns {Array<string>}
 */
export function getAllSitemaps() {
    return getAllSitemapsStmt.all().map(row => row.url);
}

/**
 * Updates the last scanned date for a sitemap.
 * @param {string} url 
 */
export function updateSitemapScanDate(url) {
    updateSitemapDateStmt.run(new Date().toISOString(), url);
}

/**
 * Gets the total count of discovered sitemaps.
 */
export function getSitemapCount() {
    return getSitemapCountStmt.get().count;
}

export function getTrackedPages() {
    return getTrackedPagesStmt.all();
}

export function getEnabledTrackedPages() {
    return getTrackedPages().filter(row => Number(row.enabled) === 1);
}

export function getTrackedPageById(id) {
    return getTrackedPageByIdStmt.get(id);
}

export function saveTrackedPage(url, type = 'html', outputBase = '') {
    const normalizedUrl = normalizeUrl(String(url || '').trim());
    const normalizedType = String(type || 'html').trim().toLowerCase() === 'rss' ? 'rss' : 'html';
    const normalizedOutputBase = String(outputBase || '').trim();
    const timestamp = new Date().toISOString();

    insertTrackedPageStmt.run(
        normalizedUrl,
        normalizedUrl,
        normalizedType,
        normalizedOutputBase,
        normalizedUrl,
        normalizedUrl,
        timestamp,
        normalizedUrl,
        normalizedUrl,
        normalizedUrl
    );

    return db.prepare('SELECT * FROM tracked_pages WHERE url = ?').get(normalizedUrl);
}

export function setTrackedPageEnabled(id, enabled) {
    updateTrackedPageEnabledStmt.run(enabled ? 1 : 0, id);
    return getTrackedPageById(id);
}

export function deleteTrackedPage(id) {
    deleteTrackedPageStmt.run(id);
}

export function updateTrackedPageRunResult(id, { lastRunAt, lastStatus, lastNewUrls }) {
    updateTrackedPageRunResultStmt.run(
        lastRunAt || new Date().toISOString(),
        String(lastStatus || '').trim() || null,
        Number.isFinite(Number(lastNewUrls)) ? Number(lastNewUrls) : 0,
        id
    );
}

function getSettingValue(key, fallback = null) {
    const row = getSettingStmt.get(key);
    if (!row || row.value === undefined || row.value === null) {
        return fallback;
    }
    return row.value;
}

function setSettingValue(key, value) {
    upsertSettingStmt.run(key, String(value));
}

export function getPageTrackerSchedule() {
    const intervalHours = Number(getSettingValue('page_tracker_interval_hours', '1'));
    const enabled = getSettingValue('page_tracker_enabled', '1') === '1';
    return {
        intervalHours: Number.isFinite(intervalHours) && intervalHours > 0 ? intervalHours : 1,
        enabled
    };
}

export function setPageTrackerSchedule(intervalHours, enabled = true) {
    const normalizedHours = Math.max(1, Number(intervalHours) || 1);
    setSettingValue('page_tracker_interval_hours', normalizedHours);
    setSettingValue('page_tracker_enabled', enabled ? '1' : '0');
    return getPageTrackerSchedule();
}

export function setPageTrackerEnabled(enabled) {
    const current = getPageTrackerSchedule();
    return setPageTrackerSchedule(current.intervalHours, enabled);
}

export function getDomainVariable(domain) {
    const normalizedDomain = normalizeDomain(domain);
    if (!normalizedDomain) return '';
    const row = getDomainVariableStmt.get(normalizedDomain);
    return row?.variable ? String(row.variable).trim() : '';
}

export function setDomainVariable(domain, variable) {
    const normalizedDomain = normalizeDomain(domain);
    const normalizedVariable = String(variable || '').trim();
    if (!normalizedDomain || !normalizedVariable) {
        throw new Error('Domain and variable are required');
    }

    upsertDomainVariableStmt.run(normalizedDomain, normalizedVariable, new Date().toISOString());
    return {
        domain: normalizedDomain,
        variable: normalizedVariable
    };
}

export function saveSocialLead(lead) {
    const domain = normalizeDomain(lead?.domain);
    const platform = String(lead?.platform || '').trim().toLowerCase();
    const sourceUrl = normalizeUrl(String(lead?.sourceUrl || '').trim());
    const socialLink = String(lead?.socialLink || '').trim();
    const username = String(lead?.username || '').trim();
    const category = String(lead?.category || '').trim() || 'link';
    const domainVariable = String(lead?.domainVariable || '').trim();
    const discoveredAt = String(lead?.discoveredAt || '').trim() || new Date().toISOString();
    const updatedAt = String(lead?.updatedAt || '').trim() || discoveredAt;

    if (!domain || !platform || !sourceUrl || !socialLink) {
        return;
    }

    const leadKey = buildSocialLeadKey(platform, socialLink, username);

    try {
        upsertSocialLeadStmt.run(
            domain,
            platform,
            leadKey,
            sourceUrl,
            socialLink,
            username,
            category,
            domainVariable,
            discoveredAt,
            updatedAt
        );
    } catch (err) {
        console.error(`Database Error (Social Lead): ${err.message}`);
    }
}

function migrateSocialLeadsFromCsv() {
    const resultDir = path.join(process.cwd(), 'result');
    if (!fs.existsSync(resultDir)) {
        return;
    }

    let processedRows = 0;

    const importAllResultsFile = (filePath) => {
        if (!fs.existsSync(filePath)) return;

        const content = fs.readFileSync(filePath, 'utf8');
        if (!content.trim()) return;

        const lines = content.split('\n').filter(line => line.trim());
        if (lines.length < 2) return;

        const header = parseCsvLine(lines[0]);
        const domainIndex = header.indexOf('Domain');
        const platformIndex = header.indexOf('Platform');
        const sourceUrlIndex = header.indexOf('Source URL');
        const socialLinkIndex = header.indexOf('Social Link');
        const usernameIndex = header.indexOf('Username');
        const categoryIndex = header.indexOf('Category');
        const domainVariableIndex = header.indexOf('Domain Variable');
        const timestampIndex = header.indexOf('Timestamp');

        if (domainIndex === -1 || platformIndex === -1 || sourceUrlIndex === -1 || socialLinkIndex === -1) {
            return;
        }

        for (let i = 1; i < lines.length; i++) {
            const values = parseCsvLine(lines[i]);
            saveSocialLead({
                domain: values[domainIndex],
                platform: values[platformIndex],
                sourceUrl: values[sourceUrlIndex],
                socialLink: values[socialLinkIndex],
                username: usernameIndex >= 0 ? values[usernameIndex] : '',
                category: categoryIndex >= 0 ? values[categoryIndex] : 'link',
                domainVariable: domainVariableIndex >= 0 ? values[domainVariableIndex] : '',
                discoveredAt: timestampIndex >= 0 ? values[timestampIndex] : '',
                updatedAt: timestampIndex >= 0 ? values[timestampIndex] : ''
            });
            processedRows++;
        }
    };

    const importDomainFolderFiles = () => {
        const entries = fs.readdirSync(resultDir, { withFileTypes: true })
            .filter(entry => entry.isDirectory() && !entry.name.startsWith('.'));

        entries.forEach(entry => {
            const domain = normalizeDomain(entry.name);
            if (!domain) return;

            const domainDir = path.join(resultDir, entry.name);
            fs.readdirSync(domainDir, { withFileTypes: true })
                .filter(fileEntry => fileEntry.isFile() && fileEntry.name.toLowerCase().endsWith('.csv'))
                .forEach(fileEntry => {
                    const filePath = path.join(domainDir, fileEntry.name);
                    const content = fs.readFileSync(filePath, 'utf8');
                    if (!content.trim()) return;

                    const lines = content.split('\n').filter(line => line.trim());
                    if (lines.length < 2) return;

                    const header = parseCsvLine(lines[0]);
                    const sourceUrlIndex = header.indexOf('Source URL');
                    const socialLinkIndex = header.indexOf('Social Link');
                    const usernameIndex = header.indexOf('Username');
                    const categoryIndex = header.indexOf('Category');
                    const domainVariableIndex = header.indexOf('Domain Variable');
                    const timestampIndex = header.indexOf('Timestamp');
                    const platform = path.basename(fileEntry.name, '.csv').toLowerCase();

                    if (sourceUrlIndex === -1 || socialLinkIndex === -1 || !platform) {
                        return;
                    }

                    for (let i = 1; i < lines.length; i++) {
                        const values = parseCsvLine(lines[i]);
                        saveSocialLead({
                            domain,
                            platform,
                            sourceUrl: values[sourceUrlIndex],
                            socialLink: values[socialLinkIndex],
                            username: usernameIndex >= 0 ? values[usernameIndex] : '',
                            category: categoryIndex >= 0 ? values[categoryIndex] : 'link',
                            domainVariable: domainVariableIndex >= 0 ? values[domainVariableIndex] : '',
                            discoveredAt: timestampIndex >= 0 ? values[timestampIndex] : '',
                            updatedAt: timestampIndex >= 0 ? values[timestampIndex] : ''
                        });
                        processedRows++;
                    }
                });
        });
    };

    try {
        importAllResultsFile(path.join(resultDir, 'all_results.csv'));
        importDomainFolderFiles();

        if (processedRows > 0) {
            console.log(`🔄 Synced ${processedRows} social lead row(s) from CSV into SQLite`);
        }
    } catch (err) {
        console.error(`Database Error (Social Lead CSV Migration): ${err.message}`);
    }
}

export function getPageTrackerLastSummary() {
    const raw = getSettingValue('page_tracker_last_summary', '');
    if (!raw) return null;
    try {
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

export function savePageTrackerLastSummary(summary) {
    setSettingValue('page_tracker_last_summary', JSON.stringify(summary || null));
}

/**
 * Migrates data from the old CSV file to SQLite.
 */
export function migrateCsvToDb() {
    const csvPath = path.join(process.cwd(), 'result', 'scraping_status.csv');
    if (!fs.existsSync(csvPath)) return;

    console.log('🔄 Migrating CSV history to SQLite database...');
    
    try {
        const content = fs.readFileSync(csvPath, 'utf8');
        const lines = content.split('\n');
        let count = 0;

        const insertMany = db.transaction((rows) => {
            for (const row of rows) {
                insertStmt.run(row.url, row.status, row.message, row.timestamp, 0, 'legacy', extractDomain(row.url));
            }
        });

        const rowsToInsert = [];

        // Skip header (i=1)
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;

            // Regex to handle potential quotes in URL or Message
            // Format: Timestamp,URL,Status,Message
            // Simple split might fail if commas in message, but let's try robust matching
            
            // Standard CSV parsing for our specific format
            // We know Timestamp is first, URL second.
            const parts = line.split(',');
            if (parts.length < 3) continue;

            const timestamp = parts[0];
            let url = parts[1];
            let status = normalizeStatus(parts[2]);
            let message = parts.slice(3).join(','); // Join rest as message

            // Clean quotes if present
            if (url.startsWith('"') && url.endsWith('"')) url = url.slice(1, -1);
            if (message.startsWith('"') && message.endsWith('"')) message = message.slice(1, -1);

            rowsToInsert.push({ url, status, message, timestamp });
            count++;
        }

        if (rowsToInsert.length > 0) {
            insertMany(rowsToInsert);
            console.log(`✅ Successfully migrated ${count} records to database.`);
            
            // Rename CSV to indicate it's archived
            const backupPath = csvPath + '.bak';
            fs.renameSync(csvPath, backupPath);
            console.log(`📦 Archived old CSV to: ${backupPath}`);
        }

    } catch (err) {
        console.error(`❌ Migration Failed: ${err.message}`);
    }
}

/**
 * Deduplicates URLs in the database by normalizing them (removing trailing slashes).
 */
export function deduplicateUrls() {
    // console.log('🧹 Checking for duplicate URLs...');
    const allUrlsStmt = db.prepare('SELECT url, status, message, timestamp, no_unique_profile, source_type FROM scraped_urls');
    const deleteStmt = db.prepare('DELETE FROM scraped_urls WHERE url = ?');
    
    try {
        const rows = allUrlsStmt.all();
        const map = new Map();
        let duplicatesFound = 0;

        for (const row of rows) {
            const normalized = normalizeUrl(row.url);
            
            if (!map.has(normalized)) {
                map.set(normalized, [row]);
            } else {
                map.get(normalized).push(row);
            }
        }

        const updates = [];
        const deletions = [];

        for (const [key, group] of map) {
            if (group.length > 1) {
                duplicatesFound++;
                // Sort to find the best one
                // Priority: Success > No_Result > Failed
                // Tie-breaker: Latest timestamp
                group.sort((a, b) => {
                    const statusScore = s => {
                        const normalized = normalizeStatus(s);
                        if (normalized === 'Success') return 3;
                        if (normalized === 'No_Result') return 2;
                        if (normalized === 'Failed') return 1;
                        return 0;
                    };
                    const scoreA = statusScore(a.status);
                    const scoreB = statusScore(b.status);
                    if (scoreA !== scoreB) return scoreB - scoreA;
                    return new Date(b.timestamp) - new Date(a.timestamp);
                });

                const winner = group[0];
                const losers = group.slice(1);
                
                losers.forEach(l => deletions.push(l.url));
                
                if (winner.url !== key) {
                     updates.push({
                         newUrl: key,
                         data: {
                             ...winner,
                             status: normalizeStatus(winner.status)
                         },
                         oldUrl: winner.url
                     });
                }
            } else {
                // Single entry. Check if it needs normalization
                const row = group[0];
                if (row.url !== key) {
                    updates.push({
                         newUrl: key,
                         data: {
                             ...row,
                             status: normalizeStatus(row.status)
                         },
                         oldUrl: row.url
                     });
                }
            }
        }
        
        if (deletions.length > 0 || updates.length > 0) {
            console.log(`⚠️  Found ${duplicatesFound} sets of duplicates/unnormalized URLs. Cleaning up...`);
            
            const cleanupTx = db.transaction(() => {
                for (const url of deletions) {
                    deleteStmt.run(url);
                }
                for (const up of updates) {
                    // If we rename, we delete old first to avoid PK conflict (though old might have been deleted if it was in deletions list?? No, logic separates them)
                    // If 'oldUrl' was a winner, it is NOT in deletions.
                    deleteStmt.run(up.oldUrl);
                    insertStmt.run(
                        up.newUrl,
                        up.data.status,
                        up.data.message,
                        up.data.timestamp,
                        up.data.no_unique_profile,
                        up.data.source_type || 'legacy',
                        extractDomain(up.newUrl)
                    );
                }
            });
            
            cleanupTx();
            console.log(`✅ Deduplication complete. Removed ${deletions.length} rows, normalized ${updates.length} rows.`);
        }

    } catch (e) {
        console.error(`Deduplication failed: ${e.message}`);
    }
}

// Auto-run migration on import if DB is empty but CSV exists
const count = getHistoryCount();
if (count === 0) {
    migrateCsvToDb();
}

// Always check for duplicates/normalization on startup
deduplicateUrls();

/* ====================================================================== *
 *  Source-aware Coverage & No-Result Exports
 * ====================================================================== *
 * Powers the Telegram menus that let the user pull lists of URLs by
 * (source_type × domain × coverage bucket).
 *
 * Source filtering:
 *   - 'page_tracker': URLs the bot discovered itself via RSS/HTML/JSON
 *      sources. Manageable in size, worth manually enriching.
 *   - 'sitemap': URLs from XML sitemap scans. Often >100k; we expose a
 *      no-result-only export so users can spot-check failures by domain.
 *   - 'manual': URLs the user typed/pasted/uploaded.
 *   - 'legacy': pre-existing rows from before source tagging was added.
 *      Treated like 'sitemap' for export purposes (assumed bulk).
 *
 * IMPORTANT: numbers in the UI must reconcile back to /stats. Every
 * helper here filters scraped_urls.status='Success' (for coverage) or
 * 'No_Result' (for no-result), so the buckets visibly sum to the
 * stats total minus 'Failed'.
 * ====================================================================== */

/**
 * One-shot startup backfill: populate `scraped_urls.domain` for any rows
 * inserted before the column existed. Safe to run repeatedly — it only
 * touches rows where domain IS NULL. Logs how many were updated.
 */
export function backfillScrapedUrlsDomain() {
    try {
        const need = db.prepare("SELECT COUNT(*) AS n FROM scraped_urls WHERE domain IS NULL").get();
        if (!need || need.n === 0) return { updated: 0 };

        const rows = db.prepare("SELECT url FROM scraped_urls WHERE domain IS NULL").all();
        const upd = db.prepare("UPDATE scraped_urls SET domain = ? WHERE url = ?");
        const tx = db.transaction((items) => {
            for (const r of items) {
                upd.run(extractDomain(r.url), r.url);
            }
        });
        tx(rows);
        return { updated: rows.length };
    } catch (err) {
        console.error(`Domain backfill failed: ${err.message}`);
        return { updated: 0 };
    }
}

/**
 * Coverage summary for a given source_type filter (or all sources).
 *   total_success        rows where status='Success'
 *   ig_only              has Instagram lead, no LinkedIn lead
 *   li_only              has LinkedIn lead, no Instagram lead
 *   both                 has both
 *   neither              no IG, no LI (may have website-only or nothing)
 *
 * Buckets sum to total_success.
 *
 * @param {{ sourceType?: string|null }} [opts]
 *        sourceType: 'page_tracker' | 'sitemap' | 'manual' | null=all
 */
export function getCoverageSummary({ sourceType = null } = {}) {
    const whereSource = sourceType ? `AND s.source_type = '${sourceType.replace(/'/g, "''")}'` : '';
    const sql = `
        SELECT
            (SELECT COUNT(*) FROM scraped_urls s
                WHERE s.status='Success' ${whereSource}) AS total_success,
            (SELECT COUNT(*) FROM scraped_urls s
                WHERE s.status='Success' ${whereSource}
                  AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
                  AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
            ) AS ig_only,
            (SELECT COUNT(*) FROM scraped_urls s
                WHERE s.status='Success' ${whereSource}
                  AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
                  AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
            ) AS li_only,
            (SELECT COUNT(*) FROM scraped_urls s
                WHERE s.status='Success' ${whereSource}
                  AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
                  AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
            ) AS both,
            (SELECT COUNT(*) FROM scraped_urls s
                WHERE s.status='Success' ${whereSource}
                  AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform IN ('instagram','linkedin'))
            ) AS neither
    `;
    try {
        return db.prepare(sql).get();
    } catch (err) {
        console.error(`Coverage summary failed: ${err.message}`);
        return { total_success: 0, ig_only: 0, li_only: 0, both: 0, neither: 0 };
    }
}

/**
 * Internal helper that runs a coverage query and returns rows enriched
 * with concatenated link lists. The `bucket` arg picks one of:
 *   'ig_only' | 'li_only' | 'neither' | 'both'
 */
function runCoverageQuery(bucket, { sourceType = null, domain = null } = {}) {
    const whereSource = sourceType ? `AND s.source_type = ?` : '';
    const whereDomain = domain ? `AND s.domain = ?` : '';
    const params = [];
    if (sourceType) params.push(sourceType);
    if (domain) params.push(domain);

    let bucketWhere = '';
    switch (bucket) {
        case 'ig_only':
            bucketWhere = `
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
            `;
            break;
        case 'li_only':
            bucketWhere = `
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
            `;
            break;
        case 'both':
            bucketWhere = `
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
            `;
            break;
        case 'neither':
            bucketWhere = `
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform IN ('instagram','linkedin'))
            `;
            break;
        default:
            return [];
    }

    const sql = `
        SELECT
            s.url,
            s.timestamp,
            s.domain,
            s.source_type,
            (SELECT GROUP_CONCAT(social_link, '|')
                FROM social_leads WHERE source_url=s.url AND platform='instagram') AS instagram_links,
            (SELECT GROUP_CONCAT(social_link, '|')
                FROM social_leads WHERE source_url=s.url AND platform='linkedin') AS linkedin_links,
            (SELECT GROUP_CONCAT(social_link, '|')
                FROM social_leads WHERE source_url=s.url AND platform='website') AS website_links
        FROM scraped_urls s
        WHERE s.status = 'Success'
          ${whereSource}
          ${whereDomain}
          ${bucketWhere}
        ORDER BY s.timestamp DESC
    `;
    try {
        return db.prepare(sql).all(...params);
    } catch (err) {
        console.error(`Coverage query (${bucket}) failed: ${err.message}`);
        return [];
    }
}

export const getUrlsWithInstagramButNoLinkedin = (opts = {}) => runCoverageQuery('ig_only', opts);
export const getUrlsWithLinkedinButNoInstagram = (opts = {}) => runCoverageQuery('li_only', opts);
export const getUrlsWithoutAnySocial           = (opts = {}) => runCoverageQuery('neither', opts);

/**
 * Domain breakdown for a coverage bucket. Used to render the Step-2
 * domain selector menu. Sorted by count DESC so the worst-offending
 * domains are at the top.
 */
export function getCoverageBreakdownByDomain(bucket, { sourceType = null } = {}) {
    const whereSource = sourceType ? `AND s.source_type = ?` : '';
    const params = sourceType ? [sourceType] : [];

    let bucketWhere = '';
    switch (bucket) {
        case 'ig_only':
            bucketWhere = `
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
            `;
            break;
        case 'li_only':
            bucketWhere = `
                AND EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='linkedin')
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform='instagram')
            `;
            break;
        case 'neither':
            bucketWhere = `
                AND NOT EXISTS (SELECT 1 FROM social_leads sl WHERE sl.source_url=s.url AND sl.platform IN ('instagram','linkedin'))
            `;
            break;
        default:
            return [];
    }
    const sql = `
        SELECT s.domain, COUNT(*) AS n
        FROM scraped_urls s
        WHERE s.status='Success'
          AND s.domain IS NOT NULL
          ${whereSource}
          ${bucketWhere}
        GROUP BY s.domain
        ORDER BY n DESC
    `;
    try {
        return db.prepare(sql).all(...params);
    } catch (err) {
        console.error(`Coverage breakdown failed: ${err.message}`);
        return [];
    }
}

/**
 * Source-tagged No-Result counts. Used to render the No-Result Export
 * top-level menu. Buckets sum to total_no_result.
 */
export function getNoResultSummary() {
    try {
        const total = db.prepare("SELECT COUNT(*) AS n FROM scraped_urls WHERE status='No_Result'").get();
        const bySource = db.prepare(`
            SELECT COALESCE(source_type, 'legacy') AS source_type, COUNT(*) AS n
            FROM scraped_urls
            WHERE status='No_Result'
            GROUP BY source_type
        `).all();
        const out = { total: total?.n || 0, page_tracker: 0, sitemap: 0, manual: 0, legacy: 0 };
        for (const row of bySource) {
            out[row.source_type] = row.n;
        }
        return out;
    } catch (err) {
        console.error(`No-result summary failed: ${err.message}`);
        return { total: 0, page_tracker: 0, sitemap: 0, manual: 0, legacy: 0 };
    }
}

/**
 * Domain breakdown for No-Result, optionally scoped to one source_type.
 * `null` source means "all sources combined".
 */
export function getNoResultBreakdownByDomain({ sourceType = null } = {}) {
    const whereSource = sourceType ? `AND source_type = ?` : '';
    const params = sourceType ? [sourceType] : [];
    const sql = `
        SELECT domain, COUNT(*) AS n
        FROM scraped_urls
        WHERE status='No_Result' AND domain IS NOT NULL ${whereSource}
        GROUP BY domain
        ORDER BY n DESC
    `;
    try {
        return db.prepare(sql).all(...params);
    } catch (err) {
        console.error(`No-result breakdown failed: ${err.message}`);
        return [];
    }
}

/** No-Result URLs filtered by source and/or domain. */
export function getNoResultUrls({ sourceType = null, domain = null } = {}) {
    const where = ["status='No_Result'"];
    const params = [];
    if (sourceType) { where.push('source_type = ?'); params.push(sourceType); }
    if (domain)     { where.push('domain = ?');      params.push(domain); }
    const sql = `
        SELECT url, timestamp, domain, source_type, message
        FROM scraped_urls
        WHERE ${where.join(' AND ')}
        ORDER BY timestamp DESC
    `;
    try {
        return db.prepare(sql).all(...params);
    } catch (err) {
        console.error(`No-result query failed: ${err.message}`);
        return [];
    }
}

export default db;
