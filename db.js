import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DB_PATH = path.join(process.cwd(), 'history.db');
const db = new Database(DB_PATH);

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

// Prepared Statements for Speed
const insertStmt = db.prepare('INSERT OR REPLACE INTO scraped_urls (url, status, message, timestamp, no_unique_profile) VALUES (?, ?, ?, ?, ?)');
const insertSitemapStmt = db.prepare('INSERT OR IGNORE INTO discovered_sitemaps (url, parent_url, last_scanned) VALUES (?, ?, ?)');
const getAllSitemapsStmt = db.prepare('SELECT url FROM discovered_sitemaps');
const updateSitemapDateStmt = db.prepare('UPDATE discovered_sitemaps SET last_scanned = ? WHERE url = ?');

const checkStmt = db.prepare('SELECT 1 FROM scraped_urls WHERE url = ?');
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
const updateTrackedPageRunResultStmt = db.prepare(`
    UPDATE tracked_pages
    SET last_run_at = ?, last_status = ?, last_new_urls = ?
    WHERE id = ?
`);
const getSettingStmt = db.prepare('SELECT value FROM app_settings WHERE key = ?');
const upsertSettingStmt = db.prepare('INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)');

/**
 * Normalizes a URL by removing the trailing slash.
 * @param {string} url 
 * @returns {string}
 */
export function normalizeUrl(url) {
    if (!url) return '';
    return url.replace(/\/$/, '');
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
 * Logs a scraping result.
 * @param {string} url 
 * @param {string} status 
 * @param {string} message 
 * @param {number} profileCount - Number of unique profiles found (optional)
 */
export function logScrapeResult(url, status, message, profileCount = 0) {
    if (!url) return;
    const normalized = normalizeUrl(url);
    const normalizedStatus = normalizeStatus(status);
    try {
        insertStmt.run(normalized, normalizedStatus || status, message, new Date().toISOString(), profileCount);
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
                insertStmt.run(row.url, row.status, row.message, row.timestamp, 0);
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
    const allUrlsStmt = db.prepare('SELECT url, status, message, timestamp, no_unique_profile FROM scraped_urls');
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
                    insertStmt.run(up.newUrl, up.data.status, up.data.message, up.data.timestamp, up.data.no_unique_profile);
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

export default db;
