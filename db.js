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

/**
 * Normalizes a URL by removing the trailing slash.
 * @param {string} url 
 * @returns {string}
 */
export function normalizeUrl(url) {
    if (!url) return '';
    return url.replace(/\/$/, '');
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
    try {
        insertStmt.run(normalized, status, message, new Date().toISOString(), profileCount);
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
    let stmt;
    if (status === 'All') {
        stmt = db.prepare('SELECT url FROM scraped_urls');
        const rows = stmt.all();
        return rows.map(row => row.url);
    } else {
        stmt = db.prepare('SELECT url FROM scraped_urls WHERE status = ?');
        const rows = stmt.all(status);
        return rows.map(row => row.url);
    }
}

/**
 * Exports URLs with a specific status to a text file.
 * @param {string} status - The status to export.
 * @param {string} filename - The output filename.
 */
export function exportUrlsByStatus(status, filename) {
    const urls = getUrlsByStatus(status);
    if (urls.length === 0) return 0;
    
    const outputPath = path.join(process.cwd(), filename);
    fs.writeFileSync(outputPath, urls.join('\n'));
    return urls.length;
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
            let status = parts[2];
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
                    const statusScore = s => s === 'Success' ? 3 : (s === 'No_Result' ? 2 : 1);
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
                         data: winner,
                         oldUrl: winner.url
                     });
                }
            } else {
                // Single entry. Check if it needs normalization
                const row = group[0];
                if (row.url !== key) {
                    updates.push({
                         newUrl: key,
                         data: row,
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
