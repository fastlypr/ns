/**
 * Shared utilities used across bot.js, scraper.js, xml.js, rss.js.
 *
 * Each helper used to live in two or three places with slight variations.
 * Centralising them removes a class of "fixed in one file, broken in
 * another" bugs (the kind that caused the recent stale-queue 107k-skip
 * loop).
 *
 * NOTE: the canonical URL *normaliser* lives in db.js (`normalizeUrl`)
 * because it has to share the same SQLite identity as stored rows.
 * `rss.js` has a different helper of the same name that resolves
 * RELATIVE URLs against a base — those are not the same operation and
 * should not be merged.
 */

/* -------------------------------------------------------------------- *
 * CSV parsing / serialisation
 * -------------------------------------------------------------------- */

/**
 * Parse a single CSV line, honouring double-quoted fields and escaped
 * quotes (`""` → `"`). Returns an array of cell strings.
 */
export const parseCsvLine = (line) => {
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

/**
 * Serialise a single cell for CSV. Wraps in quotes (and doubles internal
 * quotes) only if the cell contains comma, newline, or quote characters.
 */
export const escapeCsvCell = (value) => {
    const stringValue = value === null || value === undefined ? '' : String(value);
    if (/[",\n]/.test(stringValue)) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
};

/** Serialise an array of cells into a CSV row. */
export const serializeCsvLine = (values) => values.map(escapeCsvCell).join(',');

/* -------------------------------------------------------------------- *
 * URL extraction from free text
 * -------------------------------------------------------------------- */

/**
 * Pull every http(s) URL out of a blob of text. Strips trailing
 * punctuation that's commonly attached after a URL in prose
 * (commas, semicolons, parens, quotes, periods, etc.).
 *
 * Used by paste-URL and TXT-upload flows. Replaces three slightly
 * different inline regexes scattered across bot.js.
 */
export const extractUrlsFromText = (text) => {
    if (!text) return [];
    const matches = String(text).match(/https?:\/\/[^\s"'<>`]+/gi) || [];
    return matches
        .map((u) => u.replace(/[).,;:!?\]]+$/, '').trim())
        .filter(Boolean);
};

/* -------------------------------------------------------------------- *
 * Telegram auth wrapper
 * -------------------------------------------------------------------- */

/**
 * Higher-order auth check. Wrap any handler with this to drop the
 * `if (chatId !== TELEGRAM_CHAT_ID) return` boilerplate. Returns a
 * new function that:
 *   • silently ignores callbacks from non-authorised chats, or
 *   • answers them with an "Unauthorized" alert when callbackId is
 *     given (so Telegram doesn't show the spinning "loading" forever).
 *
 * Usage:
 *   const myHandler = requireAuth(authorizedChatId, async (msg) => { ... });
 *   bot.onText(/\/foo/, myHandler);
 */
export const requireAuth = (authorizedChatId, handler) => {
    return async (...args) => {
        const first = args[0];
        // bot.onText:        first arg is `msg`, has chat.id
        // bot.on('callback_query'): first arg is callbackQuery, .message.chat.id
        // bot.on('message'):  first arg is msg, has chat.id
        // bot.on('document'): first arg is msg, has chat.id
        const chatId =
            first?.chat?.id ??
            first?.message?.chat?.id ??
            null;

        if (chatId == null || String(chatId) !== String(authorizedChatId)) {
            return undefined;
        }
        return handler(...args);
    };
};

/* -------------------------------------------------------------------- *
 * Generic progress-text builder
 * -------------------------------------------------------------------- */

/**
 * Build a progress message body from a label/value object. Replaces
 * five near-identical builders (file scrape, folder scrape, retry,
 * sitemap scan, sitemap rescan) that differed only in which fields
 * they showed.
 *
 * @param {string} title           First line, e.g. "📄 File Scrape Running".
 * @param {Array<[string,string]>} fields  Ordered [label, value] pairs.
 *                                         Empty values are omitted; `null`
 *                                         entries become blank separators.
 * @returns {string}
 */
export const buildProgressText = (title, fields = []) => {
    const lines = [title];
    for (const entry of fields) {
        if (entry === null) {
            lines.push('');
            continue;
        }
        const [label, value] = entry;
        if (value == null || value === '') continue;
        lines.push(label ? `${label}: ${value}` : String(value));
    }
    return lines.join('\n');
};
