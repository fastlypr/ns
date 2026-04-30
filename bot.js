import TelegramBot from 'node-telegram-bot-api';
import { createServer } from 'http';
import os from 'os';
import { randomBytes } from 'crypto';
import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DOWNLOAD_LINK_BASE_URL, DOWNLOAD_LINK_PORT } from './config.js';
import fs from 'fs';
import path from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import {
    parseCsvLine,
    escapeCsvCell,
    serializeCsvLine,
    extractUrlsFromText as extractUrlsFromTextUtil,
    requireAuth,
    buildProgressText
} from './utils.js';
import {
    scrapeSingleUrlAndProcess,
    scrapeUrlsFromInputFile,
    runScraper,
    processToScrapeFolder,
    runScraperFromQueue,
    initializeScraper,
    extractSmartArticleUrls,
    getProxyMode,
    setProxyMode,
    getProxyConfigSummary,
    updateWebshareProxyList,
    updateCrawlbaseToken,
    testProxyProvider
} from './scraper.js';
import { runSitemapScraper, runBulkSitemapScraper, rescanSavedSitemaps } from './xml.js';
import { runTrackedPagesOnce, extractTrackedPageUrlsFromSitemap } from './rss.js';
import {
    getAllSitemaps,
    getHistoryCount,
    exportUrlsByStatus,
    getUrlsByStatus,
    normalizeUrl as normalizeDbUrl,
    getTrackedPages,
    getEnabledTrackedPages,
    getTrackedPageById,
    saveTrackedPage,
    setTrackedPageEnabled,
    deleteTrackedPage,
    updateTrackedPageRunResult,
    getPageTrackerSchedule,
    setPageTrackerSchedule,
    setPageTrackerEnabled,
    getPageTrackerLastSummary,
    savePageTrackerLastSummary,
    getDomainVariable,
    setDomainVariable,
    enqueueUrls,
    makeBatchId,
    listQueueBatches,
    countQueued,
    deleteQueueBatch,
    purgeDoneFromQueue,
    purgeAlreadyScrapedFromQueue,
    normalizeQueuedUrls,
    getQueuedUrls,
    urlExists,
    removeQueuedUrl,
    DB_FILE_PATH,
    closeDatabase
} from './db.js';

// Utility function to escape MarkdownV2 special characters
const escapeMarkdownV2 = (text) => {
    // Escape all characters that are special in MarkdownV2, except for '*' (which we use for bolding)
    // and '\n' (which we use for newlines).
    // The order matters: escape '\' first to avoid double-escaping new backslashes.
    return text.replace(/\\/g, '\\\\') // Escape backslashes first
               .replace(/([_[\]()~`>#+\-=|{}.!])/g, '\\$1'); // Escape other special characters
};

const formatHelpLine = (emoji, command, description) =>
    `${emoji} \`${command}\` ${escapeMarkdownV2(description)}`;

const RETRY_OPTIONS = {
    failed: {
        statuses: ['Failed'],
        label: 'Failed URLs'
    },
    no_result: {
        statuses: ['No_Result'],
        label: 'No Result URLs'
    },
    both: {
        statuses: ['Failed', 'No_Result'],
        label: 'Failed + No Result URLs'
    }
};

const PAGE_TRACKER_INTERVAL_OPTIONS = [1, 2, 6];
const execFileAsync = promisify(execFile);
const DEPLOY_STATUS_FILE = path.join(process.cwd(), '.deploy_status.json');
const DEPLOY_SCRIPT_PATH = path.join(process.cwd(), 'deploy_bot.sh');
const DOWNLOAD_LINK_TTL_MS = 30 * 60 * 1000;
const downloadLinkTokens = new Map();
let downloadServerStarted = false;

const getResultDirPath = () => path.join(process.cwd(), 'result');

const getAllResultsPath = () => path.join(getResultDirPath(), 'all_results.csv');

const getHistoryDbPath = () => path.join(process.cwd(), 'history.db');

const getFallbackDownloadHost = () => {
    const interfaces = os.networkInterfaces();

    for (const entries of Object.values(interfaces)) {
        for (const entry of entries || []) {
            if (entry && entry.family === 'IPv4' && !entry.internal) {
                return entry.address;
            }
        }
    }

    return 'localhost';
};

const getDownloadBaseUrl = () => {
    if (DOWNLOAD_LINK_BASE_URL) {
        return DOWNLOAD_LINK_BASE_URL.replace(/\/+$/, '');
    }

    return `http://${getFallbackDownloadHost()}:${DOWNLOAD_LINK_PORT}`;
};

const cleanupExpiredDownloadLinks = () => {
    const now = Date.now();
    for (const [token, entry] of downloadLinkTokens.entries()) {
        if (!entry || entry.expiresAt <= now) {
            downloadLinkTokens.delete(token);
        }
    }
};

const createHistoryDbDownloadLink = () => {
    cleanupExpiredDownloadLinks();
    const token = randomBytes(24).toString('hex');
    downloadLinkTokens.set(token, {
        type: 'history_db',
        expiresAt: Date.now() + DOWNLOAD_LINK_TTL_MS
    });

    return {
        url: `${getDownloadBaseUrl()}/download/history-db/${token}`,
        configuredBaseUrl: Boolean(DOWNLOAD_LINK_BASE_URL)
    };
};

const startDownloadServer = () => {
    if (downloadServerStarted) return;
    downloadServerStarted = true;

    const server = createServer((req, res) => {
        cleanupExpiredDownloadLinks();

        if (!req.url || req.method !== 'GET') {
            res.writeHead(405, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('Method not allowed');
            return;
        }

        let requestUrl;
        try {
            requestUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        } catch {
            res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('Invalid request');
            return;
        }

        const tokenMatch = requestUrl.pathname.match(/^\/download\/history-db\/([a-f0-9]+)$/i);
        if (!tokenMatch) {
            res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('Not found');
            return;
        }

        const token = tokenMatch[1];
        const tokenEntry = downloadLinkTokens.get(token);
        if (!tokenEntry || tokenEntry.type !== 'history_db' || tokenEntry.expiresAt <= Date.now()) {
            downloadLinkTokens.delete(token);
            res.writeHead(410, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('This download link has expired.');
            return;
        }

        const historyDbPath = getHistoryDbPath();
        if (!fs.existsSync(historyDbPath)) {
            downloadLinkTokens.delete(token);
            res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('history.db was not found.');
            return;
        }

        downloadLinkTokens.delete(token);
        res.writeHead(200, {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename="history.db"',
            'Cache-Control': 'no-store'
        });

        const readStream = fs.createReadStream(historyDbPath);
        readStream.on('error', () => {
            if (!res.headersSent) {
                res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
            }
            res.end('Failed to stream history.db.');
        });
        readStream.pipe(res);
    });

    server.listen(DOWNLOAD_LINK_PORT, '0.0.0.0', () => {
        console.log(`Temporary download server listening on port ${DOWNLOAD_LINK_PORT}`);
    });
};

const listCsvFilesInFolder = (folderName) => {
    const safeFolderName = path.basename(folderName || '');
    if (!safeFolderName || safeFolderName.startsWith('.')) {
        return [];
    }

    const folderPath = path.join(getResultDirPath(), safeFolderName);
    if (!fs.existsSync(folderPath)) {
        return [];
    }

    return fs.readdirSync(folderPath, { withFileTypes: true })
        .filter(entry => entry.isFile() && !entry.name.startsWith('.') && entry.name.toLowerCase().endsWith('.csv'))
        .map(entry => path.basename(entry.name))
        .sort((a, b) => a.localeCompare(b));
};

const listResultFoldersWithCsv = () => {
    const resultDir = getResultDirPath();
    if (!fs.existsSync(resultDir)) {
        return [];
    }

    return fs.readdirSync(resultDir, { withFileTypes: true })
        .filter(entry => entry.isDirectory() && !entry.name.startsWith('.'))
        .map(entry => path.basename(entry.name))
        .filter(folderName => listCsvFilesInFolder(folderName).length > 0)
        .sort((a, b) => a.localeCompare(b));
};

const RESULTS_ROOT_PAGE_SIZE = 8;
const DOMAIN_VARIABLES_PAGE_SIZE = 8;
const resultsViewStates = new Map();
const domainVariableViewStates = new Map();

const getResultsViewState = (chatId) =>
    resultsViewStates.get(chatId.toString()) || { page: 0, query: '' };

const setResultsViewState = (chatId, state = {}) => {
    const currentState = getResultsViewState(chatId);
    const nextPage = Object.prototype.hasOwnProperty.call(state, 'page')
        ? Math.max(0, Number(state.page) || 0)
        : currentState.page;
    const nextQuery = Object.prototype.hasOwnProperty.call(state, 'query')
        ? String(state.query || '').trim()
        : currentState.query;

    resultsViewStates.set(chatId.toString(), {
        page: nextPage,
        query: nextQuery
    });
};

const clearResultsViewState = (chatId) => {
    resultsViewStates.set(chatId.toString(), { page: 0, query: '' });
};

const getDomainVariableViewState = (chatId) =>
    domainVariableViewStates.get(chatId.toString()) || { page: 0 };

const setDomainVariableViewState = (chatId, state = {}) => {
    const currentState = getDomainVariableViewState(chatId);
    const nextPage = Object.prototype.hasOwnProperty.call(state, 'page')
        ? Math.max(0, Number(state.page) || 0)
        : currentState.page;

    domainVariableViewStates.set(chatId.toString(), { page: nextPage });
};

const clearDomainVariableViewState = (chatId) => {
    domainVariableViewStates.set(chatId.toString(), { page: 0 });
};

const getResultsRootMenuState = (chatId) => {
    const currentState = getResultsViewState(chatId);
    const allFolders = listResultFoldersWithCsv();
    const query = String(currentState.query || '').trim();
    const normalizedQuery = query.toLowerCase();
    const filteredFolders = normalizedQuery
        ? allFolders.filter(folderName => folderName.toLowerCase().includes(normalizedQuery))
        : allFolders;
    const totalPages = Math.max(1, Math.ceil(filteredFolders.length / RESULTS_ROOT_PAGE_SIZE));
    const safePage = filteredFolders.length === 0
        ? 0
        : Math.min(currentState.page || 0, totalPages - 1);

    setResultsViewState(chatId, { page: safePage, query });

    return {
        query,
        allFolders,
        filteredFolders,
        totalPages,
        page: safePage,
        pagedFolders: filteredFolders.slice(
            safePage * RESULTS_ROOT_PAGE_SIZE,
            (safePage + 1) * RESULTS_ROOT_PAGE_SIZE
        ),
        hasAllResults: fs.existsSync(getAllResultsPath()),
        hasHistoryDb: fs.existsSync(getHistoryDbPath()),
        hasAnyItem: fs.existsSync(getAllResultsPath()) || fs.existsSync(getHistoryDbPath()) || allFolders.length > 0
    };
};

const buildResultsRootKeyboard = (menuState) => {
    const inlineKeyboard = [];

    if (menuState.hasAllResults) {
        inlineKeyboard.push([
            { text: '📄 all_results.csv', callback_data: 'results_root_all' }
        ]);
    }

    if (menuState.hasHistoryDb) {
        inlineKeyboard.push([
            { text: '🗄️ history.db backup', callback_data: 'results_root_db' }
        ]);
        inlineKeyboard.push([
            { text: '🌐 history.db browser link', callback_data: 'results_root_db_link' }
        ]);
    }

    if (menuState.allFolders.length > 0 || menuState.query) {
        inlineKeyboard.push([
            { text: '🔎 Search Domains', callback_data: 'results_search' }
        ]);
    }

    menuState.pagedFolders.forEach(folderName => {
        inlineKeyboard.push([
            { text: `📁 ${folderName}`, callback_data: `results_folder:${folderName}` }
        ]);
    });

    if (menuState.totalPages > 1) {
        inlineKeyboard.push([
            { text: '⬅️ Prev', callback_data: 'results_page:prev' },
            { text: `📄 ${menuState.page + 1}/${menuState.totalPages}`, callback_data: 'results_page:stay' },
            { text: 'Next ➡️', callback_data: 'results_page:next' }
        ]);
    }

    if (menuState.query) {
        inlineKeyboard.push([
            { text: '❌ Clear Search', callback_data: 'results_search_clear' }
        ]);
    }

    inlineKeyboard.push([
        { text: '⬅️ Back to Home', callback_data: 'home_back' }
    ]);

    return { inline_keyboard: inlineKeyboard };
};

const getDomainVariableMenuState = (chatId) => {
    const currentState = getDomainVariableViewState(chatId);
    const domains = listResultFoldersWithCsv();
    const totalPages = Math.max(1, Math.ceil(domains.length / DOMAIN_VARIABLES_PAGE_SIZE));
    const safePage = domains.length === 0 ? 0 : Math.min(currentState.page || 0, totalPages - 1);

    setDomainVariableViewState(chatId, { page: safePage });

    return {
        domains,
        totalPages,
        page: safePage,
        pagedDomains: domains.slice(
            safePage * DOMAIN_VARIABLES_PAGE_SIZE,
            (safePage + 1) * DOMAIN_VARIABLES_PAGE_SIZE
        )
    };
};

const formatDomainVariableButtonText = (domain) => {
    const currentVariable = getDomainVariable(domain);
    if (!currentVariable) {
        return `🧩 ${domain}`;
    }

    const shortVariable = currentVariable.length > 18 ? `${currentVariable.slice(0, 15)}...` : currentVariable;
    return `🧩 ${domain} • ${shortVariable}`;
};

const buildDomainVariableKeyboard = (menuState) => {
    const inlineKeyboard = [
        [{ text: '📥 Export CSV', callback_data: 'domain_variable_export_csv' }],
        [{ text: '📤 Upload CSV', callback_data: 'domain_variable_import_csv_wait' }],
        ...menuState.pagedDomains.map(domain => ([
        { text: formatDomainVariableButtonText(domain), callback_data: `domain_variable_select:${domain}` }
        ]))
    ];

    if (menuState.totalPages > 1) {
        inlineKeyboard.push([
            { text: '⬅️ Prev', callback_data: 'domain_variable_page:prev' },
            { text: `📄 ${menuState.page + 1}/${menuState.totalPages}`, callback_data: 'domain_variable_page:stay' },
            { text: 'Next ➡️', callback_data: 'domain_variable_page:next' }
        ]);
    }

    inlineKeyboard.push([
        { text: '⬅️ Back to Results', callback_data: 'home_section:results' }
    ]);

    return { inline_keyboard: inlineKeyboard };
};

const buildDomainVariablePromptKeyboard = () => ({
    inline_keyboard: [
        [{ text: '⬅️ Back to Domain Variables', callback_data: 'home_open:domain_variables' }]
    ]
});

const buildFolderFilesKeyboard = (folderName) => {
    const safeFolderName = path.basename(folderName || '');
    const inlineKeyboard = [[
        { text: '⬅️ Back to Results', callback_data: 'results_back_root' }
    ]];

    listCsvFilesInFolder(safeFolderName).forEach(fileName => {
        inlineKeyboard.push([
            { text: `📄 ${fileName}`, callback_data: `results_file:${safeFolderName}:${fileName}` }
        ]);
    });

    return { inline_keyboard: inlineKeyboard };
};

// CSV / URL / auth helpers moved to utils.js to avoid drift between bot.js and scraper.js.
// (Imported via the shared `import` block at the top of this file.)

const rewriteCsvFile = (filePath, transformRows) => {
    if (!fs.existsSync(filePath)) return;

    const content = fs.readFileSync(filePath, 'utf8');
    if (!content.trim()) return;

    const lines = content.split('\n');
    const header = parseCsvLine(lines[0]);
    let variableIndex = header.indexOf('Domain Variable');

    if (variableIndex === -1) {
        header.push('Domain Variable');
        variableIndex = header.length - 1;
    }

    const rewrittenLines = lines.map((line, index) => {
        if (!line.trim()) return line;
        if (index === 0) return serializeCsvLine(header);

        const values = parseCsvLine(line);
        while (values.length <= variableIndex) {
            values.push('');
        }

        return serializeCsvLine(transformRows(values, { header, variableIndex }));
    });

    fs.writeFileSync(filePath, rewrittenLines.join('\n'));
};

const syncDomainVariableToResultFiles = (domain, variable) => {
    const safeDomain = String(domain || '').trim().toLowerCase().replace(/^www\./, '');
    const normalizedVariable = String(variable || '').trim();
    if (!safeDomain || !normalizedVariable) return;

    const allResultsPath = getAllResultsPath();
    rewriteCsvFile(allResultsPath, (values, { header, variableIndex }) => {
        const domainIndex = header.indexOf('Domain');
        const rowDomain = String(values[domainIndex] || '').trim().toLowerCase().replace(/^www\./, '');
        if (rowDomain === safeDomain) {
            values[variableIndex] = normalizedVariable;
        }
        return values;
    });

    const domainDir = path.join(getResultDirPath(), safeDomain);
    if (!fs.existsSync(domainDir)) return;

    fs.readdirSync(domainDir, { withFileTypes: true })
        .filter(entry => entry.isFile() && entry.name.toLowerCase().endsWith('.csv'))
        .forEach(entry => {
            const filePath = path.join(domainDir, entry.name);
            rewriteCsvFile(filePath, (values, { variableIndex }) => {
                values[variableIndex] = normalizedVariable;
                return values;
            });
        });
};

const createDomainVariableExportFile = () => {
    const exportDir = ensureTelegramUploadDir();
    const exportPath = path.join(exportDir, `domain_variables_${Date.now()}.csv`);
    const lines = [
        serializeCsvLine(['Domain', 'Domain Variable']),
        ...listResultFoldersWithCsv().map(domain =>
            serializeCsvLine([domain, getDomainVariable(domain)])
        )
    ];

    fs.writeFileSync(exportPath, `${lines.join('\n')}\n`);
    return exportPath;
};

const normalizeImportHeader = (value) =>
    String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');

const importDomainVariablesFromCsv = (filePath) => {
    const content = fs.readFileSync(filePath, 'utf8');
    if (!content.trim()) {
        throw new Error('CSV file is empty');
    }

    const lines = content.split('\n').filter(line => line.trim());
    if (lines.length === 0) {
        throw new Error('CSV file is empty');
    }

    const header = parseCsvLine(lines[0]).map(normalizeImportHeader);
    const domainIndex = header.findIndex(value => value === 'domain');
    const variableIndex = header.findIndex(value =>
        value === 'domain_variable' || value === 'variable'
    );

    if (domainIndex === -1 || variableIndex === -1) {
        throw new Error('CSV must include Domain and Domain Variable columns');
    }

    const latestByDomain = new Map();
    let invalidCount = 0;
    let skippedCount = 0;

    for (let i = 1; i < lines.length; i++) {
        const values = parseCsvLine(lines[i]);
        const domain = String(values[domainIndex] || '').trim().toLowerCase().replace(/^www\./, '');
        const variable = String(values[variableIndex] || '').trim();

        if (!domain && !variable) {
            continue;
        }

        if (!domain || !variable) {
            invalidCount++;
            continue;
        }

        const previousValue = latestByDomain.get(domain);
        if (previousValue === variable) {
            skippedCount++;
            continue;
        }

        latestByDomain.set(domain, variable);
    }

    let updatedCount = 0;
    for (const [domain, variable] of latestByDomain.entries()) {
        const currentVariable = getDomainVariable(domain);
        if (currentVariable === variable) {
            skippedCount++;
            continue;
        }

        setDomainVariable(domain, variable);
        syncDomainVariableToResultFiles(domain, variable);
        updatedCount++;
    }

    return {
        updatedCount,
        skippedCount,
        invalidCount
    };
};

const sendDocumentFile = (chatId, filePath, caption) =>
    bot.sendDocument(chatId, filePath, { caption });

const safeAnswerCallbackQuery = async (callbackQueryId, options = {}) => {
    try {
        await bot.answerCallbackQuery(callbackQueryId, options);
    } catch (error) {
        const description = error?.response?.body?.description || error?.message || '';
        if (
            description.includes('query is too old') ||
            description.includes('query ID is invalid')
        ) {
            console.warn(`Callback query acknowledgement skipped: ${description}`);
            return;
        }
        throw error;
    }
};

const sendOrEditMenu = async (chatId, text, replyMarkup, message = null) => {
    if (message && message.message_id) {
        try {
            await bot.editMessageText(text, {
                chat_id: chatId,
                message_id: message.message_id,
                reply_markup: replyMarkup,
                disable_web_page_preview: true
            });
            return;
        } catch (error) {
            const description = error?.response?.body?.description || error?.message || '';
            if (description.includes('message is not modified')) {
                return;
            }
            // Log other edit errors so we can diagnose silent button failures
            // (e.g. BUTTON_DATA_INVALID from oversized callback_data).
            console.error(`[sendOrEditMenu] editMessageText failed: ${description}`);
        }
    }

    try {
        await bot.sendMessage(chatId, text, {
            reply_markup: replyMarkup,
            disable_web_page_preview: true
        });
    } catch (error) {
        const description = error?.response?.body?.description || error?.message || '';
        console.error(`[sendOrEditMenu] sendMessage failed: ${description}`);
        // Best-effort fallback: show the text alone so the user isn't stuck
        try {
            await bot.sendMessage(chatId, `⚠️ Menu render failed: ${description}\n\n${text}`);
        } catch { /* give up */ }
    }
};

const showHomeMenu = async (chatId, message = null) => {
    await sendOrEditMenu(chatId, 'Choose a section:', buildHomeMenuKeyboard(), message);
};

const showHomeInfoPage = async (chatId, title, lines, message = null) => {
    await sendOrEditMenu(chatId, [title, '', ...lines].join('\n'), buildHomeBackKeyboard(), message);
};

const showScrapeFileSelectionMenu = async (chatId, message = null) => {
    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) {
        fs.mkdirSync(toScrapeDir);
    }

    const inlineKeyboard = fs.readdirSync(toScrapeDir)
        .filter(f => f.endsWith('.txt') || f.endsWith('.csv'))
        .map(f => [{ text: `📄 ${f}`, callback_data: `scrape_file_select:${f}` }]);

    inlineKeyboard.push([{ text: '⬅️ Back to Home', callback_data: 'home_back' }]);

    if (inlineKeyboard.length === 1) {
        await sendOrEditMenu(chatId, 'No .txt or .csv files found in the "to scrape" folder.', buildHomeBackKeyboard(), message);
        return;
    }

    await sendOrEditMenu(chatId, 'Select a file to scrape:', { inline_keyboard: inlineKeyboard }, message);
};

const showResultsRootMenu = async (chatId, message = null, statusLine = '') => {
    const menuState = getResultsRootMenuState(chatId);
    if (!menuState.hasAnyItem) {
        await sendOrEditMenu(chatId, 'No result or backup files are available yet.', undefined, message);
        return;
    }

    const keyboard = buildResultsRootKeyboard(menuState);
    const lines = ['Select a result or backup file to download:'];
    if (menuState.query) {
        lines.push(`🔎 Filter: ${menuState.query}`);
        lines.push(`Matches: ${menuState.filteredFolders.length}`);
    }
    if (menuState.totalPages > 1) {
        lines.push(`📄 Page: ${menuState.page + 1}/${menuState.totalPages}`);
    }
    if (menuState.query && menuState.filteredFolders.length === 0) {
        lines.push('No matching domains found.');
    }
    if (statusLine) {
        lines.push(statusLine);
    }

    await sendOrEditMenu(chatId, lines.join('\n'), keyboard, message);
};

const showDomainVariableMenu = async (chatId, message = null, statusLine = '') => {
    const menuState = getDomainVariableMenuState(chatId);
    if (menuState.domains.length === 0) {
        await sendOrEditMenu(chatId, 'No result domains are available yet.', buildResultsMenuKeyboard(), message);
        return;
    }

    const lines = ['Choose a domain to set its variable, or use CSV for bulk update:'];
    if (menuState.totalPages > 1) {
        lines.push(`📄 Page: ${menuState.page + 1}/${menuState.totalPages}`);
    }
    if (statusLine) {
        lines.push(statusLine);
    }

    await sendOrEditMenu(chatId, lines.join('\n'), buildDomainVariableKeyboard(menuState), message);
};

const buildDomainVariablePromptText = (domain) => {
    const currentVariable = getDomainVariable(domain);
    return [
        'Domain Variable',
        `Domain: ${domain}`,
        `Current: ${currentVariable || 'Not Set'}`,
        '',
        'Send the variable in your next message.',
        'Example: NY Weekly'
    ].join('\n');
};

const showFolderFilesMenu = async (chatId, folderName, message = null, statusLine = '') => {
    const safeFolderName = path.basename(folderName || '');
    const files = listCsvFilesInFolder(safeFolderName);

    if (files.length === 0) {
        return false;
    }

    const lines = [`📁 ${safeFolderName}`, 'Select a CSV file to download:'];
    if (statusLine) {
        lines.push(statusLine);
    }

    await sendOrEditMenu(
        chatId,
        lines.join('\n'),
        buildFolderFilesKeyboard(safeFolderName),
        message
    );
    return true;
};

const listSitemapDomains = () => {
    const domains = new Set();

    getAllSitemaps().forEach(url => {
        try {
            domains.add(new URL(url).hostname);
        } catch (error) {
            // Ignore invalid URLs saved in sitemap history.
        }
    });

    return Array.from(domains).sort((a, b) => a.localeCompare(b));
};

const getRetryOption = (retryKey) => RETRY_OPTIONS[retryKey] || RETRY_OPTIONS.both;

const collectRetryUrls = (retryKey, domain = 'all') => {
    const retryOption = getRetryOption(retryKey);
    const urls = Array.from(new Set(
        retryOption.statuses.flatMap(status => getUrlsByStatus(status))
    ));

    if (domain === 'all') {
        return urls;
    }

    return urls.filter(url => {
        try {
            return new URL(url).hostname === domain;
        } catch (error) {
            return false;
        }
    });
};

const listRetryDomains = (retryKey) => {
    const domains = new Set();

    collectRetryUrls(retryKey).forEach(url => {
        try {
            domains.add(new URL(url).hostname);
        } catch (error) {
            // Ignore invalid URLs in retry history.
        }
    });

    return Array.from(domains).sort((a, b) => a.localeCompare(b));
};

const buildRetryTypeKeyboard = () => ({
    inline_keyboard: [
        [{ text: '❌ Failed', callback_data: 'retry_type:failed' }],
        [{ text: '⚠️ No Result', callback_data: 'retry_type:no_result' }],
        [{ text: '♻️ Failed + No Result', callback_data: 'retry_type:both' }],
        [{ text: '⬅️ Back to Results', callback_data: 'home_section:results' }]
    ]
});

const buildRetryDomainKeyboard = (retryKey) => {
    const inlineKeyboard = [[
        { text: '🌐 All Domains', callback_data: `retry_run:${retryKey}:all` }
    ]];

    listRetryDomains(retryKey).forEach(domain => {
        inlineKeyboard.push([
            { text: `🌍 ${domain}`, callback_data: `retry_run:${retryKey}:${encodeURIComponent(domain)}` }
        ]);
    });

    inlineKeyboard.push([
        { text: '⬅️ Back to Retry Types', callback_data: 'retry_back_types' }
    ]);

    return { inline_keyboard: inlineKeyboard };
};

const buildSitemapRescanKeyboard = () => {
    const inlineKeyboard = [[
        { text: '🌐 Rescan All', callback_data: 'sitemap_rescan_select:all' }
    ]];

    listSitemapDomains().forEach(domain => {
        inlineKeyboard.push([
            { text: `🗺️ ${domain}`, callback_data: `sitemap_rescan_select:${domain}` }
        ]);
    });

    inlineKeyboard.push([
        { text: '⬅️ Back to Home', callback_data: 'home_back' }
    ]);

    return { inline_keyboard: inlineKeyboard };
};

const runSitemapRescanFlow = async (chatId, targetDomain) => {
    await withBotStatus('sitemap_rescan', `rescan:${targetDomain || 'all'}`, async () => {
        let trackerMessage = await sendPlainMessage('🗺️ XML Rescan Running\nStarting...', chatId);
        let lastTrackerUpdateAt = 0;

        const updateTracker = async (summary, currentSitemapUrl = '', force = false) => {
            const now = Date.now();
            if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
                return;
            }

            lastTrackerUpdateAt = now;
            const trackerText = buildSitemapRescanProgressText(summary, currentSitemapUrl);

            try {
                await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
            } catch (error) {
                trackerMessage = await sendPlainMessage(trackerText, chatId);
            }
        };

        const summary = await rescanSavedSitemaps(targetDomain === 'all' ? 'all' : targetDomain, {
            onProgress: async ({ stage, summary: progressSummary, currentSitemapUrl }) => {
                await updateTracker(progressSummary, currentSitemapUrl, stage === 'start' || stage === 'complete');
            }
        });

        await updateTracker(summary, '', true);
        await sendPlainMessage(buildSitemapRescanSummaryText(summary), chatId);
    });
};

const BOT_COMMANDS = [
    { command: 'help', description: 'Show available commands' },
    { command: 'status', description: 'Get current bot status' },
    { command: 'stop', description: 'Stop the currently running task' },
    { command: 'scrape_url', description: 'Scrape a single URL' },
    { command: 'scrape_file', description: 'Scrape URLs from a file in "to scrape" folder' },
    { command: 'scrape_folder', description: 'Process all files in "to scrape" folder' },
    { command: 'sitemap', description: 'Scrape a sitemap recursively' },
    { command: 'sitemap_bulk', description: 'Bulk scrape sitemaps from sitemaps.txt' },
    { command: 'sitemap_rescan', description: 'Rescan saved sitemaps for new URLs' },
    { command: 'stats', description: 'Get scraping history statistics' },
    { command: 'download_results', description: 'Download result CSV files' },
    { command: 'export_urls', description: 'Export URLs by status (Success, Failed, All)' },
    { command: 'retry_failed', description: 'Retry URLs that previously failed or had no result' },
];

const buildHelpMessage = () => [
    '*Scraper Bot Commands*',
    '',
    '🚀 *Scraping*',
    formatHelpLine('🔗', '/scrape_url <url>', '- Scrape one article URL'),
    formatHelpLine('📄', '/scrape_file', '- Pick one file from "to scrape"'),
    formatHelpLine('📁', '/scrape_folder', '- Process all files in "to scrape"'),
    '',
    '🗺️ *Sitemaps*',
    formatHelpLine('🧭', '/sitemap <url>', '- Scan one sitemap recursively'),
    formatHelpLine('🌐', '/sitemap_bulk', '- Scan all sitemap entries from sitemaps.txt'),
    formatHelpLine('🔄', '/sitemap_rescan [domain/all]', '- Rescan saved sitemap URLs'),
    '',
    '📡 *Page Tracker*',
    'Manage tracked homepage or feed URLs from the Telegram menu.',
    '',
    '📊 *Results*',
    formatHelpLine('📡', '/status', '- Show current bot status'),
    formatHelpLine('🛑', '/stop', '- Stop the currently running task'),
    formatHelpLine('📈', '/stats', '- Show scraping statistics'),
    formatHelpLine('📂', '/download_results', '- Browse and download result CSV files'),
    formatHelpLine('📤', '/export_urls <status>', '- Export URLs by status'),
    formatHelpLine('♻️', '/retry_failed', '- Retry failed and no-result URLs'),
    '',
    'ℹ️ *Quick Help*',
    formatHelpLine('❓', '/help', '- Show this menu'),
    '',
    '*Examples*',
    '💡 `/scrape_url https://example.com/article`',
    '💡 `/sitemap https://example.com/sitemap.xml`',
    '💡 `/export_urls Success`'
].join('\n');

const buildHomeMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '📰 Article', callback_data: 'home_section:article' }],
        [{ text: '🗺️ XML', callback_data: 'home_section:xml' }],
        [{ text: '📡 Page Tracker', callback_data: 'home_section:page_tracker' }],
        [{ text: '📂 Results', callback_data: 'home_section:results' }],
        [{ text: '⚙️ System', callback_data: 'home_section:system' }]
    ]
});

const buildHomeBackKeyboard = () => ({
    inline_keyboard: [
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const buildArticleMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '🔗 Scrape URL', callback_data: 'home_usage:scrape_url' }],
        [{ text: '📄 Scrape File', callback_data: 'home_open:scrape_file' }],
        [{ text: '📤 Upload TXT / CSV', callback_data: 'home_wait:upload_txt' }],
        [{ text: '✍️ Paste URLs', callback_data: 'home_wait:paste_urls' }],
        [{ text: '📁 Scrape Folder', callback_data: 'home_action:scrape_folder' }],
        [{ text: '🗂 Scrape Queue (Run All)', callback_data: 'queue_run_all' }],
        [{ text: '🗂 Queue Menu', callback_data: 'home_open:queue_menu' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

/**
 * Renders the queue overview: global counts + up to 10 batches (most recent first).
 * "Run All Queued" pulls all Queued rows regardless of batch; per-batch buttons
 * filter to a single batch. Purge Done clears finished rows (safe — history
 * remains in `scraped_urls`).
 *
 * Batch IDs can be up to ~120 chars, but Telegram's callback_data hard limit
 * is 64 bytes. We therefore expose each batch via a short numeric index and
 * resolve the full ID via `queueBatchIndex` on click.
 */
const queueBatchIndex = new Map(); // index (string) -> batch_id
let queueBatchCounter = 0;

const registerQueueBatchIndex = (batchId) => {
    const id = (batchId == null ? '' : String(batchId));
    // Reuse an existing short index if present
    for (const [idx, bid] of queueBatchIndex.entries()) {
        if (bid === id) return idx;
    }
    const idx = String(queueBatchCounter++);
    queueBatchIndex.set(idx, id);
    return idx;
};

const resolveQueueBatchIndex = (idx) => {
    if (queueBatchIndex.has(idx)) return queueBatchIndex.get(idx);
    // Back-compat: if the callback carries a raw batch id (short enough), use as-is
    return idx;
};

const buildQueueMenuKeyboard = () => {
    const batches = listQueueBatches().slice(0, 10);
    const rows = [];
    const total = countQueued();
    rows.push([{ text: `▶️ Run All Queued (${total})`, callback_data: 'queue_run_all' }]);
    for (const b of batches) {
        const bid = b.batch_id || '';
        const idx = registerQueueBatchIndex(bid);
        const shown = bid ? (bid.length > 28 ? bid.slice(0, 25) + '…' : bid) : '(no batch)';
        const label = `${b.queued ? '🟡' : (b.done ? '✅' : '⬜')} ${shown} — ${b.queued}/${b.total}`;
        rows.push([{ text: label.slice(0, 64), callback_data: `queue_batch:${idx}` }]);
    }
    rows.push([{ text: '🧹 Purge Done', callback_data: 'queue_purge_done' }]);
    rows.push([{ text: '⬅️ Back to Home', callback_data: 'home_back' }]);
    return { inline_keyboard: rows };
};

const buildQueueBatchKeyboard = (batchId) => {
    const idx = registerQueueBatchIndex(batchId || '');
    return {
        inline_keyboard: [
            [{ text: '▶️ Run This Batch', callback_data: `queue_run_batch:${idx}` }],
            [{ text: '🗑 Delete Batch',    callback_data: `queue_del_batch:${idx}` }],
            [{ text: '⬅️ Back to Queue',   callback_data: 'home_open:queue_menu' }]
        ]
    };
};

const buildXmlMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '🧭 Scan Sitemap URL', callback_data: 'home_usage:sitemap' }],
        [{ text: '📤 Upload TXT', callback_data: 'home_wait:upload_sitemap_txt' }],
        [{ text: '✍️ Paste URLs', callback_data: 'home_wait:paste_sitemap_urls' }],
        [{ text: '🌐 Sitemap Bulk', callback_data: 'home_action:sitemap_bulk' }],
        [{ text: '🔄 Sitemap Rescan', callback_data: 'home_open:sitemap_rescan' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const formatTrackedPageTypeLabel = (type) =>
    String(type || 'html').toLowerCase() === 'rss' ? 'RSS / Atom Feed' : 'HTML Page';

const formatTrackedPageShortLabel = (url, type) => {
    try {
        const parsed = new URL(url);
        const host = parsed.hostname.replace(/^www\./, '');
        const trimmedPath = parsed.pathname === '/' ? '' : parsed.pathname.replace(/\/$/, '');
        const shortPath = trimmedPath.length > 24 ? `${trimmedPath.slice(0, 21)}...` : trimmedPath;
        const icon = String(type || 'html').toLowerCase() === 'rss' ? '📰' : '🌐';
        return `${icon} ${host}${shortPath}`;
    } catch {
        return `🌐 ${url}`;
    }
};

const buildPageTrackerMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '➕ Add Website Page', callback_data: 'page_tracker_add' }],
        [{ text: '📥 Import Page Sitemap', callback_data: 'page_tracker_import_sitemap' }],
        [{ text: '📋 View Tracked Pages', callback_data: 'page_tracker_list' }],
        [{ text: '▶️ Run Tracker Now', callback_data: 'page_tracker_run_all' }],
        [{ text: '⏱ Tracker Schedule', callback_data: 'page_tracker_schedule' }],
        [{ text: '📊 Last Tracker Summary', callback_data: 'page_tracker_last_summary' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const buildTrackedPagesKeyboard = () => {
    const inlineKeyboard = getTrackedPages().map(page => ([
        { text: formatTrackedPageShortLabel(page.url, page.type), callback_data: `page_tracker_view:${page.id}` }
    ]));

    inlineKeyboard.push([{ text: '⬅️ Back to Page Tracker', callback_data: 'home_section:page_tracker' }]);
    return { inline_keyboard: inlineKeyboard };
};

const buildTrackedPageDetailKeyboard = (pageId, enabled) => ({
    inline_keyboard: [
        [{ text: '▶️ Run Now', callback_data: `page_tracker_run_one:${pageId}` }],
        [{ text: enabled ? '⏸ Pause' : '▶️ Resume', callback_data: `page_tracker_toggle:${pageId}` }],
        [{ text: '🗑 Delete', callback_data: `page_tracker_delete:${pageId}` }],
        [{ text: '⬅️ Back to List', callback_data: 'page_tracker_list' }]
    ]
});

const buildPageTrackerAddTypeKeyboard = () => ({
    inline_keyboard: [
        [{ text: '🌐 HTML Page', callback_data: 'page_tracker_add_type:html' }],
        [{ text: '📰 RSS / Atom Feed', callback_data: 'page_tracker_add_type:rss' }],
        [{ text: '⬅️ Back to Page Tracker', callback_data: 'home_section:page_tracker' }]
    ]
});

const buildPageTrackerScheduleKeyboard = () => {
    const schedule = getPageTrackerSchedule();
    const inlineKeyboard = PAGE_TRACKER_INTERVAL_OPTIONS.map(hours => ([{
        text: `${schedule.enabled && schedule.intervalHours === hours ? '✅' : '▫️'} Every ${hours} Hour${hours === 1 ? '' : 's'}`,
        callback_data: `page_tracker_schedule_set:${hours}`
    }]));

    inlineKeyboard.push([{
        text: schedule.enabled ? '⏸ Pause Scheduler' : '▶️ Resume Scheduler',
        callback_data: schedule.enabled ? 'page_tracker_schedule_pause' : 'page_tracker_schedule_resume'
    }]);
    inlineKeyboard.push([{ text: '⬅️ Back to Page Tracker', callback_data: 'home_section:page_tracker' }]);

    return { inline_keyboard: inlineKeyboard };
};

const buildResultsMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '📂 Download Results', callback_data: 'home_open:download_results' }],
        [{ text: '🧩 Domain Variable', callback_data: 'home_open:domain_variables' }],
        [{ text: '📤 Export URLs', callback_data: 'home_usage:export_urls' }],
        [{ text: '♻️ Retry URLs', callback_data: 'home_open:retry_menu' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const buildSystemMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '📡 Bot Status', callback_data: 'home_view:status' }],
        [{ text: '📈 Statistics', callback_data: 'home_view:stats' }],
        [{ text: '🛡️ Proxy Mode', callback_data: 'home_open:proxy_mode' }],
        [{ text: '⬆️ Update & Restart', callback_data: 'home_action:deploy_update' }],
        [{ text: '💾 Download DB', callback_data: 'home_action:db_download' }],
        [{ text: '📥 Upload DB (Restore)', callback_data: 'home_wait:upload_db' }],
        [{ text: '❓ Help', callback_data: 'home_view:help' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const formatProxyModeLabel = (mode) => {
    switch (mode) {
        case 'DIRECT_ONLY':
            return 'Direct Only';
        case 'WEBSHARE_ONLY':
            return 'Webshare Only';
        case 'CRAWLBASE_ONLY':
            return 'Crawlbase Only';
        default:
            return 'Auto';
    }
};

const buildProxyModeKeyboard = () => {
    const currentMode = getProxyMode();
    const formatOption = (mode, label) => ({
        text: `${currentMode === mode ? '✅' : '▫️'} ${label}`,
        callback_data: `proxy_mode_set:${mode}`
    });

    return {
        inline_keyboard: [
            [formatOption('AUTO', 'Auto')],
            [formatOption('DIRECT_ONLY', 'Direct Only')],
            [formatOption('WEBSHARE_ONLY', 'Webshare Only')],
            [formatOption('CRAWLBASE_ONLY', 'Crawlbase Only')],
            [{ text: '📝 Update Webshare', callback_data: 'proxy_input:webshare' }],
            [{ text: '🔑 Update Crawlbase', callback_data: 'proxy_input:crawlbase' }],
            [{ text: '🧪 Test Webshare', callback_data: 'proxy_test:webshare' }],
            [{ text: '🧪 Test Crawlbase', callback_data: 'proxy_test:crawlbase' }],
            [{ text: '⬅️ Back to System', callback_data: 'home_section:system' }]
        ]
    };
};

const buildProxyMenuText = (statusLine = '') => {
    const summary = getProxyConfigSummary();
    const lines = [
        'Choose proxy mode:',
        `Current: ${formatProxyModeLabel(summary.mode)}`,
        `Webshare Proxies: ${summary.webshareCount}`,
        `Crawlbase Token: ${summary.crawlbaseConfigured ? 'Set' : 'Not Set'}`
    ];

    if (statusLine) {
        lines.push('', statusLine);
    }

    return lines.join('\n');
};

const buildPageTrackerMenuText = (statusLine = '') => {
    const schedule = getPageTrackerSchedule();
    const trackedPages = getTrackedPages();
    const lines = [
        'Website Page Tracker',
        `Tracked Pages: ${trackedPages.length}`,
        `Enabled: ${trackedPages.filter(page => Number(page.enabled) === 1).length}`,
        `Schedule: ${schedule.enabled ? `Every ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}` : 'Paused'}`
    ];

    if (statusLine) {
        lines.push('', statusLine);
    }

    return lines.join('\n');
};

const buildPageTrackerScheduleText = (statusLine = '') => {
    const schedule = getPageTrackerSchedule();
    const lines = [
        'Tracker Schedule',
        `Current: Every ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}`,
        `Status: ${schedule.enabled ? 'Enabled' : 'Paused'}`
    ];

    if (statusLine) {
        lines.push('', statusLine);
    }

    return lines.join('\n');
};

const buildTrackedPageDetailText = (page, statusLine = '') => {
    const schedule = getPageTrackerSchedule();
    const lines = [
        'Tracked Website Page',
        `URL: ${page.url}`,
        `Status: ${Number(page.enabled) === 1 ? 'Enabled' : 'Paused'}`,
        `Checks Every: ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}`,
        `Last Run: ${page.last_run_at || 'Never'}`,
        `Last New URLs: ${page.last_status ? (page.last_new_urls || 0) : 'Never'}`
    ];

    if (statusLine) {
        lines.push('', statusLine);
    }

    return lines.join('\n');
};

const buildPageTrackerProgressText = (summary) => {
    const percent = summary.totalSources === 0 ? 0 : Math.round((summary.processedSources / summary.totalSources) * 100);

    return [
        '📡 Page Tracker Running',
        `Sources: ${summary.processedSources}/${summary.totalSources}`,
        `${buildProgressBar(summary.processedSources, summary.totalSources)} ${percent}%`,
        '',
        `Current: ${summary.currentUrl || '-'}`,
        `🆕 New URLs: ${summary.newUrlsFound}`,
        `📄 Queued Files: ${summary.queuedFiles}`,
        `❌ Errors: ${summary.errors}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ].join('\n');
};

const buildPageTrackerSummaryText = (summary) => {
    return [
        '✅ Page Tracker Complete',
        `Sources Checked: ${summary.processedSources}/${summary.totalSources}`,
        `New URLs Found: ${summary.newUrlsFound}`,
        `Queued Files: ${summary.queuedFiles}`,
        `Errors: ${summary.errors}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        `Trigger: ${summary.trigger || 'manual'}`
    ].join('\n');
};

const buildLastPageTrackerSummaryText = () => {
    const summary = getPageTrackerLastSummary();
    if (!summary) {
        return 'No page tracker runs recorded yet.';
    }

    return [
        'Last Tracker Summary',
        `Ran At: ${summary.ranAt || 'Unknown'}`,
        `Trigger: ${summary.trigger || 'manual'}`,
        `Sources Checked: ${summary.processedSources}/${summary.totalSources}`,
        `New URLs Found: ${summary.newUrlsFound}`,
        `Queued Files: ${summary.queuedFiles}`,
        `Errors: ${summary.errors}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ].join('\n');
};

const importTrackedPagesFromSitemap = async (sitemapUrl) => {
    const existingUrls = new Set(getTrackedPages().map(page => normalizeDbUrl(page.url)));
    const importedUrls = await extractTrackedPageUrlsFromSitemap(sitemapUrl);

    let addedCount = 0;
    let skippedCount = 0;

    importedUrls.forEach(url => {
        const normalizedUrl = normalizeDbUrl(url);
        if (existingUrls.has(normalizedUrl)) {
            skippedCount += 1;
            return;
        }

        saveTrackedPage(normalizedUrl, 'html');
        existingUrls.add(normalizedUrl);
        addedCount += 1;
    });

    return {
        sitemapUrl,
        totalFound: importedUrls.length,
        addedCount,
        skippedCount
    };
};

const notifyDeployStatusIfPresent = async () => {
    if (!fs.existsSync(DEPLOY_STATUS_FILE)) {
        return;
    }

    try {
        const raw = fs.readFileSync(DEPLOY_STATUS_FILE, 'utf8');
        const status = JSON.parse(raw);
        const lines = [];

        if (status.status === 'restarting') {
            lines.push('✅ Bot update complete');
        } else if (status.status === 'failed') {
            lines.push('❌ Bot update failed');
        } else {
            lines.push('ℹ️ Bot update status');
        }

        if (status.fromCommit) lines.push(`From: ${status.fromCommit}`);
        if (status.toCommit) lines.push(`To: ${status.toCommit}`);
        if (status.step) lines.push(`Step: ${status.step}`);
        if (status.updatedAt) lines.push(`Time: ${status.updatedAt}`);

        await sendPlainMessage(lines.join('\n'), TELEGRAM_CHAT_ID);
    } catch (error) {
        console.error(`Failed to report deploy status: ${error.message}`);
    } finally {
        try {
            fs.unlinkSync(DEPLOY_STATUS_FILE);
        } catch (error) {
            console.error(`Failed to clear deploy status file: ${error.message}`);
        }
    }
};

const runDeployUpdate = async (chatId, message = null) => {
    await sendOrEditMenu(
        chatId,
        'Starting update from GitHub...\nThe bot will restart automatically if the update succeeds.',
        buildSystemMenuKeyboard(),
        message
    );

    try {
        await runAuxTask('deploy', 'Update & Restart bot', async () => {
            await execFileAsync('bash', [DEPLOY_SCRIPT_PATH], { cwd: process.cwd() });
        });
    } catch (error) {
        const stderr = error?.stderr ? String(error.stderr).trim() : '';
        const stdout = error?.stdout ? String(error.stdout).trim() : '';
        const details = stderr || stdout || error.message || 'Unknown error';
        await sendOrEditMenu(
            chatId,
            `❌ Update failed\n${details}`,
            buildSystemMenuKeyboard(),
            message
        );
    }
};

const showPageTrackerMenu = async (chatId, message = null, statusLine = '') => {
    await sendOrEditMenu(chatId, buildPageTrackerMenuText(statusLine), buildPageTrackerMenuKeyboard(), message);
};

const showTrackedPagesMenu = async (chatId, message = null) => {
    const pages = getTrackedPages();
    if (pages.length === 0) {
        await sendOrEditMenu(chatId, 'No tracked website pages yet.', buildPageTrackerMenuKeyboard(), message);
        return;
    }

    await sendOrEditMenu(chatId, 'Tracked Website Pages:', buildTrackedPagesKeyboard(), message);
};

const showTrackedPageDetail = async (chatId, pageId, message = null, statusLine = '') => {
    const page = getTrackedPageById(pageId);
    if (!page) {
        await sendOrEditMenu(chatId, 'Tracked URL not found.', buildTrackedPagesKeyboard(), message);
        return false;
    }

    await sendOrEditMenu(chatId, buildTrackedPageDetailText(page, statusLine), buildTrackedPageDetailKeyboard(page.id, Number(page.enabled) === 1), message);
    return true;
};

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env variables.');
    process.exit(1);
}

/**
 * One-shot startup migration: read every legacy `to scrape/*.txt|*.csv`,
 * enqueue the URLs into the DB queue (one batch per file so users can still
 * scrape them in isolation), then move the file into `to scrape/.migrated/`
 * so it doesn't get re-ingested on the next boot.
 *
 * The DB queue is now the source of truth; the folder is only kept as a
 * drop-zone for manual URL lists (users can still put a file there and the
 * next boot will fold it in).
 */
const ingestLegacyToScrapeFolder = () => {
    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) return;

    let files;
    try {
        files = fs.readdirSync(toScrapeDir).filter(f => f.endsWith('.txt') || f.endsWith('.csv'));
    } catch (err) {
        console.error(`Queue ingest: failed to read ${toScrapeDir}: ${err.message}`);
        return;
    }
    if (files.length === 0) return;

    const migratedDir = path.join(toScrapeDir, '.migrated');
    try {
        if (!fs.existsSync(migratedDir)) fs.mkdirSync(migratedDir, { recursive: true });
    } catch (err) {
        console.error(`Queue ingest: failed to create ${migratedDir}: ${err.message}`);
        return;
    }

    let totalInserted = 0;
    for (const file of files) {
        const filePath = path.join(toScrapeDir, file);
        try {
            const content = fs.readFileSync(filePath, 'utf8');
            const urls = (content.match(/https?:\/\/[^\s"'<>`]+/g) || [])
                .map(u => u.trim()).filter(Boolean);
            if (urls.length > 0) {
                const slug = file.replace(/\.(txt|csv)$/i, '').slice(0, 60);
                const batchId = makeBatchId('file_ingest', slug);
                const { inserted } = enqueueUrls(urls, { source: `file:${file}`, batchId });
                totalInserted += inserted;
                console.log(`📥 Queue ingest: ${file} → ${inserted} URL(s) (batch ${batchId})`);
            }
            fs.renameSync(filePath, path.join(migratedDir, `${Date.now()}_${file}`));
        } catch (err) {
            console.error(`Queue ingest: failed on ${file}: ${err.message}`);
        }
    }
    if (totalInserted > 0) {
        console.log(`✅ Queue ingest complete: ${totalInserted} URL(s) from ${files.length} file(s) moved to .migrated/`);
    }
};

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
initializeScraper();
ingestLegacyToScrapeFolder();
// Migrate legacy queue rows: rewrite each queued URL into its canonical
// normalized form so the cheap SQL purge (next step) joins correctly
// with `scraped_urls`. Also dedup any rows whose normalized form
// collides with an existing queued URL.
try {
    const { rewritten, dropped } = normalizeQueuedUrls();
    if (rewritten || dropped) {
        console.log(`🧹 Queue normalization: rewrote ${rewritten} URL(s), dropped ${dropped} duplicate(s).`);
    }
} catch (err) {
    console.error(`Queue normalization failed: ${err.message}`);
}
// Reconcile queue with scrape history: drop any queued URLs that are
// already in `scraped_urls`, so we don't burn time "Skipping" them.
try {
    const removed = purgeAlreadyScrapedFromQueue();
    if (removed > 0) {
        console.log(`🧹 Queue startup cleanup: removed ${removed} already-scraped URL(s) from the queue.`);
    }
} catch (err) {
    console.error(`Queue startup cleanup failed: ${err.message}`);
}
startDownloadServer();
bot.setMyCommands(BOT_COMMANDS).catch(error => {
    console.error(`Failed to set Telegram bot commands: ${error.message}`);
});

// Store bot status (e.g., 'idle', 'scraping', 'sitemap_bulk')
let botStatus = 'idle';
let currentTask = null;
// AbortController for the currently-running foreground task (scrape, sitemap, etc.)
// /stop triggers .abort() on this; scraper worker loops check signal.aborted at URL boundaries.
let currentAbortController = null;
let auxiliaryTaskCounter = 0;
const auxiliaryTasks = new Map();
const AUXILIARY_TASK_RETENTION_MS = 5 * 60 * 1000;
let pageTrackerTimer = null;
let pageTrackerRunInProgress = false;
const pendingTrackedPageAdds = new Map();

const listRunningAuxTasks = () =>
    Array.from(auxiliaryTasks.values()).filter(task => task.status === 'running');

const registerAuxTask = (type, label) => {
    const taskId = `${type}:${++auxiliaryTaskCounter}`;
    auxiliaryTasks.set(taskId, {
        id: taskId,
        type,
        label,
        status: 'running',
        startedAt: Date.now()
    });
    return taskId;
};

const finalizeAuxTask = (taskId, status = 'done', error = null) => {
    const task = auxiliaryTasks.get(taskId);
    if (!task) {
        return;
    }

    task.status = status;
    task.error = error;
    task.finishedAt = Date.now();

    const cleanupTimer = setTimeout(() => {
        auxiliaryTasks.delete(taskId);
    }, AUXILIARY_TASK_RETENTION_MS);

    if (typeof cleanupTimer.unref === 'function') {
        cleanupTimer.unref();
    }
};

const runAuxTask = async (type, label, action) => {
    const taskId = registerAuxTask(type, label);

    try {
        return await action(taskId);
    } catch (error) {
        finalizeAuxTask(taskId, 'failed', error?.message || 'Unknown error');
        throw error;
    } finally {
        const task = auxiliaryTasks.get(taskId);
        if (task && task.status === 'running') {
            finalizeAuxTask(taskId, 'done');
        }
    }
};

// Send message helper
export const sendMessage = (text, chatId = TELEGRAM_CHAT_ID, parseMode = 'MarkdownV2') => {
    return bot.sendMessage(chatId, text, {
        parse_mode: parseMode,
        disable_web_page_preview: true
    });
};

const sendPlainMessage = (text, chatId = TELEGRAM_CHAT_ID) => {
    return bot.sendMessage(chatId, text, { disable_web_page_preview: true });
};

const safeEditMessageText = async (chatId, messageId, text) => {
    try {
        await bot.editMessageText(text, {
            chat_id: chatId,
            message_id: messageId,
            disable_web_page_preview: true
        });
    } catch (error) {
        const description = error?.response?.body?.description || error?.message || '';
        const statusCode = error?.response?.statusCode || error?.code;
        const lower = description.toLowerCase();

        // No-op: text didn't change
        if (lower.includes('message is not modified')) return;

        // Transient — skip this tick, the next update will re-try editing the same message.
        // Sending a replacement message here is what caused duplicate tracker spam.
        if (
            statusCode === 429 ||
            lower.includes('too many requests') ||
            lower.includes('retry after') ||
            lower.includes('timeout') ||
            lower.includes('socket hang up') ||
            lower.includes('econnreset') ||
            lower.includes('etimedout') ||
            lower.includes('eai_again') ||
            lower.includes('network')
        ) {
            return;
        }

        // Only bubble up when the message truly can no longer be edited,
        // so the caller can fall back to sending a new tracker.
        throw error;
    }
};

/**
 * Wrap a Telegram handler with chat-id authorisation. Drops 17 copies
 * of the inline `if (chatId !== TELEGRAM_CHAT_ID) { sendMessage('Unauthorized'); return }`
 * boilerplate.
 *
 * Works for /command handlers (`msg`), document handlers (`msg`), plain
 * message handlers (`msg`), and callback_query handlers (where
 * `callbackQuery.message.chat.id` is the chat).
 */
const auth = (handler) => async (...args) => {
    const first = args[0];
    const chatId = first?.chat?.id ?? first?.message?.chat?.id ?? null;
    if (chatId == null || String(chatId) !== String(TELEGRAM_CHAT_ID)) {
        if (chatId != null) {
            try { await bot.sendMessage(chatId, 'Unauthorized access.'); } catch { /* ignore */ }
        }
        return undefined;
    }
    return handler(...args);
};

const LIVE_TRACKER_INTERVAL_MS = 10 * 1000;

/**
 * Live-tracker helper. Replaces the inline trackerMessage/lastTrackerUpdateAt/
 * try-edit-then-fallback pattern that was duplicated in
 * runFileScrape, runFolderScrape, runTrackedUrlListScrape, runRetryScrape,
 * runSitemapRescanFlow, runPageTrackerFlow, etc.
 *
 * Usage:
 *   const tracker = await createLiveTracker(chatId, () => buildText(state));
 *   await tracker.update(() => buildText(state));            // throttled
 *   await tracker.update(() => buildText(state), true);      // force flush
 *
 * Edit failures fall back to a fresh message; the next edit targets the
 * new message_id automatically.
 *
 * Returns an object {update, message} where `message` is the most-recent
 * Telegram message object (in case caller needs message_id later).
 */
const createLiveTracker = async (chatId, render, { intervalMs = LIVE_TRACKER_INTERVAL_MS } = {}) => {
    let trackerMessage = await sendPlainMessage(render(), chatId);
    let lastUpdateAt = 0;

    const update = async (renderFn = render, force = false) => {
        const now = Date.now();
        if (!force && now - lastUpdateAt < intervalMs) {
            return;
        }
        lastUpdateAt = now;
        const text = typeof renderFn === 'function' ? renderFn() : String(renderFn);

        if (!trackerMessage || !trackerMessage.message_id) {
            trackerMessage = await sendPlainMessage(text, chatId);
            return;
        }
        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, text);
        } catch (error) {
            // Edit failed — usually rate-limited or message-too-old. Fall
            // back to sending a new tracker message so the user keeps seeing
            // progress. Subsequent edits target the new message id.
            console.error(`[Tracker] Edit failed, falling back to new message: ${error?.message || error}`);
            trackerMessage = await sendPlainMessage(text, chatId);
        }
    };

    return {
        update,
        get message() { return trackerMessage; },
        get messageId() { return trackerMessage?.message_id; }
    };
};

const pendingChatInputs = new Map();

const setPendingChatInput = (chatId, state) => {
    pendingChatInputs.set(chatId.toString(), state);
};

const getPendingChatInput = (chatId) => pendingChatInputs.get(chatId.toString()) || null;

const clearPendingChatInput = (chatId) => {
    pendingChatInputs.delete(chatId.toString());
};

const setPendingTrackedPageAdd = (chatId, state) => {
    pendingTrackedPageAdds.set(chatId.toString(), state);
};

const getPendingTrackedPageAdd = (chatId) => pendingTrackedPageAdds.get(chatId.toString()) || null;

const clearPendingTrackedPageAdd = (chatId) => {
    pendingTrackedPageAdds.delete(chatId.toString());
};

const ensureTelegramUploadDir = () => {
    const uploadDir = path.join(process.cwd(), 'telegram_uploads');
    if (!fs.existsSync(uploadDir)) {
        fs.mkdirSync(uploadDir);
    }
    return uploadDir;
};

const sanitizeUploadFileName = (fileName) => {
    const safeName = path.basename(fileName || 'telegram_urls.txt');
    return safeName.replace(/[^a-zA-Z0-9._-]/g, '_');
};

const extractUrlsFromText = (text) => {
    const matches = text.match(/https?:\/\/[^\s]+/gi) || [];
    const cleaned = matches
        .map(url => url.replace(/[),.;!?]+$/g, ''))
        .filter(url => {
            try {
                new URL(url);
                return true;
            } catch (error) {
                return false;
            }
        });

    return Array.from(new Set(cleaned));
};

const downloadTelegramInputFile = async (document) => {
    const uploadDir = ensureTelegramUploadDir();
    const downloadedPath = await bot.downloadFile(document.file_id, uploadDir);
    const finalPath = path.join(uploadDir, `${Date.now()}_${sanitizeUploadFileName(document.file_name)}`);

    if (downloadedPath !== finalPath) {
        fs.renameSync(downloadedPath, finalPath);
    }

    return finalPath;
};

/**
 * Send the current SQLite DB file to Telegram so the user can keep an
 * off-server backup or seed a fresh VM.
 *
 * Telegram bot API caps outgoing documents at 50 MB. If the DB exceeds
 * that, we surface a clear message rather than failing silently.
 */
const sendDatabaseBackup = async (chatId) => {
    try {
        if (!fs.existsSync(DB_FILE_PATH)) {
            await sendPlainMessage('No database file found on disk.', chatId);
            return;
        }
        const stats = fs.statSync(DB_FILE_PATH);
        const sizeMb = stats.size / (1024 * 1024);
        if (stats.size > 50 * 1024 * 1024) {
            await sendPlainMessage(
                `Database is ${sizeMb.toFixed(1)} MB which exceeds Telegram's 50 MB upload limit.\n` +
                'Use SCP/rsync from the server, or compress and split the file before downloading.',
                chatId
            );
            return;
        }
        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        await bot.sendDocument(
            chatId,
            DB_FILE_PATH,
            { caption: `💾 history.db backup\nSize: ${sizeMb.toFixed(2)} MB\nTaken: ${ts}` },
            { filename: `history-${ts}.db`, contentType: 'application/x-sqlite3' }
        );
    } catch (err) {
        console.error(`DB backup failed: ${err.message}`);
        await sendPlainMessage(`❌ DB backup failed: ${err.message}`, chatId);
    }
};

/**
 * Replace the running history.db with an uploaded SQLite file.
 * Steps:
 *   1. Sanity-check the uploaded file (header + open trial).
 *   2. Move current DB to history.db.bak.<ts> as a safety net.
 *   3. Move uploaded file into place.
 *   4. Close DB connection and exit (systemd restarts the service,
 *      reopening the new DB at boot).
 */
const restoreDatabaseFromUpload = async (chatId, uploadedPath) => {
    try {
        // Quick sanity check: SQLite files start with the magic string
        // "SQLite format 3\0" (16 bytes).
        const fd = fs.openSync(uploadedPath, 'r');
        const header = Buffer.alloc(16);
        fs.readSync(fd, header, 0, 16, 0);
        fs.closeSync(fd);
        if (!header.toString('utf8').startsWith('SQLite format 3')) {
            await sendPlainMessage('❌ Uploaded file is not a SQLite database (bad header).', chatId);
            try { fs.unlinkSync(uploadedPath); } catch {}
            return;
        }

        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        const backupPath = `${DB_FILE_PATH}.bak.${ts}`;

        await sendPlainMessage(
            `📥 Restoring database…\n` +
            `• Backing up current DB to: ${path.basename(backupPath)}\n` +
            `• Replacing with uploaded file\n` +
            `• Bot will exit; systemd will restart and open the new DB.`,
            chatId
        );

        // 1. Close the current DB so we can move the file safely.
        closeDatabase();

        // 2. Back up the existing DB (if any).
        if (fs.existsSync(DB_FILE_PATH)) {
            fs.renameSync(DB_FILE_PATH, backupPath);
        }

        // 3. Move uploaded file into place.
        fs.renameSync(uploadedPath, DB_FILE_PATH);

        await sendPlainMessage(
            `✅ Restore staged. Exiting now — service should restart in a few seconds.`,
            chatId
        );

        // 4. Exit so systemd restarts the service with the new DB.
        // Small delay so the message above has time to flush.
        setTimeout(() => process.exit(0), 1500);
    } catch (err) {
        console.error(`DB restore failed: ${err.message}`);
        await sendPlainMessage(`❌ DB restore failed: ${err.message}`, chatId);
    }
};

const extractUrlsFromFile = (filePath) => {
    if (!fs.existsSync(filePath)) {
        return [];
    }

    return extractUrlsFromText(fs.readFileSync(filePath, 'utf8'));
};

const looksLikeSitemapUrl = (url) => {
    const lowerUrl = String(url || '').toLowerCase();
    return lowerUrl.includes('sitemap') || lowerUrl.endsWith('.xml');
};

const formatElapsed = (elapsedMs = 0) => {
    const totalSeconds = Math.max(0, Math.floor(elapsedMs / 1000));
    const minutes = String(Math.floor(totalSeconds / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    return `${minutes}:${seconds}`;
};

const formatUpdatedTime = () =>
    new Date().toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });

const buildProgressBar = (processed, total, size = 10) => {
    if (!total) {
        return '░'.repeat(size);
    }

    const ratio = Math.max(0, Math.min(1, processed / total));
    const filled = Math.round(ratio * size);
    return `${'▓'.repeat(filled)}${'░'.repeat(size - filled)}`;
};

const shortenText = (text, maxLength = 60) => {
    if (!text) {
        return '-';
    }

    return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text;
};

const buildFileScrapeProgressText = (fileName, summary) => {
    const percent = summary.totalUrls === 0 ? 0 : Math.round((summary.processedUrls / summary.totalUrls) * 100);
    return [
        '📄 File Scrape Running',
        `File: ${fileName}`,
        '',
        `URLs: ${summary.processedUrls}/${summary.totalUrls}`,
        `${buildProgressBar(summary.processedUrls, summary.totalUrls)} ${percent}%`,
        '',
        `✅ Success: ${summary.successCount}`,
        `❌ Failed: ${summary.failedCount}`,
        `⚠️ No Result: ${summary.noResultCount}`,
        `⏭️ Skipped (Already Scraped): ${summary.skippedCount || 0}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        '',
        `🔗 Current: ${summary.currentUrl || '-'}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ].join('\n');
};

const buildFileScrapeSummaryText = (fileName, summary) => {
    return [
        '✅ Scrape Complete',
        `File: ${fileName}`,
        `Processed: ${summary.processedUrls}/${summary.totalUrls}`,
        `Success: ${summary.successCount}`,
        `Failed: ${summary.failedCount}`,
        `No Result: ${summary.noResultCount}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `Skipped: ${summary.skippedCount}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ].join('\n');
};

const buildFolderScrapeProgressText = (summary) => {
    const percent = summary.totalUrls === 0 ? 0 : Math.round((summary.processedUrls / summary.totalUrls) * 100);
    const activeFileCount = summary.totalFiles === 0
        ? 0
        : Math.min(summary.processedFiles + (summary.processedFiles < summary.totalFiles ? 1 : 0), summary.totalFiles);

    return [
        '📁 Folder Scrape Running',
        `Folder: ${summary.folderName || 'to scrape'}`,
        '',
        `Files: ${activeFileCount}/${summary.totalFiles}`,
        `Current File: ${summary.currentFile || '-'}`,
        '',
        `URLs: ${summary.processedUrls}/${summary.totalUrls}`,
        `${buildProgressBar(summary.processedUrls, summary.totalUrls)} ${percent}%`,
        '',
        `✅ Success: ${summary.successCount}`,
        `❌ Failed: ${summary.failedCount}`,
        `⚠️ No Result: ${summary.noResultCount}`,
        `⏭️ Skipped (Already Scraped): ${summary.skippedCount || 0}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        '',
        `🔗 Current: ${summary.currentUrl || '-'}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ].join('\n');
};

const buildFolderScrapeSummaryText = (summary) => {
    return [
        '✅ Folder Scrape Complete',
        `Folder: ${summary.folderName || 'to scrape'}`,
        `Files Processed: ${summary.processedFiles}/${summary.totalFiles}`,
        `Total URLs: ${summary.processedUrls}/${summary.totalUrls}`,
        `Success: ${summary.successCount}`,
        `Failed: ${summary.failedCount}`,
        `No Result: ${summary.noResultCount}`,
        `Skipped (Already Scraped): ${summary.skippedCount || 0}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ].join('\n');
};

const runTrackedFileScrape = async (chatId, filePath) => {
    const fileName = path.basename(filePath);
    const initialSummary = {
        totalUrls: 0,
        processedUrls: 0,
        successCount: 0,
        failedCount: 0,
        noResultCount: 0,
        linkedinLeadCount: 0,
        instagramLeadCount: 0,
        currentUrl: '',
        elapsedMs: 0
    };

    let trackerMessage = await sendPlainMessage(buildFileScrapeProgressText(fileName, initialSummary), chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
            return;
        }

        lastTrackerUpdateAt = now;
        const trackerText = buildFileScrapeProgressText(fileName, summary);

        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
        } catch (error) {
            trackerMessage = await sendPlainMessage(trackerText, chatId);
        }
    };

    const summary = await scrapeUrlsFromInputFile(filePath, {
        signal: getCurrentAbortSignal(),
        onProgress: async (progress) => {
            await updateTracker(progress, progress.stage === 'start' || progress.stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage(`No valid URLs found in ${fileName}.`, chatId);
        return;
    }

    await updateTracker(summary, true);
    const wasStopped = getCurrentAbortSignal()?.aborted;
    const summaryHeader = wasStopped ? '🛑 Scrape Stopped by /stop' : null;
    const summaryText = buildFileScrapeSummaryText(fileName, summary);
    await sendPlainMessage(summaryHeader ? `${summaryHeader}\n${summaryText}` : summaryText, chatId);
};

const runTrackedFolderScrape = async (chatId, toScrapeDir) => {
    const initialSummary = {
        folderName: path.basename(toScrapeDir),
        totalFiles: 0,
        processedFiles: 0,
        currentFile: '-',
        totalUrls: 0,
        processedUrls: 0,
        successCount: 0,
        failedCount: 0,
        noResultCount: 0,
        currentUrl: '',
        elapsedMs: 0
    };

    let trackerMessage = await sendPlainMessage(buildFolderScrapeProgressText(initialSummary), chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
            return;
        }

        lastTrackerUpdateAt = now;
        const trackerText = buildFolderScrapeProgressText(summary);

        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
        } catch (error) {
            trackerMessage = await sendPlainMessage(trackerText, chatId);
        }
    };

    const summary = await processToScrapeFolder(toScrapeDir, {
        signal: getCurrentAbortSignal(),
        onProgress: async ({ stage, summary: progressSummary }) => {
            await updateTracker(progressSummary, stage === 'start' || stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage('No valid URLs found in the folder.', chatId);
        return;
    }

    await updateTracker(summary, true);
    const wasStopped = getCurrentAbortSignal()?.aborted;
    const summaryHeader = wasStopped ? '🛑 Folder Scrape Stopped by /stop' : null;
    const summaryText = buildFolderScrapeSummaryText(summary);
    await sendPlainMessage(summaryHeader ? `${summaryHeader}\n${summaryText}` : summaryText, chatId);
};

const runTrackedUrlListScrape = async (chatId, label, urls, force = false) => {
    const initialSummary = {
        totalUrls: urls.length,
        processedUrls: 0,
        successCount: 0,
        failedCount: 0,
        noResultCount: 0,
        linkedinLeadCount: 0,
        instagramLeadCount: 0,
        currentUrl: '',
        elapsedMs: 0
    };

    let trackerMessage = await sendPlainMessage(buildFileScrapeProgressText(label, initialSummary), chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
            return;
        }

        lastTrackerUpdateAt = now;
        const trackerText = buildFileScrapeProgressText(label, summary);

        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
        } catch (error) {
            trackerMessage = await sendPlainMessage(trackerText, chatId);
        }
    };

    const summary = await runScraper(urls, null, force, {
        signal: getCurrentAbortSignal(),
        onProgress: async (progress) => {
            await updateTracker(progress, progress.stage === 'start' || progress.stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage(`No valid URLs found in ${label}.`, chatId);
        return;
    }

    await updateTracker(summary, true);
    const wasStopped = getCurrentAbortSignal()?.aborted;
    const summaryHeader = wasStopped ? '🛑 Scrape Stopped by /stop' : null;
    const summaryText = buildFileScrapeSummaryText(label, summary);
    await sendPlainMessage(summaryHeader ? `${summaryHeader}\n${summaryText}` : summaryText, chatId);
};

const buildRetryProgressText = (targetLabel, typeLabel, summary) => {
    const percent = summary.totalUrls === 0 ? 0 : Math.round((summary.processedUrls / summary.totalUrls) * 100);

    return [
        '♻️ Retry Running',
        `Target: ${targetLabel}`,
        `Type: ${typeLabel}`,
        '',
        `URLs: ${summary.processedUrls}/${summary.totalUrls}`,
        `${buildProgressBar(summary.processedUrls, summary.totalUrls)} ${percent}%`,
        '',
        `✅ Success: ${summary.successCount}`,
        `❌ Failed: ${summary.failedCount}`,
        `⚠️ No Result: ${summary.noResultCount}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        '',
        `🔗 Current: ${summary.currentUrl || '-'}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ].join('\n');
};

const buildRetrySummaryText = (targetLabel, typeLabel, summary) => {
    return [
        '✅ Retry Complete',
        `Target: ${targetLabel}`,
        `Type: ${typeLabel}`,
        `Processed: ${summary.processedUrls}/${summary.totalUrls}`,
        `Success: ${summary.successCount}`,
        `Failed: ${summary.failedCount}`,
        `No Result: ${summary.noResultCount}`,
        `👔 LinkedIn Leads: ${summary.linkedinLeadCount || 0}`,
        `📸 Instagram Leads: ${summary.instagramLeadCount || 0}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ].join('\n');
};

const runTrackedRetryScrape = async (chatId, targetLabel, typeLabel, urls) => {
    const initialSummary = {
        totalUrls: urls.length,
        processedUrls: 0,
        successCount: 0,
        failedCount: 0,
        noResultCount: 0,
        linkedinLeadCount: 0,
        instagramLeadCount: 0,
        currentUrl: '',
        elapsedMs: 0
    };

    let trackerMessage = await sendPlainMessage(buildRetryProgressText(targetLabel, typeLabel, initialSummary), chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
            return;
        }

        lastTrackerUpdateAt = now;
        const trackerText = buildRetryProgressText(targetLabel, typeLabel, summary);

        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
        } catch (error) {
            trackerMessage = await sendPlainMessage(trackerText, chatId);
        }
    };

    const summary = await runScraper(urls, null, true, {
        onProgress: async (progress) => {
            await updateTracker(progress, progress.stage === 'start' || progress.stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage(`No URLs found for ${typeLabel} on ${targetLabel}.`, chatId);
        return;
    }

    await updateTracker(summary, true);
    await sendPlainMessage(buildRetrySummaryText(targetLabel, typeLabel, summary), chatId);
};

const buildSitemapScanProgressText = (summary) => {
    const percent = summary.totalRootSitemaps === 0
        ? 0
        : Math.round((summary.processedRootSitemaps / summary.totalRootSitemaps) * 100);

    return [
        '🗺️ XML Scan Running',
        `Source: ${summary.sourceLabel}`,
        '',
        `Sitemaps: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps}`,
        `${buildProgressBar(summary.processedRootSitemaps, summary.totalRootSitemaps)} ${percent}%`,
        '',
        `🌐 Visited: ${summary.uniqueSitemapsVisited}`,
        `🆕 URLs Found: ${summary.newUrlsFound}`,
        `💾 Saved: ${summary.newUrlsSaved}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        '',
        `🔗 Current: ${summary.currentSitemapUrl || '-'}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ].join('\n');
};

const buildSitemapScanSummaryText = (summary) => {
    const lines = [
        '✅ XML Scan Complete',
        `Source: ${summary.sourceLabel}`,
        `Sitemaps: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps}`,
        `Visited: ${summary.uniqueSitemapsVisited} sitemap files`,
        `URLs Found: ${summary.newUrlsFound}`,
        `Saved: ${summary.newUrlsSaved}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ];

    if (summary.savedFiles.length === 1) {
        lines.push(`Saved file: ${summary.savedFiles[0]}`);
    } else if (summary.savedFiles.length > 1) {
        lines.push(`Saved files: ${summary.savedFiles.length}`);
    } else {
        lines.push('Saved files: none');
    }

    return lines.join('\n');
};

const runTrackedSitemapScan = async (chatId, sourceLabel, sitemapUrls) => {
    const startedAt = Date.now();
    const summary = {
        sourceLabel,
        totalRootSitemaps: sitemapUrls.length,
        processedRootSitemaps: 0,
        uniqueSitemapsVisited: 0,
        newUrlsFound: 0,
        newUrlsSaved: 0,
        savedFiles: [],
        currentSitemapUrl: sitemapUrls[0] || '',
        elapsedMs: 0
    };

    let trackerMessage = await sendPlainMessage(buildSitemapScanProgressText(summary), chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
            return;
        }

        lastTrackerUpdateAt = now;
        summary.elapsedMs = Date.now() - startedAt;
        const trackerText = buildSitemapScanProgressText(summary);

        try {
            await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
        } catch (error) {
            trackerMessage = await sendPlainMessage(trackerText, chatId);
        }
    };

    for (const sitemapUrl of sitemapUrls) {
        summary.currentSitemapUrl = sitemapUrl;
        await updateTracker(false);

        const result = await runSitemapScraper(sitemapUrl);
        summary.processedRootSitemaps += 1;
        summary.uniqueSitemapsVisited += result?.totalSitemapsVisited || 0;
        summary.newUrlsFound += result?.totalUrlsFound || 0;
        summary.newUrlsSaved += result?.newUrlsSaved || 0;

        if (result?.filePath) {
            summary.savedFiles.push(path.basename(result.filePath));
        }

        await updateTracker(false);
    }

    summary.currentSitemapUrl = '';
    summary.elapsedMs = Date.now() - startedAt;
    await updateTracker(true);
    await sendPlainMessage(buildSitemapScanSummaryText(summary), chatId);
};

const buildSitemapRescanProgressText = (summary, currentSitemapUrl = '') => {
    const targetLabel = summary.target === 'all' ? 'All' : summary.target;
    const percent = summary.totalRootSitemaps === 0
        ? 0
        : Math.round((summary.processedRootSitemaps / summary.totalRootSitemaps) * 100);
    const lines = [
        '🗺️ XML Rescan Running',
        `Target: ${targetLabel}`,
        '',
        `Sitemaps: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps}`,
        `${buildProgressBar(summary.processedRootSitemaps, summary.totalRootSitemaps)} ${percent}%`,
        '',
        `🌐 Visited: ${summary.uniqueSitemapsVisited}`,
        `🆕 New URLs: ${summary.newUrlsFound}`,
        `⏱ Elapsed: ${formatElapsed(summary.elapsedMs)}`,
        '',
        `🔗 Current: ${currentSitemapUrl || '-'}`,
        `🕒 Updated: ${formatUpdatedTime()}`
    ];

    return lines.join('\n');
};

const buildSitemapRescanSummaryText = (summary) => {
    const targetLabel = summary.target === 'all' ? 'All' : summary.target;
    const lines = [
        '✅ XML Rescan Complete',
        `Target: ${targetLabel}`,
        `Sitemaps: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps}`,
        `Skipped: ${summary.skippedRootSitemaps}`,
        `Visited: ${summary.uniqueSitemapsVisited} sitemap files`,
        `New URLs found: ${summary.newUrlsFound}`,
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ];

    if (summary.filePath) {
        lines.push(`Saved file: ${path.basename(summary.filePath)}`);
    } else {
        lines.push('Saved file: none');
    }

    return lines.join('\n');
};

const startPageTrackerScheduler = () => {
    if (pageTrackerTimer) {
        clearInterval(pageTrackerTimer);
        pageTrackerTimer = null;
    }

    const schedule = getPageTrackerSchedule();
    if (!schedule.enabled) {
        return;
    }

    pageTrackerTimer = setInterval(() => {
        runPageTrackerFlow(null, 'scheduled').catch(error => {
            console.error(`Scheduled page tracker failed: ${error.message}`);
        });
    }, schedule.intervalHours * 60 * 60 * 1000);

    if (typeof pageTrackerTimer.unref === 'function') {
        pageTrackerTimer.unref();
    }
};

const persistPageTrackerSummary = (summary) => {
    const ranAt = new Date().toISOString();

    (summary.sourceSummaries || []).forEach(sourceSummary => {
        if (!sourceSummary?.trackedPageId) {
            return;
        }

        updateTrackedPageRunResult(sourceSummary.trackedPageId, {
            lastRunAt: ranAt,
            lastStatus: sourceSummary.status,
            lastNewUrls: sourceSummary.newUrlsCount || 0
        });
    });

    savePageTrackerLastSummary({
        ...summary,
        ranAt
    });
};

const runPageTrackerFlow = async (chatId = null, trigger = 'manual', trackedPages = null) => {
    if (pageTrackerRunInProgress) {
        if (chatId) {
            await sendPlainMessage('Page Tracker is already running.', chatId);
        }
        return null;
    }

    if (botStatus !== 'idle') {
        if (chatId) {
            await sendPlainMessage(`Bot is busy with '${botStatus}'. Page Tracker will retry on the next interval.`, chatId);
        } else {
            console.log(`[PageTracker] Skipped (${trigger}): bot busy with '${botStatus}'.`);
        }
        return null;
    }

    // Don't slip into the brief gap between the auto-sitemap rescan and
    // its subsequent queue-drain phase. The cycle-in-progress flag spans
    // both phases.
    if (autoSitemapCycleInProgress) {
        if (chatId) {
            await sendPlainMessage('Auto-sitemap cycle is running. Page Tracker will retry on the next interval.', chatId);
        } else {
            console.log(`[PageTracker] Skipped (${trigger}): auto-sitemap cycle in progress.`);
        }
        return null;
    }

    const pagesToRun = Array.isArray(trackedPages) && trackedPages.length > 0
        ? trackedPages
        : getEnabledTrackedPages();

    if (pagesToRun.length === 0) {
        if (chatId) {
            await sendPlainMessage('No enabled tracked URLs found.', chatId);
        }
        return null;
    }

    // Wrap the actual work in withBotStatus so botStatus !== 'idle' while
    // we run. This makes the auto-sitemap cycle and any manual scrape
    // request defer until the page tracker finishes — strict serialization.
    return runAuxTask('page_tracker', 'Page Tracker', async () => {
        return withBotStatus('page_tracker', `page_tracker:${trigger}`, async () => {
        pageTrackerRunInProgress = true;
        let trackerMessage = null;
        let lastTrackerUpdateAt = 0;

        const updateTracker = async (summary, force = false) => {
            if (!chatId) {
                return;
            }

            const now = Date.now();
            if (!force && now - lastTrackerUpdateAt < LIVE_TRACKER_INTERVAL_MS) {
                return;
            }

            lastTrackerUpdateAt = now;
            const trackerText = buildPageTrackerProgressText(summary);

            if (!trackerMessage) {
                trackerMessage = await sendPlainMessage(trackerText, chatId);
                return;
            }

            try {
                await safeEditMessageText(chatId, trackerMessage.message_id, trackerText);
            } catch (error) {
                trackerMessage = await sendPlainMessage(trackerText, chatId);
            }
        };

        try {
            const summary = await runTrackedPagesOnce({
                trackedPages: pagesToRun,
                onProgress: async ({ stage, summary: progressSummary }) => {
                    await updateTracker(progressSummary, stage === 'start' || stage === 'complete');
                },
                concurrencyOverride: 1
            });

            const completedSummary = {
                ...summary,
                trigger
            };

            persistPageTrackerSummary(completedSummary);
            await updateTracker(completedSummary, true);

            if (chatId) {
                await sendPlainMessage(buildPageTrackerSummaryText(completedSummary), chatId);
            } else if (completedSummary.newUrlsFound > 0 || completedSummary.errors > 0) {
                await sendPlainMessage(buildPageTrackerSummaryText(completedSummary), TELEGRAM_CHAT_ID);
            }

            return completedSummary;
        } finally {
            pageTrackerRunInProgress = false;
        }
        }); // end withBotStatus
    });
};

const withBotStatus = async (status, task, action) => {
    setBotStatus(status, task);
    const controller = new AbortController();
    currentAbortController = controller;
    try {
        // action receives the signal so callers can pass it into scraper/sitemap runners.
        await action(controller.signal);
    } finally {
        if (currentAbortController === controller) {
            currentAbortController = null;
        }
        setBotStatus('idle', null);
    }
};

const getCurrentAbortSignal = () => currentAbortController?.signal;

/* ------------------------------------------------------------------
 * Auto XML Rescan + Queue Drain
 * ------------------------------------------------------------------
 * Every N hours, rescan all saved sitemaps. Any new URLs discovered
 * are enqueued by rescanSavedSitemaps() itself. If the queue has
 * pending work afterwards, kick off a scrape run in the same tick.
 *
 * Mutual exclusion with manual tasks: we only start a cycle when
 * botStatus === 'idle'; otherwise we skip this tick and try again on
 * the next interval. The scan itself runs inside withBotStatus, so
 * manual actions will see botStatus !== 'idle' and decline to start
 * until the auto cycle finishes.
 * ------------------------------------------------------------------ */
const AUTO_SITEMAP_INTERVAL_HOURS = 5;
let autoSitemapTimer = null;
let autoSitemapCycleInProgress = false;

const runAutoSitemapCycle = async (trigger = 'scheduled') => {
    if (autoSitemapCycleInProgress) {
        console.log(`[AutoSitemap] Cycle skipped (${trigger}): previous cycle still running.`);
        return;
    }
    if (botStatus !== 'idle') {
        console.log(`[AutoSitemap] Cycle skipped (${trigger}): bot busy with '${botStatus}'. Will retry next interval.`);
        return;
    }
    if (pageTrackerRunInProgress) {
        console.log(`[AutoSitemap] Cycle skipped (${trigger}): page tracker is running. Will retry next interval.`);
        return;
    }

    autoSitemapCycleInProgress = true;
    const startedAt = new Date();
    console.log(`[AutoSitemap] Cycle starting (${trigger}) at ${startedAt.toISOString()}`);

    try {
        // Phase 1 — Rescan all saved sitemaps. Runs under withBotStatus so
        // it blocks manual work for its duration.
        let scanSummary = null;
        try {
            await withBotStatus('sitemap_rescan', `auto_rescan:${trigger}`, async () => {
                scanSummary = await rescanSavedSitemaps('all', {
                    onProgress: async () => { /* silent for scheduled runs */ }
                });
            });
        } catch (err) {
            console.error(`[AutoSitemap] Rescan failed: ${err.message}`);
        }

        const newUrls = scanSummary?.newUrlsSaved ?? scanSummary?.newUrlsFound ?? 0;
        if (scanSummary) {
            console.log(`[AutoSitemap] Rescan complete — new URLs queued: ${newUrls}`);
            // Telegram notification, silent if chat id is unset
            if (TELEGRAM_CHAT_ID) {
                try {
                    await sendPlainMessage(
                        `🤖 Auto Sitemap Rescan complete\n` +
                        `New URLs queued: ${newUrls}\n` +
                        `Root sitemaps visited: ${scanSummary.uniqueSitemapsVisited ?? '-'}`,
                        TELEGRAM_CHAT_ID
                    );
                } catch { /* ignore notification failures */ }
            }
        }

        // Phase 2 — If the queue has pending URLs, drain it (single scrape run).
        const pending = countQueued();
        if (pending === 0) {
            console.log(`[AutoSitemap] Queue empty after rescan — nothing to scrape.`);
            return;
        }

        // Pre-scrape purge, identical to the manual Run All path, to avoid
        // wasted "Skipping (Already Scraped)" work.
        try {
            const removed = purgeAlreadyScrapedFromQueue();
            if (removed > 0) {
                console.log(`[AutoSitemap] Pre-scrape cleanup removed ${removed} already-scraped URL(s).`);
            }
        } catch (err) {
            console.error(`[AutoSitemap] Pre-scrape cleanup failed: ${err.message}`);
        }

        const rawUrls = getQueuedUrls({});
        const urls = [];
        for (const u of rawUrls) {
            if (urlExists(u)) {
                removeQueuedUrl(u);
            } else {
                urls.push(u);
            }
        }

        if (urls.length === 0) {
            console.log(`[AutoSitemap] Nothing new to scrape after filtering.`);
            return;
        }

        console.log(`[AutoSitemap] Draining queue: ${urls.length} URL(s)`);
        if (TELEGRAM_CHAT_ID) {
            try {
                await sendPlainMessage(
                    `🤖 Auto-scraping ${urls.length} queued URL(s) discovered by sitemap rescan.`,
                    TELEGRAM_CHAT_ID
                );
            } catch { /* ignore */ }
        }

        try {
            await withBotStatus('scraping', `auto_queue:${trigger}`, async () => {
                await runTrackedUrlListScrape(
                    TELEGRAM_CHAT_ID,
                    `AutoQueue:${trigger}`,
                    urls
                );
            });
        } catch (err) {
            console.error(`[AutoSitemap] Queue drain failed: ${err.message}`);
        }
    } finally {
        autoSitemapCycleInProgress = false;
        const finishedAt = new Date();
        const durationSec = Math.round((finishedAt - startedAt) / 1000);
        console.log(`[AutoSitemap] Cycle finished in ${durationSec}s`);
    }
};

const startAutoSitemapScheduler = () => {
    if (autoSitemapTimer) {
        clearInterval(autoSitemapTimer);
        autoSitemapTimer = null;
    }
    const ms = AUTO_SITEMAP_INTERVAL_HOURS * 60 * 60 * 1000;
    autoSitemapTimer = setInterval(() => {
        runAutoSitemapCycle('scheduled').catch(err => {
            console.error(`[AutoSitemap] Unhandled scheduler error: ${err.message}`);
        });
    }, ms);
    if (typeof autoSitemapTimer.unref === 'function') {
        autoSitemapTimer.unref();
    }
    console.log(`[AutoSitemap] Scheduler started — every ${AUTO_SITEMAP_INTERVAL_HOURS}h.`);
};

startPageTrackerScheduler();
startAutoSitemapScheduler();
notifyDeployStatusIfPresent().catch(error => {
    console.error(`Failed to process deploy status notification: ${error.message}`);
});

// Handle /start command
bot.onText(/\/start/, auth((msg) => {
    clearPendingChatInput(msg.chat.id);
    clearPendingTrackedPageAdd(msg.chat.id);
    showHomeMenu(msg.chat.id);
    // Set bot commands for better discoverability in Telegram UI
    bot.setMyCommands(BOT_COMMANDS).catch(error => {
        console.error(`Failed to set Telegram bot commands: ${error.message}`);
    });
}));

bot.onText(/\/help/, auth((msg) => {
    bot.sendMessage(msg.chat.id, buildHelpMessage(), { parse_mode: 'MarkdownV2' });
}));

bot.onText(/^\/stop$/, auth(async (msg) => {
    if (!currentAbortController || currentAbortController.signal.aborted) {
        await bot.sendMessage(
            msg.chat.id,
            botStatus === 'idle'
                ? 'ℹ️ No task is currently running.'
                : `ℹ️ Current task (${botStatus}) is already stopping.`
        );
        return;
    }

    const stoppedTask = currentTask || botStatus;
    currentAbortController.abort();
    await bot.sendMessage(
        msg.chat.id,
        `🛑 Stop requested. The current task (${stoppedTask}) will halt after finishing any in-flight URLs. A final summary will follow shortly.`
    );
}));

bot.onText(/\/status/, auth((msg) => {
    const status = getBotStatus();
    const auxTaskSummary = status.auxTasks.length === 0
        ? 'None'
        : status.auxTasks.map((task, index) => `${index + 1}. ${task.label}`).join(' | ');
    const trackerSchedule = getPageTrackerSchedule();

    sendMessage(
        `*Bot Status:* ${escapeMarkdownV2(status.status)}\n*Current Task:* ${escapeMarkdownV2(status.task || 'None')}\n*Other Tasks:* ${escapeMarkdownV2(auxTaskSummary)}\n*Page Tracker:* ${escapeMarkdownV2(trackerSchedule.enabled ? `Every ${trackerSchedule.intervalHours} hour${trackerSchedule.intervalHours === 1 ? '' : 's'}` : 'Paused')}`
    );
}));

bot.onText(/\/scrape_url (.+)/, auth(async (msg, match) => {
    const url = match[1];
    const safeUrl = escapeMarkdownV2(url);
    await withBotStatus('scraping', `single_url:${url}`, async () => {
        sendMessage(`*Scraping single URL:* ${safeUrl}`);
        await scrapeSingleUrlAndProcess(url);
        sendMessage('*Single URL scraping complete!*');
    });
}));

bot.onText(/\/scrape_file/, auth(async (msg) => {
    await showScrapeFileSelectionMenu(msg.chat.id);
}));

bot.on('document', auth(async (msg) => {
    const fileName = path.basename(msg.document?.file_name || '');
    const lowerFileName = fileName.toLowerCase();
    const pendingInput = getPendingChatInput(msg.chat.id);

    // DB restore flow — must come before .txt/.csv gate.
    if (pendingInput?.type === 'upload_db') {
        clearPendingChatInput(msg.chat.id);
        if (!lowerFileName.endsWith('.db') && !lowerFileName.endsWith('.sqlite') && !lowerFileName.endsWith('.sqlite3')) {
            await sendPlainMessage('Please send a .db / .sqlite / .sqlite3 file for restore.', msg.chat.id);
            return;
        }
        if (botStatus !== 'idle') {
            await sendPlainMessage(`Bot is busy with '${botStatus}'. Stop the task first, then upload again.`, msg.chat.id);
            return;
        }
        try {
            const downloadedPath = await downloadTelegramInputFile(msg.document);
            await restoreDatabaseFromUpload(msg.chat.id, downloadedPath);
        } catch (err) {
            console.error(`DB upload handling failed: ${err.message}`);
            await sendPlainMessage(`❌ DB upload failed: ${err.message}`, msg.chat.id);
        }
        return;
    }

    if (!lowerFileName.endsWith('.txt') && !lowerFileName.endsWith('.csv')) {
        if (
            pendingInput?.type === 'upload_txt' ||
            pendingInput?.type === 'upload_sitemap_txt' ||
            pendingInput?.type === 'domain_variable_import_csv'
        ) {
            await sendPlainMessage('Please send a .txt or .csv file.', msg.chat.id);
        }
        return;
    }

    clearPendingChatInput(msg.chat.id);

    try {
        const filePath = await downloadTelegramInputFile(msg.document);
        if (pendingInput?.type === 'domain_variable_import_csv') {
            if (!lowerFileName.endsWith('.csv')) {
                await sendPlainMessage('Please send a .csv file for bulk domain variable import.', msg.chat.id);
                return;
            }

            const result = importDomainVariablesFromCsv(filePath);
            await showDomainVariableMenu(
                msg.chat.id,
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
                `✅ Updated ${result.updatedCount} • Skipped ${result.skippedCount} • Invalid ${result.invalidCount}`
            );
            return;
        }

        if (pendingInput?.type === 'upload_sitemap_txt') {
            const sitemapUrls = extractUrlsFromFile(filePath);
            if (sitemapUrls.length === 0) {
                await sendPlainMessage('No valid sitemap URLs found in the uploaded file.', msg.chat.id);
                return;
            }

            await withBotStatus('sitemap', `telegram_sitemap_file:${path.basename(filePath)}`, async () => {
                await runTrackedSitemapScan(msg.chat.id, `Upload: ${path.basename(filePath)}`, sitemapUrls);
            });
            return;
        }

        await withBotStatus('scraping', `telegram_file:${path.basename(filePath)}`, async () => {
            await runTrackedFileScrape(msg.chat.id, filePath);
        });
    } catch (error) {
        console.error(`Failed to handle Telegram document: ${error.message}`);
        await sendPlainMessage('Failed to download or process the uploaded file.', msg.chat.id);
    }
}));

bot.on('message', auth(async (msg) => {
    if (!msg.text || msg.text.startsWith('/')) {
        return;
    }

    const pendingInput = getPendingChatInput(msg.chat.id);
    if (!pendingInput) {
        const sitemapUrls = extractUrlsFromText(msg.text);
        if (sitemapUrls.length > 0 && sitemapUrls.every(looksLikeSitemapUrl)) {
            await withBotStatus('sitemap', `telegram_sitemap_input:${sitemapUrls.length}`, async () => {
                await runTrackedSitemapScan(msg.chat.id, 'Telegram Input', sitemapUrls);
            });
        }
        return;
    }

    if (pendingInput.type === 'proxy_update_webshare') {
        clearPendingChatInput(msg.chat.id);
        try {
            const summary = updateWebshareProxyList(msg.text);
            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(`✅ Webshare proxies updated. Loaded: ${summary.webshareCount}`),
                buildProxyModeKeyboard(),
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null
            );
        } catch (error) {
            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(`❌ Failed to update Webshare proxies: ${error.message}`),
                buildProxyModeKeyboard(),
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null
            );
        }
        return;
    }

    if (pendingInput.type === 'proxy_update_crawlbase') {
        clearPendingChatInput(msg.chat.id);
        try {
            const summary = updateCrawlbaseToken(msg.text);
            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(`✅ Crawlbase token updated. Status: ${summary.crawlbaseConfigured ? 'Set' : 'Not Set'}`),
                buildProxyModeKeyboard(),
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null
            );
        } catch (error) {
            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(`❌ Failed to update Crawlbase token: ${error.message}`),
                buildProxyModeKeyboard(),
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null
            );
        }
        return;
    }

    if (pendingInput.type === 'page_tracker_add_url') {
        const inputUrl = String(msg.text || '').trim();
        try {
            new URL(inputUrl);
        } catch (error) {
            await sendPlainMessage('Please send a valid http or https URL.', msg.chat.id);
            return;
        }

        clearPendingChatInput(msg.chat.id);
        clearPendingTrackedPageAdd(msg.chat.id);

        if (looksLikeSitemapUrl(inputUrl)) {
            try {
                const result = await importTrackedPagesFromSitemap(inputUrl);
                await showPageTrackerMenu(
                    msg.chat.id,
                    pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
                    `✅ Imported ${result.addedCount} page URL(s) from sitemap. Skipped ${result.skippedCount} existing URL(s).`
                );
            } catch (error) {
                await showPageTrackerMenu(
                    msg.chat.id,
                    pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
                    `❌ Failed to import page sitemap: ${error.message}`
                );
            }
            return;
        }

        const savedPage = saveTrackedPage(inputUrl, 'html');
        await showTrackedPageDetail(
            msg.chat.id,
            savedPage.id,
            pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
            '✅ Website page added for tracking'
        );
        return;
    }

    if (pendingInput.type === 'page_tracker_import_sitemap') {
        const inputUrl = String(msg.text || '').trim();
        try {
            new URL(inputUrl);
        } catch (error) {
            await sendPlainMessage('Please send a valid sitemap URL.', msg.chat.id);
            return;
        }

        clearPendingChatInput(msg.chat.id);

        try {
            const result = await importTrackedPagesFromSitemap(inputUrl);
            await showPageTrackerMenu(
                msg.chat.id,
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
                `✅ Imported ${result.addedCount} page URL(s) from sitemap. Skipped ${result.skippedCount} existing URL(s).`
            );
        } catch (error) {
            await showPageTrackerMenu(
                msg.chat.id,
                pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
                `❌ Failed to import page sitemap: ${error.message}`
            );
        }
        return;
    }

    if (pendingInput.type === 'domain_variable_set') {
        const variable = String(msg.text || '').trim();
        if (!variable) {
            await sendPlainMessage('Send the variable text for this domain.', msg.chat.id);
            return;
        }

        clearPendingChatInput(msg.chat.id);
        const savedVariable = setDomainVariable(pendingInput.domain, variable);
        syncDomainVariableToResultFiles(savedVariable.domain, savedVariable.variable);
        await showDomainVariableMenu(
            msg.chat.id,
            pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
            `✅ ${savedVariable.domain} → ${savedVariable.variable}`
        );
        return;
    }

    if (pendingInput.type === 'domain_variable_import_csv') {
        await sendPlainMessage('Please upload a .csv file for bulk domain variable import.', msg.chat.id);
        return;
    }

    if (pendingInput.type === 'results_search') {
        const query = String(msg.text || '').trim();
        if (!query) {
            await sendPlainMessage('Send part of a domain name to search.', msg.chat.id);
            return;
        }

        clearPendingChatInput(msg.chat.id);
        setResultsViewState(msg.chat.id, { query, page: 0 });
        await showResultsRootMenu(
            msg.chat.id,
            pendingInput.menuMessageId ? { message_id: pendingInput.menuMessageId } : null,
            `🔎 Showing results for "${query}"`
        );
        return;
    }

    if (pendingInput.type === 'upload_txt') {
        await sendPlainMessage('Please send a .txt or .csv file.', msg.chat.id);
        return;
    }

    if (pendingInput.type === 'upload_sitemap_txt') {
        await sendPlainMessage('Please send a .txt or .csv file with sitemap URLs.', msg.chat.id);
        return;
    }

    if (pendingInput.type === 'paste_sitemap_urls') {
        const sitemapUrls = extractUrlsFromText(msg.text);
        if (sitemapUrls.length === 0) {
            await sendPlainMessage('No valid sitemap URLs found. Send full http or https URLs.', msg.chat.id);
            return;
        }

        clearPendingChatInput(msg.chat.id);

        await withBotStatus('sitemap', `telegram_sitemap_input:${sitemapUrls.length}`, async () => {
            await runTrackedSitemapScan(msg.chat.id, 'Telegram Input', sitemapUrls);
        });
        return;
    }

    if (pendingInput.type !== 'paste_urls') {
        return;
    }

    const urls = extractSmartArticleUrls(msg.text);
    if (urls.length === 0) {
        await sendPlainMessage('No valid article URLs found. You can paste plain URLs or CSV text with URLs.', msg.chat.id);
        return;
    }

    clearPendingChatInput(msg.chat.id);

    await withBotStatus('scraping', `telegram_input:${urls.length}`, async () => {
        await runTrackedUrlListScrape(msg.chat.id, 'Telegram Input', urls);
    });
}));

bot.on('callback_query', async (callbackQuery) => {
    // Callback queries need a custom auth path: instead of silently
    // dropping, we answer the callback so Telegram doesn't show the
    // perpetual loading spinner on the unauthorized client.
    const msg = callbackQuery.message;
    const data = callbackQuery.data || '';
    const isAuthorized = msg && msg.chat && msg.chat.id.toString() === TELEGRAM_CHAT_ID;

    if (!isAuthorized) {
        await safeAnswerCallbackQuery(callbackQuery.id, {
            text: 'Unauthorized',
            show_alert: true
        });
        return;
    }

    clearPendingChatInput(msg.chat.id);

    if (data === 'home_back') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'home_section:article') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'Article options:', buildArticleMenuKeyboard(), msg);
        return;
    }

    if (data === 'home_section:xml') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'XML options:', buildXmlMenuKeyboard(), msg);
        return;
    }

    if (data === 'home_section:page_tracker') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showPageTrackerMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'home_section:results') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'Results options:', buildResultsMenuKeyboard(), msg);
        return;
    }

    if (data === 'home_section:system') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'System options:', buildSystemMenuKeyboard(), msg);
        return;
    }

    if (data === 'home_open:scrape_file') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showScrapeFileSelectionMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'page_tracker_add') {
        clearPendingTrackedPageAdd(msg.chat.id);
        setPendingChatInput(msg.chat.id, {
            type: 'page_tracker_add_url',
            menuMessageId: msg.message_id
        });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            'Add Website Page\n\nSend the website homepage or category page URL in your next message.\n\nExamples:\nhttps://nyweekly.com/\nhttps://nyweekly.com/category/business/',
            buildPageTrackerMenuKeyboard(),
            msg
        );
        return;
    }

    if (data === 'page_tracker_import_sitemap') {
        clearPendingTrackedPageAdd(msg.chat.id);
        setPendingChatInput(msg.chat.id, {
            type: 'page_tracker_import_sitemap',
            menuMessageId: msg.message_id
        });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            'Import Page Sitemap\n\nSend a page sitemap URL in your next message.\n\nExample:\nhttps://nyweekly.com/page-sitemap.xml',
            buildPageTrackerMenuKeyboard(),
            msg
        );
        return;
    }

    if (data === 'page_tracker_list') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showTrackedPagesMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'page_tracker_run_all') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await runPageTrackerFlow(msg.chat.id, 'manual');
        return;
    }

    if (data === 'page_tracker_schedule') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            buildPageTrackerScheduleText(),
            buildPageTrackerScheduleKeyboard(),
            msg
        );
        return;
    }

    if (data === 'page_tracker_last_summary') {
        clearPendingTrackedPageAdd(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            buildLastPageTrackerSummaryText(),
            buildPageTrackerMenuKeyboard(),
            msg
        );
        return;
    }

    if (data === 'home_wait:upload_txt') {
        setPendingChatInput(msg.chat.id, { type: 'upload_txt' });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Upload TXT',
            [
                'Send a .txt or .csv file in this chat.',
                'The bot will start scraping as soon as the file arrives.'
            ],
            msg
        );
        return;
    }

    if (data === 'home_wait:paste_urls') {
        setPendingChatInput(msg.chat.id, { type: 'paste_urls' });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Paste URLs',
            [
                'Send one or more full URLs in your next message.',
                'You can paste one URL per line or a space-separated list.'
            ],
            msg
        );
        return;
    }

    if (data === 'home_wait:upload_sitemap_txt') {
        setPendingChatInput(msg.chat.id, { type: 'upload_sitemap_txt' });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Upload XML TXT',
            [
                'Send a .txt or .csv file with sitemap URLs in this chat.',
                'The bot will start scanning as soon as the file arrives.'
            ],
            msg
        );
        return;
    }

    if (data === 'home_wait:paste_sitemap_urls') {
        setPendingChatInput(msg.chat.id, { type: 'paste_sitemap_urls' });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Paste Sitemap URLs',
            [
                'Send one or more sitemap URLs in your next message.',
                'You can paste one URL per line or a space-separated list.'
            ],
            msg
        );
        return;
    }

    if (data.startsWith('page_tracker_add_type:')) {
        const pendingTrackedPageAdd = getPendingTrackedPageAdd(msg.chat.id);
        if (!pendingTrackedPageAdd?.url) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Send the tracked URL again first.', show_alert: true });
            await showPageTrackerMenu(msg.chat.id, msg);
            return;
        }

        const selectedType = path.basename(data.slice('page_tracker_add_type:'.length)).toLowerCase() === 'rss' ? 'rss' : 'html';
        clearPendingTrackedPageAdd(msg.chat.id);
        const savedPage = saveTrackedPage(pendingTrackedPageAdd.url, selectedType);

        await safeAnswerCallbackQuery(callbackQuery.id, {
            text: `Tracked URL added as ${formatTrackedPageTypeLabel(selectedType)}`
        });
        await showTrackedPageDetail(msg.chat.id, savedPage.id, msg, '✅ Tracked URL added');
        return;
    }

    if (data.startsWith('page_tracker_view:')) {
        clearPendingTrackedPageAdd(msg.chat.id);
        const pageId = Number(data.slice('page_tracker_view:'.length));
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showTrackedPageDetail(msg.chat.id, pageId, msg);
        return;
    }

    if (data.startsWith('page_tracker_run_one:')) {
        clearPendingTrackedPageAdd(msg.chat.id);
        const pageId = Number(data.slice('page_tracker_run_one:'.length));
        const trackedPage = getTrackedPageById(pageId);

        if (!trackedPage) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Tracked URL not found', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        await runPageTrackerFlow(msg.chat.id, 'manual', [trackedPage]);
        return;
    }

    if (data.startsWith('page_tracker_toggle:')) {
        clearPendingTrackedPageAdd(msg.chat.id);
        const pageId = Number(data.slice('page_tracker_toggle:'.length));
        const trackedPage = getTrackedPageById(pageId);

        if (!trackedPage) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Tracked URL not found', show_alert: true });
            return;
        }

        const updatedPage = setTrackedPageEnabled(pageId, Number(trackedPage.enabled) !== 1);
        await safeAnswerCallbackQuery(callbackQuery.id, {
            text: Number(updatedPage.enabled) === 1 ? 'Tracker resumed' : 'Tracker paused'
        });
        await showTrackedPageDetail(msg.chat.id, pageId, msg, Number(updatedPage.enabled) === 1 ? '✅ Tracker resumed' : '⏸ Tracker paused');
        return;
    }

    if (data.startsWith('page_tracker_delete:')) {
        clearPendingTrackedPageAdd(msg.chat.id);
        const pageId = Number(data.slice('page_tracker_delete:'.length));
        deleteTrackedPage(pageId);
        await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Tracked URL deleted' });
        await showTrackedPagesMenu(msg.chat.id, msg);
        return;
    }

    if (data.startsWith('page_tracker_schedule_set:')) {
        clearPendingTrackedPageAdd(msg.chat.id);
        const intervalHours = Number(data.slice('page_tracker_schedule_set:'.length)) || 1;
        const schedule = setPageTrackerSchedule(intervalHours, true);
        startPageTrackerScheduler();
        await safeAnswerCallbackQuery(callbackQuery.id, {
            text: `Schedule set to every ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}`
        });
        await sendOrEditMenu(
            msg.chat.id,
            buildPageTrackerScheduleText(`✅ Schedule updated to every ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}`),
            buildPageTrackerScheduleKeyboard(),
            msg
        );
        return;
    }

    if (data === 'page_tracker_schedule_pause') {
        clearPendingTrackedPageAdd(msg.chat.id);
        setPageTrackerEnabled(false);
        startPageTrackerScheduler();
        await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Scheduler paused' });
        await sendOrEditMenu(
            msg.chat.id,
            buildPageTrackerScheduleText('⏸ Scheduler paused'),
            buildPageTrackerScheduleKeyboard(),
            msg
        );
        return;
    }

    if (data === 'page_tracker_schedule_resume') {
        clearPendingTrackedPageAdd(msg.chat.id);
        const schedule = setPageTrackerEnabled(true);
        startPageTrackerScheduler();
        await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Scheduler resumed' });
        await sendOrEditMenu(
            msg.chat.id,
            buildPageTrackerScheduleText(`✅ Scheduler resumed. Every ${schedule.intervalHours} hour${schedule.intervalHours === 1 ? '' : 's'}`),
            buildPageTrackerScheduleKeyboard(),
            msg
        );
        return;
    }

    if (data === 'home_open:download_results') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        clearResultsViewState(msg.chat.id);
        await showResultsRootMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'home_open:domain_variables') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        clearDomainVariableViewState(msg.chat.id);
        await showDomainVariableMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'home_open:retry_menu') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'Choose which URLs to retry:', buildRetryTypeKeyboard(), msg);
        return;
    }

    if (data === 'home_open:queue_menu') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        // Auto-clean already-scraped URLs from the queue before rendering
        // so the displayed count is accurate (not inflated by rows whose
        // urls already live in scraped_urls).
        let removedExisting = 0;
        try {
            removedExisting = purgeAlreadyScrapedFromQueue();
        } catch (err) {
            console.error(`Queue menu pre-render purge failed: ${err.message}`);
        }
        const total = countQueued();
        const headerLines = [];
        if (removedExisting > 0) {
            headerLines.push(`🧹 Auto-cleaned ${removedExisting} already-scraped URL(s).`);
        }
        headerLines.push(
            total === 0
                ? '🗂 Scrape Queue is empty.'
                : `🗂 Scrape Queue — ${total} URL(s) pending across ${listQueueBatches().length} batch(es).`
        );
        await sendOrEditMenu(msg.chat.id, headerLines.join('\n'), buildQueueMenuKeyboard(), msg);
        return;
    }

    if (data.startsWith('queue_batch:')) {
        const idx = data.slice('queue_batch:'.length);
        const batchId = resolveQueueBatchIndex(idx);
        await safeAnswerCallbackQuery(callbackQuery.id);
        const b = listQueueBatches().find(x => (x.batch_id || '') === batchId);
        const text = b
            ? `🗂 Batch: ${batchId || '(no batch)'}\nSource: ${b.source || '-'}\nQueued: ${b.queued} · Scraping: ${b.scraping} · Done: ${b.done} · Failed: ${b.failed}\nFirst seen: ${b.first_seen}\nLast update: ${b.last_updated}`
            : `🗂 Batch: ${batchId} (not found)`;
        await sendOrEditMenu(msg.chat.id, text, buildQueueBatchKeyboard(batchId), msg);
        return;
    }

    if (data === 'queue_run_all' || data.startsWith('queue_run_batch:')) {
        const batchId = data.startsWith('queue_run_batch:')
            ? resolveQueueBatchIndex(data.slice('queue_run_batch:'.length))
            : null;
        await safeAnswerCallbackQuery(callbackQuery.id);

        // Purge already-scraped URLs from the queue FIRST so we don't waste
        // minutes (or hours) "skipping" 100k+ rows that are already in
        // scraped_urls. This is a single SQL statement and completes in ms
        // even on huge queues.
        try {
            const removed = purgeAlreadyScrapedFromQueue();
            if (removed > 0) {
                await sendPlainMessage(
                    `🧹 Cleaned ${removed} already-scraped URL(s) from the queue before running.`,
                    msg.chat.id
                );
            }
        } catch (err) {
            console.error(`Pre-scrape queue cleanup failed: ${err.message}`);
        }

        const rawUrls = batchId ? getQueuedUrls({ batchId }) : getQueuedUrls({});
        // Defence in depth: the SQL purge above handles exact matches, but a
        // legacy queue row whose URL normalizes differently (e.g. trailing
        // slash vs none) can still slip through. Filter again via
        // urlExists() which runs the URL through normalizeUrl() first, and
        // delete the offending queue rows so they don't come back.
        const urls = [];
        let filtered = 0;
        for (const u of rawUrls) {
            if (urlExists(u)) {
                removeQueuedUrl(u);
                filtered++;
            } else {
                urls.push(u);
            }
        }
        if (filtered > 0) {
            await sendPlainMessage(
                `🧹 Dropped ${filtered} more already-scraped URL(s) detected via fuzzy match.`,
                msg.chat.id
            );
        }
        if (urls.length === 0) {
            await sendPlainMessage('Queue is empty — nothing new to scrape.', msg.chat.id);
            return;
        }
        const label = batchId ? `Queue:${batchId}` : 'Queue:All';
        await withBotStatus('scraping', 'queue', async () => {
            await runTrackedUrlListScrape(msg.chat.id, label, urls);
        });
        return;
    }

    if (data.startsWith('queue_del_batch:')) {
        const batchId = resolveQueueBatchIndex(data.slice('queue_del_batch:'.length));
        await safeAnswerCallbackQuery(callbackQuery.id);
        const removed = deleteQueueBatch(batchId);
        await sendPlainMessage(`🗑 Deleted ${removed} row(s) from batch ${batchId}.`, msg.chat.id);
        await sendOrEditMenu(msg.chat.id, '🗂 Scrape Queue', buildQueueMenuKeyboard(), msg);
        return;
    }

    if (data === 'queue_purge_done') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        const removed = purgeDoneFromQueue();
        await sendPlainMessage(`🧹 Purged ${removed} finished row(s) from the queue.`, msg.chat.id);
        await sendOrEditMenu(msg.chat.id, '🗂 Scrape Queue', buildQueueMenuKeyboard(), msg);
        return;
    }

    if (data === 'home_open:sitemap_rescan') {
        const availableDomains = listSitemapDomains();
        await safeAnswerCallbackQuery(callbackQuery.id);

        if (availableDomains.length === 0) {
            await showHomeInfoPage(msg.chat.id, 'Sitemap Rescan', ['No saved sitemaps found.'], msg);
            return;
        }

        await sendOrEditMenu(msg.chat.id, 'Select which sitemap group to rescan:', buildSitemapRescanKeyboard(), msg);
        return;
    }

    if (data === 'home_open:proxy_mode') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            buildProxyMenuText(),
            buildProxyModeKeyboard(),
            msg
        );
        return;
    }

    if (data.startsWith('proxy_mode_set:')) {
        const selectedMode = path.basename(data.split(':')[1] || '').toUpperCase();
        const currentMode = setProxyMode(selectedMode);
        await safeAnswerCallbackQuery(callbackQuery.id, {
            text: `Proxy mode set to ${formatProxyModeLabel(currentMode)}`
        });
        await sendOrEditMenu(
            msg.chat.id,
            buildProxyMenuText(`✅ Proxy mode set to ${formatProxyModeLabel(currentMode)}`),
            buildProxyModeKeyboard(),
            msg
        );
        return;
    }

    if (data === 'proxy_input:webshare') {
        setPendingChatInput(msg.chat.id, {
            type: 'proxy_update_webshare',
            menuMessageId: msg.message_id
        });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            `${buildProxyMenuText()}\n\nPaste Webshare proxies in your next message.\nOne proxy per line.\nSend CLEAR to remove all Webshare proxies.`,
            buildProxyModeKeyboard(),
            msg
        );
        return;
    }

    if (data === 'proxy_input:crawlbase') {
        setPendingChatInput(msg.chat.id, {
            type: 'proxy_update_crawlbase',
            menuMessageId: msg.message_id
        });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            `${buildProxyMenuText()}\n\nPaste the Crawlbase token in your next message.\nSend CLEAR to remove the saved token.`,
            buildProxyModeKeyboard(),
            msg
        );
        return;
    }

    if (data.startsWith('proxy_test:')) {
        const provider = path.basename(data.split(':')[1] || '').toUpperCase();
        const providerLabel = provider === 'CRAWLBASE' ? 'Crawlbase' : 'Webshare';

        await safeAnswerCallbackQuery(callbackQuery.id);
        await runAuxTask('proxy_test', `Test ${providerLabel} proxy`, async () => {
            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(`🧪 Testing ${providerLabel}...`),
                buildProxyModeKeyboard(),
                msg
            );

            const result = await testProxyProvider(provider);
            const statusLine = result.success
                ? `✅ ${providerLabel} test passed${result.ip ? ` • IP: ${result.ip}` : ''}`
                : `❌ ${providerLabel} test failed: ${result.message}`;

            await sendOrEditMenu(
                msg.chat.id,
                buildProxyMenuText(statusLine),
                buildProxyModeKeyboard(),
                msg
            );
        });
        return;
    }

    if (data === 'home_view:status') {
        const status = getBotStatus();
        const auxTaskSummary = status.auxTasks.length === 0
            ? 'None'
            : status.auxTasks.map((task, index) => `${index + 1}. ${task.label}`).join(' | ');
        const trackerSchedule = getPageTrackerSchedule();
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Bot Status',
            [
                `Status: ${status.status}`,
                `Current Task: ${status.task || 'None'}`,
                `Other Tasks: ${auxTaskSummary}`,
                `Page Tracker: ${trackerSchedule.enabled ? `Every ${trackerSchedule.intervalHours} hour${trackerSchedule.intervalHours === 1 ? '' : 's'}` : 'Paused'}`,
                `Proxy Mode: ${formatProxyModeLabel(getProxyMode())}`
            ],
            msg
        );
        return;
    }

    if (data === 'home_view:stats') {
        const historyCount = getHistoryCount();
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Statistics',
            [`Total URLs processed: ${historyCount}`],
            msg
        );
        return;
    }

    if (data === 'home_view:help') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Help',
            [
                'Use the Home buttons to open tools in one place.',
                'You can still type /help for the full command list.',
                'Use /scrape_url, /sitemap, and /export_urls when you need to pass values.'
            ],
            msg
        );
        return;
    }

    if (data === 'home_action:deploy_update') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await runDeployUpdate(msg.chat.id, msg);
        return;
    }

    if (data === 'home_action:db_download') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendDatabaseBackup(msg.chat.id);
        return;
    }

    if (data === 'home_wait:upload_db') {
        // Refuse if a task is running — restoring would cut it off mid-flight.
        if (botStatus !== 'idle') {
            await safeAnswerCallbackQuery(callbackQuery.id, {
                text: `Bot is busy with '${botStatus}'. Stop the task first.`,
                show_alert: true
            });
            return;
        }
        setPendingChatInput(msg.chat.id, { type: 'upload_db' });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            '📥 Upload DB (Restore)',
            [
                'Send a `history.db` SQLite file as a Telegram document.',
                '',
                '⚠️  This will REPLACE the current database.',
                '⚠️  A backup of the existing DB will be saved as `history.db.bak.<timestamp>`.',
                '⚠️  After restore the bot will exit and systemd will restart it.',
                '',
                'Telegram bot API limits incoming documents to ~20 MB.'
            ],
            msg
        );
        return;
    }

    if (data === 'home_usage:scrape_url') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Scrape URL',
            ['Use:', '/scrape_url https://example.com/article'],
            msg
        );
        return;
    }

    if (data === 'home_usage:sitemap') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Scan Sitemap URL',
            ['Use:', '/sitemap https://example.com/sitemap.xml'],
            msg
        );
        return;
    }

    if (data === 'home_usage:export_urls') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Export URLs',
            ['Use:', '/export_urls Success', '/export_urls Failed', '/export_urls All'],
            msg
        );
        return;
    }

    if (data === 'retry_back_types') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'Choose which URLs to retry:', buildRetryTypeKeyboard(), msg);
        return;
    }

    if (data.startsWith('retry_type:')) {
        const retryKey = path.basename(data.slice('retry_type:'.length));
        const retryOption = getRetryOption(retryKey);

        await safeAnswerCallbackQuery(callbackQuery.id);

        if (collectRetryUrls(retryKey).length === 0) {
            await sendOrEditMenu(msg.chat.id, `No ${retryOption.label.toLowerCase()} are available to retry.`, buildRetryTypeKeyboard(), msg);
            return;
        }

        await sendOrEditMenu(
            msg.chat.id,
            `Choose domain for ${retryOption.label}:`,
            buildRetryDomainKeyboard(retryKey),
            msg
        );
        return;
    }

    if (data.startsWith('retry_run:')) {
        const payload = data.slice('retry_run:'.length);
        const separatorIndex = payload.indexOf(':');
        const retryKey = separatorIndex === -1 ? 'both' : path.basename(payload.slice(0, separatorIndex));
        const rawDomain = separatorIndex === -1 ? 'all' : payload.slice(separatorIndex + 1);
        const domain = rawDomain === 'all' ? 'all' : path.basename(decodeURIComponent(rawDomain));
        const retryOption = getRetryOption(retryKey);
        const availableDomains = listRetryDomains(retryKey);

        if (domain !== 'all' && !availableDomains.includes(domain)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Domain is no longer available', show_alert: true });
            return;
        }

        const urlsToRetry = collectRetryUrls(retryKey, domain);
        await safeAnswerCallbackQuery(callbackQuery.id);

        if (urlsToRetry.length === 0) {
            await sendOrEditMenu(msg.chat.id, 'No matching retry URLs found.', buildRetryTypeKeyboard(), msg);
            return;
        }

        const retryTargetLabel = domain === 'all' ? 'All Domains' : domain;

        await withBotStatus('retry', `retry:${retryKey}:${domain}`, async () => {
            await runTrackedRetryScrape(msg.chat.id, retryTargetLabel, retryOption.label, urlsToRetry);
        });
        return;
    }

    if (data === 'home_action:scrape_folder') {
        const toScrapeDir = path.join(process.cwd(), 'to scrape');
        if (!fs.existsSync(toScrapeDir)) {
            fs.mkdirSync(toScrapeDir);
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        await withBotStatus('scraping', 'folder', async () => {
            await runTrackedFolderScrape(msg.chat.id, toScrapeDir);
        });
        return;
    }

    if (data === 'home_action:sitemap_bulk') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await withBotStatus('sitemap_bulk', 'bulk_sitemap', async () => {
            sendMessage('*Starting bulk sitemap scraping from sitemaps.txt...*');
            await runBulkSitemapScraper();
            sendMessage('*Bulk sitemap scraping complete!*');
        });
        return;
    }

    if (data.startsWith('scrape_file_select:')) {
        const filename = path.basename(data.replace('scrape_file_select:', ''));
        const filePath = path.join(process.cwd(), 'to scrape', filename);
        if (!fs.existsSync(filePath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);

        await withBotStatus('scraping', `file:${filename}`, async () => {
            await runTrackedFileScrape(msg.chat.id, filePath);
        });
        return;
    }

    if (data === 'results_root_all') {
        const allResultsPath = getAllResultsPath();
        if (!fs.existsSync(allResultsPath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        try {
            await runAuxTask('download', 'Download all_results.csv', async () => {
                await showResultsRootMenu(msg.chat.id, msg, '⬇️ Preparing all_results.csv...');
                await sendDocumentFile(msg.chat.id, allResultsPath, '📄 all_results.csv');
                await showResultsRootMenu(msg.chat.id, msg, '✅ all_results.csv sent');
            });
        } catch (error) {
            console.error(`Failed to send all_results.csv: ${error.message}`);
            await showResultsRootMenu(msg.chat.id, msg, '❌ Failed to send all_results.csv');
        }
        return;
    }

    if (data === 'results_search') {
        setPendingChatInput(msg.chat.id, {
            type: 'results_search',
            menuMessageId: msg.message_id
        });
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showResultsRootMenu(msg.chat.id, msg, '🔎 Send part of a domain name in your next message.');
        return;
    }

    if (data === 'results_search_clear') {
        clearResultsViewState(msg.chat.id);
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showResultsRootMenu(msg.chat.id, msg, '✅ Search cleared');
        return;
    }

    if (data.startsWith('results_page:')) {
        const direction = path.basename(data.replace('results_page:', ''));
        const currentState = getResultsViewState(msg.chat.id);

        if (direction === 'prev') {
            setResultsViewState(msg.chat.id, { page: Math.max(0, (currentState.page || 0) - 1) });
            await safeAnswerCallbackQuery(callbackQuery.id);
            await showResultsRootMenu(msg.chat.id, msg);
            return;
        }

        if (direction === 'next') {
            setResultsViewState(msg.chat.id, { page: (currentState.page || 0) + 1 });
            await safeAnswerCallbackQuery(callbackQuery.id);
            await showResultsRootMenu(msg.chat.id, msg);
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        return;
    }

    if (data.startsWith('domain_variable_page:')) {
        const direction = path.basename(data.replace('domain_variable_page:', ''));
        const currentState = getDomainVariableViewState(msg.chat.id);

        if (direction === 'prev') {
            setDomainVariableViewState(msg.chat.id, { page: Math.max(0, (currentState.page || 0) - 1) });
            await safeAnswerCallbackQuery(callbackQuery.id);
            await showDomainVariableMenu(msg.chat.id, msg);
            return;
        }

        if (direction === 'next') {
            setDomainVariableViewState(msg.chat.id, { page: (currentState.page || 0) + 1 });
            await safeAnswerCallbackQuery(callbackQuery.id);
            await showDomainVariableMenu(msg.chat.id, msg);
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        return;
    }

    if (data === 'domain_variable_export_csv') {
        await safeAnswerCallbackQuery(callbackQuery.id);

        try {
            const exportPath = createDomainVariableExportFile();
            await sendDocumentFile(msg.chat.id, exportPath, '🧩 domain_variables.csv');
            await showDomainVariableMenu(msg.chat.id, msg, '✅ domain_variables.csv sent');
        } catch (error) {
            await showDomainVariableMenu(msg.chat.id, msg, `❌ Failed to export CSV: ${error.message}`);
        }
        return;
    }

    if (data === 'domain_variable_import_csv_wait') {
        setPendingChatInput(msg.chat.id, {
            type: 'domain_variable_import_csv',
            menuMessageId: msg.message_id
        });

        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            [
                'Bulk Domain Variable Import',
                '',
                'Upload a CSV file with these columns:',
                'Domain, Domain Variable'
            ].join('\n'),
            buildDomainVariablePromptKeyboard(),
            msg
        );
        return;
    }

    if (data.startsWith('domain_variable_select:')) {
        const domain = path.basename(data.replace('domain_variable_select:', '')).toLowerCase();
        if (!listResultFoldersWithCsv().includes(domain)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Domain is no longer available', show_alert: true });
            return;
        }

        setPendingChatInput(msg.chat.id, {
            type: 'domain_variable_set',
            domain,
            menuMessageId: msg.message_id
        });

        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(
            msg.chat.id,
            buildDomainVariablePromptText(domain),
            buildDomainVariablePromptKeyboard(),
            msg
        );
        return;
    }

    if (data === 'results_root_db') {
        const historyDbPath = getHistoryDbPath();
        if (!fs.existsSync(historyDbPath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        try {
            await runAuxTask('download', 'Download history.db backup', async () => {
                await showResultsRootMenu(msg.chat.id, msg, '⬇️ Preparing history.db backup...');
                await sendDocumentFile(msg.chat.id, historyDbPath, '🗄️ history.db backup');
                await showResultsRootMenu(msg.chat.id, msg, '✅ history.db backup sent');
            });
        } catch (error) {
            console.error(`Failed to send history.db backup: ${error.message}`);
            await showResultsRootMenu(msg.chat.id, msg, '❌ Failed to send history.db backup');
        }
        return;
    }

    if (data === 'results_root_db_link') {
        const historyDbPath = getHistoryDbPath();
        if (!fs.existsSync(historyDbPath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        const linkInfo = createHistoryDbDownloadLink();
        const statusLine = linkInfo.configuredBaseUrl
            ? `🔗 Browser link ready (30 min):\n${linkInfo.url}`
            : `🔗 Browser link ready (30 min):\n${linkInfo.url}\n⚠️ Set DOWNLOAD_LINK_BASE_URL for a public browser-friendly URL.`;

        await showResultsRootMenu(msg.chat.id, msg, statusLine);
        return;
    }

    if (data === 'results_back_root') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showResultsRootMenu(msg.chat.id, msg);
        return;
    }

    if (data.startsWith('sitemap_rescan_select:')) {
        const rawTarget = data.slice('sitemap_rescan_select:'.length);
        const targetDomain = rawTarget === 'all' ? 'all' : path.basename(rawTarget);
        const availableDomains = listSitemapDomains();

        if (targetDomain !== 'all' && !availableDomains.includes(targetDomain)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Domain is no longer available', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        await runSitemapRescanFlow(msg.chat.id, targetDomain);
        return;
    }

    if (data.startsWith('results_folder:')) {
        const folderName = path.basename(data.replace('results_folder:', ''));
        const rendered = await showFolderFilesMenu(msg.chat.id, folderName, msg);

        if (!rendered) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'Folder is empty or missing', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        return;
    }

    if (data.startsWith('results_file:')) {
        const payload = data.slice('results_file:'.length);
        const separatorIndex = payload.indexOf(':');
        const folderName = separatorIndex === -1 ? '' : path.basename(payload.slice(0, separatorIndex));
        const fileName = separatorIndex === -1 ? '' : path.basename(payload.slice(separatorIndex + 1));
        const availableFiles = listCsvFilesInFolder(folderName);

        if (!folderName || !fileName || !availableFiles.includes(fileName)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        const filePath = path.join(getResultDirPath(), folderName, fileName);
        if (!fs.existsSync(filePath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        try {
            await runAuxTask('download', `Download ${folderName}/${fileName}`, async () => {
                await showFolderFilesMenu(msg.chat.id, folderName, msg, `⬇️ Preparing ${fileName}...`);
                await sendDocumentFile(msg.chat.id, filePath, `📄 ${folderName} / ${fileName}`);
                await showFolderFilesMenu(msg.chat.id, folderName, msg, `✅ ${fileName} sent`);
            });
        } catch (error) {
            console.error(`Failed to send ${folderName}/${fileName}: ${error.message}`);
            await showFolderFilesMenu(msg.chat.id, folderName, msg, `❌ Failed to send ${fileName}`);
        }
        return;
    }

    await safeAnswerCallbackQuery(callbackQuery.id); // Acknowledge the callback query
});

bot.onText(/\/scrape_folder/, auth(async (msg) => {
    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) {
        fs.mkdirSync(toScrapeDir);
    }
    await withBotStatus('scraping', 'folder', async () => {
        await runTrackedFolderScrape(msg.chat.id, toScrapeDir);
    });
}));

bot.onText(/\/sitemap (.+)/, auth(async (msg, match) => {
    const sitemapUrl = match[1];
    await withBotStatus('sitemap', `single:${sitemapUrl}`, async () => {
        sendMessage(`*Scraping sitemap:* ${escapeMarkdownV2(sitemapUrl)}`);
        const result = await runSitemapScraper(sitemapUrl);
        sendMessage(`*Sitemap scraping complete!*\nTotal URLs found: ${result.totalUrlsFound}\nNew URLs saved: ${result.newUrlsSaved}`);
    });
}));

bot.onText(/\/sitemap_bulk/, auth(async () => {
    await withBotStatus('sitemap_bulk', 'bulk_sitemap', async () => {
        sendMessage(`*Starting bulk sitemap scraping from sitemaps.txt...*`);
        await runBulkSitemapScraper();
        sendMessage('*Bulk sitemap scraping complete!*');
    });
}));

bot.onText(/\/sitemap_rescan(?: (.+))?/, auth(async (msg, match) => {
    const targetDomain = match[1];

    if (targetDomain) {
        await runSitemapRescanFlow(msg.chat.id, targetDomain);
        return;
    }

    const availableDomains = listSitemapDomains();
    if (availableDomains.length === 0) {
        sendMessage('No saved sitemaps found.', msg.chat.id);
        return;
    }

    await sendOrEditMenu(
        msg.chat.id,
        'Select which sitemap group to rescan:',
        buildSitemapRescanKeyboard()
    );
}));

bot.onText(/\/stats/, auth(() => {
    const historyCount = getHistoryCount();
    sendMessage(`*Scraping Statistics:*\nTotal URLs processed: ${historyCount}`);
}));

bot.onText(/\/download_results/, auth(async (msg) => {
    clearResultsViewState(msg.chat.id);
    await showResultsRootMenu(msg.chat.id);
}));

bot.onText(/\/export_urls (.+)/, auth(async (msg, match) => {
    const status = match[1];
    sendMessage(`*Exporting URLs with status:* ${escapeMarkdownV2(status)}...`);
    const filePath = exportUrlsByStatus(status);
    if (filePath) {
        sendMessage(`*Export complete!* File saved to: ${escapeMarkdownV2(filePath)}`);
    } else {
        sendMessage(`*Export failed or no URLs found for status:* ${escapeMarkdownV2(status)}`);
    }
}));

bot.onText(/\/retry_failed/, auth(async (msg) => {
    await sendOrEditMenu(msg.chat.id, 'Choose which URLs to retry:', buildRetryTypeKeyboard());
}));

console.log('Telegram Bot started...');

// Export status setters for other modules to update
export const setBotStatus = (status, task = null) => {
    botStatus = status;
    currentTask = task;
};

export const getBotStatus = () => ({
    status: botStatus !== 'idle' ? botStatus : (listRunningAuxTasks().length > 0 ? 'background_tasks' : 'idle'),
    task: currentTask,
    auxTasks: listRunningAuxTasks().map(task => ({
        id: task.id,
        type: task.type,
        label: task.label
    }))
});


// TODO: Implement other commands here
