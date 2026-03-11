import TelegramBot from 'node-telegram-bot-api';
import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from './config.js';
import fs from 'fs';
import path from 'path';
import {
    scrapeSingleUrlAndProcess,
    scrapeUrlsFromInputFile,
    runScraper,
    processToScrapeFolder,
    initializeScraper,
    getProxyMode,
    setProxyMode
} from './scraper.js';
import { runSitemapScraper, runBulkSitemapScraper, rescanSavedSitemaps } from './xml.js';
import { getAllSitemaps, getHistoryCount, exportUrlsByStatus, getUrlsByStatus } from './db.js';

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

const getResultDirPath = () => path.join(process.cwd(), 'result');

const getAllResultsPath = () => path.join(getResultDirPath(), 'all_results.csv');

const getHistoryDbPath = () => path.join(process.cwd(), 'history.db');

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

const buildResultsRootKeyboard = () => {
    const inlineKeyboard = [];
    const allResultsPath = getAllResultsPath();
    const historyDbPath = getHistoryDbPath();

    if (fs.existsSync(allResultsPath)) {
        inlineKeyboard.push([
            { text: '📄 all_results.csv', callback_data: 'results_root_all' }
        ]);
    }

    if (fs.existsSync(historyDbPath)) {
        inlineKeyboard.push([
            { text: '🗄️ history.db backup', callback_data: 'results_root_db' }
        ]);
    }

    listResultFoldersWithCsv().forEach(folderName => {
        inlineKeyboard.push([
            { text: `📁 ${folderName}`, callback_data: `results_folder:${folderName}` }
        ]);
    });

    inlineKeyboard.push([
        { text: '⬅️ Back to Home', callback_data: 'home_back' }
    ]);

    return { inline_keyboard: inlineKeyboard };
};

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
                reply_markup: replyMarkup
            });
            return;
        } catch (error) {
            const description = error?.response?.body?.description || error?.message || '';
            if (description.includes('message is not modified')) {
                return;
            }
        }
    }

    await bot.sendMessage(chatId, text, { reply_markup: replyMarkup });
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
    const keyboard = buildResultsRootKeyboard();
    if (keyboard.inline_keyboard.length <= 1) {
        await sendOrEditMenu(chatId, 'No result or backup files are available yet.', undefined, message);
        return;
    }

    const lines = ['Select a result or backup file to download:'];
    if (statusLine) {
        lines.push(statusLine);
    }

    await sendOrEditMenu(chatId, lines.join('\n'), keyboard, message);
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
    '📊 *Results*',
    formatHelpLine('📡', '/status', '- Show current bot status'),
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
        [{ text: '📤 Upload TXT', callback_data: 'home_wait:upload_txt' }],
        [{ text: '✍️ Paste URLs', callback_data: 'home_wait:paste_urls' }],
        [{ text: '📁 Scrape Folder', callback_data: 'home_action:scrape_folder' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

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

const buildResultsMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '📂 Download Results', callback_data: 'home_open:download_results' }],
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
            [{ text: '⬅️ Back to System', callback_data: 'home_section:system' }]
        ]
    };
};

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env variables.');
    process.exit(1);
}

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
initializeScraper();
bot.setMyCommands(BOT_COMMANDS).catch(error => {
    console.error(`Failed to set Telegram bot commands: ${error.message}`);
});

// Store bot status (e.g., 'idle', 'scraping', 'sitemap_bulk')
let botStatus = 'idle';
let currentTask = null;
let auxiliaryTaskCounter = 0;
const auxiliaryTasks = new Map();
const AUXILIARY_TASK_RETENTION_MS = 5 * 60 * 1000;

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
            message_id: messageId
        });
    } catch (error) {
        const description = error?.response?.body?.description || error?.message || '';
        if (description.includes('message is not modified')) {
            return;
        }
        throw error;
    }
};

const LIVE_TRACKER_INTERVAL_MS = 60 * 1000;

const pendingChatInputs = new Map();

const setPendingChatInput = (chatId, state) => {
    pendingChatInputs.set(chatId.toString(), state);
};

const getPendingChatInput = (chatId) => pendingChatInputs.get(chatId.toString()) || null;

const clearPendingChatInput = (chatId) => {
    pendingChatInputs.delete(chatId.toString());
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
        `📄 ${fileName}`,
        `${buildProgressBar(summary.processedUrls, summary.totalUrls)} ${percent}%  •  ${summary.processedUrls}/${summary.totalUrls}`,
        `⏱ ${formatElapsed(summary.elapsedMs)}  •  ✅ ${summary.successCount}  •  ❌ ${summary.failedCount}  •  ⚠️ ${summary.noResultCount}`,
        `🔗 ${shortenText(summary.currentUrl)}`
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
        `Elapsed: ${formatElapsed(summary.elapsedMs)}`
    ].join('\n');
};

const runTrackedFileScrape = async (chatId, filePath) => {
    const fileName = path.basename(filePath);
    let trackerMessage = await sendPlainMessage(`📄 ${fileName}\n${buildProgressBar(0, 1)} 0%  •  0/0\n⏱ 00:00  •  ✅ 0  •  ❌ 0  •  ⚠️ 0\n🔗 -`, chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < 1500) {
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
        onProgress: async (progress) => {
            await updateTracker(progress, progress.stage === 'start' || progress.stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage(`No valid URLs found in ${fileName}.`, chatId);
        return;
    }

    await updateTracker(summary, true);
    await sendPlainMessage(buildFileScrapeSummaryText(fileName, summary), chatId);
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
        onProgress: async ({ stage, summary: progressSummary }) => {
            await updateTracker(progressSummary, stage === 'start' || stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage('No valid URLs found in the folder.', chatId);
        return;
    }

    await updateTracker(summary, true);
    await sendPlainMessage(buildFolderScrapeSummaryText(summary), chatId);
};

const runTrackedUrlListScrape = async (chatId, label, urls, force = false) => {
    let trackerMessage = await sendPlainMessage(`📄 ${label}\n${buildProgressBar(0, 1)} 0%  •  0/0\n⏱ 00:00  •  ✅ 0  •  ❌ 0  •  ⚠️ 0\n🔗 -`, chatId);
    let lastTrackerUpdateAt = 0;

    const updateTracker = async (summary, force = false) => {
        const now = Date.now();
        if (!force && now - lastTrackerUpdateAt < 1500) {
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
        onProgress: async (progress) => {
            await updateTracker(progress, progress.stage === 'start' || progress.stage === 'complete');
        }
    });

    if (!summary) {
        await sendPlainMessage(`No valid URLs found in ${label}.`, chatId);
        return;
    }

    await updateTracker(summary, true);
    await sendPlainMessage(buildFileScrapeSummaryText(label, summary), chatId);
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

const withBotStatus = async (status, task, action) => {
    setBotStatus(status, task);
    try {
        await action();
    } finally {
        setBotStatus('idle', null);
    }
};

// Handle /start command
bot.onText(/\/start/, (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    clearPendingChatInput(msg.chat.id);
    showHomeMenu(msg.chat.id);
    // Set bot commands for better discoverability in Telegram UI
    bot.setMyCommands(BOT_COMMANDS).catch(error => {
        console.error(`Failed to set Telegram bot commands: ${error.message}`);
    });
});

bot.onText(/\/help/, (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    bot.sendMessage(msg.chat.id, buildHelpMessage(), { parse_mode: 'MarkdownV2' });
});

bot.onText(/\/status/, (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }

    const status = getBotStatus();
    const auxTaskSummary = status.auxTasks.length === 0
        ? 'None'
        : status.auxTasks.map((task, index) => `${index + 1}. ${task.label}`).join(' | ');

    sendMessage(
        `*Bot Status:* ${escapeMarkdownV2(status.status)}\n*Current Task:* ${escapeMarkdownV2(status.task || 'None')}\n*Other Tasks:* ${escapeMarkdownV2(auxTaskSummary)}`
    );
});

bot.onText(/\/scrape_url (.+)/, async (msg, match) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    const url = match[1];
    const safeUrl = escapeMarkdownV2(url);
    await withBotStatus('scraping', `single_url:${url}`, async () => {
        sendMessage(`*Scraping single URL:* ${safeUrl}`);
        await scrapeSingleUrlAndProcess(url);
        sendMessage('*Single URL scraping complete!*');
    });
});

bot.onText(/\/scrape_file/, async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    await showScrapeFileSelectionMenu(msg.chat.id);
});

bot.on('document', async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }

    const fileName = path.basename(msg.document?.file_name || '');
    const lowerFileName = fileName.toLowerCase();
    const pendingInput = getPendingChatInput(msg.chat.id);

    if (!lowerFileName.endsWith('.txt') && !lowerFileName.endsWith('.csv')) {
        if (pendingInput?.type === 'upload_txt' || pendingInput?.type === 'upload_sitemap_txt') {
            await sendPlainMessage('Please send a .txt or .csv file.', msg.chat.id);
        }
        return;
    }

    clearPendingChatInput(msg.chat.id);

    try {
        const filePath = await downloadTelegramInputFile(msg.document);
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
});

bot.on('message', async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized || !msg.text || msg.text.startsWith('/')) {
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

    const urls = extractUrlsFromText(msg.text);
    if (urls.length === 0) {
        await sendPlainMessage('No valid URLs found. Send full http or https URLs.', msg.chat.id);
        return;
    }

    clearPendingChatInput(msg.chat.id);

    await withBotStatus('scraping', `telegram_input:${urls.length}`, async () => {
        await runTrackedUrlListScrape(msg.chat.id, 'Telegram Input', urls);
    });
});

bot.on('callback_query', async (callbackQuery) => {
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

    if (data === 'home_open:download_results') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showResultsRootMenu(msg.chat.id, msg);
        return;
    }

    if (data === 'home_open:retry_menu') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendOrEditMenu(msg.chat.id, 'Choose which URLs to retry:', buildRetryTypeKeyboard(), msg);
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
            `Choose proxy mode:\nCurrent: ${formatProxyModeLabel(getProxyMode())}`,
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
            `Choose proxy mode:\nCurrent: ${formatProxyModeLabel(currentMode)}`,
            buildProxyModeKeyboard(),
            msg
        );
        return;
    }

    if (data === 'home_view:status') {
        const status = getBotStatus();
        const auxTaskSummary = status.auxTasks.length === 0
            ? 'None'
            : status.auxTasks.map((task, index) => `${index + 1}. ${task.label}`).join(' | ');
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Bot Status',
            [
                `Status: ${status.status}`,
                `Current Task: ${status.task || 'None'}`,
                `Other Tasks: ${auxTaskSummary}`,
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

        const retryLabel = domain === 'all'
            ? `${retryOption.label} • All Domains`
            : `${retryOption.label} • ${domain}`;

        await withBotStatus('retry', `retry:${retryKey}:${domain}`, async () => {
            await runTrackedUrlListScrape(msg.chat.id, retryLabel, urlsToRetry, true);
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

bot.onText(/\/scrape_folder/, async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) {
        fs.mkdirSync(toScrapeDir);
    }
    await withBotStatus('scraping', 'folder', async () => {
        await runTrackedFolderScrape(msg.chat.id, toScrapeDir);
    });
});

bot.onText(/\/sitemap (.+)/, async (msg, match) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    const sitemapUrl = match[1];
    await withBotStatus('sitemap', `single:${sitemapUrl}`, async () => {
        sendMessage(`*Scraping sitemap:* ${escapeMarkdownV2(sitemapUrl)}`);
        const result = await runSitemapScraper(sitemapUrl);
        sendMessage(`*Sitemap scraping complete!*\nTotal URLs found: ${result.totalUrlsFound}\nNew URLs saved: ${result.newUrlsSaved}`);
    });
});

bot.onText(/\/sitemap_bulk/, async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    await withBotStatus('sitemap_bulk', 'bulk_sitemap', async () => {
        sendMessage(`*Starting bulk sitemap scraping from sitemaps.txt...*`);
        await runBulkSitemapScraper();
        sendMessage('*Bulk sitemap scraping complete!*');
    });
});

bot.onText(/\/sitemap_rescan(?: (.+))?/, async (msg, match) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
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
});

bot.onText(/\/stats/, (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    const historyCount = getHistoryCount();
    sendMessage(`*Scraping Statistics:*\nTotal URLs processed: ${historyCount}`);
});

bot.onText(/\/download_results/, async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }

    await showResultsRootMenu(msg.chat.id);
});

bot.onText(/\/export_urls (.+)/, async (msg, match) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    const status = match[1];
    sendMessage(`*Exporting URLs with status:* ${escapeMarkdownV2(status)}...`);
    const filePath = exportUrlsByStatus(status);
    if (filePath) {
        sendMessage(`*Export complete!* File saved to: ${escapeMarkdownV2(filePath)}`);
    } else {
        sendMessage(`*Export failed or no URLs found for status:* ${escapeMarkdownV2(status)}`);
    }
});

bot.onText(/\/retry_failed/, async (msg) => {
    const isAuthorized = msg.chat.id.toString() === TELEGRAM_CHAT_ID;
    if (!isAuthorized) {
        bot.sendMessage(msg.chat.id, 'Unauthorized access.');
        return;
    }
    await sendOrEditMenu(msg.chat.id, 'Choose which URLs to retry:', buildRetryTypeKeyboard());
});

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
