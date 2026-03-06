import TelegramBot from 'node-telegram-bot-api';
import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from './config.js';
import fs from 'fs';
import path from 'path';
import {
    scrapeSingleUrlAndProcess,
    scrapeUrlsFromInputFile,
    runScraper,
    processToScrapeFolder,
    retryFailedAndNoResultUrls,
    initializeScraper,
    getProxyMode,
    setProxyMode
} from './scraper.js';
import { runSitemapScraper, runBulkSitemapScraper, rescanSavedSitemaps } from './xml.js';
import { getAllSitemaps, getHistoryCount, exportUrlsByStatus } from './db.js';

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

const showResultsRootMenu = async (chatId, message = null) => {
    const keyboard = buildResultsRootKeyboard();
    if (keyboard.inline_keyboard.length <= 1) {
        await sendOrEditMenu(chatId, 'No result or backup files are available yet.', undefined, message);
        return;
    }

    await sendOrEditMenu(chatId, 'Select a result or backup file to download:', keyboard, message);
};

const showFolderFilesMenu = async (chatId, folderName, message = null) => {
    const safeFolderName = path.basename(folderName || '');
    const files = listCsvFilesInFolder(safeFolderName);

    if (files.length === 0) {
        return false;
    }

    await sendOrEditMenu(
        chatId,
        `📁 ${safeFolderName}\nSelect a CSV file to download:`,
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
        let trackerMessage = await sendPlainMessage('Starting sitemap rescan...', chatId);
        let lastTrackerUpdateAt = 0;

        const updateTracker = async (summary, currentSitemapUrl = '', force = false) => {
            const now = Date.now();
            if (!force && now - lastTrackerUpdateAt < 1500) {
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
        [{ text: '🌐 Sitemap Bulk', callback_data: 'home_action:sitemap_bulk' }],
        [{ text: '🔄 Sitemap Rescan', callback_data: 'home_open:sitemap_rescan' }],
        [{ text: '⬅️ Back to Home', callback_data: 'home_back' }]
    ]
});

const buildResultsMenuKeyboard = () => ({
    inline_keyboard: [
        [{ text: '📂 Download Results', callback_data: 'home_open:download_results' }],
        [{ text: '📤 Export URLs', callback_data: 'home_usage:export_urls' }],
        [{ text: '♻️ Retry Failed', callback_data: 'home_action:retry_failed' }],
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

// Send message helper
export const sendMessage = (text, chatId = TELEGRAM_CHAT_ID, parseMode = 'MarkdownV2') => {
    return bot.sendMessage(chatId, text, { parse_mode: parseMode });
};

const sendPlainMessage = (text, chatId = TELEGRAM_CHAT_ID) => {
    return bot.sendMessage(chatId, text);
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

const formatElapsed = (elapsedMs = 0) => {
    const totalSeconds = Math.max(0, Math.floor(elapsedMs / 1000));
    const minutes = String(Math.floor(totalSeconds / 60)).padStart(2, '0');
    const seconds = String(totalSeconds % 60).padStart(2, '0');
    return `${minutes}:${seconds}`;
};

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

const runTrackedUrlListScrape = async (chatId, label, urls) => {
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

    const summary = await runScraper(urls, null, false, {
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

const buildSitemapRescanProgressText = (summary, currentSitemapUrl = '') => {
    const targetLabel = summary.target === 'all' ? 'All' : summary.target;
    const lines = [
        'Sitemap rescan running',
        `Target: ${targetLabel}`,
        `Completed: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps} root sitemaps`,
        `Visited: ${summary.uniqueSitemapsVisited} sitemap files`,
        `New URLs found: ${summary.newUrlsFound}`
    ];

    if (currentSitemapUrl) {
        lines.push(`Current: ${currentSitemapUrl}`);
    }

    return lines.join('\n');
};

const buildSitemapRescanSummaryText = (summary) => {
    const targetLabel = summary.target === 'all' ? 'All' : summary.target;
    const lines = [
        'Sitemap rescan complete',
        `Target: ${targetLabel}`,
        `Completed: ${summary.processedRootSitemaps}/${summary.totalRootSitemaps} root sitemaps`,
        `Skipped: ${summary.skippedRootSitemaps}`,
        `Visited: ${summary.uniqueSitemapsVisited} sitemap files`,
        `New URLs found: ${summary.newUrlsFound}`
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
    sendMessage(`*Bot Status:* ${escapeMarkdownV2(status.status)}\n*Current Task:* ${escapeMarkdownV2(status.task || 'None')}`);
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
        if (pendingInput?.type === 'upload_txt') {
            await sendPlainMessage('Please send a .txt or .csv file.', msg.chat.id);
        }
        return;
    }

    clearPendingChatInput(msg.chat.id);

    try {
        const filePath = await downloadTelegramInputFile(msg.document);
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
        return;
    }

    if (pendingInput.type === 'upload_txt') {
        await sendPlainMessage('Please send a .txt or .csv file.', msg.chat.id);
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

    if (data === 'home_open:download_results') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showResultsRootMenu(msg.chat.id, msg);
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
        await safeAnswerCallbackQuery(callbackQuery.id);
        await showHomeInfoPage(
            msg.chat.id,
            'Bot Status',
            [
                `Status: ${status.status}`,
                `Current Task: ${status.task || 'None'}`,
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

    if (data === 'home_action:scrape_folder') {
        const toScrapeDir = path.join(process.cwd(), 'to scrape');
        if (!fs.existsSync(toScrapeDir)) {
            fs.mkdirSync(toScrapeDir);
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        await withBotStatus('scraping', 'folder', async () => {
            sendMessage(`*Processing files in folder:* ${escapeMarkdownV2(toScrapeDir)}`);
            await processToScrapeFolder(toScrapeDir);
            sendMessage('*Folder processing complete!*');
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

    if (data === 'home_action:retry_failed') {
        await safeAnswerCallbackQuery(callbackQuery.id);
        await withBotStatus('retry', 'retry_failed', async () => {
            sendMessage('*Retrying failed and no-result URLs...*');
            await retryFailedAndNoResultUrls();
            sendMessage('*Retry complete!*');
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
        await sendDocumentFile(msg.chat.id, allResultsPath, '📄 all_results.csv');
        return;
    }

    if (data === 'results_root_db') {
        const historyDbPath = getHistoryDbPath();
        if (!fs.existsSync(historyDbPath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);
        await sendDocumentFile(msg.chat.id, historyDbPath, '🗄️ history.db backup');
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
        await sendDocumentFile(msg.chat.id, filePath, `📄 ${folderName} / ${fileName}`);
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
        sendMessage(`*Processing files in folder:* ${escapeMarkdownV2(toScrapeDir)}`);
        await processToScrapeFolder(toScrapeDir);
        sendMessage('*Folder processing complete!*');
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
    await withBotStatus('retry', 'retry_failed', async () => {
        sendMessage(`*Retrying failed and no-result URLs...*`);
        await retryFailedAndNoResultUrls();
        sendMessage('*Retry complete!*');
    });
});

console.log('Telegram Bot started...');

// Export status setters for other modules to update
export const setBotStatus = (status, task = null) => {
    botStatus = status;
    currentTask = task;
};

export const getBotStatus = () => ({
    status: botStatus,
    task: currentTask,
});


// TODO: Implement other commands here
