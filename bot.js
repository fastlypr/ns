import TelegramBot from 'node-telegram-bot-api';
import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from './config.js';
import fs from 'fs';
import path from 'path';
import {
    scrapeSingleUrlAndProcess,
    scrapeUrlsFromInputFile,
    processToScrapeFolder,
    retryFailedAndNoResultUrls,
    initializeScraper
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

    if (fs.existsSync(allResultsPath)) {
        inlineKeyboard.push([
            { text: '📄 all_results.csv', callback_data: 'results_root_all' }
        ]);
    }

    listResultFoldersWithCsv().forEach(folderName => {
        inlineKeyboard.push([
            { text: `📁 ${folderName}`, callback_data: `results_folder:${folderName}` }
        ]);
    });

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

const showResultsRootMenu = async (chatId, message = null) => {
    const resultDir = getResultDirPath();
    if (!fs.existsSync(resultDir)) {
        await sendOrEditMenu(chatId, 'No result folder found yet.', undefined, message);
        return;
    }

    const keyboard = buildResultsRootKeyboard();
    if (keyboard.inline_keyboard.length === 0) {
        await sendOrEditMenu(chatId, 'No result CSV files are available yet.', undefined, message);
        return;
    }

    await sendOrEditMenu(chatId, 'Select a result CSV to download:', keyboard, message);
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

    return { inline_keyboard: inlineKeyboard };
};

const runSitemapRescanFlow = async (chatId, targetDomain) => {
    await withBotStatus('sitemap_rescan', `rescan:${targetDomain || 'all'}`, async () => {
        if (targetDomain && targetDomain !== 'all') {
            sendMessage(`*Rescanning saved sitemaps for:* ${escapeMarkdownV2(targetDomain)}...`, chatId);
        } else {
            sendMessage('*Rescanning ALL saved sitemaps...*', chatId);
        }

        await rescanSavedSitemaps(targetDomain === 'all' ? 'all' : targetDomain);
        sendMessage('*Sitemap rescan complete!*', chatId);
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
    bot.sendMessage(msg.chat.id, 'Welcome to your Scraper Bot! Use /help to see available commands.');
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

    const helpMessage = [
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
    bot.sendMessage(msg.chat.id, helpMessage, { parse_mode: 'MarkdownV2' });
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

    const toScrapeDir = path.join(process.cwd(), 'to scrape');
    if (!fs.existsSync(toScrapeDir)) {
        fs.mkdirSync(toScrapeDir);
    }

    const files = fs.readdirSync(toScrapeDir)
        .filter(f => f.endsWith('.txt') || f.endsWith('.csv'))
        .map(f => ({ text: f, callback_data: `scrape_file_select:${f}` }));

    if (files.length === 0) {
        sendMessage('No .txt or .csv files found in the \'to scrape\' folder. Please add files there first.');
        return;
    }

    const options = {
        reply_markup: {
            inline_keyboard: [files]
        }
    };

    bot.sendMessage(msg.chat.id, 'Please select a file to scrape:', options);
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

    if (data.startsWith('scrape_file_select:')) {
        const filename = path.basename(data.replace('scrape_file_select:', ''));
        const filePath = path.join(process.cwd(), 'to scrape', filename);
        if (!fs.existsSync(filePath)) {
            await safeAnswerCallbackQuery(callbackQuery.id, { text: 'File no longer exists', show_alert: true });
            return;
        }

        await safeAnswerCallbackQuery(callbackQuery.id);

        await withBotStatus('scraping', `file:${filename}`, async () => {
            sendMessage(`*Scraping URLs from:* ${escapeMarkdownV2(filename)}`);
            await scrapeUrlsFromInputFile(filePath);
            sendMessage('*File scraping complete!*');
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
