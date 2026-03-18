import fs from 'fs';
import path from 'path';

function loadEnvFromFile(fileName) {
    const filePath = path.join(process.cwd(), fileName);
    if (!fs.existsSync(filePath)) return;

    const envContent = fs.readFileSync(filePath, 'utf8');
    envContent.split('\n').forEach(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return;

        const equalsIndex = trimmed.indexOf('=');
        if (equalsIndex === -1) return;

        const key = trimmed.slice(0, equalsIndex).trim();
        const value = trimmed.slice(equalsIndex + 1).trim().replace(/^['"]|['"]$/g, '');

        if (!process.env[key]) {
            process.env[key] = value;
        }
    });
}

loadEnvFromFile('.env');
if (!process.env.TELEGRAM_BOT_TOKEN || !process.env.TELEGRAM_CHAT_ID) {
    loadEnvFromFile('.env.local');
}

export const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN?.trim() || '8799281310:AAFDXQUC79RheOETZttqOgOXYB8CaLKM79U';
export const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID?.trim() || '1032664219';
export const DOWNLOAD_LINK_BASE_URL = process.env.DOWNLOAD_LINK_BASE_URL?.trim() || '';
export const DOWNLOAD_LINK_PORT = Number(process.env.DOWNLOAD_LINK_PORT || '8787');
