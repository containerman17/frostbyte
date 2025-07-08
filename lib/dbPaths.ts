import path from 'node:path';
import fs from 'node:fs';
import { DATA_DIR } from '../config.js';
import { getAvailableIndexers } from '../indexer.js';

/**
 * Get the path for the blocks database
 */
export function getBlocksDbPath(chainId: string, debugEnabled: boolean): string {
    const chainDir = path.join(DATA_DIR, chainId);

    // Ensure directory exists
    if (!fs.existsSync(chainDir)) {
        fs.mkdirSync(chainDir, { recursive: true });
    }

    const dbName = debugEnabled ? 'blocks.db' : 'blocks_no_dbg.db';
    return path.join(chainDir, dbName);
}

/**
 * Get the path for an indexer database and clean up old versions
 */
export function getIndexerDbPath(
    chainId: string,
    indexerName: string,
    version: number,
    debugEnabled: boolean
): string {
    const baseDir = path.join(DATA_DIR, chainId);

    // Ensure directory exists
    if (!fs.existsSync(baseDir)) {
        fs.mkdirSync(baseDir, { recursive: true });
    }

    const dbName = debugEnabled
        ? `indexing_${indexerName}_v${version}.db`
        : `indexing_${indexerName}_v${version}_no_dbg.db`;

    // Delete old versions before returning the current path
    deleteOldIndexerVersions(baseDir, indexerName, version, debugEnabled);

    return path.join(baseDir, dbName);
}

/**
 * Delete old versions of an indexer's database
 */
function deleteOldIndexerVersions(
    baseDir: string,
    indexerName: string,
    currentVersion: number,
    debugEnabled: boolean
): void {
    if (!fs.existsSync(baseDir)) {
        return;
    }

    const files = fs.readdirSync(baseDir);
    const pattern = debugEnabled
        ? new RegExp(`^indexing_${indexerName}_v(\\d+)\\.db$`)
        : new RegExp(`^indexing_${indexerName}_v(\\d+)_no_dbg\\.db$`);

    for (const file of files) {
        const match = file.match(pattern);
        if (match) {
            const version = parseInt(match[1]!, 10);
            if (version !== currentVersion) {
                const filePath = path.join(baseDir, file);
                console.log(`[${indexerName}] Deleting old database version ${version}: ${file}`);
                fs.unlinkSync(filePath);
                // Also delete associated journal files
                const journalPath = filePath + '-journal';
                if (fs.existsSync(journalPath)) {
                    fs.unlinkSync(journalPath);
                }
                const walPath = filePath + '-wal';
                if (fs.existsSync(walPath)) {
                    fs.unlinkSync(walPath);
                }
                const shmPath = filePath + '-shm';
                if (fs.existsSync(shmPath)) {
                    fs.unlinkSync(shmPath);
                }
            }
        }
    }
}

/**
 * Find an existing indexer database for the API server (without deletion)
 */
export function findIndexerDatabase(
    chainId: string,
    indexerName: string,
    indexerVersion: number,
    debugEnabled: boolean
): string | null {
    const baseDir = path.join(DATA_DIR, chainId);

    // Check if directory exists
    if (!fs.existsSync(baseDir)) {
        return null;
    }

    const dbName = debugEnabled
        ? `indexing_${indexerName}_v${indexerVersion}.db`
        : `indexing_${indexerName}_v${indexerVersion}_no_dbg.db`;
    const dbPath = path.join(baseDir, dbName);

    if (fs.existsSync(dbPath)) {
        return dbPath;
    }

    return null;
}


export async function awaitIndexerDatabases(baseDir: string, debugEnabled: boolean, maxMs: number = 30 * 1000, intervalMs: number = 500) {
    const startTime = Date.now();

    // Get list of available indexers to know what databases to expect
    const availableIndexers = await getAvailableIndexers();
    console.log(`API worker waiting for ${availableIndexers.length} indexer databases...`);

    // If no indexers are available, skip waiting
    if (availableIndexers.length === 0) {
        console.log('No indexers available, skipping database wait');
        return;
    }

    while (true) {
        // Check if directory exists
        if (!fs.existsSync(baseDir)) {
            fs.mkdirSync(baseDir, { recursive: true });
        }

        // Look for any indexer database files
        const files = fs.readdirSync(baseDir);
        const indexerDbPattern = debugEnabled
            ? /^indexing_.*_v\d+\.db$/
            : /^indexing_.*_v\d+_no_dbg\.db$/;

        const indexerDbs = files.filter(f => indexerDbPattern.test(f));

        if (indexerDbs.length > 0) {
            console.log(`Found ${indexerDbs.length} indexer database(s): ${indexerDbs.join(', ')}`);
            return;
        }

        await new Promise(resolve => setTimeout(resolve, intervalMs));
        if (Date.now() - startTime > maxMs) {
            // Instead of throwing, just log a warning and continue
            console.warn(`Warning: No indexer databases found in ${baseDir} after ${maxMs} ms. Continuing anyway...`);
            return;
        }
    }
}
