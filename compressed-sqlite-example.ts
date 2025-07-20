import sqlite3 from 'better-sqlite3';
import { existsSync, statSync, unlinkSync } from 'fs';
import { join } from 'path';
import { execSync } from 'child_process';

// Configuration
const PLUGIN_PATH = './sqlite_zstd-v0.3.5-x86_64-unknown-linux-gnu/libsqlite_zstd.so';
const REGULAR_DB = './hello_world_regular.db';
const COMPRESSED_DB = './hello_world_compressed.db';
const NUM_RECORDS = 10000;

// Sample JSON data with repeated field names for better compression
const generateSampleJson = (id: number): string => {
    return JSON.stringify({
        id: id,
        name: `User ${id}`,
        email: `user${id}@example.com`,
        profile: {
            firstName: `FirstName${id}`,
            lastName: `LastName${id}`,
            address: {
                street: `123 Main Street`,
                city: `Sample City`,
                state: `Sample State`,
                zipCode: `12345`,
                country: `United States`
            },
            preferences: {
                theme: `dark`,
                language: `en-US`,
                notifications: {
                    email: true,
                    push: true,
                    sms: false
                },
                privacy: {
                    shareData: false,
                    trackingEnabled: false,
                    analyticsEnabled: true
                }
            }
        },
        metadata: {
            createdAt: `2024-01-15T10:30:00Z`,
            updatedAt: `2024-01-15T10:30:00Z`,
            version: `1.0.0`,
            source: `web-application`,
            tags: [`user`, `active`, `premium`],
            permissions: [`read`, `write`, `delete`],
            settings: {
                autoSave: true,
                syncEnabled: true,
                backupEnabled: true,
                compressionLevel: `high`
            }
        }
    });
};

function cleanupFiles(): void {
    [REGULAR_DB, COMPRESSED_DB].forEach(file => {
        if (existsSync(file)) {
            unlinkSync(file);
        }
    });
}

function getFileSize(filePath: string): number {
    if (!existsSync(filePath)) {
        return 0;
    }
    return statSync(filePath).size;
}

function createRegularDatabase(): void {
    console.log('Creating regular SQLite database...');

    const db = sqlite3(REGULAR_DB);

    // Create table
    db.exec(`
    CREATE TABLE hello_world (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      data TEXT NOT NULL
    )
  `);

    // Prepare insert statement
    const insert = db.prepare('INSERT INTO hello_world (data) VALUES (?)');

    // Insert data in a transaction for better performance
    const insertMany = db.transaction((records: string[]) => {
        for (const record of records) {
            insert.run(record);
        }
    });

    // Generate and insert sample data
    const sampleData: string[] = [];
    for (let i = 1; i <= NUM_RECORDS; i++) {
        sampleData.push(generateSampleJson(i));
    }

    insertMany(sampleData);

    console.log(`Inserted ${NUM_RECORDS} records into regular database`);
    db.close();
}

function createCompressedDatabase(): void {
    console.log('Creating compressed SQLite database...');

    // Use sqlite3 CLI directly as recommended by sqlite_zstd_vfs documentation
    const compressedUri = `file:${COMPRESSED_DB}?vfs=zstd&level=6&threads=4&outer_page_size=8192&outer_unsafe=true`;

    try {
        // Use sqlite3 CLI to create compressed database via VACUUM INTO
        const command = `sqlite3 "${REGULAR_DB}" -bail -cmd '.load ${PLUGIN_PATH}' "VACUUM INTO '${compressedUri}'"`;

        console.log('Executing sqlite3 CLI command for compression...');
        const output = execSync(command, { encoding: 'utf8', stdio: 'pipe' });

        console.log('Created compressed database via sqlite3 CLI VACUUM INTO');
        if (output.trim()) {
            console.log('CLI output:', output.trim());
        }
    } catch (error) {
        console.error('Failed to create compressed database with sqlite3 CLI:', error);
        throw error;
    }
}

function calculateRawDataSize(): number {
    // Calculate size of raw JSON data
    let totalSize = 0;
    for (let i = 1; i <= NUM_RECORDS; i++) {
        totalSize += Buffer.byteLength(generateSampleJson(i), 'utf8');
    }
    return totalSize;
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

console.log('SQLite Compression Example with sqlite_zstd_vfs\n');
console.log(`Plugin path: ${PLUGIN_PATH}`);
console.log(`Number of records: ${NUM_RECORDS}\n`);

// Clean up any existing databases
cleanupFiles();

try {
    // Create both databases
    createRegularDatabase();
    createCompressedDatabase();

    // Measure sizes
    const rawDataSize = calculateRawDataSize();
    const regularDbSize = getFileSize(REGULAR_DB);
    const compressedDbSize = getFileSize(COMPRESSED_DB);

    // Calculate compression ratios
    const dbCompressionRatio = ((regularDbSize - compressedDbSize) / regularDbSize) * 100;
    const rawCompressionRatio = ((rawDataSize - compressedDbSize) / rawDataSize) * 100;

    // Display results
    console.log('\n=== COMPRESSION RESULTS ===');
    console.log(`Raw JSON data size:     ${formatBytes(rawDataSize)}`);
    console.log(`Regular SQLite DB size: ${formatBytes(regularDbSize)}`);
    console.log(`Compressed DB size:     ${formatBytes(compressedDbSize)}`);
    console.log('');
    console.log(`DB compression ratio:   ${dbCompressionRatio.toFixed(1)}%`);
    console.log(`Raw data compression:   ${rawCompressionRatio.toFixed(1)}%`);
    console.log('');
    console.log(`Space saved vs regular DB: ${formatBytes(regularDbSize - compressedDbSize)}`);
    console.log(`Space saved vs raw data:   ${formatBytes(rawDataSize - compressedDbSize)}`);

    // Verify data integrity
    console.log('\n=== DATA VERIFICATION ===');
    const regularDb = sqlite3(REGULAR_DB, { readonly: true });
    const regularCount = regularDb.prepare('SELECT COUNT(*) as count FROM hello_world').get() as { count: number };
    regularDb.close();

    // Use sqlite3 CLI to read from compressed database
    const compressedUri = `file:${COMPRESSED_DB}?vfs=zstd&mode=ro`;
    const countCommand = `sqlite3 ":memory:" -bail -cmd '.load ${PLUGIN_PATH}' -cmd '.open ${compressedUri}' "SELECT COUNT(*) FROM hello_world"`;

    const compressedCountOutput = execSync(countCommand, { encoding: 'utf8' }).trim();
    const compressedCount = { count: parseInt(compressedCountOutput) };

    console.log(`Regular DB record count:    ${regularCount.count}`);
    console.log(`Compressed DB record count: ${compressedCount.count}`);
    console.log(`Data integrity: ${regularCount.count === compressedCount.count ? 'PASS' : 'FAIL'}`);

} catch (error) {
    console.error('Error during example execution:', error);
    throw error;
} finally {
    // Cleanup
    console.log('\nCleaning up temporary files...');
    cleanupFiles();
}

