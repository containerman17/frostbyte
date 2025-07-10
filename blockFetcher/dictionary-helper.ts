#!/usr/bin/env node --loader ts-node/esm

/**
 * Helper script for dictionary training workflow
 * 
 * This script helps with the dictionary training process which requires
 * the external zstd CLI tool to be installed.
 */

import { execSync } from 'child_process';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { BlockDB } from './BlockDB.js';

function checkZstdInstalled(): boolean {
    try {
        execSync('zstd --version', { stdio: 'ignore' });
        return true;
    } catch {
        return false;
    }
}

function trainDictionary(samplesPath: string, outputPath: string, dictSize: number = 262144): boolean {
    if (!checkZstdInstalled()) {
        console.error('Error: zstd is not installed');
        console.error('Please install zstd:');
        console.error('  Ubuntu/Debian: sudo apt-get install zstd');
        console.error('  macOS: brew install zstd');
        console.error('  Other: https://github.com/facebook/zstd');
        process.exit(1);
    }

    if (!existsSync(samplesPath)) {
        console.error(`Error: Samples directory not found: ${samplesPath}`);
        process.exit(1);
    }

    console.log(`Training dictionary from samples in ${samplesPath}...`);
    console.log(`Dictionary size: ${dictSize} bytes`);

    try {
        const cmd = `zstd --train -r "${samplesPath}" -o "${outputPath}" --maxdict=${dictSize}`;
        const output = execSync(cmd, { encoding: 'utf8' });
        console.log(output);
        console.log(`Dictionary saved to: ${outputPath}`);
        return true;
    } catch (error: any) {
        console.error('Dictionary training failed:', error.message);
        return false;
    }
}

async function exportSamples(dbPath: string, outputDir: string): Promise<void> {
    const blockDB = new BlockDB({ path: dbPath, isReadonly: false, hasDebug: false });
    try {
        await blockDB.exportDictionaryTrainingSamples(outputDir);
    } finally {
        blockDB.close();
    }
}

async function loadDictionary(dbPath: string, dictionaryPath: string): Promise<void> {
    if (!existsSync(dictionaryPath)) {
        console.error(`Error: Dictionary file not found: ${dictionaryPath}`);
        process.exit(1);
    }

    const blockDB = new BlockDB({ path: dbPath, isReadonly: false, hasDebug: false });
    try {
        const dictionary = readFileSync(dictionaryPath);
        blockDB.setDictionary('blocks', dictionary);
        console.log('Dictionary loaded successfully into BlockDB');
    } finally {
        blockDB.close();
    }
}

function showLoadInstructions(dbPath: string, dictionaryPath: string): void {
    console.log('\nTo load the dictionary into BlockDB:');
    console.log('```typescript');
    console.log(`import { BlockDB } from './BlockDB.js';`);
    console.log(`import { readFileSync } from 'fs';`);
    console.log('');
    console.log(`const blockDB = new BlockDB({ path: '${dbPath}', isReadonly: false, hasDebug: false });`);
    console.log(`const dictionary = readFileSync('${dictionaryPath}');`);
    console.log(`blockDB.setDictionary('blocks', dictionary);`);
    console.log(`blockDB.close();`);
    console.log('```');
}

// Main script
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length < 1) {
        console.log('Usage:');
        console.log('  ts-node dictionary-helper.ts export <db-path> [output-dir]');
        console.log('  ts-node dictionary-helper.ts train <samples-dir> [dict-path] [dict-size]');
        console.log('  ts-node dictionary-helper.ts load <db-path> <dict-path>');
        console.log('  ts-node dictionary-helper.ts full <db-path> [temp-dir]');
        console.log('');
        console.log('Commands:');
        console.log('  export  - Export training samples from BlockDB');
        console.log('  train   - Train dictionary from exported samples');
        console.log('  load    - Load trained dictionary into BlockDB');
        console.log('  full    - Complete workflow: export, train, and load');
        console.log('');
        console.log('Examples:');
        console.log('  ts-node dictionary-helper.ts export ./blocks.db ./samples');
        console.log('  ts-node dictionary-helper.ts train ./samples ./blocks.dict');
        console.log('  ts-node dictionary-helper.ts load ./blocks.db ./blocks.dict');
        console.log('  ts-node dictionary-helper.ts full ./blocks.db ./dict-workspace');
        process.exit(1);
    }

    const command = args[0];

    try {
        switch (command) {
            case 'export': {
                const dbPath = args[1];
                const outputDir = args[2] || './dict-samples';
                
                if (!dbPath) {
                    console.error('Error: db-path is required');
                    process.exit(1);
                }
                
                await exportSamples(dbPath, outputDir);
                break;
            }

            case 'train': {
                const samplesDir = args[1];
                const dictPath = args[2] || './blocks.dict';
                const dictSize = args[3] ? parseInt(args[3]) : 262144;
                
                if (!samplesDir) {
                    console.error('Error: samples-dir is required');
                    process.exit(1);
                }
                
                trainDictionary(samplesDir, dictPath, dictSize);
                break;
            }

            case 'load': {
                const dbPath = args[1];
                const dictPath = args[2];
                
                if (!dbPath || !dictPath) {
                    console.error('Error: both db-path and dict-path are required');
                    process.exit(1);
                }
                
                await loadDictionary(dbPath, dictPath);
                break;
            }

            case 'full': {
                const dbPath = args[1];
                const workDir = args[2] || './dict-workspace';
                const samplesDir = join(workDir, 'samples');
                const dictPath = join(workDir, 'blocks.dict');

                if (!dbPath) {
                    console.error('Error: db-path is required');
                    process.exit(1);
                }

                console.log('=== Dictionary Training Workflow ===\n');
                
                console.log('Step 1: Exporting samples from BlockDB...');
                await exportSamples(dbPath, samplesDir);
                console.log('');
                
                console.log('Step 2: Training dictionary...');
                const trained = trainDictionary(samplesDir, dictPath);
                if (!trained) {
                    console.error('Dictionary training failed');
                    process.exit(1);
                }
                console.log('');
                
                console.log('Step 3: Loading dictionary into BlockDB...');
                await loadDictionary(dbPath, dictPath);
                console.log('');
                
                console.log('Dictionary compression setup complete!');
                break;
            }

            default:
                console.error(`Unknown command: ${command}`);
                process.exit(1);
        }
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch(console.error);
}