#!/usr/bin/env node

/**
 * Helper script for dictionary training workflow
 * 
 * This script helps with the dictionary training process which requires
 * the external zstd CLI tool to be installed.
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

function checkZstdInstalled() {
    try {
        execSync('zstd --version', { stdio: 'ignore' });
        return true;
    } catch {
        return false;
    }
}

function trainDictionary(samplesPath, outputPath, dictSize = 262144) {
    if (!checkZstdInstalled()) {
        console.error('Error: zstd is not installed');
        console.error('Please install zstd:');
        console.error('  Ubuntu/Debian: sudo apt-get install zstd');
        console.error('  macOS: brew install zstd');
        console.error('  Other: https://github.com/facebook/zstd');
        process.exit(1);
    }

    if (!fs.existsSync(samplesPath)) {
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
    } catch (error) {
        console.error('Dictionary training failed:', error.message);
        return false;
    }
}

function loadDictionaryToBlockDB(dbPath, dictionaryPath) {
    // This is a placeholder - in real usage, you would:
    // 1. Create a BlockDB instance
    // 2. Load the dictionary file
    // 3. Call blockDB.setDictionary('blocks', dictionaryData)
    
    console.log('\nTo load the dictionary into BlockDB:');
    console.log('```javascript');
    console.log(`const { BlockDB } = require('./BlockDB');`);
    console.log(`const fs = require('fs');`);
    console.log('');
    console.log(`const blockDB = new BlockDB({ path: '${dbPath}', isReadonly: false, hasDebug: false });`);
    console.log(`const dictionary = fs.readFileSync('${dictionaryPath}');`);
    console.log(`blockDB.setDictionary('blocks', dictionary);`);
    console.log('```');
}

// Main script
if (require.main === module) {
    const args = process.argv.slice(2);
    
    if (args.length < 1) {
        console.log('Usage:');
        console.log('  node dictionary-helper.js export <db-path> [output-dir]');
        console.log('  node dictionary-helper.js train <samples-dir> [dict-path] [dict-size]');
        console.log('  node dictionary-helper.js full <db-path> [temp-dir]');
        console.log('');
        console.log('Commands:');
        console.log('  export  - Export training samples from BlockDB');
        console.log('  train   - Train dictionary from exported samples');
        console.log('  full    - Complete workflow: export, train, and show load instructions');
        console.log('');
        console.log('Examples:');
        console.log('  node dictionary-helper.js export ./blocks.db ./samples');
        console.log('  node dictionary-helper.js train ./samples ./blocks.dict');
        console.log('  node dictionary-helper.js full ./blocks.db ./dict-workspace');
        process.exit(1);
    }

    const command = args[0];

    switch (command) {
        case 'export': {
            const dbPath = args[1];
            const outputDir = args[2] || './dict-samples';
            
            console.log('Note: This requires BlockDB instance to export samples.');
            console.log('Run this in your application context:');
            console.log('```javascript');
            console.log(`const blockDB = new BlockDB({ path: '${dbPath}', isReadonly: false, hasDebug: false });`);
            console.log(`blockDB.exportDictionaryTrainingSamples('${outputDir}');`);
            console.log('```');
            break;
        }

        case 'train': {
            const samplesDir = args[1];
            const dictPath = args[2] || './blocks.dict';
            const dictSize = parseInt(args[3]) || 262144;
            
            trainDictionary(samplesDir, dictPath, dictSize);
            break;
        }

        case 'full': {
            const dbPath = args[1];
            const workDir = args[2] || './dict-workspace';
            const samplesDir = path.join(workDir, 'samples');
            const dictPath = path.join(workDir, 'blocks.dict');

            console.log('=== Dictionary Training Workflow ===\n');
            
            console.log('Step 1: Export samples from BlockDB');
            console.log('Run this in your application:');
            console.log('```javascript');
            console.log(`const blockDB = new BlockDB({ path: '${dbPath}', isReadonly: false, hasDebug: false });`);
            console.log(`blockDB.exportDictionaryTrainingSamples('${samplesDir}');`);
            console.log('```');
            console.log('');
            
            console.log('Step 2: After exporting, run this command to train:');
            console.log(`  node dictionary-helper.js train "${samplesDir}" "${dictPath}"`);
            console.log('');
            
            console.log('Step 3: Load dictionary into BlockDB');
            loadDictionaryToBlockDB(dbPath, dictPath);
            break;
        }

        default:
            console.error(`Unknown command: ${command}`);
            process.exit(1);
    }
}