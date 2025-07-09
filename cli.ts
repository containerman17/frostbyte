#!/usr/bin/env node
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { createPluginTemplate } from './lib/pluginTemplate.js';

const argv = yargs(hideBin(process.argv))
    .command('run', 'Run the indexer', {
        'plugins-dir': {
            describe: 'Directory containing plugin files',
            type: 'string',
            demandOption: true,
        },
        'data-dir': {
            describe: 'Directory for storing data',
            type: 'string',
            default: './data',
        },
    })
    .command('init', 'Initialize a new plugin', {
        name: {
            describe: 'Name of the plugin',
            type: 'string',
            demandOption: true,
        },
        type: {
            describe: 'Type of plugin to create',
            type: 'string',
            choices: ['indexing', 'api'],
            default: 'indexing',
        },
        'plugins-dir': {
            describe: 'Directory to create the plugin in',
            type: 'string',
            default: './plugins',
        },
    })
    .demandCommand(1, 'You need at least one command')
    .help()
    .argv as any;

async function main() {
    const command = argv._[0];

    if (command === 'run') {
        // Set environment variables
        const env = { ...process.env };

        env['PLUGIN_DIRS'] = path.resolve(argv['plugins-dir']);
        env['DATA_DIR'] = path.resolve(argv['data-dir']);

        // Run the start script
        const __dirname = path.dirname(fileURLToPath(import.meta.url));

        // Check if we're running from source (development) or dist (production)
        const tsStartPath = path.join(__dirname, 'start.ts');
        const jsStartPath = path.join(__dirname, 'start.js');
        const startPath = existsSync(tsStartPath) ? tsStartPath : jsStartPath;

        // Find tsx - try multiple locations
        let tsxPath: string | undefined;

        // Try local node_modules first
        const localTsx = path.join(__dirname, 'node_modules', '.bin', 'tsx');
        if (existsSync(localTsx)) {
            tsxPath = localTsx;
        } else {
            // If not found locally, use npx to run tsx
            // This will work for global installations
            const child = spawn('npx', ['--yes', 'tsx', startPath], {
                env,
                stdio: 'inherit',
            });

            child.on('exit', (code) => {
                process.exit(code || 0);
            });
            return;
        }

        const child = spawn(tsxPath, [startPath], {
            env,
            stdio: 'inherit',
        });

        child.on('exit', (code) => {
            process.exit(code || 0);
        });
    } else if (command === 'init') {
        await createPluginTemplate(argv.name, argv['plugins-dir'], argv.type as 'indexing' | 'api');
    }
}

main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
});
