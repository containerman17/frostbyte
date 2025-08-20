#!/usr/bin/env -S node --experimental-strip-types
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { createPluginTemplate } from './lib/pluginTemplate.ts';

const argv = yargs(hideBin(process.argv))
    .command('run', 'Run the indexer', {
        'plugins-dir': {
            alias: 'p',
            describe: 'Directory containing plugin files',
            type: 'string',
            demandOption: true,
        },
        'data-dir': {
            alias: 'd',
            describe: 'Directory for storing data',
            type: 'string',
            default: './data',
        },
    })
    .command('init', 'Initialize a new plugin', {
        name: {
            alias: 'n',
            describe: 'Name of the plugin',
            type: 'string',
            demandOption: true,
        },
        type: {
            alias: 't',
            describe: 'Type of plugin to create',
            type: 'string',
            choices: ['indexing', 'api'],
            demandOption: true,
        },
        'plugins-dir': {
            alias: 'p',
            describe: 'Directory to create the plugin in',
            type: 'string',
            default: './plugins',
        },
    })
    .demandCommand(1, 'You need at least one command')
    .help()
    .alias('help', 'h')
    .alias('version', 'v')
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
        const startPath = path.join(__dirname, 'start.ts');

        const child = spawn('node', ['--experimental-strip-types', startPath], {
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
