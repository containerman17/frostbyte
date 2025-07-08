#!/usr/bin/env node
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import path from 'node:path';
import { spawn } from 'node:child_process';

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
        const startPath = path.join(__dirname, 'start.js');
        const child = spawn(process.execPath, [startPath], {
            env,
            stdio: 'inherit',
        });

        child.on('exit', (code) => {
            process.exit(code || 0);
        });
    } else if (command === 'init') {
        const { createPluginTemplate } = await import('./lib/pluginTemplate.js');
        await createPluginTemplate(argv.name, argv['plugins-dir']);
    }
}

main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
});
