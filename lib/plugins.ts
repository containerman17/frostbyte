import { IndexingPlugin, ApiPlugin } from "./types";
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

function isIndexingPlugin(plugin: any): plugin is IndexingPlugin {
    return plugin &&
        typeof plugin.name === 'string' &&
        typeof plugin.version === 'number' &&
        typeof plugin.usesTraces === 'boolean' &&
        typeof plugin.initialize === 'function' &&
        typeof plugin.handleTxBatch === 'function' &&
        !plugin.requiredIndexers; // API plugins have requiredIndexers
}

function isApiPlugin(plugin: any): plugin is ApiPlugin {
    return plugin &&
        typeof plugin.name === 'string' &&
        Array.isArray(plugin.requiredIndexers) &&
        typeof plugin.registerRoutes === 'function' &&
        !plugin.version && // API plugins don't have version
        !plugin.handleTxBatch; // API plugins don't handle transactions
}

export async function loadIndexingPlugins(pluginsDirs: string[]): Promise<IndexingPlugin[]> {
    const plugins: IndexingPlugin[] = [];

    for (const pluginsDir of pluginsDirs) {
        if (!fs.existsSync(pluginsDir)) {
            console.warn(`Plugin directory not found: ${pluginsDir}`);
            continue;
        }

        const pluginFiles = fs.readdirSync(pluginsDir);
        for (const file of pluginFiles) {
            if (file.endsWith('.ts') || file.endsWith('.js')) {
                const pluginPath = path.join(pluginsDir, file);
                // Use file URL for proper ESM loading
                const fileUrl = pathToFileURL(pluginPath).href;
                const plugin = await import(fileUrl);
                const defaultExport = plugin.default;

                if (isIndexingPlugin(defaultExport)) {
                    plugins.push(defaultExport);
                    console.log(`Loaded indexing plugin: ${file} (${defaultExport.name})`);
                }
            }
        }
    }
    return plugins;
}

export async function loadApiPlugins(pluginsDirs: string[]): Promise<ApiPlugin[]> {
    const plugins: ApiPlugin[] = [];

    for (const pluginsDir of pluginsDirs) {
        if (!fs.existsSync(pluginsDir)) {
            console.warn(`Plugin directory not found: ${pluginsDir}`);
            continue;
        }

        const pluginFiles = fs.readdirSync(pluginsDir);
        for (const file of pluginFiles) {
            if (file.endsWith('.ts') || file.endsWith('.js')) {
                const pluginPath = path.join(pluginsDir, file);
                // Use file URL for proper ESM loading
                const fileUrl = pathToFileURL(pluginPath).href;
                const plugin = await import(fileUrl);
                const defaultExport = plugin.default;

                if (isApiPlugin(defaultExport)) {
                    plugins.push(defaultExport);
                    console.log(`Loaded API plugin: ${file} (${defaultExport.name})`);
                }
            }
        }
    }
    return plugins;
}
