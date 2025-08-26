import { IndexingPlugin, ApiPlugin } from "./types";
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

function findPluginFiles(dir: string, fileList: string[] = []): string[] {
    if (!fs.existsSync(dir)) {
        return fileList;
    }

    const files = fs.readdirSync(dir);

    for (const file of files) {
        const filePath = path.join(dir, file);
        const stat = fs.statSync(filePath);

        if (stat.isDirectory()) {
            // Recursively search subdirectories
            findPluginFiles(filePath, fileList);
        } else if (file.endsWith('.ts') || file.endsWith('.js')) {
            fileList.push(filePath);
        }
    }

    return fileList;
}

function isIndexingPlugin(plugin: any): plugin is IndexingPlugin<any> {
    return plugin &&
        typeof plugin.name === 'string' &&
        typeof plugin.version === 'number' &&
        typeof plugin.usesTraces === 'boolean' &&
        typeof plugin.initialize === 'function' &&
        typeof plugin.extractData === 'function' &&
        typeof plugin.saveExtractedData === 'function' &&
        !plugin.requiredIndexers; // API plugins have requiredIndexers
}

function isApiPlugin(plugin: any): plugin is ApiPlugin {
    return plugin &&
        typeof plugin.name === 'string' &&
        Array.isArray(plugin.requiredIndexers) &&
        typeof plugin.registerRoutes === 'function' &&
        !plugin.extractData && !plugin.saveExtractedData; // API plugins don't handle transactions
}

export async function loadIndexingPlugins(pluginsDirs: string[]): Promise<IndexingPlugin<any>[]> {
    const plugins: IndexingPlugin<any>[] = [];

    for (const pluginsDir of pluginsDirs) {
        if (!fs.existsSync(pluginsDir)) {
            console.warn(`Plugin directory not found: ${pluginsDir}`);
            continue;
        }

        const pluginFiles = findPluginFiles(pluginsDir);
        for (const pluginPath of pluginFiles) {
            // Use file URL for proper ESM loading
            const fileUrl = pathToFileURL(pluginPath).href;
            const plugin = await import(fileUrl);
            const defaultExport = plugin.default;

            if (isIndexingPlugin(defaultExport)) {
                plugins.push(defaultExport);
                // console.log(`Loaded indexing plugin: ${path.relative(pluginsDir, pluginPath)} (${defaultExport.name})`);
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

        const pluginFiles = findPluginFiles(pluginsDir);
        for (const pluginPath of pluginFiles) {
            // Use file URL for proper ESM loading
            const fileUrl = pathToFileURL(pluginPath).href;
            const plugin = await import(fileUrl);
            const defaultExport = plugin.default;

            if (isApiPlugin(defaultExport)) {
                plugins.push(defaultExport);
                console.log(`Loaded API plugin: ${path.relative(pluginsDir, pluginPath)} (${defaultExport.name})`);
            }
        }
    }
    return plugins;
}
