import { IndexerModule } from "./types";
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

export async function loadPlugins(pluginsDirs: string[]): Promise<IndexerModule[]> {
    const plugins: IndexerModule[] = [];

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
                plugins.push(plugin.default);
                console.log(`Loaded plugin: ${file}`);
            }
        }
    }
    return plugins;
}
